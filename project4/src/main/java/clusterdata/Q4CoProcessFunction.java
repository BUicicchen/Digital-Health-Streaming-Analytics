package clusterdata;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

// when glucose rises by x% within y minutes, determine how often insulin is given within 15 minutes of the start of this period
public class Q4CoProcessFunction<T> extends KeyedCoProcessFunction<Integer, TupGlucose, TupInsulin, T> {
    // a rise in an attribute by x% within y minutes
    private final Double riseRatioX;
    private final long risePeriodY;
    // determine how often insulin is given within a certain period of the start of this rise
    private final long freqPeriod;
    // determine whether the output is string or tuple because we need to deal with passing the data down the line
    private final boolean isDirectOutput;
    // whether System.out.println is executed
    private static final boolean permitPrint = true;
    // the low size limit of the wait list
    private static final int waitListLowLimit = 15;
    // the high size limit of the wait list
    private static final int waitListHighLimit = 100;
    // we can reuse the state class from query 3 for stream 1
    private ValueState<Query3StateStream> stateGlucose;  // tracking the rise of a value
    private ValueState<Query4State> stateInsulin;  // tracking the interested insulin periods caused by this rise
    private final Long dataSizeLim;  // number of total tuples
    // constructor
    public Q4CoProcessFunction(Double riseRatioX, long risePeriodY, long freqPeriod, boolean isDirectOutput, Long dataSizeLim) {
        this.riseRatioX = riseRatioX;
        this.risePeriodY = risePeriodY;
        this.freqPeriod = freqPeriod;
        this.isDirectOutput = isDirectOutput;
        this.dataSizeLim = dataSizeLim;
    }
    // initialize the states for the 2 streams
    @Override
    public void open(Configuration parameters) throws Exception {
        stateGlucose = getRuntimeContext().getState(new ValueStateDescriptor<>("Q4StateStreamGlucose", Query3StateStream.class));
        stateInsulin = getRuntimeContext().getState(new ValueStateDescriptor<>("Q4StateInsulin", Query4State.class));
    }
    // process the elements from the 1st stream (glucose)
    @Override
    public void processElement1(TupGlucose dataGlucose, Context ctx, Collector<T> out) throws Exception {
        // retrieve the state
        Query3StateStream nowStateGlucose = stateGlucose.value();
        if (nowStateGlucose == null) {nowStateGlucose = new Query3StateStream(dataGlucose.patientID);}
        // collect the message to be output OR passed down stream along with the data
        CollectorCustom<String> messageCollect = new CollectorCustom<>();
        // detect whether there is a qualified increase and collect the corresponding messages
        detectRise(dataGlucose, TupGlucose.attrNameGlucose, riseRatioX, risePeriodY, nowStateGlucose, messageCollect);
        // store the glucose data AND the message to be passed down stream
        if (isDirectOutput) {
            // output string message directly
            // noinspection unchecked
            out.collect((T) dataGlucose.toString());
            if (!messageCollect.message.equals("")) {
                // noinspection unchecked
                out.collect((T) messageCollect.message);
            }
        }
        else {
            // output data along with the message, Tuple2<TupGlucose, String>
            // noinspection unchecked
            out.collect((T) new Tuple2CustomQ4(dataGlucose, messageCollect.message));
        }
        nowStateGlucose.setLastSeenTimestamp(dataGlucose.timestamp);
        nowStateGlucose.addCount();
        stateGlucose.update(nowStateGlucose);
    }
    // process the elements from the 2nd stream (insulin)
    // determine how often insulin is given within 15 minutes of the start of this period
    @Override
    public void processElement2(TupInsulin dataInsulin, Context ctx, Collector<T> out) throws Exception {
        // retrieve the state
        Query4State nowStateInsulin = stateInsulin.value();
        if (nowStateInsulin == null) {nowStateInsulin = new Query4State(dataInsulin.patientID, freqPeriod);}
        // check if the state of the 1st stream is initialize, if not, put the data into a wait list
        // we also need to force the 2nd stream to be a bit slower than the 1st stream
        if (stateGlucose.value() == null || nowStateInsulin.waitList.size() <= waitListLowLimit ||
            (stateGlucose.value() != null && stateGlucose.value().lastSeenTimestamp <= dataInsulin.timestamp)) {
            nowStateInsulin.waitList.add(dataInsulin);
            // purge half of the wait list when it gets too big
            if (nowStateInsulin.waitList.size() >= waitListHighLimit) {
                for (int i = 0; i < nowStateInsulin.waitList.size()/2; i++) {
                    if (nowStateInsulin.waitList.get(i) instanceof TupInsulin) {
                        outputDataStream2((TupInsulin) nowStateInsulin.waitList.get(i), nowStateInsulin, out);
                    }
                }
                nowStateInsulin.waitList.subList(0, nowStateInsulin.waitList.size()/2).clear();
            }
            if (stateGlucose.value() != null && dataSizeLim != null && stateGlucose.value().count >= dataSizeLim) {
                for (int i = 0; i < nowStateInsulin.waitList.size()/2; i++) {
                    if (nowStateInsulin.waitList.get(i) instanceof TupInsulin) {
                        outputDataStream2((TupInsulin) nowStateInsulin.waitList.get(i), nowStateInsulin, out);
                    }
                }
            }
        }
        else {
            // deal with the wait list if applicable
            for (TupDevice tuple : nowStateInsulin.waitList) {
                if (tuple instanceof TupInsulin) {
                    outputDataStream2((TupInsulin) tuple, nowStateInsulin, out);
                }
            }
            // clear the wait list
            nowStateInsulin.waitList.clear();
            // deal with the current element
            outputDataStream2(dataInsulin, nowStateInsulin, out);
        }
        stateInsulin.update(nowStateInsulin);
    }
    public void outputDataStream2(TupInsulin dataInsulin, Query4State nowStateInsulin, Collector<T> out) {
        // initialize the message
        String message = "";
        boolean hasAdded = nowStateInsulin.addToAllLists(Tuple2.of(dataInsulin.doseAmount, dataInsulin.timestamp));
        if (hasAdded) {message = ", recorded after increase";}
        Tuple2<Double, String> output = nowStateInsulin.checkFullList();
        if (output != null) {message += " result(s): average " + TupInsulin.attrNameDoseAmount + ": " + output.f0 + " " + output.f1;}
        if (isDirectOutput) {
            // do not pass the insulin data down the line when there are 3 streams connected
            // instead, just pass it as a message, because we want to print it later
            // noinspection unchecked
            out.collect((T) (dataInsulin + message));
        }
        // print the stream
        else if (permitPrint) {System.out.println(dataInsulin + message);}
        // pass the message down the line
        else {
            // noinspection unchecked
            out.collect((T) new Tuple2CustomQ4(null, dataInsulin + message));
        }
    }
    // monitor stream 1, detect if the value increases by a certain percentage within a certain period of time
    public void detectRise(TupDevice tuple, String attrName, Double riseRatio,
                           long risePeriod, Query3StateStream nowStateStream, Collector<String> out) throws Exception {
        // indicate the result of the detection
        int detection = QueryUtils.queryDetectRise(tuple, attrName, riseRatio, risePeriod, nowStateStream, out);
        // whether the state needs to be updated
        boolean isUpdateState = (detection != 0);
        // whether there is a qualified increase
        boolean hasIncrease = (detection == 2);
        Float dataVal = null;
        if (tuple instanceof TupGlucose) {
            if (attrName.equals(TupGlucose.attrNameGlucose)) {
                dataVal = Float.valueOf(((TupGlucose) tuple).glucose);
            }
        }
        // retrieve the timestamp of the last increase before the state is updated
        long riseStartTime = nowStateStream.timestampTrack;
        // update the state for stream 1
        if (isUpdateState) {
            nowStateStream.setValueTrack(dataVal);
            nowStateStream.setTimestampTrack(tuple.timestamp);
            stateGlucose.update(nowStateStream);
        }
        // the rising period is detected, start monitoring insulin for 15 minutes
        // here we only concern about the state of stream 2, which collects data for 15 minutes (or more)
        if (hasIncrease) {
            Query4State nowStateInsulin = stateInsulin.value();
            if (nowStateInsulin == null) {nowStateInsulin = new Query4State(tuple.patientID, freqPeriod);}
            if (out instanceof CollectorCustom) {
                ((CollectorCustom<String>) out).collectAdd("\n" + QueryUtils.warnBar + " Rise detected, start monitoring " +
                                                           TupInsulin.deviceName + " for " + freqPeriod + " minutes, rise start time: " +
                                                           riseStartTime);
            }
            nowStateInsulin.setLastRiseTime(riseStartTime);
            // if we are already accepting records,
            // this means that we will keep more records than usual due to the overlapped periods
            // add a new list to the list of lists
            nowStateInsulin.valueTimeTrackListMap.put(riseStartTime, new ArrayList<>());
            // write back to state
            stateInsulin.update(nowStateInsulin);
        }
    }
}

package clusterdata;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

// an increase in glucose followed by an increase in heart rate within a specified threshold
public class Q3CoProcessFunction extends KeyedCoProcessFunction<Integer, TupGlucose, TupSmartwatch, String> {
    // a rise in an attribute within a period of time
    private Double riseRatioGlucose;  // = 0.6;
    private final long risePeriodGlucose;  // = 1;
    private Double riseRatioSmartwatch;  // = 0.6;
    private final long risePeriodSmartwatch;  // = 1;
    private final long interRisePeriod;  // = 5;
    private static final boolean detectEndTimestamps = true;
    private static final int stateSizeLim = 50;
    private static final String[] deviceNames = new String[] {TupGlucose.deviceName, TupSmartwatch.deviceName};
    // an increase in stream 1 (by riseRatio1 within risePeriod1)
    // followed by an increase in stream 2 (by riseRatio2 within risePeriod2)
    // within a specified threshold (interRisePeriod)
    // maintain 2 states for the 2 streams separately
    private ValueState<Query3StateStream> stateGlucose;
    private ValueState<Query3StateStream> stateSmartwatch;
    private ValueState<Query3StateRise> stateRise;
    // customizable parameters
    public Q3CoProcessFunction(Double riseRatioGlucose, long risePeriodGlucose,
                               Double riseRatioSmartwatch, long risePeriodSmartwatch, long interRisePeriod) {
        this.riseRatioGlucose = riseRatioGlucose;
        if (riseRatioGlucose == null) {this.riseRatioGlucose = 0.0;}
        this.risePeriodGlucose = risePeriodGlucose;
        this.riseRatioSmartwatch = riseRatioSmartwatch;
        if (riseRatioSmartwatch == null) {this.riseRatioSmartwatch = 0.0;}
        this.risePeriodSmartwatch = risePeriodSmartwatch;
        this.interRisePeriod = interRisePeriod;
    }
    // initialize the states for the 2 streams
    @Override
    public void open(Configuration parameters) throws Exception {
        stateGlucose = getRuntimeContext().getState(new ValueStateDescriptor<>("Q3StateStreamGlucose",
                Query3StateStream.class));
        stateSmartwatch = getRuntimeContext().getState(new ValueStateDescriptor<>("Q3StateStreamSmartwatch",
                Query3StateStream.class));
        stateRise = getRuntimeContext().getState(new ValueStateDescriptor<>("Q3StateRise", Query3StateRise.class));
    }
    // process the elements from the Glucose stream
    @Override
    public void processElement1(TupGlucose dataGlucose, Context ctx, Collector<String> out) throws Exception {
        // retrieve the state
        Query3StateStream nowStateGlucose = stateGlucose.value();
        if (nowStateGlucose == null) {nowStateGlucose = new Query3StateStream(dataGlucose.patientID);}
        // detect if there is an increase
        detectRise(dataGlucose, TupGlucose.attrNameGlucose,
                   riseRatioGlucose, risePeriodGlucose, nowStateGlucose, out);
        // print out the Glucose stream
        out.collect(dataGlucose.toString());
    }
    // process the elements from the Smartwatch stream
    @Override
    public void processElement2(TupSmartwatch dataSmartwatch, Context ctx, Collector<String> out) throws Exception {
        // retrieve the state
        Query3StateStream nowStateSmartwatch = stateSmartwatch.value();
        if (nowStateSmartwatch == null) {nowStateSmartwatch = new Query3StateStream(dataSmartwatch.patientID);}
        // detect if there is an increase
        detectRise(dataSmartwatch, TupSmartwatch.attrNameMeanHeartRate,
                   riseRatioSmartwatch, risePeriodSmartwatch, nowStateSmartwatch, out);
        // print out the Smartwatch stream
        out.collect(dataSmartwatch.toString());
    }
    // monitor a stream, detect if the value increases by a certain percentage within a certain period of time
    public void detectRise(TupDevice tuple, String attrName, Double riseRatio, long risePeriod,
                           Query3StateStream nowStateStream, Collector<String> out) throws Exception {
        Float dataVal = null;
        if (tuple instanceof TupGlucose) {
            if (attrName.equals(TupGlucose.attrNameGlucose)) {
                dataVal = Float.valueOf(((TupGlucose) tuple).glucose);
            }
        }
        else if (tuple instanceof TupSmartwatch) {
            if (attrName.equals(TupSmartwatch.attrNameMeanHeartRate)) {
                dataVal = ((TupSmartwatch) tuple).meanHeartRate;
            }
        }
        int detection = QueryUtils.queryDetectRise(tuple, attrName, riseRatio, risePeriod, nowStateStream, out);
        boolean isUpdateState = (detection != 0);
        boolean hasIncrease = (detection == 2);
        // handle patterned increasing
        if (hasIncrease) {checkRiseFollow(tuple, attrName, nowStateStream, out);}
        if (isUpdateState) {
            nowStateStream.setValueTrack(dataVal);
            nowStateStream.setTimestampTrack(tuple.timestamp);
            if (tuple instanceof TupGlucose) {stateGlucose.update(nowStateStream);}
            else if (tuple instanceof TupSmartwatch) {stateSmartwatch.update(nowStateStream);}
        }
    }
    // check if the rise in stream 1 is followed by a rise in stream 2 within the threshold period of time
    public void checkRiseFollow(TupDevice tuple, String attrName,
                                Query3StateStream nowStateStream, Collector<String> out) throws Exception {
        // retrieve the state
        Query3StateRise nowRiseState = stateRise.value();
        if (nowRiseState == null) {
            nowRiseState = new Query3StateRise(tuple.patientID, deviceNames, stateSizeLim);
        }
        // unpack the tuple and obtain the items we need
        Float dataVal = null; String streamName = null;
        if (tuple instanceof TupGlucose) {
            streamName = TupGlucose.deviceName;
            if (attrName.equals(TupGlucose.attrNameGlucose)) {
                dataVal = Float.valueOf(((TupGlucose) tuple).glucose);
            }
        }
        else if (tuple instanceof TupSmartwatch) {
            streamName = TupSmartwatch.deviceName;
            if (attrName.equals(TupSmartwatch.attrNameMeanHeartRate)) {
                dataVal = ((TupSmartwatch) tuple).meanHeartRate;
            }
        }
        // store the old record & new record
        Tuple2<Float, Long> oldRecord = Tuple2.of(nowStateStream.valueTrack, nowStateStream.timestampTrack);
        Tuple2<Float, Long> newRecord = Tuple2.of(dataVal, tuple.timestamp);
        // update the stacks
        nowRiseState.addToList(streamName, Tuple2.of(oldRecord, newRecord));
        // analyze the stacks
        if (nowRiseState.isAllNonEmpty()) {
            for (Tuple2<Tuple2<Float, Long>, Tuple2<Float, Long>> glucoseTrack : nowRiseState.listMap.get(TupGlucose.deviceName)) {
                for (Tuple2<Tuple2<Float, Long>, Tuple2<Float, Long>> smartwatchTrack : nowRiseState.listMap.get(TupSmartwatch.deviceName)) {
                    // tracked glucose increments
                    Long oldGlucoseRiseStartTime = glucoseTrack.f0.f1;
                    Float oldGlucoseRiseStartValue = glucoseTrack.f0.f0;
                    Long oldGlucoseRiseEndTime = glucoseTrack.f1.f1;
                    Float oldGlucoseRiseEndValue = glucoseTrack.f1.f0;
                    // tracked smartwatch increments
                    Long newSmartwatchRiseStartTime = smartwatchTrack.f0.f1;
                    Float newSmartwatchRiseStartValue = smartwatchTrack.f0.f0;
                    Long newSmartwatchRiseEndTime = smartwatchTrack.f1.f1;
                    Float newSmartwatchRiseEndValue = smartwatchTrack.f1.f0;
                    boolean isAlert = false;
                    if (Objects.equals(tuple.timestamp, newSmartwatchRiseEndTime) ||
                        Objects.equals(tuple.timestamp, oldGlucoseRiseEndTime)) {
                        // detect w.r.t the end timestamps of the increases
                        if (detectEndTimestamps) {
                            if (oldGlucoseRiseEndTime >= 0 && newSmartwatchRiseEndTime >= 0 &&
                                    newSmartwatchRiseEndTime - oldGlucoseRiseEndTime >= 0 &&
                                    newSmartwatchRiseEndTime - oldGlucoseRiseEndTime <= interRisePeriod) {
                                isAlert = true;
                            }
                        }
                        else {  // detect w.r.t the start timestamps of the increases
                            if (oldGlucoseRiseStartTime >= 0 && newSmartwatchRiseStartTime >= 0 &&
                                    newSmartwatchRiseStartTime - oldGlucoseRiseStartTime >= 0 &&
                                    newSmartwatchRiseStartTime - oldGlucoseRiseStartTime <= interRisePeriod) {
                                isAlert = true;
                            }
                        }
                    }
                    if (isAlert) {
                        out.collect(QueryUtils.warnBar + " ALERT (PATTERN):" +
                                    " stream " + TupGlucose.deviceName + " increase:" +
                                    " from time " + oldGlucoseRiseStartTime + ", " +
                                    TupGlucose.attrNameGlucose + ": " + oldGlucoseRiseStartValue +
                                    " to time " + oldGlucoseRiseEndTime + ", " +
                                    TupGlucose.attrNameGlucose + ": " + oldGlucoseRiseEndValue +
                                    " followed by" +
                                    " stream " + TupSmartwatch.deviceName + " increase:" +
                                    " from time " + newSmartwatchRiseStartTime + ", " +
                                    TupSmartwatch.attrNameMeanHeartRate + ": " + newSmartwatchRiseStartValue +
                                    " to time " + newSmartwatchRiseEndTime + ", " +
                                    TupSmartwatch.attrNameMeanHeartRate + ": " + newSmartwatchRiseEndValue
                        );
                    }
                }
            }
        }
        // purge the tracking list if applicable
        nowRiseState.purgeList(null);
        // write back to state
        stateRise.update(nowRiseState);
    }
}
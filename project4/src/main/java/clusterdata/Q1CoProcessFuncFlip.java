package clusterdata;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

// a flipped version of Q1CoProcessFunction, switching processElement1 and processElement2
// additional type-handling measures implemented
// it is used for processing query 4 when 3 streams need to be connected
public class Q1CoProcessFuncFlip<IN1, IN2> extends KeyedCoProcessFunction<Integer, IN1, IN2, String> {
    // manually maintain the state
    private ValueState<Query1State> state;
    // the activity identifier, range thresholds & names for the different sessions
    private final Map<String, Float[]> sessionNameThreshMap;
    // the names for the attributes of interest for stream 2 (blood pressure)
    private final String[] attrNames;
    // purge threshold, when the number of tuples is greater than this quantity, half of the priority queue will be purged
    protected final int purgeThreshold;
    // whether to track the previous averages
    protected final boolean isTrackPrev;
    // constructor
    public Q1CoProcessFuncFlip(Map<String, Float[]> newSessionThreshMap, String[] newAttrNames, int purgeThreshold, boolean isTrackPrev) {
        sessionNameThreshMap = newSessionThreshMap;
        // formalize all the ranges
        for (String name : sessionNameThreshMap.keySet()) {
            Float[] actPeriodRange = QueryUtils.assignRange(sessionNameThreshMap.get(name));
            sessionNameThreshMap.put(name, actPeriodRange);
        }
        attrNames = newAttrNames;
        this.purgeThreshold = purgeThreshold;
        this.isTrackPrev = isTrackPrev;
    }
    // initialize the state
    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("Query1State", Query1State.class));
    }
    // process the elements from the 1st stream, smartwatch
    // monitor the data from smartwatch, obtain sessions, map these sessions to blood pressure
    @Override
    public void processElement2(IN2 data1, Context ctx, Collector<String> out) throws Exception {
        TupDevice tupleData1 = null;
        Float attrValue1 = null;
        if (data1 instanceof TupSmartwatch) {
            tupleData1 = (TupSmartwatch) data1;
            attrValue1 = ((TupSmartwatch) tupleData1).meanHeartRate;
        }
        else if (data1 instanceof TupFitbit) {
            tupleData1 = (TupFitbit) data1;
            attrValue1 = Float.valueOf(((TupFitbit) tupleData1).heartRate);
        }
        else if (data1 instanceof TupGlucose) {
            tupleData1 = (TupGlucose) data1;
            attrValue1 = Float.valueOf(((TupGlucose) tupleData1).glucose);
        }
        assert tupleData1 != null;
        assert attrValue1 != null;
        // obtain the timer service
        TimerService timerService = ctx.timerService();
        // retrieve the content of the current state or initialize the state information
        Query1State nowState = state.value();
        if (nowState == null) {nowState = new Query1State(tupleData1.patientID, sessionNameThreshMap, attrNames, purgeThreshold);}
        // traverse through all the available thresholds to identify what sessions to activate
        boolean hasQualified = false;
        for (String sName : sessionNameThreshMap.keySet()) {
            // we have a qualified tuple, so the window starts and the state is maintained
            // the activity periods are identified based on meanHeartRate
            if (sessionNameThreshMap.get(sName) != null) {
                // lower bound included
                Float sessionThreshMin = sessionNameThreshMap.get(sName)[0];
                // upper bound excluded
                Float sessionThreshMax = sessionNameThreshMap.get(sName)[1];
                // the data is qualified for a particular session
                if (QueryUtils.isQualified(attrValue1, sessionThreshMin, sessionThreshMax, true, false)) {
                    // this is the base stream (the stream that we deduce sessions from)
                    nowState.collectTuple(tupleData1, sName);
                    hasQualified = true;
                }
                // the data is not qualified for this session
                else {updateTimerMap(nowState.timerBaseSessionMap, timerService, sName, tupleData1);}
            }
        }
        if (!hasQualified) {  // put into the "N/A" session, those that are disqualified by all thresholds
            nowState.collectTuple(tupleData1, DataGenerator.sessionNameNA);
        }
        // schedule a timer w.r.t the end of an "N/A" session
        else {updateTimerMap(nowState.timerBaseSessionMap, timerService, DataGenerator.sessionNameNA, tupleData1);}
        // write back to state
        state.update(nowState);
    }
    // helper function for registering timer and mapping the timestamp to session names
    public void updateTimerMap(Map<Long, ArrayList<String>> timerSessionMap, TimerService timerService,
                               String sName, TupDevice dataTuple) {
        // register timer so that the result will be emitted when the watermark passes this timestamp
        timerService.registerEventTimeTimer(dataTuple.timestamp);
        // we already have this timestamp as a key in the map
        if (timerSessionMap.containsKey(dataTuple.timestamp)) {
            timerSessionMap.get(dataTuple.timestamp).add(sName);
        }
        // we do not have this timestamp as a key in the map
        else {timerSessionMap.put(dataTuple.timestamp, new ArrayList<String>() {{add(sName);}});}
    }
    @Override
    public void processElement1(IN1 data2, Context ctx, Collector<String> out) throws Exception {
        TupDevice tupleData2 = null;
        String Tuple2CustomQ4Message = null;
        if (data2 instanceof TupBloodPressure) {
            tupleData2 = (TupBloodPressure) data2;
        }
        else if (data2 instanceof TupFitbit) {
            tupleData2 = (TupFitbit) data2;
        }
        else if (data2 instanceof TupGlucose) {
            tupleData2 = (TupGlucose) data2;
        }
        // unpack the custom tuple to retrieve the data and messages
        else if (data2 instanceof Tuple2CustomQ4) {
            if (((Tuple2CustomQ4) data2).dataTuple != null) {
                if (((Tuple2CustomQ4) data2).dataTuple instanceof TupInsulin) {
                    tupleData2 = ((Tuple2CustomQ4) data2).dataTuple;
                }
                else if (((Tuple2CustomQ4) data2).dataTuple instanceof TupGlucose) {
                    tupleData2 = ((Tuple2CustomQ4) data2).dataTuple;
                }
            }
            if (((Tuple2CustomQ4) data2).message != null && !((Tuple2CustomQ4) data2).message.equals("")) {
                Tuple2CustomQ4Message = ((Tuple2CustomQ4) data2).message;
                //out.collect(Tuple2CustomQ4Message);
            }
        }
        assert tupleData2 != null;
        // retrieve the content of the current state or initialize the state information
        Query1State nowState = state.value();
        if (nowState == null) {nowState = new Query1State(tupleData2.patientID, sessionNameThreshMap, attrNames, purgeThreshold);}
        // buffer the data into the priority queue
        nowState.mappedCollectQueue.add(tupleData2);
        // add the message to wait list if applicable
        if (Tuple2CustomQ4Message != null && !Tuple2CustomQ4Message.equals("")) {
            nowState.messageWaitList.add(Tuple2CustomQ4Message);
        }
        // write back to state
        state.update(nowState);
    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // retrieve the content of the current state or initialize the state info
        Query1State nowState = state.value();
        if (nowState == null) {nowState = new Query1State(ctx.getCurrentKey(), sessionNameThreshMap, attrNames, purgeThreshold);}
        // make sure we have the timestamp
        if (!nowState.timerBaseSessionMap.containsKey(timestamp)) {return;}
        // output the query results as the watermark passes the scheduled timer
        for (String sName : nowState.timerBaseSessionMap.get(timestamp)) {
            boolean hasCollected = false;
            // initialize the mapped session info to be printed
            StringBuilder avgInfo = new StringBuilder();
            // initialize the names for the applicable thresholds to be printed
            String sessionThreshold = QueryUtils.rangeToString(sessionNameThreshMap.get(sName),
                    true, false);
            if (sessionThreshold.equals("" + null)) {sessionThreshold = "";}
            Long sessionStartTime = null;
            Long sessionEndTime = null;
            // output the collected results triggered by the passing watermark
            while (nowState.sessionCollectMap.get(sName).size() > 0) {
                // we use priority queue to store these data such that they can be printed out in timestamp order
                TupDevice tuple = nowState.sessionCollectMap.get(sName).peek();
                if (tuple == null || tuple.timestamp > timestamp) {break;}
                tuple = nowState.sessionCollectMap.get(sName).poll();
                out.collect(tuple + ", session: " + sName + " " + sessionThreshold);
                assert tuple != null;
                if (sessionStartTime == null) {sessionStartTime = tuple.timestamp;}
                sessionEndTime = tuple.timestamp;
                hasCollected = true;
            }
            if (hasCollected) {
                out.collect(QueryUtils.windowBar + " End of session from " + sessionStartTime +
                        " to " + sessionEndTime + ": " + sName + " " + sessionThreshold);
            }
            // deal with blood pressure
            boolean hasCollectedMapped = false;
            Long sessionMappedStartTime = null;
            Long sessionMappedEndTime = null;
            ArrayList<TupDevice> sessionTuplesTemp = new ArrayList<>();
            ArrayList<TupDevice> polled = new ArrayList<>();
            while (nowState.mappedCollectQueue.size() > 0 && sessionStartTime != null && sessionEndTime != null) {
                TupDevice tuple = nowState.mappedCollectQueue.poll();
                // for each session, we need to poll the queue first and add the tuples back
                // because different sessions may overlap, so these tuples need to be checked against other sessions
                polled.add(tuple);
                if (tuple == null || tuple.timestamp > sessionEndTime) {
                    nowState.mappedCollectQueue.addAll(polled);
                    break;
                }
                if (tuple.timestamp >= sessionStartTime) {
                    String space = " "; if (sessionThreshold.equals("")) {space = "";}
                    out.collect(tuple + " included in window " + sName + space + sessionThreshold + " from " +
                            sessionStartTime + " to " + sessionEndTime);
                    sessionTuplesTemp.add(tuple);
                    // record the session start & end time
                    if (sessionMappedStartTime == null) {sessionMappedStartTime = tuple.timestamp;}
                    sessionMappedEndTime = tuple.timestamp;
                    hasCollectedMapped = true;
                }
            }
            Map<String, Float> avgResults = QueryUtils.getListTupleAvg(sessionTuplesTemp, attrNames);
            // initialize the average value info
            for (int i = 0; i < attrNames.length; i++) {
                avgInfo.append(attrNames[i]).append(": ").append(avgResults.get(attrNames[i]));
                if (i != attrNames.length - 1) {avgInfo.append(", ");}
                else {avgInfo.append(" ");}
            }
            if (hasCollectedMapped) {
                out.collect(QueryUtils.avgBar + " Session " + sName + " from " + sessionMappedStartTime + " to " +
                        sessionMappedEndTime + ", Count: " + sessionTuplesTemp.size() + ", Average(s): " + avgInfo);
                if (isTrackPrev) {
                    // this part is currently assumed to be reserved for query 4
                    String prevAvgInfo = ""; boolean hasPrev = true;
                    Float prevAvgVal = null;
                    if (sName.equals(DataGenerator.sessionNameActive)) {
                        prevAvgInfo += DataGenerator.sessionNameInactive + ": ";
                        prevAvgVal = nowState.prevAvgMap.get(DataGenerator.sessionNameInactive).get(attrNames[0]);
                        if (prevAvgVal != null) {
                            prevAvgInfo += attrNames[0] + ": " + prevAvgVal;
                        }
                        else {hasPrev = false;}
                    }
                    else if (sName.equals(DataGenerator.sessionNameInactive)) {
                        prevAvgInfo += DataGenerator.sessionNameActive + ": ";
                        prevAvgVal = nowState.prevAvgMap.get(DataGenerator.sessionNameActive).get(attrNames[0]);
                        if (prevAvgVal != null) {
                            prevAvgInfo += attrNames[0] + ": " + prevAvgVal;
                        }
                        else {hasPrev = false;}
                    }
                    if (prevAvgVal != null) {
                        float percentChange = Math.abs((prevAvgVal - avgResults.get(attrNames[0])) / prevAvgVal) * 100F;
                        percentChange = Math.round(percentChange * 100.0F) / 100.0F;
                        if (prevAvgVal.equals(avgResults.get(attrNames[0]))) {prevAvgInfo += " steady";}
                        else {
                            if (prevAvgVal > avgResults.get(attrNames[0])) {prevAvgInfo += " decreases ";}
                            else if (prevAvgVal < avgResults.get(attrNames[0])) {prevAvgInfo += " increases ";}
                            prevAvgInfo += "by " + percentChange + "%";
                        }
                    }
                    if (!hasPrev) {prevAvgInfo = "N/A";}
                    out.collect(QueryUtils.avgBar + " Previous average(s): " + prevAvgInfo);
                }
                // track the previous averages if applicable
                if (isTrackPrev) {
                    nowState.prevAvgMap.put(sName, new HashMap<String, Float>() {{
                        for (String a : avgResults.keySet()) {
                            put(a, avgResults.get(a));
                        }
                    }});
                }
            }
        }
        // output the wait-listed messages and clear the wait list
        for (String messageWait : nowState.messageWaitList) {
            out.collect(messageWait);
        }
        nowState.messageWaitList.clear();
        // purge the current mapping, ready for the new data
        nowState.timerBaseSessionMap.remove(timestamp);
        // purge the priority queue if its size gets excessive
        nowState.purgeCollectQueue();
        // write back to state
        state.update(nowState);
    }
}

package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;


// flexible "session window"
// consecutive tuples with a certain attribute value greater than the threshold will be grouped into a "window"
// Q1 - compute the average Blood Pressure value over different activity periods, e.g. running, walking, sleeping.
public class Query1 {
    // if the value is within the threshold range, this tuple will be put in a custom "session window"
    protected static final Float[][] actThreshArr = new Float[][] {{0.2F, 0.4F}, {0.5F, 0.7F}, {0.9F, null}};
    protected static final String[] actPeriodNames = new String[] {"running", "walking", "sleeping"};
    public static void main(String[] args) throws Exception {
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);
        // define the source
        // the stream of smartwatch data, the activity periods are identified based on meanHeartRate
        DataStream<TupSmartwatch> streamSmartwatch = env
                .addSource(new DataGenerator<TupSmartwatch>(1, 1, "Smartwatch",
                                                   false, 150))
                .returns(TypeInformation.of(TupSmartwatch.class))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(eventSmartwatch -> eventSmartwatch.deviceID);
        // the stream of blood pressure data
        DataStream<TupBloodPressure> streamBloodPressure = env
                .addSource(new DataGenerator<TupBloodPressure>(1, 1, "BloodPressure",
                                                      false, 150))
                .returns(TypeInformation.of(TupBloodPressure.class))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(eventBloodPressure -> eventBloodPressure.deviceID);
        // connect the 2 streams
        streamSmartwatch.connect(streamBloodPressure)
                .process(new Q1CoProcessFunc(actThreshArr, actPeriodNames))
                .print()
                .setParallelism(1);
        // Execute the program
        env.execute("Query 1 Execution");
    }
    // KeyedCoProcessFunction is of the form <key_type, left_input, right_input, output_type>
    public static class Q1CoProcessFunc extends KeyedCoProcessFunction<Integer, TupSmartwatch, TupBloodPressure, String> {
        // manually maintain the state, to be precise, a list of states
        private ValueState<Q1State[]> stateArr;
        // the activity identifier, an array of range thresholds
        private final Float[][] actPeriodRangeArr;
        private String[] sessionNames;
        // formalize all the ranges
        public Q1CoProcessFunc(Float[][] newActPeriodRangeArr, String[] newSessionName) {
            actPeriodRangeArr = newActPeriodRangeArr;
            sessionNames = newSessionName;
            for (int i = 0; i < actPeriodRangeArr.length; i++) {
                Float[] actPeriodRange = QueryUtils.assignRange(newActPeriodRangeArr[i]);
                newActPeriodRangeArr[i] = actPeriodRange;
            }
            if (sessionNames == null || sessionNames.length != actPeriodRangeArr.length) {
                sessionNames = new String[actPeriodRangeArr.length];
                for (int i = 0; i < sessionNames.length; i++) {sessionNames[i] = String.valueOf(i);}
            }
        }
        // initialize the state
        @Override
        public void open(Configuration parameters) throws Exception {
            stateArr = getRuntimeContext().getState(new ValueStateDescriptor<>("Q1State", Q1State[].class));
        }
        // process the elements from the 1st stream, smartwatch
        @Override
        public void processElement1(TupSmartwatch dataSmartwatch, Context ctx, Collector<String> out) throws Exception {
            // retrieve the content of the current state or initialize the state information
            Q1State[] nowStateArr = stateArr.value();
            if (nowStateArr == null) {
                // the number of states is certainly equal to the number of thresholds
                nowStateArr = new Q1State[actPeriodRangeArr.length];
                for (int i = 0; i < nowStateArr.length; i++) {
                    nowStateArr[i] = new Q1State(dataSmartwatch.deviceID);
                    nowStateArr[i].setStartTime(dataSmartwatch.timestamp);  // obtain the start time of this potential custom window
                }
            }
            stateArr.update(nowStateArr);
            // traverse through all the available thresholds to identify what sessions to activate
            StringBuilder activeNames = new StringBuilder();
            for (int i = 0; i < actPeriodRangeArr.length; i++) {
                Float actPeriodMin = actPeriodRangeArr[i][0];
                Float actPeriodMax = actPeriodRangeArr[i][1];
                // we have a qualified tuple, so the window starts and the state is maintained
                // the activity periods are identified based on meanHeartRate
                boolean isQualified = QueryUtils.isQualified(dataSmartwatch.meanHeartRate, actPeriodMin, actPeriodMax);
                if (isQualified) {activeNames.append(sessionNames[i]).append(" ");}
                switchSession(nowStateArr, dataSmartwatch, out, i, isQualified);
            }
            out.collect("Smartwatch " + dataSmartwatch + " " + activeNames);
        }
        // process the elements from the 2nd stream
        @Override
        public void processElement2(TupBloodPressure dataBloodPressure, Context ctx, Collector<String> out) throws Exception {
            Q1State[] nowStateArr = stateArr.value();
            StringBuilder isInclude = new StringBuilder(" included in window ");
            //String windowNow = "";
            if (nowStateArr != null) {
                for (int i = 0; i < actPeriodRangeArr.length; i++) {
                    // the window has to be open
                    // if the window has closed but the timestamp is still qualified,
                    // it should also be put into the closed window
                    if ((nowStateArr[i].isOpen && dataBloodPressure.timestamp >= nowStateArr[i].startTime) ^
                        (!nowStateArr[i].isOpen && dataBloodPressure.timestamp <= nowStateArr[i].latestTime)) {
                        isInclude.append(sessionNames[i]).append(" ");
                        // update the state's count
                        nowStateArr[i].incrCount();
                        // update the sum of the state
                        nowStateArr[i].incrSum(dataBloodPressure.dBP.floatValue());
                        //windowNow = nowStateArr[i].startTime + " " + nowStateArr[i].latestTime + " " + Arrays.toString(actPeriodRangeArr[i]);
                    }
                }
                stateArr.update(nowStateArr);
                // print out BloodPressure stream
                out.collect("BloodPressure " + dataBloodPressure + isInclude);
                //out.collect("BloodPressure " + dataBloodPressure + isInclude + windowNow);
            }
        }
        // switch ON/OFF states
        public void switchSession(Q1State[] nowStateArr, TupSmartwatch dataSmartwatch,
                                  Collector<String> out, int stateNum, boolean isSwitchOn) throws Exception {
            // if we want to turn it on and the target session window is closed, open it
            if (isSwitchOn && !nowStateArr[stateNum].isOpen) {
                // the target session window starts
                nowStateArr[stateNum].setStartTime(dataSmartwatch.timestamp);
                nowStateArr[stateNum].openWindow();
                // obtain the current end of this window, this window may not be over yet
                nowStateArr[stateNum].setLatestTime(dataSmartwatch.timestamp);
            }
            // if we want to turn it off the target session window is open
            else if (!isSwitchOn && nowStateArr[stateNum].isOpen) {
                // first, deal with its output
                if (nowStateArr[stateNum].count > 0) {
                    out.collect(sessionNames[stateNum] + " window: [" + nowStateArr[stateNum].startTime + ", " +
                                nowStateArr[stateNum].latestTime + "]" +
                                " Patient ID: " + dataSmartwatch.patientID +
                                " Device ID: " + dataSmartwatch.deviceID +
                                " count: " + nowStateArr[stateNum].count +
                                " avg: " + nowStateArr[stateNum].getAvg() +
                                " prev avg: " + nowStateArr[stateNum].prevAvg +
                                " " + templateTest.windowBar);
                }
                // finally, close it
                nowStateArr[stateNum].closeWindow(true);
            }
            // write the state back
            stateArr.update(nowStateArr);
        }
    }
}

package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Stack;

// Q4 - Identify eating period: when glucose rises by x% within y minutes, where x, y are input parameters.
// A. Determine how often insulin is given within 15 minutes of the start of this period.
// B. Identify “activity” period
// e.g., based on heart rate or steps and compute how glucose drops compared to inactivity period.
public class Query4 {
    public static void main(String[] args) throws Exception {
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(10);
        // define the source
        // stream 1: glucose
        DataStream<Tuple2<Integer, Double>> stream1 = env
                .addSource(new templateTest.testSourceFunc(true, 100, 0.0))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(event1 -> event1.f0);
        // stream 2: insulin
        // "how often" will be calculated in terms of avg
        DataStream<Tuple2<Integer, Double>> stream2 = env
                .addSource(new templateTest.testSourceFunc(true, 100, 0.0))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(event2 -> event2.f0);
        // stream 3: activity
        DataStream<Tuple2<Integer, Double>> stream3 = env
                .addSource(new templateTest.testSourceFunc(true, 100, 0.0))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(event3 -> event3.f0);
        // connect the 3 streams
        stream1.connect(stream2)
                .process(new coProcessFuncQ4A())
                .keyBy(event -> event.f0)
                .connect(stream3)
                .process(new coProcessFuncQ4B())
                .print()
                .setParallelism(1);
        // Execute the program
        env.execute("Query 4 Exe");
    }
    // KeyedCoProcessFunction attributes: key, input 1, input 2, output
    // process the first 2 streams, and pass the 1st stream down to work with the 3rd stream
    // for now, the output at this step would just be the combination of 1st and 2nd streams
    public static class coProcessFuncQ4A extends KeyedCoProcessFunction<Integer, Tuple2<Integer, Double>,
                                                                        Tuple2<Integer, Double>,
                                                                        Tuple3<Integer, Double, String>> {
        // a rise in an attribute by x% within y minutes
        private final Double riseRatioX;
        private final long risePeriodY;
        // determine how often insulin is given within a certain period of the start of this rise
        private final long defaultPeriod;
        // we can reuse the state class from query 3 for stream 1
        private ValueState<Q3StateStream> state1;  // tracking the rise of a value
        private ValueState<Q4StatePeriod> state2;  // tracking the interested insulin periods caused by this rise
        // default test thresholds
        public coProcessFuncQ4A() {riseRatioX = 0.5; risePeriodY = 4; defaultPeriod = 5;}
        // customizable parameters
        public coProcessFuncQ4A(Double riseRatioX, long risePeriodY, long defaultPeriod) {
            this.riseRatioX = riseRatioX;
            this.risePeriodY = risePeriodY;
            this.defaultPeriod = defaultPeriod;
        }
        // initialize the states for the 2 streams
        @Override
        public void open(Configuration parameters) throws Exception {
            state1 = getRuntimeContext().getState(new ValueStateDescriptor<>("Q4StateStream1", Q3StateStream.class));
            state2 = getRuntimeContext().getState(new ValueStateDescriptor<>("Q4StateStream2", Q4StatePeriod.class));
        }
        // process the elements from the 1st stream (glucose)
        @Override
        public void processElement1(Tuple2<Integer, Double> data1, Context ctx,
                                    Collector<Tuple3<Integer, Double, String>> out) throws Exception {
            Q3StateStream nowState1 = state1.value();
            if (nowState1 == null) {nowState1 = new Q3StateStream(data1.f0);}
            long nowTimestamp = ctx.timestamp();
            // collect the message to be passed down stream along with the data
            CollectorCustom<String> messageCollect = new CollectorCustom<>();
            // detect whether there is a qualified increase and collect the corresponding messages
            detectRise(data1.f0, data1.f1, nowTimestamp, 1, riseRatioX, risePeriodY, nowState1, messageCollect);
            // store the data from stream 1 AND the message to be passed down stream
            Tuple3<Integer, Double, String> temp1 = Tuple3.of(data1.f0, data1.f1,
                                                              "glucose " + messageCollect.message + " time " + ctx.timestamp());
            // pass the information down stream
            out.collect(temp1);
        }
        // process the elements from the 2nd stream (insulin)
        // determine how often insulin is given within 15 minutes of the start of this period
        @Override
        public void processElement2(Tuple2<Integer, Double> data2, Context ctx,
                                    Collector<Tuple3<Integer, Double, String>> out) throws Exception {
            Q4StatePeriod nowState2 = state2.value();
            if (nowState2 == null) {nowState2 = new Q4StatePeriod(data2.f0, defaultPeriod);}
            String message = "";
            if (nowState2.isOpen) {  // we are accepting records
                message = " recorded";
                nowState2.pushAll(Tuple2.of(data2.f1, ctx.timestamp()));
                Tuple2<Double, String> output = nowState2.checkFullStack();
                if (output != null) {
                    message += "\n output: " + output;
                }
                else {
                    message += "\n stack list size " + nowState2.stackList.size() +
                               " 1st stack time range " + nowState2.getTimeRange(0);
                }
                // if we have no stacks anymore, this session closes
                if (nowState2.stackList.size() == 0) {
                    nowState2.close();
                }
            }
            // do not pass the data from the 2nd stream down the line
            // instead, just pass it as a message
            // because we want to print it later
            Tuple3<Integer, Double, String> temp2 = Tuple3.of(data2.f0, -1.0,
                    "Stream 2 insulin " + data2 + " time " + ctx.timestamp() + message);
            out.collect(temp2);
        }
        // monitor stream 1, detect if the value increases by a certain percentage within a certain period of time
        public void detectRise(Integer key, Double dataVal, long nowTimestamp, Integer streamNum, Double riseRatio,
                               long risePeriod, Q3StateStream nowState, Collector<String> out) throws Exception {
            // indicate the result of the detection
            int detection = QueryUtils.queryDetectRise(dataVal, nowTimestamp, streamNum,
                                                       riseRatio, risePeriod, nowState, out);
            // whether the state needs to be updated
            boolean isUpdateState = (detection != 0);
            // whether there is a qualified increase
            boolean hasIncrease = (detection == 2);
            // update the state for stream 1
            if (isUpdateState) {
                nowState.setValueTrack(dataVal);
                nowState.setTimestampTrack(nowTimestamp);
                state1.update(nowState);
            }
            // the rising period is detected, start monitoring insulin for 15 minutes
            // here we only concern about the state of stream 2, which collects data for 15 minutes (or more)
            if (hasIncrease) {
                Q4StatePeriod nowState2 = state2.value();
                if (nowState2 == null) {nowState2 = new Q4StatePeriod(key, defaultPeriod);}
                // if we are currently not accepting records, open the gate
                if (!nowState2.isOpen) {
                    nowState2.open();
                }
                // if we are already accepting records,
                // this means that we will keep more records than usual due to the overlapped periods
                // add a new stack to the list of stacks
                nowState2.stackList.add(new Stack<>());
                state2.update(nowState2);
            }
        }
    }
    // KeyedCoProcessFunction attributes: key, input 1, input 2, output
    // get the activity sessions of stream 3
    // compute how glucose (stream 1) drops compared to inactivity period
    // set up the session windows for stream 3, map them to stream 1
    // for 2 consecutive windows, there must be 1 active and 1 inactive
    // obtain the avg glucose values for them and compare
    public static class coProcessFuncQ4B extends KeyedCoProcessFunction<Integer, Tuple3<Integer, Double, String>,
                                                                        Tuple2<Integer, Double>, String> {
        // we can reuse the state class from query 1 for stream 3
        // use a list of 2 states, 1st for ACTIVE, 2nd for INACTIVE
        private ValueState<Q1State[]> state3;
        private final Float[] actPeriodRange;
        private final Float actPeriodMin;
        private final Float actPeriodMax;
        public coProcessFuncQ4B() {
            actPeriodRange = new Float[] {0.5F, null};
            actPeriodMin = actPeriodRange[0];
            actPeriodMax = actPeriodRange[1];
        }
        public coProcessFuncQ4B(Float[] newActPeriodRange) {
            actPeriodRange = QueryUtils.assignRange(newActPeriodRange);
            actPeriodMin = actPeriodRange[0];
            actPeriodMax = actPeriodRange[1];
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            state3 = getRuntimeContext().getState(new ValueStateDescriptor<>("Q4StateStream3", Q1State[].class));
        }
        // process the elements from the combination of 1st & 2nd streams
        // in this case we only concern about the 1st stream (glucose)
        // but for the sake of display, we still show stream 2
        // NOTE: we have 2 types of sessions here, ACTIVE & INACTIVE
        @Override
        public void processElement1(Tuple3<Integer, Double, String> data12, Context ctx,
                                    Collector<String> out) throws Exception {
            // extract the data of stream 1 from the input
            // the "data" from stream 2 are automatically ignored
            if (data12.f1 > 0.0) {
                Tuple2<Integer, Double> data1 = Tuple2.of(data12.f0, data12.f1);
                // if applicable, put the data into the session windows
                Q1State[] nowState3Array = state3.value();
                String isInclude = "";
                // the window has to be open
                // if the window has closed but the timestamp is still qualified,
                // it should also be put into the closed window
                String windowNow = "";
                if (nowState3Array != null) {
                    for (int i = 0; i < nowState3Array.length; i++) {
                        if ((nowState3Array[i].isOpen && ctx.timestamp() >= nowState3Array[i].startTime) ^
                            (!nowState3Array[i].isOpen && ctx.timestamp() <= nowState3Array[i].latestTime)) {
                            isInclude = " included in window";
                            if (i == 0) {isInclude += " ACTIVE";} else {isInclude += " INACTIVE";}
                            // update the state's count
                            nowState3Array[i].incrCount();
                            // update the sum of the state
                            nowState3Array[i].incrSum(data1.f1.floatValue());
                            state3.update(nowState3Array);
                            windowNow = nowState3Array[i].startTime + " " + nowState3Array[i].latestTime;
                        }
                    }
                }
                out.collect("Stream 1 glucose, Time: " + ctx.timestamp() + " data " + data1.f1 + isInclude + windowNow);
            }
            // extract the string message from the input
            // for stream 2, it will just be its data
            String message = data12.f2;
            out.collect(message);
        }
        // process the elements from the 3rd stream
        // we need to track the activity sessions of stream 3
        @Override
        public void processElement2(Tuple2<Integer, Double> data3, Context ctx, Collector<String> out) throws Exception {
            // retrieve the content of the current state or initialize the state information
            Q1State[] nowState3Array = state3.value();
            if (nowState3Array == null) {
                nowState3Array = new Q1State[] {new Q1State(data3.f0), new Q1State(data3.f0)};
                // obtain the start time of this potential custom window
                for (Q1State q1State : nowState3Array) {q1State.setStartTime(ctx.timestamp());}
            }
            //for (Q1State q1State : nowState3Array) {q1State.setLastTimeStamp(ctx.timestamp());}
            state3.update(nowState3Array);
            // we have a qualified tuple, so the window starts and the state is maintained
            // the 1st state is for Qualified -> ACTIVE
            // the 2nd state is for Unqualified -> INACTIVE
            if (QueryUtils.isQualified(data3.f1.floatValue(), actPeriodMin, actPeriodMax)) {
                switchSession(nowState3Array, data3, ctx, out, 0);
            }
            // output the state when a non-qualified tuple is encountered, the current ACTIVE window is ended
            // however, this time we need to maintain state for non-qualified tuples as well
            // so the INACTIVE window is opened
            else {
                switchSession(nowState3Array, data3, ctx, out, 1);
            }
        }
        // switch between active and inactive states
        public void switchSession(Q1State[] nowState3Array, Tuple2<Integer, Double> data3, Context ctx,
                                  Collector<String> out, int stateNum) throws Exception {
            // safeguarding the validity of the input
            if (stateNum != 0 && stateNum != 1) {return;}
            // switching the content of message based on the ACTIVE/INACTIVE session
            String isActive = "ACTIVE";
            if (stateNum == 1) {isActive = "INACTIVE";}
            // print the data stream along with whether it is active
            out.collect("Stream 3 activity, Time: " + ctx.timestamp() + " ID: " + data3.f0 +
                    " Value: " + data3.f1 + " " + isActive);
            // if the target session window is closed, open it
            // and close the opposite session window
            if (!nowState3Array[stateNum].isOpen) {
                // the target session window starts
                nowState3Array[stateNum].setStartTime(ctx.timestamp());
                nowState3Array[stateNum].openWindow();
                // close the opposite session window if it is open
                int oppStateNum = 1 - stateNum;
                String isOppActive = "ACTIVE";
                if (oppStateNum == 1) {isOppActive = "INACTIVE";}
                if (nowState3Array[oppStateNum].isOpen) {
                    // first, deal with its output
                    if (nowState3Array[oppStateNum].count > 0) {
                        out.collect(isOppActive + " Window: [" + nowState3Array[oppStateNum].startTime + ", " +
                                    nowState3Array[oppStateNum].latestTime +
                                    "] count: " + nowState3Array[oppStateNum].count +
                                    ", avg: " + nowState3Array[oppStateNum].getAvg() +
                                    ", prev " + isActive + " avg: " + nowState3Array[stateNum].prevAvg +
                                    " " + templateTest.windowBar);
                    }
                    // finally, close it
                    nowState3Array[oppStateNum].closeWindow(true);
                }
            }
            // obtain the current end of this window, this window may not be over yet
            nowState3Array[stateNum].setLatestTime(ctx.timestamp());
            // write the state back
            state3.update(nowState3Array);
        }
    }
}

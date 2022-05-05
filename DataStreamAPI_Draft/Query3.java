package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

// Q3 - pattern detection: Detect a sequence of events and raise an alert, e.g., an
// increase in glucose followed by an increase in heart rate within a specified threshold
// in this context, it will be an increase in stream 1 followed by an increase in stream 2
public class Query3 {
    public static void main(String[] args) throws Exception {
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(10);
        // define the source
        DataStream<Tuple2<Integer, Double>> stream1 = env
                .addSource(new templateTest.testSourceFunc(true, 100, 0.0))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(event1 -> event1.f0);
        DataStream<Tuple2<Integer, Double>> stream2 = env
                .addSource(new templateTest.testSourceFunc(true, 100, 0.0))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(event2 -> event2.f0);
        // connect the 2 streams
        stream1.connect(stream2)
                .process(new coProcessFuncQ3())
                .print()
                .setParallelism(1);
        // Execute the program
        env.execute("Query 3 Exe");
    }
    // ProcessWindowFunction attributes: input event, output event collector, key, the window
    public static class coProcessFuncQ3 extends KeyedCoProcessFunction<Integer, Tuple2<Integer, Double>,
                                                                       Tuple2<Integer, Double>, String> {
        // a rise in an attribute within a period of time
        private Double riseRatio1;
        private final long risePeriod1;
        private Double riseRatio2;
        private final long risePeriod2;
        private final long interRisePeriod;
        // an increase in stream 1 (by riseRatio1 within risePeriod1)
        // followed by an increase in stream 2 (by riseRatio2 within risePeriod2)
        // within a specified threshold (interRisePeriod)
        // maintain 2 states for the 2 streams separately
        private ValueState<Q3StateStream> state1;
        private ValueState<Q3StateStream> state2;
        private ValueState<Q3StateRise> stateRise;
        // default test thresholds
        public coProcessFuncQ3() {
            riseRatio1 = 0.6;
            risePeriod1 = 1;
            riseRatio2 = 0.6;
            risePeriod2 = 1;
            interRisePeriod = 5;
        }
        // customizable parameters
        public coProcessFuncQ3(Double riseRatio, long risePeriod,
                               Double riseRatio2, long risePeriod2, long interRisePeriod) {
            this.riseRatio1 = riseRatio;
            if (riseRatio == null) {this.riseRatio1 = 0.0;}
            this.risePeriod1 = risePeriod;
            this.riseRatio2 = riseRatio2;
            if (riseRatio2 == null) {this.riseRatio2 = 0.0;}
            this.risePeriod2 = risePeriod2;
            this.interRisePeriod = interRisePeriod;
        }
        // initialize the states for the 2 streams
        @Override
        public void open(Configuration parameters) throws Exception {
            state1 = getRuntimeContext().getState(new ValueStateDescriptor<>("Q3StateStream1", Q3StateStream.class));
            state2 = getRuntimeContext().getState(new ValueStateDescriptor<>("Q3StateStream2", Q3StateStream.class));
            stateRise = getRuntimeContext().getState(new ValueStateDescriptor<>("Q3StateRise", Q3StateRise.class));
        }
        // process the elements from the 1st stream
        @Override
        public void processElement1(Tuple2<Integer, Double> data1, Context ctx, Collector<String> out) throws Exception {
            Q3StateStream nowState1 = state1.value();
            if (nowState1 == null) {
                nowState1 = new Q3StateStream(data1.f0);
            }
            long nowTimestamp = ctx.timestamp();
            detectRise(data1.f1, nowTimestamp, 1, riseRatio1, risePeriod1, nowState1, out);
            // print out the 1st stream
            out.collect("Stream 1, Time: " + ctx.timestamp() + " ID: " + data1.f0 + " Value: " + data1.f1);
        }
        // process the elements from the 2nd stream
        @Override
        public void processElement2(Tuple2<Integer, Double> data2, Context ctx, Collector<String> out) throws Exception {
            Q3StateStream nowState2 = state2.value();
            if (nowState2 == null) {
                nowState2 = new Q3StateStream(data2.f0);
            }
            long nowTimestamp = ctx.timestamp();
            detectRise(data2.f1, nowTimestamp, 2, riseRatio2, risePeriod2, nowState2, out);
            // print out the 2nd stream
            out.collect("Stream 2, Time: " + ctx.timestamp() + " ID: " + data2.f0 + " Value: " + data2.f1);
        }
        // monitor a stream, detect if the value increases by a certain percentage within a certain period of time
        public void detectRise(Double dataVal, long nowTimestamp, Integer streamNum, Double riseRatio, long risePeriod,
                               Q3StateStream nowState, Collector<String> out) throws Exception {
            int detection = QueryUtils.queryDetectRise(dataVal, nowTimestamp, streamNum,
                                                       riseRatio, risePeriod, nowState, out);
            boolean isUpdateState = (detection != 0);
            boolean hasIncrease = (detection == 2);
            // handle patterned increasing
            if (hasIncrease) {checkRiseFollow(dataVal, nowTimestamp, streamNum, nowState, out);}
            if (isUpdateState) {
                nowState.setValueTrack(dataVal);
                nowState.setTimestampTrack(nowTimestamp);
                if (streamNum == 1) {state1.update(nowState);}
                else if (streamNum == 2) {state2.update(nowState);}
            }
        }
        // check if the rise in stream 1 is followed by a rise in stream 2 within the threshold period of time
        public void checkRiseFollow(Double dataVal, long nowTimestamp, Integer streamNum, Q3StateStream nowStreamState,
                                    Collector<String> out) throws Exception {
            Q3StateRise nowRiseState = stateRise.value();
            if (nowRiseState == null) {nowRiseState = new Q3StateRise();}
            // store the old record & new record
            Tuple2<Double, Long> oldRecord = Tuple2.of(nowStreamState.valueTrack, nowStreamState.timestampTrack);
            Tuple2<Double, Long> newRecord = Tuple2.of(dataVal, nowTimestamp);
            if (streamNum == 1) {nowRiseState.stack1.push(Tuple2.of(oldRecord, newRecord));}
            else if (streamNum == 2) {nowRiseState.stack2.push(Tuple2.of(oldRecord, newRecord));}
            while (nowRiseState.stack1.size() > 0 && nowRiseState.stack2.size() > 0) {
                Tuple2<Tuple2<Double, Long>, Tuple2<Double, Long>> oldestFrom1 = nowRiseState.getStack1Last();
                Tuple2<Tuple2<Double, Long>, Tuple2<Double, Long>> newestFrom2 = nowRiseState.stack2.get(0);
                if (!(oldestFrom1.f1.f1 >= 0 && newestFrom2.f1.f1 >= 0 &&
                      newestFrom2.f1.f1 - oldestFrom1.f1.f1 >= 0 &&
                      newestFrom2.f1.f1 - oldestFrom1.f1.f1 <= interRisePeriod)) {break;}
                out.collect("WARNING (PATTERN): stream 1 " + oldestFrom1 +
                            " stream 2 " + newestFrom2 + " " + templateTest.warnBar);
                nowRiseState.stack1.pop();
            }
            stateRise.update(nowRiseState);
        }
    }
}

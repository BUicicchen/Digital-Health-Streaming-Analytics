package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// Q2 - anomaly detection: To detect an anomaly, the query maintains a moving average, the mean of the averages
// across three consecutive windows. The query compares the current window average with the moving average
// and generates an alert if their difference is too high. Identify anomalies in BP and glucose measurements.
public class Query2 {
    // default tumbling window size, i.e., number of tuples per window
    protected static int windowSize = 5;
    public Query2() {windowSize = 5;}
    public Query2(int newWindowSize) {windowSize = newWindowSize;}
    public static void main(String[] args) throws Exception {
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(500);
        // define the source
        DataStream<Tuple2<Integer, Double>> input = env
                .addSource(new templateTest.testSourceFunc(true, 100, 5))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
        // the key is deviceID
        input.keyBy(event -> event.f0)
                // group the event by time, 5 events per window
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                // group the events by fixing the window size to be 5 tuples
                // TumblingEventTimeWindows can be used together with process
                .process(new processFunc())
                .print()
                .setParallelism(1);
        // Execute the program
        env.execute("Query 2 Exe");
    }
    // ProcessWindowFunction attributes: input event, output event collector, key, the window
    public static class processFunc extends ProcessWindowFunction<Tuple2<Integer, Double>, String, Integer, TimeWindow> {
        // manually maintain the state
        private ValueState<Q2State> state;
        // define how many consecutive windows we want to track
        private final int numWindowTrack;
        // the alert threshold
        private final Double alertThreshold;
        public processFunc() {
            // maintain N = 3 consecutive averages
            numWindowTrack = 3;
            // when the value > alertThreshold, show the alert message
            alertThreshold = 0.2;
        }
        // you can also define you own number of windows to track and the alert threshold
        public processFunc(int numWindowTrack, Double alertThreshold) {
            this.numWindowTrack = numWindowTrack;
            this.alertThreshold = alertThreshold;
        }
        // only called once to initialize the state when the program starts
        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("Q2State", Q2State.class));
        }
        // process a window
        @Override
        public void process(Integer key, Context ctx, Iterable<Tuple2<Integer, Double>> input,
                            Collector<String> out) throws Exception {
            // retrieve the current state
            Q2State nowState = state.value();
            if (nowState == null) {
                nowState = new Q2State(key, numWindowTrack);
            }
            long count = 0;  // get the quantity of data items
            Double sumVal = 0.0;  // get the sum of the data
            for (Tuple2<Integer, Double> ipt: input) {
                sumVal += ipt.f1;
                // display what is in the window
                out.collect("Window: " + ctx.window() + " No. " + (count + 1) +
                        " ID: " + ipt.f0 + " Value: " + ipt.f1);
                count++;
            }
            Double nowWindowAvg = sumVal/((double) count);  // calculate the avg
            if (nowWindowAvg - nowState.getMovingAvg() >= alertThreshold) {
                out.collect("Window: " + ctx.window() + " WARNING " + templateTest.warnBar);
            }
            out.collect("Window: " + ctx.window() + " count: " + count + " avg: " + nowWindowAvg
                    + " " + templateTest.windowBar);
            // maintain 3 consecutive averages
            nowState.updateTrack(nowWindowAvg, ctx.window());
            // write the state back
            state.update(nowState);
            out.collect("Avgs: " + nowState.getAvgStr() + templateTest.avgBar);
        }
    }
}
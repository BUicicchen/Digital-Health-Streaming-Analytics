package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// a useful link about windows: https://programming.vip/docs/learn-more-about-tumbling-window-in-flink.html
// multiple windows: https://developpaper.com/multi-window-analysis-of-flink/

// flexible "session window"
// consecutive tuples with a certain attribute value greater than the threshold will be grouped into a "window"
// I don't think they are truly windows since no window operator is used here,
// but they still serve like flexible windows
public class templateQ1 {
    // if the value is above this threshold, this tuple will be put in a custom "session window"
    protected static double actThreshold = 0.5;
    public static void main(String[] args) throws Exception {
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(500);
        // define the source
        DataStream<Tuple2<Integer, Double>> input = env.addSource(new templateTest.testSourceFunc())
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
        // the key is deviceID
        input.keyBy(event -> event.f0)
                .process(new customWindow())
                .print()
                .setParallelism(1);
        // Execute the program
        env.execute("Test Q1 Exe");
    }
    // KeyedProcessFunction is of the form <key_type, input_type, output_type>
    // output_type corresponds to the datatype you provide to out.collect()
    // reference: https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/process_function/
    public static class customWindow extends KeyedProcessFunction<Integer, Tuple2<Integer, Double>, String> {
        // the state of this process function
        private ValueState<CustomState> state;
        // I'm not particularly clear what this open() function does, but it's necessary
        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("CustomState", CustomState.class));
        }
        @Override // process one tuple, "value" is the input
        public void processElement(Tuple2<Integer, Double> value, Context ctx, Collector<String> out) throws Exception {
            // retrieve the content of the current state or initialize the state information
            CustomState nowState = state.value();
            if (nowState == null) {
                nowState = new CustomState();
                nowState.setKey(value.f0);  // obtain the key
                nowState.setStartTime(ctx.timestamp());  // obtain the start of this custom window
            }
            // overall, you will see all the tuples, with "window" information interleaved
            // we have a qualified tuple, so the window starts and the state is maintained
            if (value.f1 > actThreshold) {
                // print the qualified data stream
                out.collect("Time: " + ctx.timestamp() + " ID: " + value.f0 + " Value: " + value.f1);
                // update the state's count
                nowState.incrCount();
                // update the sum of the state
                nowState.incrSum(value.f1);
                // obtain the current end of this window, this window may not be over yet
                nowState.setLatestTime(ctx.timestamp());
                // write the state back
                state.update(nowState);
            }
            // output the state when a non-qualified tuple is encountered, the current window is ended
            else if (state != null && nowState.getCount() > 0) {
                out.collect("Window: " + nowState.getStartTime() + " " + nowState.getLatestTime() +
                            " count: " + nowState.getCount() +
                            " avg: " + nowState.getAvg() + " " + templateTest.windowBar);
                // refresh the state
                state.clear();
            }
            if (value.f1 <= actThreshold) {
                // print the data stream when the tuple is not qualified
                out.collect("Time: " + ctx.timestamp() + " ID: " + value.f0 + " Value: " + value.f1);
            }
            // schedule the next timer 1 tuple from the current event time
            // use it together with onTimer(), currently not used for the reason explained below
            //ctx.timerService().registerEventTimeTimer(nowState.getLatestTime() + 1);
        }
        // determine what to do when the next timer deadline arrives
        // currently not used because it does not seem to work, and it is time based,
        // so I suppose it won't be very useful
        /*@Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // get the state for the key that scheduled the timer
            CustomState result = state.value();
            // check if this is an outdated timer or the latest timer
            if (timestamp == result.getLatestTime() + 1) {
                // we see a value below the threshold
                if (result.getCurrValue() <= actThreshold) {
                    // emit the state on timeout
                    out.collect("Window: " + result.getStartTime() + " " + result.getLatestTime() +
                                " count: " + result.getCount() +
                                " avg: " + result.getAvg() + " " + templateTest.windowBar);
                }
                else {
                    // schedule the next timer 1 tuple from the current event time
                    ctx.timerService().registerEventTimeTimer(result.getLatestTime() + 1);
                }
            }
        }*/
    }
}

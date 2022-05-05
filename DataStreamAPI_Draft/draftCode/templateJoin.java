package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// test for the joining functionality
public class templateJoin {
    // default tumbling window size, i.e., number of tuples per window
    // it appears to be that the window size does not restrict the window of the outputs correctly,
    // I think the reason comes from the tumbling window join mechanism
    protected static final int windowSize = 5;
    public static void main(String[] args) throws Exception {
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(500);
        // define the two sources
        DataStream<Tuple2<Integer, Double>> stream1 = env.addSource(new templateTest.testSourceFunc(5))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(event -> event.f0);
        DataStream<Tuple2<Integer, Double>> stream2 = env.addSource(new templateTest.testSourceFunc(5))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(event -> event.f0);
        // join the two sources based on the key and assign tumbling windows
        // see https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/joining/
        stream1.join(stream2)
                .where(event1 -> event1.f0)
                .equalTo(event2 -> event2.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new joinFunc())
                .keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .process(new processJoinFunc())
                .print()
                .setParallelism(1);
        // Execute the program
        env.execute("Test Join Exe");
    }
    // the join function for producing the joined tuple
    public static class joinFunc implements JoinFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>,
                                                         Tuple3<Integer, Double, Double>> {
        // join function datatype is of the form <input_left, input_right, output>
        @Override
        public Tuple3<Integer, Double, Double> join(Tuple2<Integer, Double> input1, Tuple2<Integer, Double> input2) {
            // combine the two tuples and form a new tuple
            return Tuple3.of(input1.f0, input1.f1, input2.f1);
        }
    }
    // ProcessWindowFunction attributes: input event, output event collector, key, the window
    // the input event is the joined tuple
    // display the joined results
    public static class processJoinFunc extends ProcessWindowFunction<Tuple3<Integer, Double, Double>,
                                                                      String, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context ctx, Iterable<Tuple3<Integer, Double, Double>> input, Collector<String> out) {
            long count = 0;  // get the quantity of data items
            for (Tuple3<Integer, Double, Double> ipt: input) {
                out.collect("Window: " + ctx.window() + " No. " + (count + 1) +
                            " ID: " + ipt.f0 + " Value Left: " + ipt.f1 + " Value Right: " + ipt.f2);
                count += 1;
            }
            out.collect("Window: " + ctx.window() + " count: " + count + " " + templateTest.windowBar);
        }
    }
}

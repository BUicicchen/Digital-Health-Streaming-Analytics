package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class tempTestOrder {
    public static void main(String[] args) throws Exception {
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(500);
        // define the source
        DataStream<Tuple2<Integer, Double>> input = env
                .addSource(new templateTest.testSrcOutOrder(2))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
        input.keyBy(event -> event.f0)
                .process(new RestoreOrder())
                .keyBy(event -> event.f0)
                .process(new processFunc())
                .print()
                .setParallelism(1);
        // Execute the program
        env.execute("Test order Exe");
    }
    public static class processFunc extends KeyedProcessFunction<Integer, Tuple2<Integer, Double>, String> {
        @Override
        public void processElement(Tuple2<Integer, Double> input, Context ctx, Collector<String> out) throws Exception {
            out.collect(ctx.timestamp() + ", " + input);
        }
    }
}

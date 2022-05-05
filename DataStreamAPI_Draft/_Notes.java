package clusterdata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
// process function: consecutive windows
// window is checkpointed, but source is not
// restarting the source, it restarts from scratch, it will not continue from where it left off
// custom source can implement checkpointed interface
// watermarks frequency does not affect the correctness of the window, it only affects how quickly the window knows it is complete
// the 2 seconds are in terms of the timestamp, not the actual clock time, but the frequency is the clock time
public class TestSource {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// the added source can be declared as a separate class implementing ParallelSourceFunction
		env.getConfig().setAutoWatermarkInterval(500);  // watermarks are required to trigger the windows
		DataStream<Tuple2<Integer, Long>> input = env.addSource(new ParallelSourceFunction<Tuple2<Integer, Double>>() {
			@Override
			public void run(SourceContext<Tuple2<Integer, Double>> ctx) throws Exception {
				// call distributions...
				// generate timestamp
				Tuple2<> event = Tuple2.of(1, 2.0);
				long timestamp = 23;
				while (true) {
					for (int i = 0; i < 10; i++) {
						ctx.collectWithTimestamp(event, timestamp);
					}
					timestamp++;
					// sleep function can slow down the stream
				}
			}
			// the canceling is not needed
			@Override
			public void cancel() {}
		})  // how the watermarks will be formed, the watermarks can also be defined by the source
		.setParallelism(1)  // the parallelism of this operator only, other operators still use the default parallelism
		.enableCheckpointing(2000)  // set a checkpoint every 2 seconds, see the information of a checkpoint in Overview, duration for each operator, if we kill the task manager and restart, it will start with the checkpoint
		.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
		.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
			@Override
			public Integer getKey(Tuple2<Integer, Long> e) throws Exception {
				return e.f0;
			}
		}).window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))  // group the event by time, 1000 events per window
		.reduce(new ReduceFunction<Tuple2<Integer, Long>>() {
			@Override
			public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> e1, Tuple2<Integer, Long> e2) throws Exception {
				return Tuple2.of(e1.f0, e1.f1 + e2.f1);
			}
		})
		.print();
		// print is the sink, we expect to see pairs of (task ID, key, 1000)
		// task ID corresponds to number of cores of the computer
		/*
		input.keyBy(event -> event.getField(1)).process(
			
		)*/
		// execution
		env.execute("Test");
	}
}
// building jar: <mainClass>
// flink-conf.yaml change
// masters and workers contain the addresses of job managers and task managers
// add localhost to conf/workers
// parallelism <= the available number of clusters

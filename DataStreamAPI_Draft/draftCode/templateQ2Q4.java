package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

// a useful link about windows: https://programming.vip/docs/learn-more-about-tumbling-window-in-flink.html
// multiple windows: https://developpaper.com/multi-window-analysis-of-flink/

public class templateQ2Q4 {
    // a rise in an attribute within a period of time
    protected static Double riseRatio = 0.2;
    protected static int risePeriod = 10;
    protected static Double valueTemp = -1.0;  // keep track on the rise of a value
    protected static long timeTemp = 0;  // keep track on the corresponding timestamp
    // maintain N = 3 consecutive averages
    protected static ArrayList<Double> avgTracker = new ArrayList<>();
    protected static ArrayList<Window> windowTracker = new ArrayList<>();
    protected static int avgTrackSize = 3;
    // default tumbling window size, i.e., number of tuples per window
    protected static final int windowSize = 5;
    public static void main(String[] args) throws Exception {
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(500);
        // define the source
        DataStream<Tuple2<Integer, Double>> input = env.addSource(new templateTest.testSourceFunc(5))
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
        env.execute("Test Q2Q4 Exe");
    }
    // ProcessWindowFunction attributes: input event, output event collector, key, the window
    public static class processFunc extends ProcessWindowFunction<Tuple2<Integer, Double>, String, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context ctx, Iterable<Tuple2<Integer, Double>> input, Collector<String> out) {
            long count = 0;  // get the quantity of data items
            Double sumVal = 0.0;  // get the sum of the data
            long countTime = ctx.window().getStart();
            for (Tuple2<Integer, Double> ipt: input) {
                // when there is a decrease, we just move on
                if (ipt.f1 <= valueTemp || valueTemp < 0) {
                    timeTemp = countTime;
                    valueTemp = ipt.f1;
                }
                // a certain period of time passed, but there is still no significant increase
                else if (countTime - timeTemp > risePeriod && !isRisePercent(valueTemp, ipt.f1, riseRatio)) {
                    timeTemp = countTime;
                    valueTemp = ipt.f1;
                }
                // the attribute increased by an above-threshold ratio within a period of time
                else if (countTime - timeTemp <= risePeriod && isRisePercent(valueTemp, ipt.f1, riseRatio)) {
                    out.collect("WARNING: " + (timeTemp + 1) + ": " + valueTemp + " new: " + (countTime + 1) + ": "
                                + ipt.f1 + " " + templateTest.warnBar);
                    timeTemp = countTime;
                    valueTemp = ipt.f1;
                }
                sumVal += ipt.f1;
                countTime += 1;
                out.collect("Window: " + ctx.window() + " No. " + (count + 1) +
                            " ID: " + ipt.f0 + " Value: " + ipt.f1);
                count++;
            }
            Double avgVal = sumVal/((double) count);  // calculate the avg
            out.collect("Window: " + ctx.window() + " count: " + count + " avg: " + avgVal
                        + " " + templateTest.windowBar);
            // maintain 3 consecutive averages
            updateTrack(avgVal, ctx.window());
            out.collect("Avgs: " + getAvgStr() + templateTest.avgBar);
        }
    }
    // update the tracking of N = 3 consecutive averages
    public static void updateTrack(Double newAvg, Window newWindow) {
        if (avgTracker.size() == avgTrackSize) {  // the tracker list is full
            for (int i = 0; i < avgTracker.size() - 1; i++) {
                avgTracker.set(i, avgTracker.get(i + 1));
                windowTracker.set(i, windowTracker.get(i + 1));
            }
            avgTracker.set(avgTracker.size() - 1, newAvg);
            windowTracker.set(avgTracker.size() - 1, newWindow);
        }
        else {
            avgTracker.add(newAvg);
            windowTracker.add(newWindow);
        }
    }
    // get the N = 3 averages in string form
    public static String getAvgStr() {
        String avgStr = "";
        for (int i = 0; i < avgTracker.size(); i++) {
            avgStr += windowTracker.get(i) + " " + avgTracker.get(i) + " ";
        }
        return avgStr;
    }
    // check whether a value increased by a certain percentage
    public static boolean isRisePercent(Double oldVal, Double newVal, Double ratio) {
        if (newVal <= oldVal) {return false;}
        if ((newVal - oldVal)/oldVal >= ratio) {return true;}
        return false;
    }
}

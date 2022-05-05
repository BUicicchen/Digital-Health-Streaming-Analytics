package DataStreamAPI;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;
// the helper class for other templates
public class SourceGenerator {
    // the printed bars for visual clearance
    protected static final String windowBar = "========================================";
    protected static final String avgBar =    "----------------------------------------";
    protected static final String warnBar =   "########################################";
    public static class Smartwatch implements ParallelSourceFunction<Tuple3<Integer, Integer, Float>> {
        // test event is of schema: <ID, test_data>, and a timestamp
        protected Integer deviceID;
        // random number generator
        protected static Random randGen = new Random();
        // the starting timestamp
        protected static long timestamp;
        public Smartwatch() {
            deviceID = 1;
            timestamp = 0;
        }
        public Smartwatch(Integer deviceID) {
            this.deviceID = deviceID;
            timestamp = 0;
        }
        public Smartwatch(Integer deviceID, long timestamp) {
            this.deviceID = deviceID;
            this.timestamp = timestamp;
        }
        @Override  // consider using array instead of TupleN (where N is an integer)
        public void run(SourceContext<Tuple3<Integer, Integer, Float>> ctx) throws Exception {
            // apply the distribution & generate timestamp
            while (true) {
                long activity_class = (timestamp/10)%3;
                int steps;
                double mean_heart;
                if (activity_class == 0){
                    steps = 0;
                    mean_heart = randGen.nextDouble()*10+60;
                } else if (activity_class == 1){
                    steps = randGen.nextInt(100);
                    mean_heart = randGen.nextDouble()*20 + 70;
                } else {
                    steps = randGen.nextInt(800)+200;
                    mean_heart = randGen.nextDouble()*30 + 90;
                }
                Tuple3<Integer, Integer, Float> event = Tuple3.of(deviceID, steps, mean_heart);
                ctx.collectWithTimestamp(event, timestamp);
                timestamp++;  // increment the timestamp
                // the sleep function can slow down the stream
                TimeUnit.MILLISECONDS.sleep(200);
            }
        }
        @Override  // the cancel function is not needed
        public void cancel() {}
    }
    // generate testing string data, used for the example here:
    // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/process_function/
    public static class BloodPressureMonitor implements ParallelSourceFunction<Tuple3<Integer, Integer, Integer>> {
        // test event is of schema: <ID, test_data>, and a timestamp
        protected Integer deviceID;
        // random number generator
        protected static Random randGen = new Random();
        // the starting timestamp
        protected static long timestamp;
        public BloodPressureMonitor() {
            deviceID = 1;
            timestamp = 0;
        }
        public BloodPressureMonitor(Integer deviceID) {
            this.deviceID = deviceID;
            timestamp = 0;
        }
        public BloodPressureMonitor(Integer deviceID, long timestamp) {
            this.deviceID = deviceID;
            this.timestamp = timestamp;
        }
        @Override  // consider using array instead of TupleN (where N is an integer)
        public void run(SourceContext<Tuple3<Integer, Integer, Integer>> ctx) throws Exception {
            // apply the distribution & generate timestamp

            while (true) {
                long activity_class = (timestamp/10)%3;
                int dbp, sbp;
                if (activity_class == 0){
                    dbp = randGen.nextInt(20) + 45;
                    sbp = randGen.nextInt(30) + 75;
                } else if (activity_class == 1){
                    dbp = randGen.nextInt(20) + 60;
                    sbp = randGen.nextInt(30) + 90;
                } else {
                    dbp = randGen.nextInt(20) + 60;
                    sbp = randGen.nextInt(60) + 160;
                }
                Tuple3<Integer, Integer, Integer> event = Tuple3.of(deviceID, dbp, sbp);
                ctx.collectWithTimestamp(event, timestamp);
                timestamp++;  // increment the timestamp

                // the sleep function can slow down the stream
                TimeUnit.MILLISECONDS.sleep(200);
            }
        }
        @Override  // the cancel function is not needed
        public void cancel() {}
    }

    public static class GlucoseMonitor implements ParallelSourceFunction<Tuple2<Integer, Integer>> {
        // test event is of schema: <ID, test_data>, and a timestamp
        protected Integer deviceID;
        // random number generator
        protected static Random randGen = new Random();
        // the starting timestamp
        protected static long timestamp;
        public GlucoseMonitor() {
            deviceID = 1;
            timestamp = 0;
        }
        public GlucoseMonitor(Integer deviceID) {
            this.deviceID = deviceID;
            timestamp = 0;
        }
        public GlucoseMonitor(Integer deviceID, long timestamp) {
            this.deviceID = deviceID;
            this.timestamp = timestamp;
        }
        @Override  // consider using array instead of TupleN (where N is an integer)
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            // apply the distribution & generate timestamp

            while (true) {
                int glucose = randGen.nextInt(140);
                Tuple2<Integer, Integer> event = Tuple2.of(deviceID, glucose);
                ctx.collectWithTimestamp(event, timestamp);
                timestamp++;  // increment the timestamp

                // the sleep function can slow down the stream
                TimeUnit.MILLISECONDS.sleep(200);
            }
        }
        @Override  // the cancel function is not needed
        public void cancel() {}
    }

    public static class FitBit implements ParallelSourceFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        // test event is of schema: <ID, test_data>, and a timestamp
        protected Integer deviceID;
        // random number generator
        protected static Random randGen = new Random();
        // the starting timestamp
        protected static long timestamp;
        public FitBit() {
            deviceID = 1;
            timestamp = 0;
        }
        public FitBit(Integer deviceID) {
            this.deviceID = deviceID;
            timestamp = 0;
        }
        public FitBit(Integer deviceID, long timestamp) {
            this.deviceID = deviceID;
            this.timestamp = timestamp;
        }
        @Override  // consider using array instead of TupleN (where N is an integer)
        public void run(SourceContext<Tuple5<Integer, Integer, Integer, Integer, Integer>> ctx) throws Exception {
            // apply the distribution & generate timestamp

            while (true) {
                long activity_class = (timestamp/10)%3;
                int steps, heart_rate;
                if (activity_class == 0){
                    steps = 0;
                    heart_rate = randGen.nextInt(10)+60;
                } else if (activity_class == 1){
                    steps = randGen.nextInt(100);
                    heart_rate = randGen.nextInt(20) + 70;
                } else {
                    steps = randGen.nextInt(800)+200;
                    heart_rate = randGen.nextInt(30) + 90;
                }
                Tuple5<Integer, Integer, Integer, Integer, Integer> event = Tuple5.of(deviceID, activity_class, activity_class, steps, heart_rate);
                ctx.collectWithTimestamp(event, timestamp);
                timestamp++;  // increment the timestamp

                // the sleep function can slow down the stream
                TimeUnit.MILLISECONDS.sleep(200);
            }
        }
        @Override  // the cancel function is not needed
        public void cancel() {}
    }

    public static class InsulinTracker implements ParallelSourceFunction<Tuple2<Integer, Integer>> {
        // test event is of schema: <ID, test_data>, and a timestamp
        protected Integer deviceID;
        // random number generator
        protected static Random randGen = new Random();
        // the starting timestamp
        protected static long timestamp;
        public InsulinTracker() {
            deviceID = 1;
            timestamp = 0;
        }
        public InsulinTracker(Integer deviceID) {
            this.deviceID = deviceID;
            timestamp = 0;
        }
        public InsulinTracker(Integer deviceID, long timestamp) {
            this.deviceID = deviceID;
            this.timestamp = timestamp;
        }
        @Override  // consider using array instead of TupleN (where N is an integer)
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            // apply the distribution & generate timestamp

            while (true) {
                int insulin = randGen.nextInt(20);
                Tuple2<Integer, Integer> event = Tuple2.of(deviceID, insulin);
                ctx.collectWithTimestamp(event, timestamp);
                timestamp++;  // increment the timestamp

                // the sleep function can slow down the stream
                TimeUnit.MILLISECONDS.sleep(200);
            }
        }
        @Override  // the cancel function is not needed
        public void cancel() {}
    }
}

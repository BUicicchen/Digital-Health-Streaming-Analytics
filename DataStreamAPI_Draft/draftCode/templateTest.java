package clusterdata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
// the helper class for all the other test templates
public class templateTest {
    // the printed bars for visual clearance
    protected static final String windowBar = "========================================";
    protected static final String avgBar =    "----------------------------------------";
    protected static final String warnBar =   "########################################";
    public static class testSourceFunc implements ParallelSourceFunction<Tuple2<Integer, Double>> {
        // test event is of schema: <ID, test_data>, and a timestamp
        protected Integer deviceID;
        // random number generator
        protected static Random randGen = new Random();
        // the starting timestamp
        protected static long timestampStart;
        // default timestamp incrementation range
        protected static final long defaultTSRange = 1;
        // default starting timestamp
        protected static final long defaultTSStart = 0;
        // the incrementation of timestamp
        protected long timestampRange;
        // whether the incrementation of timestamp will fluctuate
        // if this is true, then each time a new tuple is generated,
        // the increase in timestamp will be a random number in the range of [1, timestampRange]
        protected boolean isFluctuate;
        // if the data stream will be slowed down, we need to have its sleep time
        // a sleep time of 200 is a reasonable slow-down
        protected int sleepTime;
        // since the data is random double between 0 and 1, for testing multiple streams,
        // we need to scale up the number for the sake of distinguishing them
        protected double addedGen;
        public testSourceFunc() {
            deviceID = 1;
            timestampStart = defaultTSStart;
            timestampRange = defaultTSRange;
            isFluctuate = false;
            sleepTime = 0;
            addedGen = 0.0;
        }
        public testSourceFunc(boolean isSleep, int sleepTime, double addedGen) {
            deviceID = 1;
            timestampStart = defaultTSStart;
            timestampRange = defaultTSRange;
            isFluctuate = false;
            if (isSleep) {this.sleepTime = sleepTime;}
            this.addedGen = addedGen;
        }
        public testSourceFunc(Integer deviceID) {
            this.deviceID = deviceID;
            timestampStart = defaultTSStart;
            timestampRange = defaultTSRange;
            isFluctuate = false;
            sleepTime = 0;
            addedGen = 0.0;
        }
        public testSourceFunc(Integer deviceID, long newStartTimestamp) {
            this.deviceID = deviceID;
            timestampStart = newStartTimestamp;
            timestampRange = defaultTSRange;
            isFluctuate = false;
            sleepTime = 0;
            addedGen = 0.0;
        }
        public testSourceFunc(Integer deviceID, long newTimestamp, long timestampRange) {
            this.deviceID = deviceID;
            timestampStart = newTimestamp;
            this.timestampRange = timestampRange;
            this.isFluctuate = false;
            sleepTime = 0;
            addedGen = 0.0;
        }
        public testSourceFunc(Integer deviceID, long newTimestamp, long timestampRange, boolean isFluctuate) {
            this.deviceID = deviceID;
            timestampStart = newTimestamp;
            this.timestampRange = timestampRange;
            this.isFluctuate = isFluctuate;
            sleepTime = 0;
            addedGen = 0.0;
        }
        @Override  // consider using array instead of TupleN (where N is an integer)
        public void run(SourceContext<Tuple2<Integer, Double>> ctx) throws Exception {
            // apply the distribution & generate timestamp
            long timestamp = timestampStart;
            while (true) {
                // get a random double as testing data
                double randData = randGen.nextDouble() + addedGen;
                Tuple2<Integer, Double> event = Tuple2.of(deviceID, randData);
                ctx.collectWithTimestamp(event, timestamp);
                long timestampIncr = timestampRange;
                if (isFluctuate && (defaultTSRange < timestampRange)) {
                    timestampIncr = ThreadLocalRandom.current().nextLong(defaultTSRange, timestampRange + 1);
                }
                timestamp += timestampIncr;  // increment the timestamp
                // the sleep function can slow down the stream
                if (sleepTime > 0) {TimeUnit.MILLISECONDS.sleep(sleepTime);}
            }
        }
        @Override  // the cancel function is not needed
        public void cancel() {}
    }
    // generate testing string data, used for the example here:
    // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/process_function/
    public static class testSourceFunc2 implements ParallelSourceFunction<Tuple2<String, String>> {
        // random number generator
        protected static Random randGen = new Random();
        // the starting timestamp
        protected static long timestamp;
        @Override  // consider using array instead of TupleN (where N is an integer)
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            // apply the distribution & generate timestamp
            while (true) {
                // get a random double as testing data
                double randData = randGen.nextDouble();
                Tuple2<String, String> event = Tuple2.of("A", String.valueOf(randData));
                ctx.collectWithTimestamp(event, timestamp);
                timestamp++;  // increment the timestamp
            }
        }
        @Override  // the cancel function is not needed
        public void cancel() {}
    }
    // test source for generating out-of-order data w.r.t timestamp
    public static class testSrcOutOrder implements ParallelSourceFunction<Tuple2<Integer, Double>> {
        // random number generator
        protected static Random randGen = new Random();
        protected final long randSize;
        public testSrcOutOrder() {randSize = 10;}
        public testSrcOutOrder(long randSize) {this.randSize = randSize;}
        @Override  // consider using array instead of TupleN (where N is an integer)
        public void run(SourceContext<Tuple2<Integer, Double>> ctx) throws Exception {
            // apply the distribution & generate timestamp
            long timestamp = 0;
            while (true) {
                // every 10 tuples, there will be out-of-order pattern introduced
                ArrayList<Long> numList = new ArrayList<>();
                for (long i = timestamp; i < timestamp + randSize; i++) {numList.add(i);}
                Collections.shuffle(numList);
                for (Long ts : numList) {
                    // get a random double as testing data
                    double randData = randGen.nextDouble();
                    Tuple2<Integer, Double> event = Tuple2.of(1, randData);
                    ctx.collectWithTimestamp(event, ts);
                    System.out.println("gen " + ts + " " + event);
                    // the sleep function can slow down the stream
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                timestamp += randSize;
            }
        }
        @Override  // the cancel function is not needed
        public void cancel() {}
    }
}

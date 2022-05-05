package clusterdata;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DataGenerator<T> implements ParallelSourceFunction<T> {
    protected static final Map<String, Class<?>> deviceTypes = new HashMap<String, Class<?>>() {{
        put("BloodPressure", TupBloodPressure.class);
        put("Fitbit", TupFitbit.class);
        put("Glucose", TupGlucose.class);
        put("Insulin", TupInsulin.class);
        put("Smartwatch", TupSmartwatch.class);
    }};
    protected Integer patientID;
    protected Integer deviceID;
    protected String deviceType;
    // random number generator
    protected Random randGen = new Random();
    // the starting timestamp
    protected long timestampStart;
    // the incrementation of timestamp
    protected long timestampRange;
    // whether the incrementation of timestamp will fluctuate
    // if this is true, then each time a new tuple is generated,
    // the increase in timestamp will be a random number in the range of [1, timestampRange]
    protected boolean isFluctuate;
    // if the data stream will be slowed down, we need to have its sleep time
    // a sleep time of 200 is a reasonable slow-down
    protected int sleepTime;
    public DataGenerator() {
        patientID = null;
        deviceID = null;
        deviceType = null;
        timestampStart = 0;
        timestampRange = 1;
        isFluctuate = false;
        sleepTime = 0;
    }
    public DataGenerator(Integer patientID, Integer deviceID, String deviceType, boolean isFluctuate, int sleepTime) {
        this.patientID = patientID;
        this.deviceID = deviceID;
        this.deviceType = deviceType;
        timestampStart = 0;
        timestampRange = 1;
        this.isFluctuate = isFluctuate;
        this.sleepTime = sleepTime;
    }
    @Override  // consider using array instead of TupleN (where N is an integer)
    public void run(SourceContext<T> ctx) throws Exception {
        if (!deviceTypes.containsKey(deviceType)) {return;}
        // apply the distribution & generate timestamp
        long timestamp = timestampStart;
        while (true) {
            TupDevice event = null;
            switch (deviceType) {
                case "BloodPressure":
                    Integer dBP = randGen.nextInt(100);  // range: [0, 100)
                    Integer sBP = randGen.nextInt(100);
                    event = new TupBloodPressure(deviceID, patientID, timestamp, dBP, sBP);
                    break;
                case "Fitbit":
                    Integer METs = randGen.nextInt(100);
                    Integer intensity = randGen.nextInt(100);
                    Integer steps = randGen.nextInt(100);
                    Integer heartRate = randGen.nextInt(100);
                    event = new TupFitbit(deviceID, patientID, timestamp, METs, intensity, steps, heartRate);
                    break;
                case "Glucose":
                    Integer glucose = randGen.nextInt(100);
                    event = new TupGlucose(deviceID, patientID, timestamp, glucose);
                    break;
                case "Insulin":
                    Integer doseAmount = randGen.nextInt(100);
                    event = new TupInsulin(deviceID, patientID, timestamp, doseAmount);
                    break;
                case "Smartwatch":
                    Integer stepsSinceLast = randGen.nextInt(100);
                    Float meanHeartRate = randGen.nextFloat();
                    event = new TupSmartwatch(deviceID, patientID, timestamp, stepsSinceLast, meanHeartRate);
                    break;
            }
            // collect the generated fata
            // noinspection unchecked
            T eventConv = (T) event;
            ctx.collectWithTimestamp(eventConv, timestamp);
            // increment the timestamp
            if (isFluctuate && (1 < timestampRange)) {
                timestamp += ThreadLocalRandom.current().nextLong(1, timestampRange + 1);
            }
            else {timestamp += timestampRange;}
            // the sleep function can slow down the stream
            if (sleepTime > 0) {TimeUnit.MILLISECONDS.sleep(sleepTime);}
        }
    }
    @Override  // the cancel function is N/A, but still required
    public void cancel() {}
}
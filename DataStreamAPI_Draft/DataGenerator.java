package clusterdata;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

public class DataGenerator<T> implements ParallelSourceFunction<T> {
    protected static final Map<String, Class<?>> deviceTypes = new HashMap<String, Class<?>>() {{
        put(TupBloodPressure.deviceName, TupBloodPressure.class);
        put(TupFitbit.deviceName, TupFitbit.class);
        put(TupGlucose.deviceName, TupGlucose.class);
        put(TupInsulin.deviceName, TupInsulin.class);
        put(TupSmartwatch.deviceName, TupSmartwatch.class);
    }};
    protected static final String distNameUniform = "uniform";
    protected static final String distNameExp = "exponential";
    protected Integer numPatient;
    protected String deviceType;
    // use exponential or uniform for distribution type
    protected String distributionType;
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
    public int randSize;
    public DataGenerator(Integer numPatient, String deviceType, boolean isFluctuate, int sleepTime, String distributionType) {
        this.numPatient = numPatient;
        this.deviceType = deviceType;
        timestampStart = 0;
        timestampRange = 1;
        this.isFluctuate = isFluctuate;
        this.sleepTime = sleepTime;
        this.distributionType = distributionType;
        randSize = 0;
    }
    public DataGenerator(Integer numPatient, String deviceType, boolean isFluctuate, int sleepTime,
                         String distributionType, int randSize) {
        this.numPatient = numPatient;
        this.deviceType = deviceType;
        timestampStart = 0;
        timestampRange = 1;
        this.isFluctuate = isFluctuate;
        this.sleepTime = sleepTime;
        this.distributionType = distributionType;
        this.randSize = randSize;
    }
    // obtain the exponentially distributed probability of a value given lambda & lower-bound
    public Double expDistributionPr(Double lambda, Double x, Double lowerBound, Double yShift) {
        if (lambda > 1D || lambda < 0D || lowerBound < 0D || x < lowerBound || Math.abs(yShift) > 1D) {return null;}
        return lambda * Math.pow(Math.exp(1), -1D * lambda * (x - lowerBound)) + yShift;
    }
    // choose a number with exponential distribution with minBound and maxBound
    public Integer chooseExpDis(Double lambda, Integer minBound, Integer maxBound, Double yShift){
        Integer result = -1;
        int numBound = maxBound - minBound;
        ArrayList<Integer> numbers = new ArrayList<>(numBound);
        for (Integer i = minBound; i < maxBound; i++){
            numbers.add(i);
        }
        Collections.shuffle((numbers));
        Integer current;
        Double probability, dice;
        for (int i = 0; i < numBound; i++){
            current = numbers.get(i);
            probability = expDistributionPr(lambda, (double) current, (double) minBound, yShift);
            dice = randGen.nextDouble();
            if (probability >= dice){
                result = current;
            }
        }
        if (result < 0){
            result = numbers.get(0);
        }
        return result;
    }
    @Override  // consider using array instead of TupleN (where N is an integer)
    public void run(SourceContext<T> ctx) throws Exception {
        ArrayList<T> shuffledOutput = new ArrayList<>();
        if (!deviceTypes.containsKey(deviceType)) {return;}
        // apply the distribution & generate timestamp
        long timestamp = timestampStart;
        while (true) {
            for (int patientID = 0; patientID < numPatient; patientID++){
                TupDevice event = null;
                Integer dBP = 0, sBP = 0, METs = 0, intensity = 0, steps = 0, heartRate = 0, glucose = 0, doseAmount = 0;
                float meanHeartRate = 0.0F;
                long activity_class = (timestamp / 10) % 4;
                /*
                divide timestamp into 4 sections: sleeping, walking, eating and running
                sleeping: dbp 45~65, sbp 75~105, heartrate 60~70, step 0, glucose 100~140
                walking: dbp 55~80, sbp 85~120, heartrate 70~90, step 50~200, glucose 70~110
                eating: dbp 55~80, sbp 105~140, heartrate 90~110, step 0, glucose 140~180
                running: dbp 55~80, sbp 185~220, heartrate 110~130, step 850~1000, glucose 100~140, insulin 1~25
                */
                if (distributionType.equals(distNameExp)) {
                    if (activity_class == 0) {
                        dBP = chooseExpDis(0.5, 45, 65, 0.0);
                        sBP = chooseExpDis(0.5, 75, 105, 0.0);
                        heartRate = chooseExpDis(0.5, 50, 70, 0.0);
                        meanHeartRate = (float) (chooseExpDis(0.5, 50, 70, 0.0) +
                                                randGen.nextDouble());
                        //steps = 0;  // already assigned
                        glucose = chooseExpDis(0.5, 100, 140, 0.0);
                        //doseAmount = 0;  // already assigned
                        METs = 1;
                        intensity = 1;
                    }
                    else if (activity_class == 1) {
                        dBP = chooseExpDis(0.5, 55, 80, 0.0);
                        sBP = chooseExpDis(0.5, 85, 120, 0.0);
                        heartRate = chooseExpDis(0.5, 70, 90, 0.0);
                        meanHeartRate = (float) (chooseExpDis(0.5, 70, 90, 0.0) +
                                                randGen.nextDouble());
                        steps = chooseExpDis(0.5, 50, 200, 0.0);
                        glucose = chooseExpDis(0.5, 70, 110, 0.0);
                        //doseAmount = 0;  // already assigned
                        METs = 2;
                        intensity = 2;
                    }
                    else if (activity_class == 2) {
                        dBP = chooseExpDis(0.5, 55, 80, 0.0);
                        sBP = chooseExpDis(0.5, 105, 140, 0.0);
                        heartRate = chooseExpDis(0.5, 90, 110, 0.0);
                        meanHeartRate = (float) (chooseExpDis(0.5, 90, 110, 0.0) +
                                                randGen.nextDouble());
                        //steps = 0;  // already assigned
                        glucose = chooseExpDis(0.5, 140, 180, 0.0);
                        //doseAmount = 0;  // already assigned
                        METs = 1;
                        intensity = 1;
                    }
                    else if (activity_class == 3) {
                        dBP = chooseExpDis(0.5, 55, 80, 0.0);
                        sBP = chooseExpDis(0.5, 185, 220, 0.0);
                        heartRate = chooseExpDis(0.5, 110, 130, 0.0);
                        meanHeartRate = (float) (chooseExpDis(0.5, 110, 130, 0.0) +
                                                randGen.nextDouble());
                        steps = chooseExpDis(0.5, 850, 1000, 0.0);
                        glucose = chooseExpDis(0.5, 100, 140, 0.0);
                        doseAmount = chooseExpDis(0.5, 1, 25, 0.0);
                        METs = 3;
                        intensity = 3;
                    }
                }
                else if (distributionType.equals(distNameUniform)) {
                    if (activity_class == 0) {
                        dBP = randGen.nextInt(20) + 45;
                        sBP = randGen.nextInt(30) + 75;
                        heartRate = randGen.nextInt(20) + 50;
                        meanHeartRate = (float) (randGen.nextDouble() * 20 + 50);
                        //steps = 0;  // already assigned
                        glucose = randGen.nextInt(40) + 100;
                        //doseAmount = 0;  // already assigned
                        METs = 1;
                        intensity = 1;
                    }
                    else if (activity_class == 1) {
                        dBP = randGen.nextInt(25) + 55;
                        sBP = randGen.nextInt(35) + 85;
                        heartRate = randGen.nextInt(20) + 70;
                        meanHeartRate = (float) (randGen.nextDouble() * 20 + 70);
                        steps = randGen.nextInt(150) + 50;
                        glucose = randGen.nextInt(40) + 70;
                        //doseAmount = 0;  // already assigned
                        METs = 2;
                        intensity = 2;
                    }
                    else if (activity_class == 2) {
                        dBP = randGen.nextInt(25) + 55;
                        sBP = randGen.nextInt(35) + 105;
                        heartRate = randGen.nextInt(20) + 90;
                        meanHeartRate = (float) (randGen.nextDouble() * 20 + 90);
                        //steps = 0;  // already assigned
                        glucose = randGen.nextInt(180) + 140;
                        //doseAmount = 0;  // already assigned
                        METs = 1;
                        intensity = 1;
                    }
                    else if (activity_class == 3) {
                        dBP = randGen.nextInt(25) + 55;
                        sBP = randGen.nextInt(35) + 185;
                        heartRate = randGen.nextInt(20) + 110;
                        meanHeartRate = (float) (randGen.nextDouble() * 20 + 110);
                        steps = randGen.nextInt(150) + 850;
                        glucose = randGen.nextInt(40) + 100;
                        doseAmount = randGen.nextInt(25);
                        METs = 3;
                        intensity = 3;
                    }
                }
                else {return;}
                // Every 85 in timestamp, there is an anomaly. It will be in the sleeping period.
                // Heart rate will drop to 40~50 and glucose level will drop to 50~70.
                if (timestamp / 85 == 1) {
                    heartRate = randGen.nextInt(10) + 40;
                    meanHeartRate = (float) (randGen.nextDouble() * 10 + 40);
                    glucose = randGen.nextInt(20) + 50;
                }
                int deviceID;
                switch (deviceType) {
                    case TupBloodPressure.deviceName:
                        deviceID = patientID * 5;
                        event = new TupBloodPressure(deviceID, patientID, timestamp, dBP, sBP);
                        break;
                    case TupFitbit.deviceName:
                        deviceID = 1 + patientID * 5;
                        event = new TupFitbit(deviceID, patientID, timestamp, METs, intensity, steps, heartRate);
                        break;
                    case TupGlucose.deviceName:
                        deviceID = 2 + patientID * 5;
                        event = new TupGlucose(deviceID, patientID, timestamp, glucose);
                        break;
                    case TupInsulin.deviceName:
                        deviceID = 3 + patientID * 5;
                        event = new TupInsulin(deviceID, patientID, timestamp, doseAmount);
                        break;
                    case TupSmartwatch.deviceName:
                        deviceID = 4 + patientID * 5;
                        event = new TupSmartwatch(deviceID, patientID, timestamp, steps, meanHeartRate);
                        break;
                }
                // collect the generated fata
                // noinspection unchecked
                T eventConv = (T) event;
                if (randSize <= 1) {
                    // collect the generated data
                    ctx.collectWithTimestamp(eventConv, timestamp);
                }
                else {
                    // for every randSize = 10 tuples, mess up the timestamp order and output them
                    if (shuffledOutput.size() == randSize) {
                        Collections.shuffle(shuffledOutput);
                        for (T elem : shuffledOutput) {
                            Long timeTemp = -1L;
                            if (elem instanceof TupDevice) {
                                timeTemp = ((TupDevice) elem).timestamp;
                            }
                            ctx.collectWithTimestamp(elem, timeTemp);
                        }
                        shuffledOutput = new ArrayList<>();
                    }
                    shuffledOutput.add(eventConv);
                }
                // increment the timestamp
                if (isFluctuate && (1 < timestampRange)) {
                    timestamp += ThreadLocalRandom.current().nextLong(1, timestampRange + 1);
                }
                else {
                    timestamp += timestampRange;
                }
                // the sleep function can slow down the stream
                if (sleepTime > 0) {
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                }
            }
        }
    }
    @Override  // the cancel function is N/A, but still required
    public void cancel() {}
}

package clusterdata;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
    protected static final String distNameRandom = "random";
    protected static final String sessionNameSleep = "sleeping";
    protected static final String sessionNameWalk = "walking";
    protected static final String sessionNameRun = "running";
    protected static final String sessionNameEat = "eating";
    protected static final String sessionNameActive = "ACTIVE";
    protected static final String sessionNameInactive = "INACTIVE";
    // the session name for those who are not qualified to any session
    protected static final String sessionNameNA = "N/A";
    // data config
    /*
    divide timestamp into 4 sections: sleeping, walking, eating and running
    sleeping: dbp 45~65, sbp 75~105, heart rate 50~70, step 0, glucose 100~140, insulin 0
    walking: dbp 55~80, sbp 85~120, heart rate 70~90, step 50~200, glucose 70~110, insulin 0
    eating: dbp 55~80, sbp 105~140, heart rate 90~110, step 0, glucose 140~180, insulin 0
    running: dbp 55~80, sbp 185~220, heart rate 110~130, step 850~1000, glucose 100~140, insulin 1~25
    */
    // sleeping
    protected static final Integer dbpSleepLow = 45; protected static final Integer dbpSleepHigh = 65;
    protected static final Integer sbpSleepLow = 75; protected static final Integer sbpSleepHigh = 105;
    protected static final Integer heartRateSleepLow = 50; protected static final Integer heartRateSleepHigh = 70;
    protected static final Integer stepSleep = 0;
    protected static final Integer glucoseSleepLow = 100; protected static final Integer glucoseSleepHigh = 140;
    protected static final Integer insulinSleep = 0;
    protected static final Integer METsSleep = 1; protected static final Integer intensitySleep = 1;
    protected static final Integer stepLastSleep = 0;
    // walking
    protected static final Integer dbpWalkLow = 55; protected static final Integer dbpWalkHigh = 80;
    protected static final Integer sbpWalkLow = 85; protected static final Integer sbpWalkHigh = 120;
    protected static final Integer heartRateWalkLow = 70; protected static final Integer heartRateWalkHigh = 90;
    protected static final Integer stepWalkLow = 50; protected static final Integer stepWalkHigh = 200;
    protected static final Integer glucoseWalkLow = 70; protected static final Integer glucoseWalkHigh = 110;
    protected static final Integer insulinWalk = 0;
    protected static final Integer METsWalk = 2; protected static final Integer intensityWalk = 2;
    protected static final Integer stepLastWalkLow = 50; protected static final Integer stepLastWalkHigh = 190;
    // eating
    protected static final Integer dbpEatLow = 55; protected static final Integer dbpEatHigh = 80;
    protected static final Integer sbpEatLow = 105; protected static final Integer sbpEatHigh = 140;
    protected static final Integer heartRateEatLow = 90; protected static final Integer heartRateEatHigh = 110;
    protected static final Integer stepEat = 0;
    protected static final Integer glucoseEatLow = 140; protected static final Integer glucoseEatHigh = 180;
    protected static final Integer insulinEat = 0;
    protected static final Integer METsEat = 1; protected static final Integer intensityEat = 1;
    protected static final Integer stepLastEat = 0;
    // running
    protected static final Integer dbpRunLow = 55; protected static final Integer dbpRunHigh = 80;
    protected static final Integer sbpRunLow = 185; protected static final Integer sbpRunHigh = 220;
    protected static final Integer heartRateRunLow = 110; protected static final Integer heartRateRunHigh = 130;
    protected static final Integer stepRunLow = 850; protected static final Integer stepRunHigh = 1000;
    protected static final Integer glucoseRunLow = 100; protected static final Integer glucoseRunHigh = 140;
    protected static final Integer insulinRunLow = 1; protected static final Integer insulinRunHigh = 25;
    protected static final Integer METsRun = 3; protected static final Integer intensityRun = 3;
    protected static final Integer stepLastRunLow = 850; protected static final Integer stepLastRunHigh = 990;
    // N/A
    protected static final Integer dbpNALow = 55; protected static final Integer dbpNAHigh = 70;
    protected static final Integer sbpNALow = 185; protected static final Integer sbpNAHigh = 210;
    protected static final Integer heartRateNALow = 110; protected static final Integer heartRateNAHigh = 120;
    protected static final Integer stepNALow = 850; protected static final Integer stepNAHigh = 990;
    protected static final Integer glucoseNALow = 140; protected static final Integer glucoseNAHigh = 200;
    protected static final Integer insulinNALow = 1; protected static final Integer insulinNAHigh = 20;
    protected static final Integer stepLastNALow = 850; protected static final Integer stepLastNAHigh = 980;
    // anomaly
    protected static final Integer heartRateAnomalyLow = 40; protected static final Integer heartRateAnomalyHigh = 50;
    protected static final Integer glucoseAnomalyLow = 50; protected static final Integer glucoseAnomalyHigh = 70;
    // config for exponential distribution
    protected static final Double expLambda = 0.5;
    protected static final Double expYShift = 0.0;
    // the period in which anomaly occurs by talking the modulo
    protected long anomalyPeriod = 85;
    // when timestamp % anomalyPeriod <= anomalyRemainder, the anomaly should occur
    protected long anomalyRemainder = 3;
    // average period of an activity
    protected long avgActPeriod = 10L;
    protected Integer patientNum;
    protected String deviceType;
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
    // the level of our-of-order-ness
    protected int randSize;
    // the limit of the quantity of tuples generated, "null" means no limit
    protected Long totalLimit;
    // whether to save the data to file
    protected boolean isSaveFile;
    public DataGenerator(Integer patientNum, String deviceType, String distributionType,
                         long timestampStart, long timestampRange, boolean isFluctuate,
                         int sleepTime, int randSize, Long totalLimit, boolean isSaveFile) {
        this.patientNum = patientNum;
        this.deviceType = deviceType;
        this.distributionType = distributionType;
        this.timestampStart = timestampStart;
        this.timestampRange = timestampRange;
        this.isFluctuate = isFluctuate;
        this.sleepTime = sleepTime;
        this.randSize = randSize;
        this.totalLimit = totalLimit;
        this.isSaveFile = isSaveFile;
    }
    // whether the number of data items generated reaches its limit
    public static boolean reachLimit(long count, Long lim) {
        if (lim == null) {return true;}
        else {return count <= lim;}
    }
    // obtain the exponentially distributed probability of a value given lambda & lower-bound
    public static Double expDistributionPr(Double lambda, Double x, Double lowerBound, Double yShift) {
        if (lambda > 1D || lambda < 0D || lowerBound < 0D || x < lowerBound || Math.abs(yShift) > 1D) {return null;}
        return lambda * Math.pow(Math.exp(1), -1D * lambda * (x - lowerBound)) + yShift;
    }
    // choose an integer with exponential distribution with minBound and maxBound
    public Integer chooseExpDis(Double lambda, Integer minBound, Integer maxBound, Double yShift){
        Integer result = -1;
        int numBound = maxBound - minBound;
        ArrayList<Integer> numbers = new ArrayList<>(numBound);
        for (Integer i = minBound; i < maxBound; i++){
            numbers.add(i);
        }
        Collections.shuffle((numbers));
        Integer current;
        Double probability;
        double dice;
        for (int i = 0; i < numBound; i++){
            current = numbers.get(i);
            probability = expDistributionPr(lambda, current.doubleValue(), minBound.doubleValue(), yShift);
            dice = randGen.nextDouble();
            assert probability != null;
            if (probability >= dice){
                result = current;
            }
        }
        if (result < 0) {
            result = numbers.get(0);
        }
        return result;
    }
    public static Integer getDeviceID(Integer pID, String deviceTypeName) {
        switch (deviceTypeName) {
            case TupBloodPressure.deviceName:
                return pID * 10 + 1;
            case TupFitbit.deviceName:
                return pID * 10 + 2;
            case TupGlucose.deviceName:
                return pID * 10 + 3;
            case TupInsulin.deviceName:
                return pID * 10 + 4;
            case TupSmartwatch.deviceName:
                return pID * 10 + 5;
        }
        return null;
    }
    @Override  // consider using array instead of TupleN (where N is an integer)
    public void run(SourceContext<T> ctx) throws Exception {
        if (!deviceTypes.containsKey(deviceType)) {return;}
        String fileTime = null;
        String deviceTypeUnderline = null;
        if (isSaveFile) {
            // add time info to file name
            fileTime = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").format(LocalDateTime.now());
            deviceTypeUnderline = addUnderline(deviceType);
        }
        Map<Integer, ArrayList<T>> shuffledOutput = new HashMap<>();
        // apply the distribution & generate timestamp
        long timestamp = timestampStart;
        // count the number of tuples for each patient
        long count = 0;
        while (reachLimit(count, totalLimit)) {
            for (int patientID = 1; patientID <= patientNum; patientID++) {
                Integer deviceID = getDeviceID(patientID, deviceType);
                TupDevice event = null;
                switch (distributionType) {
                    case distNameUniform:
                        event = applyDistUniform(deviceID, patientID, timestamp, deviceType, getSessionName(timestamp));
                        break;
                    case distNameExp:
                        event = applyDistExponential(deviceID, patientID, timestamp, deviceType, getSessionName(timestamp));
                        break;
                    case distNameRandom:
                        event = applyDistRand(deviceID, patientID, timestamp, deviceType);
                        break;
                }
                // noinspection unchecked
                T eventConv = (T) event;
                if (randSize <= 1) {
                    // collect the data if data saving is enabled
                    ArrayList<String> dataLine = null;
                    if (isSaveFile && event != null) {dataLine = tupleToArrList(event);}
                    outputDataStream(ctx, eventConv, timestamp, deviceTypeUnderline, fileTime, dataLine);
                }
                else {
                    // for every randSize = 10 tuples, mess up the timestamp order and output them
                    if (shuffledOutput.containsKey(patientID) && shuffledOutput.get(patientID).size() == randSize) {
                        Collections.shuffle(shuffledOutput.get(patientID));
                        for (T elem : shuffledOutput.get(patientID)) {
                            Long timeTemp;
                            if (elem instanceof TupDevice) {
                                timeTemp = ((TupDevice) elem).timestamp;
                                outputDataStream(ctx, elem, timeTemp, deviceTypeUnderline, fileTime, tupleToArrList((TupDevice) elem));
                            }
                        }
                        shuffledOutput.get(patientID).clear();
                    }
                    if (!shuffledOutput.containsKey(patientID)) {
                        shuffledOutput.put(patientID, new ArrayList<>());
                    }
                    shuffledOutput.get(patientID).add(eventConv);
                }
            }
            // increment the timestamp
            if (isFluctuate && (1 < timestampRange)) {
                timestamp += ThreadLocalRandom.current().nextLong(1, timestampRange + 1);
            }
            else {timestamp += timestampRange;}
            // the sleep function can slow down the stream
            if (sleepTime > 0) {TimeUnit.MILLISECONDS.sleep(sleepTime);}
            // increment the count
            count++;
        }
    }
    @Override  // the cancel function is N/A, but still required
    public void cancel() {}
    // output data
    public void outputDataStream(SourceContext<T> ctx, T eventConv, long timestamp,
                                 String deviceNameConv, String fileTime, ArrayList<String> dataLine) {
        // collect the generated data
        ctx.collectWithTimestamp(eventConv, timestamp);
        // save the data to file if permitted
        if (isSaveFile && eventConv instanceof TupDevice) {
            writeToCSV(System.getProperty("user.dir") + "/" + deviceNameConv + "_PID" + ((TupDevice) eventConv).patientID +
                       "_DID" + ((TupDevice) eventConv).deviceID + "_" + fileTime + ".csv", dataLine);
        }
    }
    // get session name based on timestamp
    public String getSessionName(long timestamp) {
        int remainder = (int) ((timestamp / avgActPeriod) % 4L);
        String result = sessionNameNA;
        switch (remainder) {
            case 0: result = sessionNameSleep; break;
            case 1: result = sessionNameWalk; break;
            case 2: result = sessionNameEat; break;
            case 3: result = sessionNameRun; break;
        }
        return result;
    }
    // random distribution
    public TupDevice applyDistRand(Integer dID, Integer pID, long timestamp, String deviceTypeName) {
        switch (deviceTypeName) {
            case TupBloodPressure.deviceName:
                Integer dBP = randGen.nextInt(100);  // range: [0, 100)
                Integer sBP = randGen.nextInt(100);
                return new TupBloodPressure(dID, pID, timestamp, dBP, sBP);
            case TupFitbit.deviceName:
                Integer METs = randGen.nextInt(100);
                Integer intensity = randGen.nextInt(100);
                Integer steps = randGen.nextInt(100);
                Integer heartRate = randGen.nextInt(100);
                return new TupFitbit(dID, pID, timestamp, METs, intensity, steps, heartRate);
            case TupGlucose.deviceName:
                Integer glucose = randGen.nextInt(100);
                return new TupGlucose(dID, pID, timestamp, glucose);
            case TupInsulin.deviceName:
                Integer doseAmount = randGen.nextInt(100);
                return new TupInsulin(dID, pID, timestamp, doseAmount);
            case TupSmartwatch.deviceName:
                Integer stepsSinceLast = randGen.nextInt(100);
                Float meanHeartRate = ((Integer) randGen.nextInt(200)).floatValue() + randGen.nextFloat();
                return new TupSmartwatch(dID, pID, timestamp, stepsSinceLast, meanHeartRate);
        }
        return null;
    }
    // control the distribution of data: uniform
    public TupDevice applyDistUniform(Integer dID, Integer pID, long timestamp, String deviceTypeName, String actName) {
        // Every 85 in timestamp, there is an anomaly. It will be in the sleeping period.
        // Heart rate will drop to 40~50 and glucose level will drop to 50~70.
        boolean isAbnormal = (timestamp % anomalyPeriod > 0 && timestamp % anomalyPeriod <= anomalyRemainder);
        TupDevice tuple = null;
        switch (deviceTypeName) {
            // note: nextInt has range: [0, 100)
            case TupBloodPressure.deviceName:
                Integer dBP = null; Integer sBP = null;
                switch (actName) {
                    case sessionNameSleep:
                        dBP = randGen.nextInt(dbpSleepHigh - dbpSleepLow) + dbpSleepLow;
                        sBP = randGen.nextInt(sbpSleepHigh - sbpSleepLow) + sbpSleepLow; break;
                    case sessionNameWalk:
                        dBP = randGen.nextInt(dbpWalkHigh - dbpWalkLow) + dbpWalkLow;
                        sBP = randGen.nextInt(sbpWalkHigh - sbpWalkLow) + sbpWalkLow; break;
                    case sessionNameEat:
                        dBP = randGen.nextInt(dbpEatHigh - dbpEatLow) + dbpEatLow;
                        sBP = randGen.nextInt(sbpEatHigh - sbpEatLow) + sbpEatLow; break;
                    case sessionNameRun:
                        dBP = randGen.nextInt(dbpRunHigh - dbpRunLow) + dbpRunLow;
                        sBP = randGen.nextInt(sbpRunHigh - sbpRunLow) + sbpRunLow; break;
                    case sessionNameNA:
                        dBP = randGen.nextInt(dbpNAHigh - dbpNALow) + dbpNALow;
                        sBP = randGen.nextInt(sbpNAHigh - sbpNALow) + sbpNALow; break;
                }
                tuple = new TupBloodPressure(dID, pID, timestamp, dBP, sBP);
                break;
            case TupFitbit.deviceName:
                Integer METs = null; Integer intensity = null; Integer steps = null; Integer heartRate = null;
                switch (actName) {
                    case sessionNameSleep: METs = METsSleep; intensity = intensitySleep;
                        steps = stepSleep;
                        heartRate = randGen.nextInt(heartRateSleepHigh - heartRateSleepLow) + heartRateSleepLow; break;
                    case sessionNameWalk: METs = METsWalk; intensity = intensityWalk;
                        steps = randGen.nextInt(stepWalkHigh - stepWalkLow) + stepWalkLow;
                        heartRate = randGen.nextInt(heartRateWalkHigh - heartRateWalkLow) + heartRateWalkLow; break;
                    case sessionNameEat: METs = METsEat; intensity = intensityEat;
                        steps = stepEat;
                        heartRate = randGen.nextInt(heartRateEatHigh - heartRateEatLow) + heartRateEatLow; break;
                    case sessionNameRun: METs = METsRun; intensity = intensityRun;
                        steps = randGen.nextInt(stepRunHigh - stepRunLow) + stepRunLow;
                        heartRate = randGen.nextInt(heartRateRunHigh - heartRateRunLow) + heartRateRunLow; break;
                    case sessionNameNA: METs = randGen.nextInt(4); intensity = randGen.nextInt(4);
                        steps = randGen.nextInt(stepNAHigh - stepNALow) + stepNALow;
                        heartRate = randGen.nextInt(heartRateNAHigh - heartRateNALow) + heartRateNALow; break;
                }
                // apply anomaly if applicable
                if (isAbnormal) {heartRate = randGen.nextInt(heartRateAnomalyHigh - heartRateAnomalyLow) + heartRateAnomalyLow;}
                tuple = new TupFitbit(dID, pID, timestamp, METs, intensity, steps, heartRate);
                break;
            case TupGlucose.deviceName:
                Integer glucose = null;
                switch (actName) {
                    case sessionNameSleep:
                        glucose = randGen.nextInt(glucoseSleepHigh - glucoseSleepLow) + glucoseSleepLow; break;
                    case sessionNameWalk:
                        glucose = randGen.nextInt(glucoseWalkHigh - glucoseWalkLow) + glucoseWalkLow; break;
                    case sessionNameEat:
                        glucose = randGen.nextInt(glucoseEatHigh - glucoseEatLow) + glucoseEatLow; break;
                    case sessionNameRun:
                        glucose = randGen.nextInt(glucoseRunHigh - glucoseRunLow) + glucoseRunLow; break;
                    case sessionNameNA:
                        glucose = randGen.nextInt(glucoseNAHigh - glucoseNALow) + glucoseNALow; break;
                }
                // apply anomaly if applicable
                if (isAbnormal) {glucose = randGen.nextInt(glucoseAnomalyHigh - glucoseAnomalyLow) + glucoseAnomalyLow;}
                tuple = new TupGlucose(dID, pID, timestamp, glucose);
                break;
            case TupInsulin.deviceName:
                Integer doseAmount = null;
                switch (actName) {
                    case sessionNameSleep: doseAmount = insulinSleep; break;
                    case sessionNameWalk: doseAmount = insulinWalk; break;
                    case sessionNameEat: doseAmount = insulinEat; break;
                    case sessionNameRun:
                        doseAmount = randGen.nextInt(insulinRunHigh - insulinRunLow) + insulinRunLow; break;
                    case sessionNameNA:
                        doseAmount = randGen.nextInt(insulinNAHigh - insulinNALow) + insulinNALow; break;
                }
                tuple = new TupInsulin(dID, pID, timestamp, doseAmount);
                break;
            case TupSmartwatch.deviceName:
                Integer stepsSinceLast = null; Float meanHeartRate = null;
                switch (actName) {
                    case sessionNameSleep:
                        stepsSinceLast = stepLastSleep;
                        meanHeartRate = randGen.nextFloat() * (heartRateSleepHigh.floatValue() - heartRateSleepLow.floatValue()) +
                                heartRateSleepLow.floatValue(); break;
                    case sessionNameWalk:
                        stepsSinceLast = randGen.nextInt(stepLastWalkHigh - stepLastWalkLow) + stepLastWalkLow;
                        meanHeartRate = randGen.nextFloat() * (heartRateWalkHigh.floatValue() - heartRateWalkLow.floatValue()) +
                                heartRateWalkLow.floatValue(); break;
                    case sessionNameEat:
                        stepsSinceLast = stepLastEat;
                        meanHeartRate = randGen.nextFloat() * (heartRateEatHigh.floatValue() - heartRateEatLow.floatValue()) +
                                heartRateEatLow.floatValue(); break;
                    case sessionNameRun:
                        stepsSinceLast = randGen.nextInt(stepLastRunHigh - stepLastRunLow) + stepLastRunLow;
                        meanHeartRate = randGen.nextFloat() * (heartRateRunHigh.floatValue() - heartRateRunLow.floatValue()) +
                                heartRateRunLow.floatValue(); break;
                    case sessionNameNA:
                        stepsSinceLast = randGen.nextInt(stepLastNAHigh - stepLastNALow) + stepLastNALow;
                        meanHeartRate = randGen.nextFloat() * (heartRateNAHigh.floatValue() - heartRateNALow.floatValue()) +
                                heartRateNALow.floatValue(); break;
                }
                // apply anomaly if applicable
                if (isAbnormal) {
                    meanHeartRate = randGen.nextFloat() * (heartRateAnomalyHigh.floatValue() - heartRateAnomalyLow.floatValue()) +
                            heartRateAnomalyLow.floatValue();
                }
                tuple = new TupSmartwatch(dID, pID, timestamp, stepsSinceLast, meanHeartRate);
                break;
        }
        return tuple;
    }
    // control the distribution of data: exponential
    public TupDevice applyDistExponential(Integer dID, Integer pID, long timestamp, String deviceTypeName, String actName) {
        // Every 85 in timestamp, there is an anomaly. It will be in the sleeping period.
        // Heart rate will drop to 40~50 and glucose level will drop to 50~70.
        boolean isAbnormal = (timestamp % anomalyPeriod > 0 && timestamp % anomalyPeriod <= anomalyRemainder);
        TupDevice tuple = null;
        switch (deviceTypeName) {
            case TupBloodPressure.deviceName:
                Integer dBP = null; Integer sBP = null;
                switch (actName) {
                    case sessionNameSleep:
                        dBP = chooseExpDis(expLambda, dbpSleepLow, dbpSleepHigh, expYShift);
                        sBP = chooseExpDis(expLambda, sbpSleepLow, sbpSleepHigh, expYShift); break;
                    case sessionNameWalk:
                        dBP = chooseExpDis(expLambda, dbpWalkLow, dbpWalkHigh, expYShift);
                        sBP = chooseExpDis(expLambda, sbpWalkLow, sbpWalkHigh, expYShift); break;
                    case sessionNameEat:
                        dBP = chooseExpDis(expLambda, dbpEatLow, dbpEatHigh, expYShift);
                        sBP = chooseExpDis(expLambda, sbpEatLow, sbpEatHigh, expYShift); break;
                    case sessionNameRun:
                        dBP = chooseExpDis(expLambda, dbpRunLow, dbpRunHigh, expYShift);
                        sBP = chooseExpDis(expLambda, sbpRunLow, sbpRunHigh, expYShift); break;
                    case sessionNameNA:
                        dBP = chooseExpDis(expLambda, dbpNALow, dbpNAHigh, expYShift);
                        sBP = chooseExpDis(expLambda, sbpNALow, sbpNAHigh, expYShift); break;
                }
                tuple = new TupBloodPressure(dID, pID, timestamp, dBP, sBP);
                break;
            case TupFitbit.deviceName:
                Integer METs = null; Integer intensity = null; Integer steps = null; Integer heartRate = null;
                switch (actName) {
                    case sessionNameSleep:
                        METs = METsSleep; intensity = intensitySleep; steps = stepSleep;
                        heartRate = chooseExpDis(expLambda, heartRateSleepLow, heartRateSleepHigh, expYShift); break;
                    case sessionNameWalk:
                        METs = METsWalk; intensity = intensityWalk;
                        steps = chooseExpDis(expLambda, stepWalkLow, stepWalkHigh, expYShift);
                        heartRate = chooseExpDis(expLambda, heartRateWalkLow, heartRateWalkHigh, expYShift); break;
                    case sessionNameEat:
                        METs = METsEat; intensity = intensityEat; steps = stepEat;
                        heartRate = chooseExpDis(expLambda, heartRateEatLow, heartRateEatHigh, expYShift); break;
                    case sessionNameRun:
                        METs = METsRun; intensity = intensityRun;
                        steps = chooseExpDis(expLambda, stepRunLow, stepRunHigh, expYShift);
                        heartRate = chooseExpDis(expLambda, heartRateRunLow, heartRateRunHigh, expYShift); break;
                    case sessionNameNA:
                        break;
                }
                // apply anomaly if applicable
                if (isAbnormal) {heartRate = randGen.nextInt(heartRateAnomalyHigh - heartRateAnomalyLow) + heartRateAnomalyLow;}
                tuple = new TupFitbit(dID, pID, timestamp, METs, intensity, steps, heartRate);
                break;
            case TupGlucose.deviceName:
                Integer glucose = null;
                switch (actName) {
                    case sessionNameSleep:
                        glucose = chooseExpDis(expLambda, glucoseSleepLow, glucoseSleepHigh, expYShift); break;
                    case sessionNameWalk:
                        glucose = chooseExpDis(expLambda, glucoseWalkLow, glucoseWalkHigh, expYShift); break;
                    case sessionNameEat:
                        glucose = chooseExpDis(expLambda, glucoseEatLow, glucoseEatHigh, expYShift); break;
                    case sessionNameRun:
                        glucose = chooseExpDis(expLambda, glucoseRunLow, glucoseRunHigh, expYShift); break;
                    case sessionNameNA:
                        glucose = chooseExpDis(expLambda, glucoseNALow, glucoseNAHigh, expYShift); break;
                }
                // apply anomaly if applicable
                if (isAbnormal) {glucose = randGen.nextInt(glucoseAnomalyHigh - glucoseAnomalyLow) + glucoseAnomalyLow;}
                tuple = new TupGlucose(dID, pID, timestamp, glucose);
                break;
            case TupInsulin.deviceName:
                Integer doseAmount = null;
                switch (actName) {
                    case sessionNameSleep: doseAmount = insulinSleep; break;
                    case sessionNameWalk: doseAmount = insulinWalk; break;
                    case sessionNameEat: doseAmount = insulinEat; break;
                    case sessionNameRun:
                        doseAmount = chooseExpDis(expLambda, insulinRunLow, insulinRunHigh, expYShift); break;
                    case sessionNameNA:
                        doseAmount = chooseExpDis(expLambda, insulinNALow, insulinNAHigh, expYShift); break;
                }
                tuple = new TupInsulin(dID, pID, timestamp, doseAmount);
                break;
            case TupSmartwatch.deviceName:
                Integer stepsSinceLast = null; Float meanHeartRate = null;
                switch (actName) {
                    case sessionNameSleep:
                        stepsSinceLast = stepLastSleep;
                        meanHeartRate = chooseExpDis(expLambda, heartRateSleepLow, heartRateSleepHigh, expYShift) + randGen.nextFloat();
                        break;
                    case sessionNameWalk:
                        stepsSinceLast = chooseExpDis(expLambda, stepLastWalkLow, stepLastWalkHigh, expYShift);
                        meanHeartRate = chooseExpDis(expLambda, heartRateWalkLow, heartRateWalkHigh, expYShift) + randGen.nextFloat();
                        break;
                    case sessionNameEat:
                        stepsSinceLast = stepLastEat;
                        meanHeartRate = chooseExpDis(expLambda, heartRateEatLow, heartRateEatHigh, expYShift) + randGen.nextFloat();
                        break;
                    case sessionNameRun:
                        stepsSinceLast = chooseExpDis(expLambda, stepLastRunLow, stepLastRunHigh, expYShift);
                        meanHeartRate = chooseExpDis(expLambda, heartRateRunLow, heartRateRunHigh, expYShift) + randGen.nextFloat();
                        break;
                    case sessionNameNA:
                        stepsSinceLast = chooseExpDis(expLambda, stepLastNALow, stepLastNAHigh, expYShift);
                        meanHeartRate = chooseExpDis(expLambda, heartRateNALow, heartRateNAHigh, expYShift) + randGen.nextFloat();
                        break;
                }
                // apply anomaly if applicable
                if (isAbnormal) {
                    meanHeartRate = randGen.nextFloat() * (heartRateAnomalyHigh.floatValue() - heartRateAnomalyLow.floatValue()) +
                            heartRateAnomalyLow.floatValue();
                }
                tuple = new TupSmartwatch(dID, pID, timestamp, stepsSinceLast, meanHeartRate);
                break;
        }
        return tuple;
    }
    // file parser, read tuple data from a CSV file
    public static ArrayList<TupDevice> readDataFromCSV(String filename, String deviceTypeName) {
        List<String> lines;
        ArrayList<TupDevice> result = new ArrayList<>();
        try {
            lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
            // assume that the file does not contain the names of alias for the 1st line
            for (String line : lines) {
                ArrayList<String> attrList = new ArrayList<>(Arrays.asList(line.split(",")));
                TupDevice tuple = null;
                switch (deviceTypeName) {
                    case TupBloodPressure.deviceName:
                        tuple = new TupBloodPressure(
                                Integer.valueOf(attrList.get(0)),  // device ID
                                Integer.valueOf(attrList.get(1)),  // patient ID
                                Long.valueOf(attrList.get(2)),  // timestamp
                                Integer.valueOf(attrList.get(3)),  // DBP
                                Integer.valueOf(attrList.get(4))  // SBP
                        );
                        break;
                    case TupFitbit.deviceName:
                        tuple = new TupFitbit(
                                Integer.valueOf(attrList.get(0)),  // device ID
                                Integer.valueOf(attrList.get(1)),  // patient ID
                                Long.valueOf(attrList.get(2)),  // timestamp
                                Integer.valueOf(attrList.get(3)),  // METs
                                Integer.valueOf(attrList.get(4)),  // intensity
                                Integer.valueOf(attrList.get(5)),  // steps
                                Integer.valueOf(attrList.get(6))  // heart rate
                        );
                        break;
                    case TupGlucose.deviceName:
                        tuple = new TupGlucose(
                                Integer.valueOf(attrList.get(0)),  // device ID
                                Integer.valueOf(attrList.get(1)),  // patient ID
                                Long.valueOf(attrList.get(2)),  // timestamp
                                Integer.valueOf(attrList.get(3))  // glucose
                        );
                        break;
                    case TupInsulin.deviceName:
                        tuple = new TupInsulin(
                                Integer.valueOf(attrList.get(0)),  // device ID
                                Integer.valueOf(attrList.get(1)),  // patient ID
                                Long.valueOf(attrList.get(2)),  // timestamp
                                Integer.valueOf(attrList.get(3))  // dose amount
                        );
                        break;
                    case TupSmartwatch.deviceName:
                        tuple = new TupSmartwatch(
                                Integer.valueOf(attrList.get(0)),  // device ID
                                Integer.valueOf(attrList.get(1)),  // patient ID
                                Long.valueOf(attrList.get(2)),  // timestamp
                                Integer.valueOf(attrList.get(3)),  // steps since last
                                Float.valueOf(attrList.get(4))// mean heart rate
                        );
                        break;
                }
                result.add(tuple);
            }
        }
        catch (IOException ioe) {System.out.println("ERROR read: " + filename + " not found."); ioe.printStackTrace();}
        return result;
    }
    // replace spaces with underlines
    public static String addUnderline(String str) {
        StringBuilder result = new StringBuilder();
        for (char c : str.toCharArray()) {
            if (c == ' ') {result.append('_');}
            else {result.append(c);}
        }
        return result.toString();
    }
    // write to a CSV file
    public static void writeToCSV(String filename, ArrayList<String> content) {
        // create a CSV file if it does not exist
        File file = new File(filename);
        if (!file.exists()) {
            try {
                boolean created = file.createNewFile();
                if (!created) {
                    System.out.println("ERROR write CSV: creating file " + filename + " failed.");
                    return;
                }
            }
            catch (IOException ioe) {System.out.println("ERROR write CSV: creating file " + filename + " IOException.");}
        }
        // append data records to the CSV file
        try {
            FileWriter writer = new FileWriter(filename, true);
            for (int i = 0; i < content.size(); i++) {
                writer.append(content.get(i));
                if (i < content.size() - 1) {writer.append(",");}
                else {writer.append("\n");}
            }
            writer.flush();
            writer.close();
        }
        catch (IOException ioe) {System.out.println("ERROR write CSV: " + filename + " not found.");}
    }
    // convert a tuple to CSV form array list data
    public ArrayList<String> tupleToArrList(TupDevice tuple) {
        // collect the data if data saving is enabled
        ArrayList<String> dataLine = new ArrayList<>();
        dataLine.add(tuple.deviceID.toString());
        dataLine.add(tuple.patientID.toString());
        dataLine.add(String.valueOf(tuple.timestamp));
        if (tuple instanceof TupBloodPressure) {
            dataLine.add(((TupBloodPressure) tuple).dBP.toString());
            dataLine.add(((TupBloodPressure) tuple).sBP.toString());
        }
        else if (tuple instanceof TupFitbit) {
            dataLine.add(((TupFitbit) tuple).METs.toString());
            dataLine.add(((TupFitbit) tuple).intensity.toString());
            dataLine.add(((TupFitbit) tuple).steps.toString());
            dataLine.add(((TupFitbit) tuple).heartRate.toString());
        }
        else if (tuple instanceof TupGlucose) {
            dataLine.add(((TupGlucose) tuple).glucose.toString());
        }
        else if (tuple instanceof TupInsulin) {
            dataLine.add(((TupInsulin) tuple).doseAmount.toString());
        }
        else if (tuple instanceof TupSmartwatch) {
            dataLine.add(((TupSmartwatch) tuple).stepsSinceLast.toString());
            dataLine.add(((TupSmartwatch) tuple).meanHeartRate.toString());
        }
        return dataLine;
    }
}
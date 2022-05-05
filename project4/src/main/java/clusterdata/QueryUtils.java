package clusterdata;

import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

// store the shared functions
public class QueryUtils {
    // the printed bars for visual clearance
    protected static final String windowBar = repeatStr("=", 40);
    protected static final String avgBar = repeatStr("-", 40);
    protected static final String addBar = repeatStr("+", 40);
    protected static final String warnBar = repeatStr("#", 40);
    protected static final String normBar = repeatStr("~", 40);
    // given a string, repeat it for N times
    public static String repeatStr(String s, int N) {
        if (N <= 0) {return null;}
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < N; i++) {result.append(s);}
        return result.toString();
    }
    // helper function for initializing/formalizing the threshold range
    public static Float[] assignRange(Float[] actPeriodRange) {
        if (actPeriodRange == null) {return null;}
        // not need to formalize
        if (actPeriodRange.length == 2 && (actPeriodRange[0] == null || actPeriodRange[1] == null ||
                                           actPeriodRange[0] <= actPeriodRange[1])) {return actPeriodRange;}
        // need to formalize
        Float actPeriodMin;
        Float actPeriodMax;
        // there is no range
        if (actPeriodRange.length == 0) {actPeriodMin = null; actPeriodMax = null;}
        // there is only 1 number, by default set it as the lower bound
        else if (actPeriodRange.length == 1) {actPeriodMin = actPeriodRange[0]; actPeriodMax = null;}
        // there are more than or equal to 2 numbers for the range
        else {
            if (actPeriodRange[0] != null && actPeriodRange[1] != null && actPeriodRange[0] > actPeriodRange[1]) {
                actPeriodMin = actPeriodRange[1];
                actPeriodMax = actPeriodRange[0];
            }
            else {actPeriodMin = actPeriodRange[0]; actPeriodMax = actPeriodRange[1];}
        }
        return new Float[] {actPeriodMin, actPeriodMax};
    }
    // convert a threshold range to string form following the mathematical standard
    public static String rangeToString(Float[] rangeThreshold, boolean includeLower, boolean includeUpper) {
        if (rangeThreshold == null || rangeThreshold.length != 2) {return "";}
        StringBuilder result = new StringBuilder();
        if (includeLower) {result.append("[");}
        else {result.append("(");}
        result.append(rangeThreshold[0]).append(", ").append(rangeThreshold[1]);
        if (includeUpper) {result.append("]");}
        else {result.append(")");}
        return result.toString();
    }
    // check if a value is qualified by the threshold
    public static boolean isQualified(Float dataVal, Float actPeriodMin, Float actPeriodMax,
                                      boolean includeLower, boolean includeUpper) {
        // null values are always wrong
        if (dataVal == null) {return false;}
        // if the thresholds are not provided, the statement is always true
        if (actPeriodMin == null && actPeriodMax == null) {return true;}
        // e.g., if there is null lower bound, then the value is always greater than the lower bound
        boolean upperCompare = true;
        boolean lowerCompare = true;
        // if we have lower bound
        if (actPeriodMin != null) {
            // whether the lower bound is inclusive
            if (includeLower) {lowerCompare = (actPeriodMin <= dataVal);}
            else {lowerCompare = (actPeriodMin < dataVal);}
        }
        // if we have upper bound
        if (actPeriodMax != null) {
            // whether the upper bound is inclusive
            if (includeUpper) {upperCompare = (dataVal <= actPeriodMax);}
            else {upperCompare = (dataVal < actPeriodMax);}
        }
        return lowerCompare && upperCompare;
    }
    // check whether a value increased by a certain percentage
    public static boolean isRisePercent(Float oldVal, Float newVal, Double ratio) {
        return (newVal - oldVal) / oldVal >= ratio;
    }
    // retrieve the percentage increase/decrease in string message form
    public static String getChangePercentStr(Float oldVal, Float newVal, Integer precision) {
        if (oldVal == null || newVal == null) {return null;}
        String result = "unchanged";
        if (oldVal.equals(newVal)) {return result;}
        else {
            // determine percentage change
            double percentChange = (Math.abs((oldVal - newVal)) / oldVal) * 100D;
            // apply rounding if applicable
            if (precision != null) {
                result = "(approx.) ";
                percentChange = Math.round(percentChange * 100.0) / 100.0;
            }
            else {result = "";}
            // determine rise or fall
            if (oldVal > newVal) {result = "decrease by " + result;}
            // oldVal < newVal
            else {result = "increase by " + result;}
            return result + percentChange + "%";
        }
    }
    // detect if the value increases by a certain percentage within a certain period of time
    public static int queryDetectRise(TupDevice tuple, String attrName, Double riseRatio,
                                      long risePeriod, Query3StateStream nowStateStream, Collector<String> out) {
        Integer precision = 2;  // round the percentage to 2 decimal points
        Float dataVal = null; String streamName = null;
        if (tuple instanceof TupGlucose) {
            streamName = TupGlucose.deviceName;
            if (attrName.equals(TupGlucose.attrNameGlucose)) {
                dataVal = Float.valueOf(((TupGlucose) tuple).glucose);
            }
        }
        else if (tuple instanceof TupSmartwatch) {
            streamName = TupSmartwatch.deviceName;
            if (attrName.equals(TupSmartwatch.attrNameMeanHeartRate)) {
                dataVal = ((TupSmartwatch) tuple).meanHeartRate;
            }
        }
        boolean isUpdateState = false;
        // ensure that all timestamps are positive
        boolean positiveTime = (nowStateStream.timestampTrack >= 0 && tuple.timestamp >= 0);
        // when there is a decrease/steady or no comparison is available yet, we just move on
        assert dataVal != null;
        boolean hasDecrease = (nowStateStream.valueTrack == null || dataVal <= nowStateStream.valueTrack);
        // a certain period of time passed, but there is still no significant increase
        boolean noSigRise = (positiveTime && tuple.timestamp - nowStateStream.timestampTrack >= risePeriod &&
                !isRisePercent(nowStateStream.valueTrack, dataVal, riseRatio));
        // the attribute increased by an above-threshold ratio within a period of time
        boolean hasIncrease = (positiveTime && tuple.timestamp - nowStateStream.timestampTrack <= risePeriod &&
                isRisePercent(nowStateStream.valueTrack, dataVal, riseRatio));
        // formatting the output message
        String outMessage = streamName + " from time " + nowStateStream.timestampTrack +
                ", old " + attrName + ": " + nowStateStream.valueTrack + " to time " + tuple.timestamp +
                ", new " + attrName + ": " + dataVal + ", " + getChangePercentStr(nowStateStream.valueTrack, dataVal, precision);
        if (hasDecrease) {
            if (nowStateStream.valueTrack != null) {
                if (dataVal.equals(nowStateStream.valueTrack)) {out.collect("MESSAGE (STEADY): " + outMessage);}
                else {out.collect(avgBar + " MESSAGE (FALL): " + outMessage);}
            }
        }
        else if (noSigRise) {
            out.collect(addBar + " MESSAGE (NO BIG RISE): " + outMessage);
        }
        else if (hasIncrease) {
            out.collect(warnBar + " WARNING (SINGLE RISE): " + outMessage);
        }
        if (hasDecrease || noSigRise || hasIncrease) {isUpdateState = true;}
        // both hasIncrease and isUpdateState are true
        if (hasIncrease) {return 2;}
        // hasIncrease is false but isUpdateState is true
        else if (isUpdateState) {return 1;}
        // neither
        else {return 0;}
    }
    // calculate the average of certain attributes of a list of tuples
    public static Map<String, Float> getListTupleAvg(ArrayList<TupDevice> tuples, String[] attrNames) {
        // map the name of the attribute to the sum of the values
        Map<String, Float> sumMap = new HashMap<>();
        // initially they are all 0's
        for (String attr : attrNames) {sumMap.put(attr, 0.0F);}
        // compute the sums for each attribute
        for (TupDevice tuple : tuples) {
            for (String attr : attrNames) {
                if (tuple instanceof TupBloodPressure) {
                    Float oldVal = sumMap.get(attr);
                    if (attr.equals(TupBloodPressure.attrNameSBP)) {
                        sumMap.put(attr, oldVal + ((TupBloodPressure) tuple).sBP);
                    }
                    else if (attr.equals(TupBloodPressure.attrNameDBP)) {
                        sumMap.put(attr, oldVal + ((TupBloodPressure) tuple).dBP);
                    }
                }
                if (tuple instanceof TupFitbit) {
                    Float oldVal = sumMap.get(attr);
                    switch (attr) {
                        case TupFitbit.attrNameHeartRate:
                            sumMap.put(attr, oldVal + ((TupFitbit) tuple).heartRate);
                            break;
                        case TupFitbit.attrNameMETs:
                            sumMap.put(attr, oldVal + ((TupFitbit) tuple).METs);
                            break;
                        case TupFitbit.attrNameIntensity:
                            sumMap.put(attr, oldVal + ((TupFitbit) tuple).intensity);
                            break;
                        case TupFitbit.attrNameSteps:
                            sumMap.put(attr, oldVal + ((TupFitbit) tuple).steps);
                            break;
                    }
                }
                if (tuple instanceof TupGlucose) {
                    Float oldVal = sumMap.get(attr);
                    if (attr.equals(TupGlucose.attrNameGlucose)) {
                        sumMap.put(attr, oldVal + ((TupGlucose) tuple).glucose);
                    }
                }
            }
        }
        // compute the averages
        for (String attr : attrNames) {
            Float oldSum = sumMap.get(attr);
            sumMap.put(attr, oldSum/tuples.size());
        }
        return sumMap;
    }
}

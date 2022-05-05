package clusterdata;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

// store the shared functions
public class QueryUtils {
    // the printed bars for visual clearance
    protected static final String windowBar = "========================================";
    protected static final String avgBar =    "----------------------------------------";
    protected static final String warnBar =   "########################################";
    // given a string, repeat it for N times
    public static String repeatStr(String s, int N) {
        if (N <= 0) {return null;}
        String result = "";
        for (int i = 0; i < N; i++) {result += s;}
        return result;
    }
    // helper function for initializing the threshold range
    public static Float[] assignRange(Float[] actPeriodRange) {
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
    // check if a value qualifies the threshold
    public static boolean isQualified(Float dataVal, Float actPeriodMin, Float actPeriodMax) {
        if (dataVal == null || (actPeriodMin == null && actPeriodMax == null)) {return false;}
        if (actPeriodMin == null) {return (dataVal <= actPeriodMax);}
        else if (actPeriodMax == null) {return (actPeriodMin <= dataVal);}
        else {return (actPeriodMin <= dataVal) && (dataVal <= actPeriodMax);}
    }
    // detect if the value increases by a certain percentage within a certain period of time
    public static int queryDetectRise(Double dataVal, long nowTimestamp, Integer streamNum, Double riseRatio,
                                      long risePeriod, Q3StateStream nowState, Collector<String> out) throws Exception {
        boolean isUpdateState = false;
        // ensure that all timestamps are positive
        boolean positiveTime = (nowState.timestampTrack >= 0 && nowTimestamp >= 0);
        // when there is a decrease/steady or no comparison is available yet, we just move on
        boolean hasDecrease = (nowState.valueTrack == null || dataVal <= nowState.valueTrack);
        // a certain period of time passed, but there is still no significant increase
        boolean noSigRise = (positiveTime && nowTimestamp - nowState.timestampTrack > risePeriod &&
                !isRisePercent(nowState.valueTrack, dataVal, riseRatio));
        // the attribute increased by an above-threshold ratio within a period of time
        boolean hasIncrease = (positiveTime && nowTimestamp - nowState.timestampTrack <= risePeriod &&
                isRisePercent(nowState.valueTrack, dataVal, riseRatio));
        // formatting the output message
        String outMessage = "stream " + streamNum + " from " + nowState.timestampTrack +
                " old value " + nowState.valueTrack + " to " + nowTimestamp + " new value " + dataVal;
        if (hasDecrease) {
            if (nowState.valueTrack == null) {out.collect("MESSAGE (NULL): " + streamNum);}
            else if (dataVal.equals(nowState.valueTrack)) {out.collect("MESSAGE (STEADY): " + outMessage);}
            else {out.collect("MESSAGE (FALL): " + outMessage);}
            isUpdateState = true;
        }
        else if (noSigRise) {
            out.collect("MESSAGE (NO SIG RISE): " + outMessage);
            isUpdateState = true;
        }
        else if (hasIncrease) {
            out.collect("WARNING (SINGLE): " + outMessage + " " + templateTest.warnBar);
            isUpdateState = true;
        }
        // both hasIncrease and isUpdateState are true
        if (hasIncrease) {return 2;}
        // hasIncrease is false but isUpdateState is true
        else if (isUpdateState) {return 1;}
        // neither
        else {return 0;}
    }
    // check whether a value increased by a certain percentage
    public static boolean isRisePercent(Double oldVal, Double newVal, Double ratio) {
        return (newVal - oldVal) / oldVal >= ratio;
    }
}

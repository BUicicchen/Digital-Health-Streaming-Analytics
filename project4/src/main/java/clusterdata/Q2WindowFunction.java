package clusterdata;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

// ProcessWindowFunction attributes: input event, output event collector, key, the window
public class Q2WindowFunction<T> extends ProcessWindowFunction<T, String, Integer, TimeWindow> {
    // manually maintain the state
    private ValueState<Query2State> state;
    // define how many consecutive windows we want to track
    // maintain N = 3 consecutive averages
    private final int numWindowTrack;
    // the alert thresholds, when the value > alertThreshold, show the alert message
    protected Map<String, Float[]> alertThresholdMap;
    // whether the current window is included in the current moving average
    private final boolean isCurrentInMovingAvg;
    // you can also define you own number of windows to track and the alert threshold
    public Q2WindowFunction(int numWindowTrack, Map<String, Float[]> alertThresholdMap, boolean isCurrentInMovingAvg) {
        this.numWindowTrack = numWindowTrack;
        this.alertThresholdMap = alertThresholdMap;
        // formalize the threshold ranges
        for (String attr : alertThresholdMap.keySet()) {
            Float[] tempRange = alertThresholdMap.get(attr);
            tempRange = QueryUtils.assignRange(tempRange);
            alertThresholdMap.put(attr, tempRange);
        }
        this.isCurrentInMovingAvg = isCurrentInMovingAvg;
    }
    // only called once to initialize the state when the program starts
    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("Query2State", Query2State.class));
    }
    // process a window
    @Override
    public void process(Integer key, Context ctx, Iterable<T> input, Collector<String> out) throws Exception {
        // retrieve the current state
        Query2State nowState = state.value();
        if (nowState == null) {nowState = new Query2State(key, numWindowTrack);}
        // count the quantity of data items
        long count = 0;
        Map<String, Float> sumValMap = new HashMap<>();  // get the sum of the data
        for (String attr : alertThresholdMap.keySet()) {sumValMap.put(attr, 0.0F);}
        // traverse this window
        for (T tuple: input) {
            Float sumTemp;
            // display what is in the window
            if (tuple instanceof TupBloodPressure) {
                // update dBP sum
                sumTemp = sumValMap.get(TupBloodPressure.attrNameDBP);
                sumTemp += ((TupBloodPressure) tuple).dBP;
                sumValMap.put(TupBloodPressure.attrNameDBP, sumTemp);
                // update sBP sum
                sumTemp = sumValMap.get(TupBloodPressure.attrNameSBP);
                sumTemp += ((TupBloodPressure) tuple).sBP;
                sumValMap.put(TupBloodPressure.attrNameSBP, sumTemp);
            }
            else if (tuple instanceof TupGlucose) {
                sumTemp = sumValMap.get(TupGlucose.attrNameGlucose);
                sumTemp += ((TupGlucose) tuple).glucose;
                sumValMap.put(TupGlucose.attrNameGlucose, sumTemp);
            }
            out.collect("Window: " + ctx.window() + " No. " + (count + 1) + " " + tuple);
            count++;
        }
        // calculate the average values in the current window for each attribute
        Map<String, Float> nowWindowAvgMap = new HashMap<>();
        for (String attr : alertThresholdMap.keySet()) {
            // map the attribute name to its average
            nowWindowAvgMap.put(attr, sumValMap.get(attr) / count);
        }
        // maintain 3 consecutive averages if the current avg participates in the computation of moving avg
        if (isCurrentInMovingAvg) {nowState.updateTrack(nowWindowAvgMap, ctx.window());}
        // output the window info
        for (String attr : alertThresholdMap.keySet()) {
            // find the absolute value difference between the current average & moving average
            Float avgDiff = Math.abs(nowWindowAvgMap.get(attr) - nowState.getMovingAvg(attr));
            // determine if we need a warning message
            boolean isWarn = !QueryUtils.isQualified(avgDiff, alertThresholdMap.get(attr)[0], alertThresholdMap.get(attr)[1],
                                                     true, false);
            String warnOrNorm;
            // do not release a warning message if we do not have enough windows kept yet
            if (isWarn && nowState.windowTracker.size() > 1) {warnOrNorm = QueryUtils.warnBar + " ALERT! ";}
            else if ((nowState.windowTracker.size() <= 1 && isCurrentInMovingAvg) ^
                     (nowState.windowTracker.size() == 0 && !isCurrentInMovingAvg)) {
                warnOrNorm = QueryUtils.normBar + " Waiting for more windows: ";
            }
            else {warnOrNorm = QueryUtils.normBar + " Normal: ";}
            // show the difference between the average values
            out.collect(warnOrNorm + attr + ", difference threshold: " +
                    QueryUtils.rangeToString(alertThresholdMap.get(attr), true, false) +
                    ", average: " + nowWindowAvgMap.get(attr) +
                    ", current moving average: " + nowState.getMovingAvg(attr) +
                    ", difference: " + avgDiff);
        }
        out.collect(QueryUtils.windowBar + " End of window: " + ctx.window() + ", count: " + count);
        // maintain 3 consecutive averages if the current avg does not participate in the computation of moving avg
        if (!isCurrentInMovingAvg) {nowState.updateTrack(nowWindowAvgMap, ctx.window());}
        // output the average values info
        for (String attr : alertThresholdMap.keySet()) {out.collect(QueryUtils.avgBar + " Tracked window(s): " + nowState.getAvgStr(attr));}
        // write back to state
        state.update(nowState);
    }
}
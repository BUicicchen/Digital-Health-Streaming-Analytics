package clusterdata;

import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.ArrayList;
import java.util.Map;

public class Query2State extends QueryState {
    // map the name of the attribute to the average value
    protected ArrayList<Map<String, Float>> avgTracker;
    // track a certain number of consecutive windows
    protected ArrayList<Window> windowTracker;
    // how many consecutive windows to track
    protected int numWindowTrack;
    // constructor
    public Query2State(Integer key, int numWindowTrack) {
        super((key));
        avgTracker = new ArrayList<>();
        windowTracker = new ArrayList<>();
        this.numWindowTrack = numWindowTrack;
    }
    // update the tracking of N = 3 consecutive averages
    public void updateTrack(Map<String, Float> newAvgMap, Window newWindow) {
        if (avgTracker.size() == numWindowTrack) {  // the tracker list is full
            for (int i = 0; i < avgTracker.size() - 1; i++) {
                avgTracker.set(i, avgTracker.get(i + 1));
                windowTracker.set(i, windowTracker.get(i + 1));
            }
            avgTracker.set(avgTracker.size() - 1, newAvgMap);
            windowTracker.set(avgTracker.size() - 1, newWindow);
        }
        else {
            avgTracker.add(newAvgMap);
            windowTracker.add(newWindow);
        }
    }
    // get the N = 3 averages in string form
    public String getAvgStr(String attr) {
        StringBuilder avgStr = new StringBuilder();
        for (int i = 0; i < avgTracker.size(); i++) {
            avgStr.append(windowTracker.get(i)).append(" ").append(attr).append(": ").append(avgTracker.get(i).get(attr)).append(", ");
        }
        avgStr.append("Moving ").append(attr).append(" average: ").append(getMovingAvg(attr)).append(" ");
        return avgStr.toString();
    }
    // get the average of the N = 3 windows
    public Float getMovingAvg(String attr) {
        Float sumAvg = 0.0F;
        for (Map<String, Float> avgMap : avgTracker) {
            sumAvg += avgMap.get(attr);
        }
        return sumAvg/avgTracker.size();
    }
}

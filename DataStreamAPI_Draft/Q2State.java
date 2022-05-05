package clusterdata;

import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.ArrayList;

public class Q2State extends QueryState {
    // pass these buffer to data stream, consider customizing the .window() function,
    // otherwise the built jar will not work
    protected static ArrayList<Double> avgTracker = new ArrayList<>();
    protected static ArrayList<Window> windowTracker = new ArrayList<>();
    // how many consecutive windows to track
    protected int numWindowTrack;
    public Q2State() {
        super();
        numWindowTrack = 3;
    }
    public Q2State(Integer key, int numWindowTrack) {
        super((key));
        this.numWindowTrack = numWindowTrack;
    }
    // update the tracking of N = 3 consecutive averages
    public void updateTrack(Double newAvg, Window newWindow) {
        if (avgTracker.size() == numWindowTrack) {  // the tracker list is full
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
    public String getAvgStr() {
        String avgStr = "";
        for (int i = 0; i < avgTracker.size(); i++) {
            avgStr += windowTracker.get(i) + " " + avgTracker.get(i) + " \n";
        }
        avgStr += "Moving avg: " + getMovingAvg() + " ";
        return avgStr;
    }
    // get the average of the N = 3 windows
    public Double getMovingAvg() {
        Double sumAvg = 0.0;
        for (Double avgVal : avgTracker) {sumAvg += avgVal;}
        return sumAvg/avgTracker.size();
    }
}

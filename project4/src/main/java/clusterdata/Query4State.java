package clusterdata;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Query4State extends QueryState {
    // dose amount & timestamp
    protected Map<Long, ArrayList<Tuple2<Integer, Long>>> valueTimeTrackListMap;
    // determine how often insulin is given within a certain period of the start of this rise
    protected final long defaultPeriod;
    // a wait list for the data
    protected ArrayList<TupDevice> waitList;
    protected Long lastRiseTime;
    public void setLastRiseTime(Long lastRiseTime) {this.lastRiseTime = lastRiseTime;}
    // keyed constructor
    public Query4State(Integer key, long defaultPeriod) {
        super(key);
        valueTimeTrackListMap = new HashMap<>();
        this.defaultPeriod = defaultPeriod;
        waitList = new ArrayList<>();
        lastRiseTime = null;
    }
    // retrieve the last element of a list from the map
    public Tuple2<Integer, Long> getListLast(long timestamp) {
        if (valueTimeTrackListMap.get(timestamp).size() > 0) {
            return valueTimeTrackListMap.get(timestamp).get(valueTimeTrackListMap.get(timestamp).size() - 1);
        }
        return null;
    }
    // retrieve the last element of a list in general
    public Tuple2<Integer, Long> getListLast(ArrayList<Tuple2<Integer, Long>> trackList) {
        if (trackList.size() > 0) {return trackList.get(trackList.size() - 1);}
        return null;
    }
    // get the time range of the list, in a list, the last element is the newest
    public long getTimeRange(long timestamp) {
        if (valueTimeTrackListMap.get(timestamp).size() == 0) {return -1;}
        if (valueTimeTrackListMap.get(timestamp).size() == 1) {return 0;}
        return getListLast(timestamp).f1 - valueTimeTrackListMap.get(timestamp).get(0).f1;
    }
    public long getTimeRange(ArrayList<Tuple2<Integer, Long>> trackList) {
        if (trackList.size() == 0) {return -1;}
        if (trackList.size() == 1) {return 0;}
        return getListLast(trackList).f1 - trackList.get(0).f1;
    }
    // push an element to all the lists
    // logically speaking, it is impossible for multiple lists to get full at the same time
    public boolean addToAllLists(Tuple2<Integer, Long> item) {
        boolean hasAdded = false;
        for (Long mapTimestamp : valueTimeTrackListMap.keySet()) {
            //if (getTimeRange(valueTimeTrackListMap.get(timestamp)) < defaultPeriod) {
            if ((item.f1 - mapTimestamp) >= 0 && (item.f1 - mapTimestamp) <= defaultPeriod) {
                valueTimeTrackListMap.get(mapTimestamp).add(item);
                hasAdded = true;
            }
        }
        return hasAdded;
    }
    // check if we have any full lists
    // logically speaking, the first list will get full first
    // it is impossible for multiple lists to get full at the same time
    // output is in the form: <average_value, string_message>
    public Tuple2<Double, String> checkFullList() {
        if (valueTimeTrackListMap.size() > 0) {  //&& getTimeRange(0) >= defaultPeriod) {
            // retrieve the timestamp corresponding to a full list
            Long fullListTimestamp = null;
            for (long timestamp : valueTimeTrackListMap.keySet()) {
                if (getTimeRange(valueTimeTrackListMap.get(timestamp)) >= defaultPeriod) {
                    fullListTimestamp = timestamp;
                    break;
                }
            }
            if (fullListTimestamp != null) {
                Double sum = 0.0;
                for (int i = 0; i < valueTimeTrackListMap.get(fullListTimestamp).size(); i++) {
                    sum += valueTimeTrackListMap.get(fullListTimestamp).get(i).f0;
                }
                Tuple2<Double, String> output = new Tuple2<>();
                // collect the average value of the attribute of this list
                output.setField(sum / getTimeRange(valueTimeTrackListMap.get(fullListTimestamp)), 0);
                // collect the start and end timestamps of this list
                output.setField("count: " + valueTimeTrackListMap.get(fullListTimestamp).size() +
                        ", time range: [" + valueTimeTrackListMap.get(fullListTimestamp).get(0).f1 +
                        ", " + getListLast(fullListTimestamp).f1 + "]", 1);
                // remove this full list
                valueTimeTrackListMap.remove(fullListTimestamp);
                return output;
            }
            return null;
        }
        return null;
    }
}

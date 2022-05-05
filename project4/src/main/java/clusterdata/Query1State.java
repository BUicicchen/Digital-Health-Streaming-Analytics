package clusterdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class Query1State extends QueryState {
    // map the on-timer timestamp to session names
    protected Map<Long, ArrayList<String>> timerBaseSessionMap;
    // map the session names to their thresholds
    protected final Map<String, Float[]> sessionNameThreshMap;
    // collect the tuples where you deduce sessions from
    protected Map<String, PriorityQueue<TupDevice>> sessionCollectMap;
    // collect the tuples where the sessions are mapped to
    protected PriorityQueue<TupDevice> mappedCollectQueue;
    // interested attributes
    protected final String[] attributesMapped;
    // record the average from the previous session window
    // for each type of session, record their corresponding previous averages of the attributes
    protected Map<String, Map<String, Float>> prevAvgMap;
    // a message wait list used for query 4
    protected ArrayList<String> messageWaitList;
    // purge threshold, when the number of tuples is greater than this quantity, half of the priority queue will be purged
    protected static int purgeThreshold;
    public Query1State(Integer key, Map<String, Float[]> newSessionNameThreshMap, String[] attributesMapped, int newPurgeThreshold) {
        super(key);
        timerBaseSessionMap = new HashMap<>();
        sessionNameThreshMap = newSessionNameThreshMap;
        sessionCollectMap = new HashMap<String, PriorityQueue<TupDevice>>() {{
            for (String sName : sessionNameThreshMap.keySet()) {
                put(sName, new PriorityQueue<>(1, new ComparatorCustom()));
            }
        }};
        mappedCollectQueue = new PriorityQueue<>(1, new ComparatorCustom());
        this.attributesMapped = attributesMapped;
        // initialize the map for tracking previous values
        prevAvgMap = new HashMap<>();
        for (String sName : sessionNameThreshMap.keySet()) {
            Map<String, Float> mapTemp = new HashMap<>();
            for (String attr : this.attributesMapped) {
                mapTemp.put(attr, null);
            }
            prevAvgMap.put(sName, mapTemp);
        }
        purgeThreshold = newPurgeThreshold;
        messageWaitList = new ArrayList<>();
    }
    // collect a tuple
    public void collectTuple(TupDevice tuple, String sName) {
        sessionCollectMap.get(sName).add(tuple);
    }
    // purge the priority queue if applicable
    public void purgeCollectQueue() {
        if (mappedCollectQueue.size() >= purgeThreshold) {
            for (int i = 0; i < purgeThreshold/2; i++) {
                mappedCollectQueue.poll();
            }
        }
    }
}

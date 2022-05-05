package clusterdata;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

// the state dedicated to query 3 to track
// if the rise in stream 1 is followed by a rise in stream 2 within the threshold period of time
public class Query3StateRise extends QueryState {
    // each list records the value after increasing and the corresponding timestamp
    // a list element is in the form: <<old value, old timestamp>, <new value, new timestamp>>
    protected Map<String,
                  ArrayList<Tuple2<Tuple2<Float, Long>,
                                   Tuple2<Float, Long>>>> listMap;
    // if any of the lists exceed this size, the first half of this list will be removed
    protected final int sizeLim;
    // constructor
    public Query3StateRise(Integer key, String[] mapNames, int sizeLim) {
        super(key);
        listMap = new HashMap<>();
        for (String name : mapNames) {
            listMap.put(name, new ArrayList<>());
        }
        this.sizeLim = sizeLim;
    }
    // check if all the lists are non-empty
    public boolean isAllNonEmpty() {
        for (String streamName : listMap.keySet()) {
            if (listMap.get(streamName).size() == 0) {return false;}
        }
        return true;
    }
    // push a value to a particular list
    public void addToList(String listName, Tuple2<Tuple2<Float, Long>, Tuple2<Float, Long>> value) {
        // add the new value
        listMap.get(listName).add(value);
    }
    // purge a list if it gets excessively large
    public void purgeList(String listName) {
        // if the list name is not given, we purge all applicable lists
        if (listName == null) {
            for (String streamName : listMap.keySet()) {
                if (listMap.get(streamName).size() > sizeLim) {
                    listMap.get(streamName).subList(0, sizeLim / 2).clear();
                }
            }
        }
        // purge a particular tracking list if applicable
        else {
            if (listMap.get(listName).size() > sizeLim) {
                listMap.get(listName).subList(0, sizeLim / 2).clear();
            }
        }
    }
}

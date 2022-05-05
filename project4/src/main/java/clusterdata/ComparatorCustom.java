package clusterdata;

import java.util.Comparator;

// custom comparator, used for priority queue w.r.t timestamp
public class ComparatorCustom implements Comparator<TupDevice> {
    @Override
    public int compare(TupDevice item1, TupDevice item2) {
        return item1.timestamp.compareTo(item2.timestamp);
    }
}

package clusterdata;

import org.apache.flink.api.java.tuple.Tuple2;
import java.util.Comparator;

// PriorityQueue<Tuple2<Tuple2<Integer, Double>, Long>> pq = new PriorityQueue<>(1, new CompareCustom());

public class CompareCustom implements Comparator<Tuple2<Tuple2<Integer, Double>, Long>> {
    @Override
    public int compare(Tuple2<Tuple2<Integer, Double>, Long> item1, Tuple2<Tuple2<Integer, Double>, Long> item2) {
        return item1.f1.compareTo(item2.f1);
    }
}

package clusterdata;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public class QueryWindowQueue<T> extends ProcessWindowFunction<T, T, Integer, TimeWindow> {
    @Override
    public void process(Integer key, Context context, Iterable<T> input, Collector<T> out) throws Exception {
        PriorityQueue<TupDevice> queue = new PriorityQueue<>(1, new ComparatorCustom());
        // add all input events to priority queue to sort them
        for (T tuple : input) {
            if (tuple instanceof TupDevice) {
                queue.add((TupDevice) tuple);
            }
        }
        // output the events in this window
        while (queue.size() > 0) {
            TupDevice output = queue.poll();
            // noinspection unchecked
            out.collect((T) output);
        }
    }
}

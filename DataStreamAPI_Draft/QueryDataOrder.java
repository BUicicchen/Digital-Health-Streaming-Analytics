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

// maintain the order of a stream in terms of timestamp
// datatype schema: key, input, output
public class QueryDataOrder extends KeyedProcessFunction<Integer, Tuple2<Integer, Double>, Tuple2<Integer, Double>> {
    // using priority queue can auto-sort the collected events by timestamp
    private ValueState<PriorityQueue<Tuple2<Tuple2<Integer, Double>, Long>>> state;
    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("sorted-events",
                TypeInformation.of(new TypeHint<PriorityQueue<Tuple2<Tuple2<Integer, Double>, Long>>>() {})));
    }
    @Override
    public void processElement(Tuple2<Integer, Double> event, Context ctx,
                               Collector<Tuple2<Integer, Double>> out) throws Exception {
        TimerService timerService = ctx.timerService();
        if (ctx.timestamp() > timerService.currentWatermark()) {
            PriorityQueue<Tuple2<Tuple2<Integer, Double>, Long>> pQueue = state.value();
            if (pQueue == null) {
                pQueue = new PriorityQueue<>(1, new CompareCustom());
            }
            // collect the data along with the timestamp
            Tuple2<Tuple2<Integer, Double>, Long> queuedEvent = Tuple2.of(event, ctx.timestamp());
            pQueue.add(queuedEvent);
            System.out.println("add " + ctx.timestamp());
            state.update(pQueue);
            //if (pQueue.size() == 10) {
            timerService.registerEventTimeTimer(ctx.timestamp());
            // retrieve the max timestamp of this range
            /*long tempMaxTime = -1;
            for (Tuple2<Tuple2<Integer, Double>, Long> item : pQueue) {
                tempMaxTime = item.f1;
            }
            System.out.println("tempMaxTime " + tempMaxTime);
            timerService.registerEventTimeTimer(tempMaxTime);*/
            //}
            //System.out.println("registerEventTimeTimer " + ctx.timestamp());
        }
    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, Double>> out) throws Exception {
        //System.out.println("onTimer " + ctx.timestamp());
        PriorityQueue<Tuple2<Tuple2<Integer, Double>, Long>> queue = state.value();
        Long watermark = ctx.timerService().currentWatermark();
        //System.out.println("watermark " + watermark);
        Tuple2<Tuple2<Integer, Double>, Long> head = queue.peek();
        //while (head != null && head.f1 <= watermark) {
        while (head != null) {
            System.out.println("order " + head.f1 + " " + head.f0);
            out.collect(head.f0);
            queue.remove(head);
            head = queue.peek();
        }
    }
}

package clusterdata;

import org.apache.flink.api.java.tuple.Tuple2;
import java.util.Stack;

// the state dedicated to query 3 to track
// if the rise in stream 1 is followed by a rise in stream 2 within the threshold period of time
public class Q3StateRise extends QueryState {
    // each stack records the value after increasing and the corresponding timestamp
    // form: <<old value, old timestamp>, <new value, new timestamp>>
    protected Stack<Tuple2<Tuple2<Double, Long>, Tuple2<Double, Long>>> stack1;
    protected Stack<Tuple2<Tuple2<Double, Long>, Tuple2<Double, Long>>> stack2;
    // default constructor
    public Q3StateRise() {
        super();
        stack1 = new Stack<>();
        stack2 = new Stack<>();
    }
    public Tuple2<Tuple2<Double, Long>, Tuple2<Double, Long>> getStack1Last() {
        if (stack1.size() > 0) {return stack1.get(stack1.size() - 1);}
        return null;
    }
    public Tuple2<Tuple2<Double, Long>, Tuple2<Double, Long>> getStack2Last() {
        if (stack2.size() > 0) {return stack2.get(stack2.size() - 1);}
        return null;
    }
}

package clusterdata;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Stack;

public class Q4StatePeriod extends QueryState {
    protected ArrayList<Stack<Tuple2<Double, Long>>> stackList;
    //protected Stack<Tuple2<Double, Long>> stack;
    // determine how often insulin is given within a certain period of the start of this rise
    protected long defaultPeriod;
    protected boolean isOpen;
        public void open() {isOpen = true;}
        public void close() {isOpen = false;}
    // default constructor
    public Q4StatePeriod() {
        super();
        //stack = new Stack<>();
        stackList = new ArrayList<>();
        isOpen = false;
        defaultPeriod = 0;
    }
    // keyed constructor
    public Q4StatePeriod(Integer key, long defaultPeriod) {
        super(key);
        //stack = new Stack<>();
        stackList = new ArrayList<>();
        isOpen = false;
        this.defaultPeriod = defaultPeriod;
    }
    // retrieve the last element of the stack
    public Tuple2<Double, Long> getStackLast(int stackNum) {
        if (stackList.get(stackNum).size() > 0) {return stackList.get(stackNum).get(stackList.get(stackNum).size() - 1);}
        return null;
    }
    public Tuple2<Double, Long> getStackLast(Stack<Tuple2<Double, Long>> stack) {
        if (stack.size() > 0) {return stack.get(stack.size() - 1);}
        return null;
    }
    // get the time range of the stack
    // in a stack, the last element is the newest
    public long getTimeRange(int stackNum) {
        if (stackList.get(stackNum).size() == 0) {return -1;}
        if (stackList.get(stackNum).size() == 1) {return 0;}
        return getStackLast(stackNum).f1 - stackList.get(stackNum).get(0).f1;
    }
    public long getTimeRange(Stack<Tuple2<Double, Long>> stack) {
        if (stack.size() == 0) {return -1;}
        if (stack.size() == 1) {return 0;}
        return getStackLast(stack).f1 - stack.get(0).f1;
    }
    // push an element to all the stacks
    public void pushAll(Tuple2<Double, Long> item) {
        for (Stack<Tuple2<Double, Long>> stack : stackList) {
            if (getTimeRange(stack) < defaultPeriod) {
                stack.push(item);
            }
        }
    }
    // check if we have any full stacks
    // logically speaking, the first stack will get full first
    // it is impossible for 2 stacks to get full at the same time
    public Tuple2<Double, String> checkFullStack() {
        if (stackList.size() > 0 && getTimeRange(0) >= defaultPeriod) {
            Double sum = 0.0;
            for (int i = 0; i < stackList.get(0).size(); i++) {
                sum += stackList.get(0).get(i).f0;
            }
            Tuple2<Double, String> output = new Tuple2<>();
            // collect the average value of the attribute of this stack
            output.setField(sum/stackList.get(0).size(), 0);
            // collect the start and end timestamps of this stack
            String startTime = "[" + stackList.get(0).get(0).f1 + ", ";
            String endTime = getStackLast(0).f1 + "]";
            output.setField(startTime + endTime, 1);
            // remove this full stack
            stackList.remove(0);
            return output;
        }
        return null;
    }
}

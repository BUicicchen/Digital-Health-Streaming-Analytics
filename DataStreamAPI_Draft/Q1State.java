package clusterdata;

import org.apache.flink.api.java.tuple.Tuple2;
// the custom state dedicated for query 1, all the attributes have their accessors and mutators
// used for handling custom session windows
public class Q1State extends QueryState {
    // the quantity of tuples so far in this "window"
    protected int count;
        public void incrCount() {count += 1;}
    // the sum of the attribute of interest from the tuples in this window so far
    // here we are connecting 2 streams, where the 1st stream has custom session window applied to it
    // we need to obtain the average values of the data from the 2nd stream that falls into those
    // session windows respectively
    protected Float sumVal;
        public void incrSum(Float added) {sumVal += added;}
    // record the average from the previous session window
    protected Float prevAvg;
    // the start time of this "window", in terms of event time instead of the clock time
    protected long startTime;
        public void setStartTime(long newStart) {startTime = newStart;}
    // the event timestamp of the latest tuple of this window
    protected long latestTime;
        public void setLatestTime(long newLate) {latestTime = newLate;}
    // test on timer state
    protected long lastTimeStamp;
        public void setLastTimeStamp(long lastTimeStamp) {this.lastTimeStamp = lastTimeStamp;}
    // the value of the concerned attribute of the latest belonging tuple
    protected Float currValue;
        public void setCurrValue(Float newVal) {currValue = newVal;}
    // whether the current window is open
    protected boolean isOpen;
        public void openWindow() {isOpen = true;}
        public void closeWindow(boolean tackPrev) {
            isOpen = false;
            // record the previous average
            if (tackPrev && count > 0) {prevAvg = getAvg();}
            // clear the current records
            count = 0;
            sumVal = 0.0F;
            //startTime = -1;
            // the latest time will not be cleared since it is still useful,
            // and it will be updated later anyway
            currValue = null;
        }
    public Q1State() {
        super();
        count = 0;
        sumVal = 0.0F;
        prevAvg = null;
        startTime = -1;
        latestTime = -1;
        currValue = null;
        isOpen = false;
    }
    public Q1State(Integer key) {
        super(key);
        count = 0;
        sumVal = 0.0F;
        prevAvg = null;
        startTime = -1;
        latestTime = -1;
        currValue = null;
        isOpen = false;
    }
    public Q1State(Integer key, int count, Float sumVal, long startTime, long latestTime, Float currValue) {
        super(key);
        this.count = count;
        this.sumVal = sumVal;
        prevAvg = null;
        this.startTime = startTime;
        this.latestTime = latestTime;
        this.currValue = currValue;
        isOpen = false;
    }
    // calculate the average value
    public Float getAvg() {
        return sumVal/(count);
    }
    // get both the key and the average value
    public Tuple2<Integer, Float> getKeyedAvg() {
        return Tuple2.of(key, getAvg());
    }
}

package clusterdata;

import org.apache.flink.api.java.tuple.Tuple2;
// a custom state, used for calculating the average of a flexible custom "session" window
// all the attributes have their accessor and mutator
public class CustomState {
    // the key of this state, corresponding to the input
    protected Integer key;
        public void setKey(Integer newKey) {key = newKey;}
        public Integer getKey() {return key;}
    // the quantity of tuples so far in this "window"
    protected int count;
        public void setCount(int newCount) {count = newCount;}
        public void incrCount() {count += 1;}
        public int getCount() {return count;}
    // the sum of the attribute of interest from the tuples in this window so far
    protected Double sumVal;
        public void setSum(Double newSum) {sumVal = newSum;}
        public void incrSum(Double added) {sumVal += added;}
    // the start time of this "window", in terms of event time instead of the clock time
    protected long startTime;
        public void setStartTime(long newStart) {startTime = newStart;}
        public long getStartTime() {return startTime;}
    // the event timestamp of the latest tuple of this window
    protected long latestTime;
        public void setLatestTime(long newLate) {latestTime = newLate;}
        public long getLatestTime() {return latestTime;}
    // test on timer state
    protected long lastTimeStamp;
        public void setLastTimeStamp(long lastTimeStamp) {this.lastTimeStamp = lastTimeStamp;}
        public long getLastTimeStamp() {return lastTimeStamp;}
    // the value of the concerned attribute of the latest belonging tuple
    protected Double currValue;
        public void setCurrValue(Double newVal) {currValue = newVal;}
        public Double getCurrValue() {return currValue;}
    public CustomState() {
        key = null;
        count = 0;
        sumVal = 0.0;
        startTime = -1;
        latestTime = -1;
        currValue = null;
    }
    public CustomState(Integer key, int count, Double sumVal, long startTime, long latestTime, Double currValue) {
        this.key = key;
        this.count = count;
        this.sumVal = sumVal;
        this.startTime = startTime;
        this.latestTime = latestTime;
        this.currValue = currValue;
    }
    // calculate the average value
    public Double getAvg() {
        return sumVal/((double) count);
    }
    // get both the key and the average value
    public Tuple2<Integer, Double> getKeyedAvg() {
        return Tuple2.of(key, getAvg());
    }
}

package clusterdata;

// the state dedicated to query 3
public class Query3StateStream extends QueryState {
    // keep track on the rise of the value of this stream
    protected Float valueTrack;
        public void setValueTrack(Float valueTrack) {this.valueTrack = valueTrack;}
    // keep track on the corresponding timestamp of this stream
    protected Long timestampTrack;
        public void setTimestampTrack(Long timestampTrack) {this.timestampTrack = timestampTrack;}
    // last seen timestamp
    protected Long lastSeenTimestamp;
        public void setLastSeenTimestamp(Long lastSeenTimestamp) {this.lastSeenTimestamp = lastSeenTimestamp;}
    protected Long count;
        public void addCount() {count += 1;}
    // keyed constructor
    public Query3StateStream(Integer key) {
        super(key);
        valueTrack = null;
        timestampTrack = -1L;
        lastSeenTimestamp = -1L;
        count = 0L;
    }
}

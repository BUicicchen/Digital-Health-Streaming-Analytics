package clusterdata;

// the state dedicated to query 3
public class Q3StateStream extends QueryState {
    // keep track on the rise of the value of this stream
    protected Double valueTrack;
        public void setValueTrack(Double valueTrack) {this.valueTrack = valueTrack;}
    // keep track on the corresponding timestamp of this stream
    protected Long timestampTrack;
        public void setTimestampTrack(Long timestampTrack) {this.timestampTrack = timestampTrack;}
    // default constructor
    public Q3StateStream() {
        super();
        valueTrack = null;
        timestampTrack = -1L;
    }
    // keyed constructor
    public Q3StateStream(Integer key) {
        super(key);
        valueTrack = null;
        timestampTrack = -1L;
    }
}

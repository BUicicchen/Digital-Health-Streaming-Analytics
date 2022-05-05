package clusterdata;
// the super class of all query states
public class QueryState {
    // the key of this state, corresponding to the input
    protected Integer key;
        public void setKey(Integer newKey) {key = newKey;}
        public Integer getKey() {return key;}
    public QueryState() {
        key = null;
    }
    public QueryState(Integer key) {
        this.key = key;
    }
}

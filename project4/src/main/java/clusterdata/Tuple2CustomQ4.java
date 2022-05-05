package clusterdata;

// a custom Tuple2 object dedicated for Query 4 in order to solve the type casting problem
// it carries a data tuple and a string message
public class Tuple2CustomQ4 {
    protected TupDevice dataTuple;
    protected String message;
    public Tuple2CustomQ4(TupDevice dataTuple, String message) {
        this.dataTuple = dataTuple;
        this.message = message;
    }
    public void setDataTuple(TupDevice dataTuple) {
        this.dataTuple = dataTuple;
    }
    public void setMessage(String message) {
        this.message = message;
    }
}

package clusterdata;

import org.apache.flink.util.Collector;
// custom Collector class
public class CollectorCustom<T> implements Collector<T> {
    protected String message;
    public CollectorCustom() {message = "";}
    @Override
    public void collect(T item) {message = (String) item;}
    @Override
    public void close() {}
    public void collectAdd(String newMessage) {
        message += newMessage;
    }
}

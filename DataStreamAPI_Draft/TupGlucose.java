package clusterdata;
// <deviceID: int, minute: long, glucose (mg/dl): int>
// the minute attribute, I suppose it is the timestamp
public class TupGlucose extends TupDevice {
    protected Integer glucose;  // milligram per deciliter
    protected static final String deviceName = "Glucose";
    public TupGlucose() {
        super();
        glucose = null;
    }
    public TupGlucose(Integer deviceID, Integer patientID, Long timestamp, Integer glucose) {
        super(deviceID, patientID, timestamp);
        this.glucose = glucose;
    }
    // convert to string
    @Override
    public String toString() {
        return (super.toString() + ", glucose: " + glucose);
    }
}

package clusterdata;
// <deviceID: int, minute: long, glucose (mg/dl): int>
// the minute attribute, I suppose it is the timestamp
public class TupGlucose extends TupDevice {
    protected Integer glucose;  // milligram per deciliter
    protected static final String deviceName = "Glucose Monitor";
    protected static final String attrNameGlucose = "glucose";
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
        return (deviceName + " " + super.toString() + ", " + attrNameGlucose + ": " + glucose);
    }
}

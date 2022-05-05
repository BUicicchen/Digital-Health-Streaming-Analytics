package clusterdata;
// Glucose monitor
// <deviceID: int, minute: long, glucose (mg/dl): int>
public class DevGlucose extends Device {
    // default constructor
    public DevGlucose() {super();}
    // parameterized constructor, device ID only
    public DevGlucose(Integer deviceID) {super(deviceID);}
    // parameterized constructor
    public DevGlucose(Integer deviceID, Integer patientID) {super(deviceID, patientID);}
    // yield one tuple of the custom type
    public TupGlucose yield(Integer glucose) {
        return (new TupGlucose(this.deviceID, this.patientID, glucose));
    }
}

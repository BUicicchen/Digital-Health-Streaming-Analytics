package clusterdata;
// Smartwatch
// <deviceID: int, timestamp: long, steps_since_last: int, mean_heart_rate: float>
public class DevSmartwatch extends Device {
    // default constructor
    public DevSmartwatch() {super();}
    // parameterized constructor, device ID only
    public DevSmartwatch(Integer deviceID) {super(deviceID);}
    // parameterized constructor
    public DevSmartwatch(Integer deviceID, Integer patientID) {super(deviceID, patientID);}
    // yield one tuple of the custom type
    public TupSmartwatch yield(Integer stepsSinceLast, Float meanHeartRate) {
        return (new TupSmartwatch(this.deviceID, this.patientID, stepsSinceLast, meanHeartRate));
    }
}

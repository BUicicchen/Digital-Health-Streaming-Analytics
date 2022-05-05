package clusterdata;

// Fitbit
// <deviceID: int, timestamp: long, METs: int, intensity: int, steps: int, heart_rate: int>
public class DevFitbit extends Device {
    // default constructor
    public DevFitbit() {super();}
    // parameterized constructor, device ID only
    public DevFitbit(Integer deviceID) {super(deviceID);}
    // parameterized constructor
    public DevFitbit(Integer deviceID, Integer patientID) {super(deviceID, patientID);}
    // yield one tuple of the custom type
    public TupFitbit yield(Integer METs, Integer intensity, Integer steps, Integer heartRate) {
        return (new TupFitbit(this.deviceID, this.patientID, METs, intensity, steps, heartRate));
    }
}

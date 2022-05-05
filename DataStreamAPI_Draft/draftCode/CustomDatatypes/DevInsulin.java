package clusterdata;
// Insulin tracker
// <deviceID: int, timestamp: long, dose_amount: int>
public class DevInsulin extends Device {
    // default constructor
    public DevInsulin() {super();}
    // parameterized constructor, device ID only
    public DevInsulin(Integer deviceID) {super(deviceID);}
    // parameterized constructor
    public DevInsulin(Integer deviceID, Integer patientID) {super(deviceID, patientID);}
    // yield one tuple of the custom type
    public TupInsulin yield(Integer doseAmount) {
        return (new TupInsulin(this.deviceID, this.patientID, doseAmount));
    }
}

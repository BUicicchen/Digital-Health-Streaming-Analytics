package clusterdata;
// Blood pressure monitor
// <deviceID: int, timestamp: long, dBP: int, sBP: int>
public class DevBloodPressure extends Device {
    public DevBloodPressure() {super();}
    public DevBloodPressure(Integer deviceID) {
        super(deviceID);
    }
    public DevBloodPressure(Integer deviceID, Integer patientID) {
        super(deviceID, patientID);
    }
    // yield one tuple of the custom type
    public TupBloodPressure yield(Integer dBP, Integer sBP) {
        return (new TupBloodPressure(this.deviceID, this.patientID, dBP, sBP));
    }
}

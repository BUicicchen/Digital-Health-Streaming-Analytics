package clusterdata;

// the super-class for all devices
// use this custom datatype instead of the default tuples for flexibility and specialization purposes
public abstract class Device {
    protected Integer deviceID;
    protected Integer patientID;
    // default constructor
    public Device() {
        deviceID = null;
        patientID = null;
    }
    // only the device ID can be defined
    public Device(Integer deviceID) {
        this.deviceID = deviceID;
        this.patientID = null;
    }
    // both device ID and patient ID can be defined
    public Device(Integer deviceID, Integer patientID) {
        this.deviceID = deviceID;
        this.patientID = patientID;
    }
    // assign the patient ID to this device
    public void setPatientID(Integer patientID) {
        this.patientID = patientID;
    }
}

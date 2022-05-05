package clusterdata;

// the abstract class for a tuple produced by an abstract device
public abstract class TupDevice {
    protected Integer deviceID;
    protected Integer patientID;
    protected Long timestamp;
    // default constructor
    public TupDevice() {
        deviceID = null;
        patientID = null;
        timestamp = null;
    }
    // both device ID and patient ID can be defined
    public TupDevice(Integer deviceID, Integer patientID, Long timestamp) {
        this.deviceID = deviceID;
        this.patientID = patientID;
        this.timestamp = timestamp;
    }
    // convert to string
    @Override
    public String toString() {
        return ("Timestamp: " + timestamp + " Device ID: " + deviceID + " Patient ID: " + patientID);
    }
}

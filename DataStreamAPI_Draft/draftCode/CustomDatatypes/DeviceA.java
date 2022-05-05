package clusterdata;

import org.apache.flink.api.java.tuple.Tuple2;

// a test example device, intended to replace the tuple datatype
public class DeviceA extends Device{
    // default constructor
    public DeviceA() {
        super();
    }
    // parameterized constructor, device ID only
    public DeviceA(Integer deviceID) {super(deviceID);}
    // parameterized constructor
    public DeviceA(int deviceID, int patientID) {
        super(deviceID, patientID);
    }
    // yield one tuple of the custom type
    public TupDeviceA yield(Double attrA, Integer attrB) {
        return (new TupDeviceA(this.deviceID, this.patientID, attrA, attrB));
    }
}

package clusterdata;

// the data type (custom "tuple") yielded by Fitbit
// <deviceID: int, timestamp: long, METs: int, intensity: int, steps: int, heart_rate: int>
public class TupFitbit extends TupDevice {
    protected Integer METs;  // metabolic equivalent of task, 1 MET = the amount of energy used while sitting quietly
    protected Integer intensity;
    protected Integer steps;
    protected Integer heartRate;
    protected static final String deviceName = "Fitbit";
    protected static final String attrNameMETs = "METs";
    protected static final String attrNameIntensity = "intensity";
    protected static final String attrNameSteps = "steps";
    protected static final String attrNameHeartRate = "heart rate";
    public TupFitbit() {
        super();
        METs = null;
        intensity = null;
        steps = null;
        heartRate = null;
    }
    public TupFitbit(Integer deviceID, Integer patientID, Long timestamp,
                     Integer METs, Integer intensity, Integer steps, Integer heartRate) {
        super(deviceID, patientID, timestamp);
        this.METs = METs;
        this.intensity = intensity;
        this.steps = steps;
        this.heartRate = heartRate;
    }
    // convert to string
    @Override
    public String toString() {
        return (deviceName + " " + super.toString() + ", " + attrNameMETs + ": " + METs + ", " + attrNameIntensity + ": " +
                intensity + ", " + attrNameSteps + ": " + steps + ", " + attrNameHeartRate + ": " + heartRate);
    }
}

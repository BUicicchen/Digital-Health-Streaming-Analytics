package clusterdata;

// the data type (custom "tuple") yielded by Fitbit
// <deviceID: int, timestamp: long, METs: int, intensity: int, steps: int, heart_rate: int>
public class TupFitbit extends TupDevice {
    protected Integer METs;  // metabolic equivalent of task, 1 MET = the amount of energy used while sitting quietly
    protected Integer intensity;
    protected Integer steps;
    protected Integer heartRate;
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
        return (super.toString() + " METs: " + METs + " intensity: " + intensity +
                " steps: " + steps + " heart rate: " + heartRate);
    }
}

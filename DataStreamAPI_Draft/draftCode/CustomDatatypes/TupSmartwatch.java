package clusterdata;
// <deviceID: int, timestamp: long, steps_since_last: int, mean_heart_rate: float>
public class TupSmartwatch extends TupDevice {
    protected Integer stepsSinceLast;
    protected Float meanHeartRate;
    // default constructor
    public TupSmartwatch() {
        super();
        stepsSinceLast = null;
        meanHeartRate = null;
    }
    // parameterized constructor
    public TupSmartwatch(Integer deviceID, Integer patientID, Long timestamp, Integer stepsSinceLast, Float meanHeartRate) {
        super(deviceID, patientID, timestamp);
        this.stepsSinceLast = stepsSinceLast;
        this.meanHeartRate = meanHeartRate;
    }
    // convert to string
    @Override
    public String toString() {
        return (super.toString() + " steps since last: " + stepsSinceLast + " mean heart rate: " + meanHeartRate);
    }
}

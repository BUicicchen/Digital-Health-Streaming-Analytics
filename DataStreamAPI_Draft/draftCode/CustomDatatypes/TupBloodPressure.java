package clusterdata;

// the data type (custom "tuple") yielded by the blood pressure monitor
// <deviceID: int, timestamp: long, dBP: int, sBP: int>
public class TupBloodPressure extends TupDevice {
    protected Integer dBP;  // diastolic blood pressure, widening of heart chambers
    protected Integer sBP;  // systolic blood pressure, contraction of heart chambers
    // default constructor
    public TupBloodPressure() {super(); dBP = null; sBP = null;}
    // parameterized constructor
    public TupBloodPressure(Integer deviceID, Integer patientID, Long timestamp, Integer dBP, Integer sBP) {
        super(deviceID, patientID, timestamp);
        this.dBP = dBP;
        this.sBP = sBP;
    }
    // convert to string
    @Override
    public String toString() {
        return (super.toString() + " dBP: " + dBP + " sBP: " + sBP);
    }
}

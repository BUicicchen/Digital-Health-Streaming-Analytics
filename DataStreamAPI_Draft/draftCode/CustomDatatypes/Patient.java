package clusterdata;
// the class for a patient
// including all the devices attached to the patient
public class Patient {
    protected Integer patientID;
    protected DevSmartwatch smartwatch;
    protected DevBloodPressure bloodPressure;
    protected DevGlucose glucoseMonitor;
    protected DevFitbit fitbit;
    protected DevInsulin insulinTracker;
    // default constructor
    public Patient() {
        patientID = null;
        smartwatch = null;
        bloodPressure = null;
        glucoseMonitor = null;
        fitbit = null;
        insulinTracker = null;
    }
    // a constructor where you can determine which devices are used and which devices are not
    public Patient(Integer patientID, DevSmartwatch smartwatch, DevBloodPressure bloodPressure,
                   DevGlucose glucoseMonitor, DevFitbit fitbit, DevInsulin insulinTracker) {
        this.patientID = patientID;
        this.smartwatch = smartwatch;
        if (this.smartwatch != null) {this.smartwatch.setPatientID(this.patientID);}
        this.bloodPressure = bloodPressure;
        if (this.bloodPressure != null) {this.bloodPressure.setPatientID(this.patientID);}
        this.glucoseMonitor = glucoseMonitor;
        if (this.glucoseMonitor != null) {this.glucoseMonitor.setPatientID(this.patientID);}
        this.fitbit = fitbit;
        if (this.fitbit != null) {this.fitbit.setPatientID(this.patientID);}
        this.insulinTracker = insulinTracker;
        if (this.insulinTracker != null) {this.insulinTracker.setPatientID(this.patientID);}
    }
}

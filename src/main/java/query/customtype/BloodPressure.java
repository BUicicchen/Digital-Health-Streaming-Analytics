package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class BloodPressure {

    @JsonProperty("patientID")
    private String patientID;

    @JsonProperty("deviceID")
    private String deviceID;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("dBP")
    private String dBP;

    @JsonProperty("sBP")
    private String sBP;

    public BloodPressure(){}

    public BloodPressure(String patientID, String deviceID, String timestamp, String dBP, String sBP){
        this.patientID = patientID;
        this.deviceID = deviceID;
        this.timestamp = timestamp;
        this.dBP = dBP;
        this.sBP = sBP;
    }

    public String getPatientID(){return patientID;}

    public String getDeviceID(){return deviceID;}

    public String getTimestamp(){return timestamp;}

    public String getDBP(){return dBP;}

    public String getSBP(){return sBP;}

    public void setPatientID(String patientID){this.patientID = patientID;}

    public void setDeviceID(String deviceID){this.deviceID = deviceID;}

    public void setTimestamp(String timestamp){this.timestamp = timestamp;}

    public void setDBP(String dBP){this.dBP = dBP;}

    public void setSBP(String sBP){this.sBP = sBP;}
}

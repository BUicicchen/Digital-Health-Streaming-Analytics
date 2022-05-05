package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Fitbit {

    @JsonProperty("patientID")
    private String patientID;

    @JsonProperty("deviceID")
    private String deviceID;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("METs")
    private String METs;

    @JsonProperty("intensity")
    private String intensity;

    @JsonProperty("steps")
    private String steps;

    @JsonProperty("heart_rate")
    private String heart_rate;

    public Fitbit(){}

    public Fitbit(String patientID, String deviceID, String timestamp, String METs, String intensity, String steps, String heart_rate){
        this.patientID = patientID;
        this.deviceID = deviceID;
        this.timestamp = timestamp;
        this.METs = METs;
        this.intensity = intensity;
        this.steps = steps;
        this.heart_rate = heart_rate;
    }

    public String getPatientID(){return patientID;}

    public String getDeviceID(){return deviceID;}

    public String getTimestamp(){return timestamp;}

    public String getMETs(){return METs;}

    public String getIntensity(){return intensity;}

    public String getSteps(){return steps;}

    public String getHeartRate(){return heart_rate;}

    public void setPatientID(String patientID){this.patientID = patientID;}

    public void setDeviceID(String deviceID){this.deviceID = deviceID;}

    public void setTimestamp(String timestamp){this.timestamp = timestamp;}

    public void setMETs(String METs){this.METs = METs;}

    public void setIntensity(String intensity){this.intensity = intensity;}

    public void setSteps(String steps){this.steps = steps;}

    public void setHeartRate(String heart_rate){this.heart_rate = heart_rate;}

}

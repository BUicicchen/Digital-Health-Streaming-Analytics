package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Smartwatch {

    @JsonProperty("patientID")
    private String patientID;

    @JsonProperty("deviceID")
    private String deviceID;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("steps_since_last")
    private String steps_since_last;

    @JsonProperty("mean_heart_rate")
    private String mean_heart_rate;

    public Smartwatch(){}

    public Smartwatch(String patientID, String deviceID, String timestamp, String steps_since_last, String mean_heart_rate){
        this.patientID = patientID;
        this.deviceID = deviceID;
        this.timestamp = timestamp;
        this.steps_since_last = steps_since_last;
        this.mean_heart_rate = mean_heart_rate;
    }

    public String getPatientID(){return patientID;}

    public String getDeviceID(){return deviceID;}

    public String getTimestamp(){return timestamp;}

    public String getStepsSinceLast(){return steps_since_last;}

    public String getMeanHeartBeat(){return mean_heart_rate;}

    public void setPatientID(String patientID){this.patientID = patientID;}

    public void setDeviceID(String deviceID){this.deviceID = deviceID;}

    public void setTimestamp(String timestamp){this.timestamp = timestamp;}

    public void setStepsSinceLast(String stepsSinceLast){this.steps_since_last = stepsSinceLast;}

    public void setMeanHeartBeat(String meanHeartBeat){this.mean_heart_rate = meanHeartBeat;}

}

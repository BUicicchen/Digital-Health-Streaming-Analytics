package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Glucose {

    @JsonProperty("patientID")
    private String patientID;

    @JsonProperty("deviceID")
    private String deviceID;

    @JsonProperty("minute")
    private String minute;

    @JsonProperty("glucose")
    private String glucose;

    public Glucose(){}

    public Glucose(String patientID, String deviceID, String minute, String glucose){
        this.patientID = patientID;
        this.deviceID = deviceID;
        this.minute = minute;
        this.glucose = glucose;
    }

    public String getPatientID(){return patientID;}

    public String getDeviceID(){return deviceID;}

    public String getMinute(){return minute;}

    public String getGlucose(){return glucose;}

    public void setPatientID(String patientID){this.patientID = patientID;}

    public void setDeviceID(String deviceID){this.deviceID = deviceID;}

    public void setMinute(String minute){this.minute = minute;}

    public void setGlucose(String glucose){this.glucose = glucose;}
}

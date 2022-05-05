package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Insulin {

    @JsonProperty("patientID")
    private String patientID;

    @JsonProperty("deviceID")
    private String deviceID;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("doseAmount")
    private String doseAmount;

    public Insulin(){}

    public Insulin(String patientID, String deviceID, String timestamp, String doseAmount){
        this.patientID = patientID;
        this.deviceID = deviceID;
        this.timestamp = timestamp;
        this.doseAmount = doseAmount;
    }

    public String getPatientID(){return patientID;}

    public String getDeviceID(){return deviceID;}

    public String getTimestamp(){return timestamp;}

    public String getDoseAmount(){return doseAmount;}

    public void setPatientID(String patientID){this.patientID = patientID;}

    public void setDeviceID(String deviceID){this.deviceID = deviceID;}

    public void setTimestamp(String timestamp){this.timestamp = timestamp;}

    public void setDoseAmount(String doseAmount){this.doseAmount = doseAmount;}
}

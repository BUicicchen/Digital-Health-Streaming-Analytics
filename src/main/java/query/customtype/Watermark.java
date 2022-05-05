package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Watermark {

    @JsonProperty("watermark")
    private String WaterMark;

    @JsonProperty("patientID")
    private String patientID;

    @JsonProperty("deviceID")
    private String deviceID;

    public Watermark(){}

    public Watermark(String watermark,String patientID, String deviceID){
        this.WaterMark = watermark;
        this.patientID = patientID;
        this.deviceID = deviceID;
    }

    public String getWatermark(){return WaterMark;}

    public String getPatientID(){return patientID;}

    public String getDeviceID(){return deviceID;}

    public void setWatermark(String ts){this.WaterMark = ts;}

    public void setPatientID(String patientID){this.patientID = patientID;}

    public void setDeviceID(String deviceID){this.deviceID = deviceID;}

}

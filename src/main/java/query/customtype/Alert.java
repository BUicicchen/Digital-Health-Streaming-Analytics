package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Alert {

    public enum AlertType{
        D_BLOOD_PRESSURE,
        S_BLOOD_PRESSURE,
        GLUCOSE,
        PATTERN
    }

    @JsonProperty("alert_type")
    private AlertType alerttype;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("patientID")
    private String patientID;

    public Alert(){}

    public AlertType getAlertType(){return alerttype;}

    public String getTimestamp(){return timestamp;}

    public String getPatientID(){return patientID;}

    public void setAlertType(AlertType at){this.alerttype = at;}

    public void setTimestamp(String ts){this.timestamp = ts;}

    public void setPatientID(String patientID){this.patientID = patientID;}
}

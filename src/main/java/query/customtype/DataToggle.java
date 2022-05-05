package query.customtype;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class DataToggle {


    public enum ToggleType{
        BLOODPRESSURE,
        GLUCOSE,
        SMARTWATCH,
        FITBIT
    }

    public enum ToggleValue{
        ON,
        OFF
    }

    @JsonProperty("toggle_type")
    private ToggleType toggletype;

    @JsonProperty("toggle_value")
    private ToggleValue togglevalue;

    public DataToggle(){}

    public DataToggle(ToggleType toggletype, ToggleValue togglevalue){
        this.toggletype = toggletype;
        this.togglevalue = togglevalue;
    }

    public ToggleType getToggleType(){return this.toggletype;}

    public ToggleValue getToggleValue(){return this.togglevalue;}

    public void setToggleType(ToggleType toggletype){this.toggletype = toggletype;}

    public void setToggleValue(ToggleValue togglevalue){this.togglevalue = togglevalue;}
}

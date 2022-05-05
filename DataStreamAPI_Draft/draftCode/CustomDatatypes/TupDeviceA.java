package clusterdata;
// the datatype yielded by test device A
public class TupDeviceA extends TupDevice {
    protected Double attrA;
        public void setAttrA(Double attrA) {this.attrA = attrA;}
    protected Integer attrB;
        public void setAttrB(Integer attrB) {this.attrB = attrB;}
    public TupDeviceA() {
        super();
        attrA = null;
        attrB = null;
    }
    public TupDeviceA(Integer deviceID, Integer patientID, Double attrA, Integer attrB) {
        super(deviceID, patientID);
        this.attrA = attrA;
        this.attrB = attrB;
    }
    // convert to string
    @Override
    public String toString() {
        return (super.toString() + " attrA: " + attrA + " attrB: " + attrB);
    }
}

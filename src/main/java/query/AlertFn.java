package query;

import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;
import query.customtype.Alert;
import query.customtype.EgressRecord;

import java.util.concurrent.CompletableFuture;

import static query.customtype.CustomTypes.ALERT_TYPE;
import static query.customtype.CustomTypes.EGRESS_RECORD_JSON_TYPE;

final class AlertFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameOf("query.fns","Alerts");
    static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME).withSupplier(AlertFn::new).build();
    private static final TypeName KAFKA_EGRESS =
            TypeName.typeNameOf("io.statefun.playground","egress");

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(ALERT_TYPE)){
            final Alert alert = message.as(ALERT_TYPE);
            Alert.AlertType at = alert.getAlertType();
            final EgressRecord egressRecord;
            String msg = "";
            if(at == Alert.AlertType.D_BLOOD_PRESSURE){
                msg = "PatientID: "+alert.getPatientID()+" Timestamp: "+alert.getTimestamp()+" Alert Type: Diastolic blood pressure alert!";
                egressRecord = new EgressRecord("alerts",msg);
            }

            else if(at == Alert.AlertType.S_BLOOD_PRESSURE){
                msg = "PatientID: "+alert.getPatientID()+" Timestamp: "+alert.getTimestamp()+" Alert Type: Systolic blood pressure alert!";
                egressRecord = new EgressRecord("alerts",msg);
            }

            else if(at == Alert.AlertType.GLUCOSE) {
                msg = "PatientID: "+alert.getPatientID()+" Timestamp: "+alert.getTimestamp()+" Alert Type: Glucose alert!";
                egressRecord = new EgressRecord("alerts",msg);
            }

            else if(at == Alert.AlertType.PATTERN) {
                msg = "PatientID: "+alert.getPatientID()+" Timestamp: "+alert.getTimestamp()+" Alert Type: Pattern alert!";
                egressRecord = new EgressRecord("alerts",msg);
            }

            else{
                msg = "PatientID: "+alert.getPatientID()+" Timestamp: "+alert.getTimestamp()+" Alert Type: Other alert!";
                egressRecord = new EgressRecord("alerts",msg);
            }

            context.send(
                    EgressMessageBuilder.forEgress(KAFKA_EGRESS)
                            .withCustomType(EGRESS_RECORD_JSON_TYPE, egressRecord)
                            .build()
            );
            System.out.println(msg);
        }else{
            throw new IllegalArgumentException("Alter: Unexpected message type: " + message.valueTypeName());
        }
        return context.done();
    }
}

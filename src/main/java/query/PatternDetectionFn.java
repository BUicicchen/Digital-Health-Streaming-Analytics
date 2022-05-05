package query;

import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import query.customtype.Alert;
import query.customtype.Glucose;
import query.customtype.Smartwatch;
import query.customtype.Watermark;

import java.util.concurrent.CompletableFuture;

import static query.customtype.CustomTypes.*;

final class PatternDetectionFn implements StatefulFunction {
    private static final ValueSpec<Double> GLUCOSE_LEFT = ValueSpec
            .named("glucose_left")
            .withDoubleType();

    private static final ValueSpec<Double> GLUCOSE_RIGHT = ValueSpec
            .named("glucose_right")
            .withDoubleType();

    private static final ValueSpec<Double> HEART_BEAT_LEFT = ValueSpec
            .named("heart_beat_left")
            .withDoubleType();

    private static final ValueSpec<Double> HEART_BEAT_RIGHT = ValueSpec
            .named("heart_beat_right")
            .withDoubleType();

    static final TypeName TYPENAME = TypeName.typeNameOf("query.fns","pattern_detection");
    static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withValueSpecs(GLUCOSE_LEFT,GLUCOSE_RIGHT,HEART_BEAT_LEFT,HEART_BEAT_RIGHT)
                    .withSupplier(PatternDetectionFn::new)
                    .build();

    private final static Double GLUCOSE_INCREASE_THRESHOLD = 0.2;
    private final static Double HEART_BEAT_INCREASE_THRESHOLD = 0.2;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        final Long nowMS = System.currentTimeMillis();
        String patientID = "";
        String timestamp = "";

        var storage = context.storage();

        Double glu_left = storage.get(GLUCOSE_LEFT).orElse(0D);
        Double glu_right = storage.get(GLUCOSE_RIGHT).orElse(0D);
        Double heart_beat_left = storage.get(HEART_BEAT_LEFT).orElse(0D);
        Double heart_beat_right = storage.get(HEART_BEAT_RIGHT).orElse(0D);

        boolean hb_not_init = false;
        boolean glu_not_init = false;
        if(heart_beat_left != 0 && heart_beat_right != 0){hb_not_init = true;}
        if(glu_left != 0 && glu_right != 0){glu_not_init = true;}

        if (message.is(SMARTWATCH_TYPE)) {
            final Smartwatch sw = message.as(SMARTWATCH_TYPE);
            patientID = sw.getPatientID();
            timestamp = sw.getTimestamp();

            Double hb = Double.valueOf(sw.getMeanHeartBeat());

            if(heart_beat_left == 0){
                heart_beat_left = hb;
            }else if(glu_right == 0){
                heart_beat_right = hb;
            }else{
                heart_beat_left = heart_beat_right;
                heart_beat_right = hb;
            }


        }
        else if(message.is(GLUCOSE_TYPE)){
            final Glucose glu = message.as(GLUCOSE_TYPE);
            patientID = glu.getPatientID();
            timestamp = glu.getMinute();

            Double g = Double.valueOf(glu.getGlucose());

            if(glu_left == 0){
                glu_left = g;
            }else if(glu_right == 0){
                glu_right = g;
            }else{
                glu_left = glu_right;
                glu_right = g;
            }
        }

        else{
            //throw new IllegalArgumentException("Pattern Detection: Unexpected message type: " + message.valueTypeName());
            return context.done();
        }

        // System.out.println(""+heart_beat_left+" "+heart_beat_right+" "+glu_left+" "+glu_right);

        storage.set(GLUCOSE_LEFT,glu_left);
        storage.set(GLUCOSE_RIGHT,glu_right);
        storage.set(HEART_BEAT_LEFT,heart_beat_left);
        storage.set(HEART_BEAT_RIGHT,heart_beat_right);

        boolean glu_increase = false;
        boolean hb_increase = false;

        if((heart_beat_right - heart_beat_left) / heart_beat_left > HEART_BEAT_INCREASE_THRESHOLD){
            hb_increase = true;
        }
        if((glu_right - glu_left) / glu_left > GLUCOSE_INCREASE_THRESHOLD){
            glu_increase = true;
        }
        if(hb_increase && hb_not_init && glu_increase && glu_not_init){
            sendAlert(context, Alert.AlertType.PATTERN,patientID,timestamp);
        }

        return context.done();
    }

    private void sendAlert(Context context, Alert.AlertType alerttype, String patientID, String timestamp){
        final Alert alert = new Alert();
        alert.setAlertType(alerttype);
        alert.setTimestamp(timestamp);
        alert.setPatientID(patientID);

        context.send(
                MessageBuilder.forAddress(AlertFn.TYPENAME, patientID)
                        .withCustomType(ALERT_TYPE, alert)
                        .build()
        );
    }
}

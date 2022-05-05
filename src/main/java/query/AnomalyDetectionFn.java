package query;

import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import query.customtype.Alert;
import query.customtype.BloodPressure;
import query.customtype.Glucose;
import query.customtype.Watermark;

import java.util.concurrent.CompletableFuture;

import static query.customtype.CustomTypes.*;

final class AnomalyDetectionFn implements StatefulFunction {


    static final TypeName TYPENAME = TypeName.typeNameOf("query.fns","anomaly_detection");
    static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(AnomalyDetectionFn::new)
                    .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        // final Long nowMS = System.currentTimeMillis();
        if(message.is(WATERMARK_TYPE)){
            final Watermark wm = message.as(WATERMARK_TYPE);
            final Watermark new_wm = new Watermark(
                    wm.getWatermark(),
                    wm.getPatientID(),
                    wm.getDeviceID()
            );
            context.send(
                    MessageBuilder.forAddress(DBPDetectionFn.TYPENAME, wm.getPatientID())
                            .withCustomType(WATERMARK_TYPE, new_wm)
                            .build()
            );
            //System.out.println("WM Message from "+ context.self()+" to "+DBPDetectionFn.TYPENAME.toString());

            context.send(
                    MessageBuilder.forAddress(SBPDetectionFn.TYPENAME, wm.getPatientID())
                            .withCustomType(WATERMARK_TYPE, new_wm)
                            .build()
            );
            //System.out.println("WM Message from "+ context.self()+" to "+SBPDetectionFn.TYPENAME.toString());

            context.send(
                    MessageBuilder.forAddress(GLUDetectionFn.TYPENAME, wm.getPatientID())
                            .withCustomType(WATERMARK_TYPE, new_wm)
                            .build()
            );
            //System.out.println("WM Message from "+ context.self()+" to "+GLUDetectionFn.TYPENAME.toString());

        }
        else if (message.is(BLOOD_PRESSURE_TYPE)) {
            final BloodPressure bp = message.as(BLOOD_PRESSURE_TYPE);
            final BloodPressure new_bp = new BloodPressure(
                    bp.getPatientID(),
                    bp.getDeviceID(),
                    bp.getTimestamp(),
                    bp.getDBP(),
                    bp.getSBP()
            );

            context.send(
                    MessageBuilder.forAddress(DBPDetectionFn.TYPENAME, bp.getPatientID())
                            .withCustomType(BLOOD_PRESSURE_TYPE, new_bp)
                            .build()
            );
            //System.out.println("BP Message from "+ context.self()+" to "+DBPDetectionFn.TYPENAME.toString());

            context.send(
                    MessageBuilder.forAddress(SBPDetectionFn.TYPENAME, bp.getPatientID())
                            .withCustomType(BLOOD_PRESSURE_TYPE, new_bp)
                            .build()
            );
            //System.out.println("BP Message from "+ context.self()+" to "+SBPDetectionFn.TYPENAME.toString());

        }
        else if(message.is(GLUCOSE_TYPE)){
            final Glucose glu = message.as(GLUCOSE_TYPE);

            final Glucose new_glu = new Glucose(
                    glu.getPatientID(),
                    glu.getDeviceID(),
                    glu.getMinute(),
                    glu.getGlucose()
            );
            context.send(
                    MessageBuilder.forAddress(GLUDetectionFn.TYPENAME, glu.getPatientID())
                            .withCustomType(GLUCOSE_TYPE, new_glu)
                            .build()
            );
            //System.out.println("GLU Message from "+ context.self()+" to "+GLUDetectionFn.TYPENAME.toString());

        }
        else{
            throw new IllegalArgumentException("Anomaly Detection: Unexpected message type: " + message.valueTypeName());
        }

        return context.done();
    }


}

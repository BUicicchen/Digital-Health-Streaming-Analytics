package query;

import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import query.customtype.BloodPressure;
import query.customtype.Glucose;
import query.customtype.Smartwatch;
import query.customtype.Insulin;
import query.customtype.Fitbit;
import query.customtype.Watermark;

import java.util.concurrent.CompletableFuture;

import static query.customtype.CustomTypes.*;
import java.util.Date;

final class RouterFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameOf("query.fns","router");
    static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(RouterFn::new)
                    .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(WATERMARK_TYPE)){
            final Watermark wm = message.as(WATERMARK_TYPE);
            final Watermark new_wm = new Watermark(
                    wm.getWatermark(),
                    wm.getPatientID(),
                    wm.getDeviceID()
            );
            // for Query 2
            context.send(
                    MessageBuilder.forAddress(AnomalyDetectionFn.TYPENAME, wm.getPatientID())
                            .withCustomType(WATERMARK_TYPE, new_wm)
                            .build()
            );
            // for Query 3
           context.send(
                   MessageBuilder.forAddress(PatternDetectionFn.TYPENAME, wm.getPatientID())
                           .withCustomType(WATERMARK_TYPE, new_wm)
                           .build()
           );

        }
        else if (message.is(SMARTWATCH_TYPE)){
            final Smartwatch sw = message.as(SMARTWATCH_TYPE);
            final Smartwatch new_sw = new Smartwatch(
                    sw.getPatientID(),
                    sw.getDeviceID(),
                    sw.getTimestamp(),
                    sw.getStepsSinceLast(),
                    sw.getMeanHeartBeat()
            );

        //     Date date = new Date();
        //     System.out.println("Heart rate arrival time: " + date.getTime());

            // for Query 1
            context.send(
                    MessageBuilder.forAddress(AverageBloodPressureFn.TYPENAME, sw.getPatientID())
                            .withCustomType(SMARTWATCH_TYPE, new_sw)
                            .build()
            );
            // for Query 3
            context.send(
                    MessageBuilder.forAddress(PatternDetectionFn.TYPENAME, sw.getPatientID())
                            .withCustomType(SMARTWATCH_TYPE, new_sw)
                            .build()
            );

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
            
        //     Date date = new Date();
        //     System.out.println("Blood Pressure arrival time: " + date.getTime());
            
            // for Query 1
            context.send(
                    MessageBuilder.forAddress(AverageBloodPressureFn.TYPENAME, bp.getPatientID())
                            .withCustomType(BLOOD_PRESSURE_TYPE, new_bp)
                            .build()
            );
            // for Query 2
            context.send(
                    MessageBuilder.forAddress(AnomalyDetectionFn.TYPENAME, bp.getPatientID())
                            .withCustomType(BLOOD_PRESSURE_TYPE, new_bp)
                            .build()
            );
            //System.out.println("BP Message from "+ context.self()+" to "+AnomalyDetectionFn.TYPENAME.toString());


        }
        else if (message.is(GLUCOSE_TYPE)){
                final Glucose glu = message.as(GLUCOSE_TYPE);
                final Glucose new_glu = new Glucose(
                        glu.getPatientID(),
                        glu.getDeviceID(),
                        glu.getMinute(),
                        glu.getGlucose()
                );
                // for Query 2
                context.send(
                        MessageBuilder.forAddress(AnomalyDetectionFn.TYPENAME, glu.getPatientID())
                                .withCustomType(GLUCOSE_TYPE, new_glu)
                                .build()
                );
                // for Query 3
                context.send(
                        MessageBuilder.forAddress(PatternDetectionFn.TYPENAME, glu.getPatientID())
                                .withCustomType(GLUCOSE_TYPE, new_glu)
                                .build()
                );
                // for Query 4
                context.send(
                        MessageBuilder.forAddress(EatingPeriodFn.TYPENAME, glu.getPatientID())
                                .withCustomType(GLUCOSE_TYPE, new_glu)
                                .build()
                );
                System.out.println("Glucose Monitor: <deviceID: "+glu.getDeviceID()+", minute: "+glu.getMinute()+", glucose (mg/dl): "+glu.getGlucose()+">");
        
        }    
        else if (message.is(INSULIN_TYPE)){
                final Insulin insulin = message.as(INSULIN_TYPE);
                final Insulin new_insulin = new Insulin(
                        insulin.getPatientID(),
                        insulin.getDeviceID(),
                        insulin.getTimestamp(),
                        insulin.getDoseAmount()
                );
                // for Query 4
                context.send(
                        MessageBuilder.forAddress(EatingPeriodFn.TYPENAME, insulin.getPatientID())
                                .withCustomType(INSULIN_TYPE, new_insulin)
                                .build()
                );
                System.out.println("Insulin Tracker: <deviceID: "+insulin.getDeviceID()+", timestamp: "+insulin.getTimestamp()+", dose_amount: "+insulin.getDoseAmount()+">");
        
        }
        else if (message.is(FITBIT_TYPE)){
                final Fitbit fitbit = message.as(FITBIT_TYPE);
                final Fitbit new_fitbit = new Fitbit(
                        fitbit.getPatientID(),
                        fitbit.getDeviceID(),
                        fitbit.getTimestamp(),
                        fitbit.getMETs(),
                        fitbit.getIntensity(),
                        fitbit.getSteps(),
                        fitbit.getHeartRate()
                );
                // for Query 4
                context.send(
                        MessageBuilder.forAddress(EatingPeriodFn.TYPENAME, fitbit.getPatientID())
                                .withCustomType(FITBIT_TYPE, new_fitbit)
                                .build()
                );
                System.out.println("Fitbit: <deviceID: "+fitbit.getDeviceID()+", timestamp: "+fitbit.getTimestamp()+", METs: "+fitbit.getMETs()+", intensity: "+fitbit.getIntensity()+", steps: "+fitbit.getSteps()+", heart_rate: "+fitbit.getHeartRate()+">");
        }

        else {
            throw new IllegalArgumentException("Router: Unexpected message type: " + message.valueTypeName());
        }
        return context.done();
    }
}

package query;

import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import query.customtype.Alert;
import query.customtype.Glucose;
import query.customtype.Watermark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.Math.abs;
import static query.customtype.CustomTypes.*;

final class GLUDetectionFn implements StatefulFunction {

    private static final ValueSpec<Long> CURR_WATERMARK = ValueSpec
            .named("glu_current_watermark")
            .withLongType();

    private static final ValueSpec<List<Double>> GLU = ValueSpec
            .named("glus")
            .withCustomType(DOUBLE_LIST_TYPE);

    private static final ValueSpec<List<Double>> TIMESTAMPS = ValueSpec
            .named("glu_timestamps")
            .withCustomType(DOUBLE_LIST_TYPE);

    static final TypeName TYPENAME = TypeName.typeNameOf("query.fns","glu_detection");

    static final StatefulFunctionSpec SPEC =
        StatefulFunctionSpec.builder(TYPENAME)
                .withValueSpecs(CURR_WATERMARK,GLU,TIMESTAMPS)
                .withSupplier(GLUDetectionFn::new)
                .build();

    private final static Integer WINDOWS_NUM = 3;

    private final static Long WINDOW_LENGTH = 5L;

    private final static Double ALERT_THRESHOLD = 15D;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if (message.is(GLUCOSE_TYPE)) {
            //System.out.println(context.self()+" got GLU Message from" +context.caller());
            final Glucose glucose = message.as(GLUCOSE_TYPE);

            Long curr_wm = context.storage().get(CURR_WATERMARK).orElse(0L);
            Double glu_ts = Double.parseDouble(glucose.getMinute());
            // store the data only when its timestamp is greater than
            // current watermark
            if(glu_ts > curr_wm){
                // read data from storage
                List<Double> glu = context.storage().get(GLU).orElse(new ArrayList<>());
                List<Double> ts = context.storage().get(TIMESTAMPS).orElse(new ArrayList<>());

                // add new data
                Double glu_glu = Double.valueOf(glucose.getGlucose());
                glu.add(glu_glu);
                ts.add(glu_ts);

                // store them back
                context.storage().set(GLU,glu);
                context.storage().set(TIMESTAMPS,ts);

                //debug
//                System.out.println("GLU message");
//                for(Double _glu: glu){
//                    System.out.println(_glu);
//                }
//                for(Double _ts: ts){
//                    System.out.println(_ts);
//                }

            }

        }
        else if (message.is(WATERMARK_TYPE)) {
            //System.out.println(context.self()+" got WM Message from" +context.caller());
            final Watermark wm = message.as(WATERMARK_TYPE);

            // update the latest watermark
            Long wtrmrk = Long.valueOf(wm.getWatermark());
            context.storage().set(CURR_WATERMARK,wtrmrk);

            // build the structure for calculation
            Double[] avg = new Double[WINDOWS_NUM];
            Integer[] count = new Integer[WINDOWS_NUM];
            Arrays.fill(avg,0D);
            Arrays.fill(count,0);
            Double[] timepoints = new Double[WINDOWS_NUM+1];
            Double right_time_point = (double) (wtrmrk - wtrmrk%WINDOW_LENGTH);
            for(int i = 0; i < WINDOWS_NUM+1; i++){
                timepoints[i] = right_time_point - (WINDOWS_NUM-i)*WINDOW_LENGTH;
            }

            // read the data
            List<Double> glu = context.storage().get(GLU).orElse(new ArrayList<>());
            List<Double> ts = context.storage().get(TIMESTAMPS).orElse(new ArrayList<>());

            // remove the old data
            for(int i = 0; i < ts.size(); i++){
                if(ts.get(i)<timepoints[0]){
                    ts.remove(i);
                    glu.remove(i);
                    i--;
                }
            }

            // store them back
            context.storage().set(GLU,glu);
            context.storage().set(TIMESTAMPS,ts);

            // fill data into the structure
            for(int i = 0; i < ts.size(); i++) {
                for(int j = 0; j < WINDOWS_NUM; j++){
                    // data[i] is in the jth window
                    if(ts.get(i) >= timepoints[j] && ts.get(i) < timepoints[j+1]){
                        avg[j] += glu.get(i);
                        count[j]++;
                    }
                }
            }
            Double s = 0D;
            Integer c = 0;
            for(int i = 0; i< WINDOWS_NUM; i++){
                s+=avg[i];
                c+=count[i];
            }
            Double overall_avg = s/c;
            Double last_avg = avg[WINDOWS_NUM-1]/count[WINDOWS_NUM-1];

            if(abs(overall_avg-last_avg) > ALERT_THRESHOLD){
                sendAlert(context,Alert.AlertType.GLUCOSE,wm.getPatientID(),wtrmrk.toString());
            }

            //debug
//            System.out.println("Watermark message");
//            for(Double _glu: glu){
//                System.out.println(_glu);
//            }
//            for(Double _ts: ts){
//                System.out.println(_ts);
//            }

        }
        else{
            throw new IllegalArgumentException("GLU Detection: Unexpected message type: " + message.valueTypeName());
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

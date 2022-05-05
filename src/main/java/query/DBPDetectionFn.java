package query;

import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import query.customtype.Alert;
import query.customtype.BloodPressure;
import query.customtype.Watermark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.Math.abs;
import static query.customtype.CustomTypes.*;

final class DBPDetectionFn implements StatefulFunction {

    static final ValueSpec<Long> CURR_WATERMARK = ValueSpec
            .named("dbp_current_watermark")
            .withLongType();

    static final ValueSpec<List<Double>> DBP = ValueSpec
            .named("dbps")
            .withCustomType(DOUBLE_LIST_TYPE);

    static final ValueSpec<List<Double>> TIMESTAMPS = ValueSpec
            .named("dbp_timestamps")
            .withCustomType(DOUBLE_LIST_TYPE);

    static final TypeName TYPENAME = TypeName.typeNameOf("query.fns", "dbp_detection");

    static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withValueSpecs(CURR_WATERMARK, DBP, TIMESTAMPS)
                    .withSupplier(DBPDetectionFn::new)
                    .build();

    private final static Integer WINDOWS_NUM = 3;

    private final static Long WINDOW_LENGTH = 5L;

    private final static Double ALERT_THRESHOLD = 15D;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if (message.is(BLOOD_PRESSURE_TYPE)) {
            //System.out.println(context.self() + " got BP Message from " + context.caller());
            final BloodPressure bp = message.as(BLOOD_PRESSURE_TYPE);

            // store all data
            Long curr_wm = context.storage().get(CURR_WATERMARK).orElse(0L);
            //System.out.println("curr_wm: "+curr_wm);
            context.storage().set(CURR_WATERMARK,curr_wm);
            Double bp_ts = Double.parseDouble(bp.getTimestamp());
            // store the data only when its timestamp is greater than
            // current watermark
            if (bp_ts > curr_wm) {
                // read data from storage
                List<Double> dbp = context.storage().get(DBP).orElse(new ArrayList<>());
                List<Double> ts = context.storage().get(TIMESTAMPS).orElse(new ArrayList<>());

                // add new data
                Double bp_dbp = Double.parseDouble(bp.getDBP());
                dbp.add(bp_dbp);
                ts.add(bp_ts);

                // store them back
                context.storage().set(DBP, dbp);
                context.storage().set(TIMESTAMPS, ts);

                //debug
//                System.out.println("DBP message");
//                for(Double _dbp: dbp){
//                    System.out.println(_dbp);
//                }
//                for(Double _ts: ts){
//                    System.out.println(_ts);
//                }

            }

        }
        else if (message.is(WATERMARK_TYPE)) {
//            System.out.println(context.self() + " got WM Message from " + context.caller());
            final Watermark wm = message.as(WATERMARK_TYPE);

//            Long wtrmrk = context.storage().get(CURR_WATERMARK).orElse(0L);
//            System.out.println("previous watermark: "+wtrmrk);

            // update the latest watermark
            Long wtrmrk = Long.parseLong(wm.getWatermark());
//            System.out.println("latest watermark: "+wtrmrk);
            context.storage().set(CURR_WATERMARK, wtrmrk);

            // build the structure for calculation
            Double[] avg = new Double[WINDOWS_NUM];
            Integer[] count = new Integer[WINDOWS_NUM];
            Arrays.fill(avg, 0D);
            Arrays.fill(count, 0);
            Double[] timepoints = new Double[WINDOWS_NUM + 1];
            Double right_time_point = (double) (wtrmrk - wtrmrk % WINDOW_LENGTH);
            for (int i = 0; i < WINDOWS_NUM + 1; i++) {
                timepoints[i] = right_time_point - (WINDOWS_NUM - i) * WINDOW_LENGTH;
            }

//            System.out.println("timepoints: ");
//            for(Double tp:timepoints){
//                System.out.println(tp);
//            }

            // read the data
            List<Double> dbp = context.storage().get(DBP).orElse(new ArrayList<>());
            List<Double> ts = context.storage().get(TIMESTAMPS).orElse(new ArrayList<>());

            //debug
//            System.out.println("All data");
//            for(Double _dbp: dbp){
//                System.out.println(_dbp);
//            }
//            for(Double _ts: ts){
//                System.out.println(_ts);
//            }

            // remove the old data
            for (int i = 0; i < ts.size(); i++) {
                if(ts.get(i)<timepoints[0]){
                    ts.remove(i);
                    dbp.remove(i);
                    i--;
                }
            }

            // store them back
            context.storage().set(DBP, dbp);
            context.storage().set(TIMESTAMPS, ts);

//            System.out.println("All data after remove");
//            for(Double _dbp: dbp){
//                System.out.println(_dbp);
//            }
//            for(Double _ts: ts){
//                System.out.println(_ts);
//            }

            // fill data into the structure
            for (int i = 0; i < ts.size(); i++) {
                for (int j = 0; j < WINDOWS_NUM; j++) {
                    // data[i] is in the jth window
                    if (ts.get(i) >= timepoints[j] && ts.get(i) < timepoints[j + 1]) {
                        avg[j] += dbp.get(i);
                        count[j]++;
                    }
                }
            }

//            System.out.println("structure data");
//            for(Double a: avg){
//                System.out.println(a);
//            }
//            for(Integer c: count){
//                System.out.println(c);
//            }

            Double s = 0D;
            Integer c = 0;
            for (int i = 0; i < WINDOWS_NUM; i++) {
                s += avg[i];
                c += count[i];
            }
            Double overall_avg = s / c;
            Double last_avg = avg[WINDOWS_NUM - 1] / count[WINDOWS_NUM - 1];

//            System.out.println("overavg: "+overall_avg);
//            System.out.println("lastavg: "+last_avg);


            if (abs(overall_avg - last_avg) > ALERT_THRESHOLD) {
                sendAlert(context, Alert.AlertType.D_BLOOD_PRESSURE, wm.getPatientID(), wtrmrk.toString());
            }

        } else {
            throw new IllegalArgumentException("DBP Detection: Unexpected message type: " + message.valueTypeName());
        }

        return context.done();
    }

    private void sendAlert(Context context, Alert.AlertType alerttype, String patientID, String timestamp) {
        Alert alert = new Alert();
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

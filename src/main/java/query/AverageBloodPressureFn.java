package query;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import query.customtype.BloodPressure;
import query.customtype.Smartwatch;
import java.util.concurrent.CompletableFuture;
import static query.customtype.CustomTypes.BLOOD_PRESSURE_TYPE;
import static query.customtype.CustomTypes.SMARTWATCH_TYPE;
import java.util.*;
import org.apache.commons.lang3.tuple.*;
import java.util.Date;

final class AverageBloodPressureFn implements StatefulFunction {

    private static final ValueSpec<Double> DBP_AVG_RUN = ValueSpec
            .named("dbp_avg_run")
            .withDoubleType();
    private static final ValueSpec<Double> DBP_AVG_EAT = ValueSpec
            .named("dbp_avg_eat")
            .withDoubleType();
    private static final ValueSpec<Double> DBP_AVG_WALK = ValueSpec
            .named("dbp_avg_walk")
            .withDoubleType();
    private static final ValueSpec<Double> DBP_AVG_SLEEP = ValueSpec
            .named("dbp_avg_sleep")
            .withDoubleType();

    private static final ValueSpec<Integer> DBP_COUNT_RUN = ValueSpec
            .named("dbp_count_run")
            .withIntType();
    private static final ValueSpec<Integer> DBP_COUNT_EAT = ValueSpec
            .named("dbp_count_eat")
            .withIntType();
    private static final ValueSpec<Integer> DBP_COUNT_WALK = ValueSpec
            .named("dbp_count_walk")
            .withIntType();
    private static final ValueSpec<Integer> DBP_COUNT_SLEEP = ValueSpec
            .named("dbp_count_sleep")
            .withIntType();

    private static final ValueSpec<Double> SBP_AVG_RUN = ValueSpec
            .named("sbp_avg_run")
            .withDoubleType();
    private static final ValueSpec<Double> SBP_AVG_EAT = ValueSpec
            .named("sbp_avg_eat")
            .withDoubleType();
    private static final ValueSpec<Double> SBP_AVG_WALK = ValueSpec
            .named("sbp_avg_walk")
            .withDoubleType();
    private static final ValueSpec<Double> SBP_AVG_SLEEP = ValueSpec
            .named("sbp_avg_sleep")
            .withDoubleType();

    private static final ValueSpec<Integer> SBP_COUNT_RUN = ValueSpec
            .named("sbp_count_run")
            .withIntType();
    private static final ValueSpec<Integer> SBP_COUNT_EAT= ValueSpec
            .named("sbp_count_eat")
            .withIntType();
    private static final ValueSpec<Integer> SBP_COUNT_WALK = ValueSpec
            .named("sbp_count_walk")
            .withIntType();
    private static final ValueSpec<Integer> SBP_COUNT_SLEEP = ValueSpec
            .named("sbp_count_sleep")
            .withIntType();

    private static final ValueSpec<Integer> COUNT = ValueSpec
            .named("count")
            .withIntType();

    private static final ValueSpec<String> ACTIVITY_STREAM = ValueSpec
            .named("activity_stream")
            .withUtf8StringType();
            

    static final TypeName TYPENAME = TypeName.typeNameOf("query.fns","dbpsbp_avg_detection");
    static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withValueSpecs(
                    DBP_AVG_RUN,DBP_AVG_EAT,DBP_AVG_WALK,DBP_AVG_SLEEP,
                    DBP_COUNT_RUN,DBP_COUNT_EAT,DBP_COUNT_WALK,DBP_COUNT_SLEEP,
                    SBP_AVG_RUN,SBP_AVG_EAT,SBP_AVG_WALK,SBP_AVG_SLEEP,
                    SBP_COUNT_RUN,SBP_COUNT_EAT,SBP_COUNT_WALK,SBP_COUNT_SLEEP,
                    COUNT, ACTIVITY_STREAM)
                    .withSupplier(AverageBloodPressureFn::new)
                    .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if (message.is(BLOOD_PRESSURE_TYPE)) {
            final BloodPressure bp = message.as(BLOOD_PRESSURE_TYPE);
            System.out.println("Query 1: Blood Pressure Monitor Timestamp: " + bp.getTimestamp() + ", Device ID: " + bp.getDeviceID() + ", Patient ID: " + bp.getPatientID() + ", dBP: " + bp.getDBP() + ", sBP: " + bp.getSBP());

            var storage = context.storage();
            Double dbp_avg_run = storage.get(DBP_AVG_RUN).orElse(0.0);
            Double dbp_avg_eat = storage.get(DBP_AVG_EAT).orElse(0.0);
            Double dbp_avg_walk = storage.get(DBP_AVG_WALK).orElse(0.0);
            Double dbp_avg_sleep = storage.get(DBP_AVG_SLEEP).orElse(0.0);
            Integer dbp_count_run = storage.get(DBP_COUNT_RUN).orElse(0);
            Integer dbp_count_eat = storage.get(DBP_COUNT_EAT).orElse(0);
            Integer dbp_count_walk = storage.get(DBP_COUNT_WALK).orElse(0);
            Integer dbp_count_sleep = storage.get(DBP_COUNT_SLEEP).orElse(0);
            Double sbp_avg_run = storage.get(SBP_AVG_RUN).orElse(202.5);
            Double sbp_avg_eat = storage.get(SBP_AVG_EAT).orElse(122.5);
            Double sbp_avg_walk = storage.get(SBP_AVG_WALK).orElse(102.5);
            Double sbp_avg_sleep = storage.get(SBP_AVG_SLEEP).orElse(90D);
            Integer sbp_count_run = storage.get(SBP_COUNT_RUN).orElse(0);
            Integer sbp_count_eat = storage.get(SBP_COUNT_EAT).orElse(0);
            Integer sbp_count_walk = storage.get(SBP_COUNT_WALK).orElse(0);
            Integer sbp_count_sleep = storage.get(SBP_COUNT_SLEEP).orElse(0);
            Integer count = storage.get(COUNT).orElse(0);

            Long current = Long.valueOf(bp.getTimestamp());
            Double dbp = Double.valueOf(bp.getDBP());
            Double sbp = Double.valueOf(bp.getSBP());
            
            String activity_stream_str = storage.get(ACTIVITY_STREAM).orElse("");
            List<String> activity_stream_lst = new ArrayList<String>();
            if (activity_stream_str != "") {
                activity_stream_lst = Arrays.asList(activity_stream_str.split(":"));
            }
            String activity_stream = "";

            boolean found = false;
            for (int i = 0; i < activity_stream_lst.size(); i++) {
                String[] item = activity_stream_lst.get(i).split(",");
                // pair structure: (timestamp, activity)
                Pair<Long, String> pair = new MutablePair<>(Long.parseLong(item[0]), item[1]);
                Long timestamp = pair.getKey();
                String activity = pair.getValue();

                if (i < activity_stream_lst.size() - 1) {
                        String[] item2 = activity_stream_lst.get(i+1).split(",");
                        // pair structure: (timestamp, activity)
                        Pair<Long, String> pair2 = new MutablePair<>(Long.parseLong(item2[0]), item2[1]);
                        Long timestamp2 = pair2.getKey();
                        if (current >= timestamp2) {
                                continue;
                        }
                }
                
                if (found == false) {
                        if (current >= timestamp) {
                                if (activity.equals("sleep")) {
                                        dbp_avg_sleep = (dbp_avg_sleep * dbp_count_sleep + dbp) / ++dbp_count_sleep;
                                        sbp_avg_sleep = (sbp_avg_sleep * sbp_count_sleep + sbp) / ++sbp_count_sleep;
                                        // Date date = new Date();
                                        // System.out.println("Finish processing time: " + date.getTime());
                                        System.out.println("Query 1: Average sleeping: dBP: " + dbp_avg_sleep + " sBP: " + sbp_avg_sleep);
                                        if (dbp_count_sleep % 10 == 0) {
                                                System.out.println("Query 1: Session sleeping [" + (count-9) + ", " + count + "] Count: " + dbp_count_sleep + " Average: dBP: " + dbp_avg_sleep + " sBP: " + sbp_avg_sleep + " ============================");
                                                dbp_avg_sleep = 0.0;
                                                sbp_avg_sleep = 0.0;
                                        }
                                } else if (activity.equals("walk")) {
                                        dbp_avg_walk = (dbp_avg_walk * dbp_count_walk + dbp) / ++dbp_count_walk;
                                        sbp_avg_walk = (sbp_avg_walk * sbp_count_walk + sbp) / ++sbp_count_walk;
                                        // Date date = new Date();
                                        // System.out.println("Finish processing time: " + date.getTime());
                                        System.out.println("Query 1: Average walking: dBP: " + dbp_avg_walk + " sBP: " + sbp_avg_walk);
                                        if (dbp_count_walk % 10 == 0) {
                                                System.out.println("Query 1: Session walking [" + (count-9) + ", " + count + "] Count: " + dbp_count_walk + " Average: dBP: " + dbp_avg_walk + " sBP: " + sbp_avg_walk + " ============================");
                                                dbp_avg_walk = 0.0;
                                                sbp_avg_walk = 0.0;
                                        }
                                } else if (activity.equals("eat")) {
                                        dbp_avg_eat = (dbp_avg_eat * dbp_count_eat + dbp) / ++dbp_count_eat;
                                        sbp_avg_eat = (sbp_avg_eat * sbp_count_eat + sbp) / ++sbp_count_eat;
                                        // Date date = new Date();
                                        // System.out.println("Finish processing time: " + date.getTime());
                                        System.out.println("Query 1: Average eating: dBP: " + dbp_avg_eat + " sBP: " + sbp_avg_eat);
                                        if (dbp_count_eat % 10 == 0) {
                                                System.out.println("Query 1: Session eating [" + (count-9) + ", " + count + "] Count: " + dbp_count_eat + " Average: dBP: " + dbp_avg_eat + " sBP: " + sbp_avg_eat + " ============================");
                                                dbp_avg_eat = 0.0;
                                                sbp_avg_eat = 0.0;
                                        }
                                } else if (activity.equals("run")) {
                                        dbp_avg_run = (dbp_avg_run * dbp_count_run + dbp) / ++dbp_count_run;
                                        sbp_avg_run = (sbp_avg_run * sbp_count_run + sbp) / ++sbp_count_run;
                                        // Date date = new Date();
                                        // System.out.println("Finish processing time: " + date.getTime());
                                        System.out.println("Query 1: Average running: dBP: " + dbp_avg_run + " sBP: " + sbp_avg_run);
                                        if (dbp_count_run % 10 == 0) {
                                                System.out.println("Query 1: Session running [" + (count-9) + ", " + count + "] Count:" + dbp_count_run + " Average: dBP: " + dbp_avg_run + " sBP: " + sbp_avg_run + " ============================");
                                                dbp_avg_run = 0.0;
                                                sbp_avg_run = 0.0;
                                        }
                                }
                                count++;
                                found = true;
                        }
                } else {
                        if (activity_stream == "") {
                                activity_stream += (timestamp + "," + activity);
                        } else {
                                activity_stream += (":" + timestamp + "," + activity);
                        } 
                }
            }

            

            // store it back
            storage.set(DBP_AVG_RUN,dbp_avg_run);
            storage.set(DBP_AVG_EAT,dbp_avg_eat);
            storage.set(DBP_AVG_WALK,dbp_avg_walk);
            storage.set(DBP_AVG_SLEEP,dbp_avg_sleep);
            storage.set(DBP_COUNT_RUN,dbp_count_run);
            storage.set(DBP_COUNT_EAT,dbp_count_eat);
            storage.set(DBP_COUNT_WALK,dbp_count_walk);
            storage.set(DBP_COUNT_SLEEP,dbp_count_sleep);
            storage.set(SBP_AVG_RUN,sbp_avg_run);
            storage.set(SBP_AVG_EAT,sbp_avg_eat);
            storage.set(SBP_AVG_WALK,sbp_avg_walk);
            storage.set(SBP_AVG_SLEEP,sbp_avg_sleep);
            storage.set(SBP_COUNT_RUN,sbp_count_run);
            storage.set(SBP_COUNT_EAT,sbp_count_eat);
            storage.set(SBP_COUNT_WALK,sbp_count_walk);
            storage.set(SBP_COUNT_SLEEP,sbp_count_sleep);
            storage.set(COUNT,count);
            storage.set(ACTIVITY_STREAM,activity_stream);

        }
        else if (message.is(SMARTWATCH_TYPE)) {
                final Smartwatch smartwatch = message.as(SMARTWATCH_TYPE);
                var storage = context.storage();
                String curr_activity = storage.get(ACTIVITY_STREAM).orElse("");
                var heart_rate = Double.valueOf(smartwatch.getMeanHeartBeat());
                if (heart_rate >= 50 && heart_rate < 70) {
                        // Date date = new Date();
                        // System.out.println("Finish processing time: " + date.getTime());
                        System.out.println("Query 1: Smartwatch Timestamp: " + smartwatch.getTimestamp() + ", Device ID: " + smartwatch.getDeviceID() + ", Patient ID: " + smartwatch.getPatientID() + ", steps since last: " + smartwatch.getStepsSinceLast() + ", mean_heart_rate: " + smartwatch.getMeanHeartBeat() + " sleeping [50.0, 70.0]");
                        if (curr_activity == "")
                                curr_activity += (smartwatch.getTimestamp().toString()+",sleep");
                        else
                                curr_activity += (":"+smartwatch.getTimestamp().toString()+",sleep");
                } else if (heart_rate >= 70 && heart_rate < 90) {
                        // Date date = new Date();
                        // System.out.println("Finish processing time: " + date.getTime());
                        System.out.println("Query 1: Smartwatch Timestamp: " + smartwatch.getTimestamp() + ", Device ID: " + smartwatch.getDeviceID() + ", Patient ID: " + smartwatch.getPatientID() + ", steps since last: " + smartwatch.getStepsSinceLast() + ", mean_heart_rate: " + smartwatch.getMeanHeartBeat() + " walking [70.0, 90.0]");
                        if (curr_activity == "")
                                curr_activity += (smartwatch.getTimestamp().toString()+",walk");
                        else
                                curr_activity += (":"+smartwatch.getTimestamp().toString()+",walk");
                } else if (heart_rate >= 90 && heart_rate < 110) {
                        // Date date = new Date();
                        // System.out.println("Finish processing time: " + date.getTime());
                        System.out.println("Query 1: Smartwatch Timestamp: " + smartwatch.getTimestamp() + ", Device ID: " + smartwatch.getDeviceID() + ", Patient ID: " + smartwatch.getPatientID() + ", steps since last: " + smartwatch.getStepsSinceLast() + ", mean_heart_rate: " + smartwatch.getMeanHeartBeat() + " eating [90.0, 110.0]");
                        if (curr_activity == "")
                                curr_activity += (smartwatch.getTimestamp().toString()+",eat");
                        else
                                curr_activity += (":"+smartwatch.getTimestamp().toString()+",eat");
                } else if (heart_rate >= 110 && heart_rate < 130) {
                        // Date date = new Date();
                        // System.out.println("Finish processing time: " + date.getTime());
                        System.out.println("Query 1: Smartwatch Timestamp: " + smartwatch.getTimestamp() + ", Device ID: " + smartwatch.getDeviceID() + ", Patient ID: " + smartwatch.getPatientID() + ", steps since last: " + smartwatch.getStepsSinceLast() + ", mean_heart_rate: " + smartwatch.getMeanHeartBeat() + " running [110.0, 130.0]");
                        if (curr_activity == "")
                                curr_activity += (smartwatch.getTimestamp().toString()+",run");
                        else
                                curr_activity += (":"+smartwatch.getTimestamp().toString()+",run");
                }
                storage.set(ACTIVITY_STREAM,curr_activity);
        }
        else {
        //     throw new IllegalArgumentException("DBP SBP Detection: Unexpected message type: " + message.valueTypeName());
        }

        return context.done();
    }
}

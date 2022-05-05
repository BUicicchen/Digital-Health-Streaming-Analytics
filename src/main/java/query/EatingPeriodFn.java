// eating period: when glucose rises by x% (x=50) within y (y=10) minutes
// inactivity: sleep 50~70
// activity: walk 70~90, eat 90~110, run 110~130
// ===============================
// "Eating period detected!"
// "Insulin occurrence: " + String.valueOf(insulinCount)
// "Inactivity period: glucose level is " + String.valueOf(glucose) + ", with a " + String.valueOf(-1*glucose_inactivity) + " decrease within a time period of " + time

package query;
import org.apache.commons.lang3.tuple.*;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import query.customtype.Glucose;
import query.customtype.Insulin;
import query.customtype.Fitbit;
import java.util.concurrent.CompletableFuture;
import static query.customtype.CustomTypes.*;
import java.util.*;

final class EatingPeriodFn implements StatefulFunction {

    private static final ValueSpec<String> GLUCOSE_STREAM = ValueSpec
            .named("glucose_stream")
            .withUtf8StringType();
    private static final ValueSpec<String> INSULIN_STREAM = ValueSpec
            .named("insulin_stream")
            .withUtf8StringType();
    private static final ValueSpec<Double> GLUCOSE_LAST = ValueSpec
            .named("glucose_last")
            .withDoubleType();
    private static final ValueSpec<Double> GLUCOSE_INACTIVITY = ValueSpec
            .named("glucose_inactivity")
            .withDoubleType();
    private static final ValueSpec<Long> GLUCOSE_INACTIVITY_TIMESTAMP = ValueSpec
            .named("glucose_inactivity_timestamp")
            .withLongType();
    private static final ValueSpec<Double> GLUCOSE_ACTIVITY = ValueSpec
            .named("glucose_activity")
            .withDoubleType();
    private static final ValueSpec<Long> GLUCOSE_ACTIVITY_TIMESTAMP = ValueSpec
            .named("glucose_activity_timestamp")
            .withLongType();


    static final TypeName TYPENAME = TypeName.typeNameOf("query.fns","eating_period");
    static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withValueSpecs(GLUCOSE_STREAM,INSULIN_STREAM,GLUCOSE_LAST,GLUCOSE_INACTIVITY,GLUCOSE_INACTIVITY_TIMESTAMP,GLUCOSE_ACTIVITY,GLUCOSE_ACTIVITY_TIMESTAMP)
                    .withSupplier(EatingPeriodFn::new)
                    .build();

    
    // eating period: when glucose rises by x% within y minutes
    public static Double x = 50.0; // percentage
    public static Long y = (long) 10; // minutes

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        if (message.is(GLUCOSE_TYPE)) { // if GLUCOSE_TYPE, determine if eating period, if yes, calculate insulin amount
            final Glucose glu = message.as(GLUCOSE_TYPE);
            final Long curr_minute = Long.parseLong(glu.getMinute());
            final Double curr_glu = Double.parseDouble(glu.getGlucose());
            Long startEatingPeriod = (long) 0; Long endEatingPeriod = (long) 0;

            var storage = context.storage();
            String glucose_stream_str = storage.get(GLUCOSE_STREAM).orElse("");
            // glucose_stream_lst structure: "minute1,glucoseVal1:minute2,glucoseVal2:minute3,glucoseVal3"
            List<String> glucose_stream_lst = new ArrayList<String>();
            if (glucose_stream_str != "") {
                glucose_stream_lst = Arrays.asList(glucose_stream_str.split(":"));
            }
            // glucose_stream structure: ["minute1,glucoseVal1","minute2,glucoseVal2","minute3,glucoseVal3"]
            List<Pair<Long, Double>> glucose_stream = new ArrayList<Pair<Long, Double>>();
            boolean found = false;

            for (int i = 0; i < glucose_stream_lst.size(); i++) {
                String[] item = glucose_stream_lst.get(i).split(",");
                // pair structure: (minute, glucoseVal)
                Pair<Long, Double> pair = new MutablePair<>(Long.parseLong(item[0]), Double.parseDouble(item[1]));
                Double glucoseVal = pair.getValue();
                Long prev_minute = pair.getKey();
                if (curr_minute - prev_minute <= y) { // if within time period
                    glucose_stream.add(pair);
                    // % increase = 100 * (final-initial)/initial
                    if ( 100 * (curr_glu - glucoseVal) / glucoseVal >= x && !found) {
                        System.out.println("Query 4 Eating period detected!");
                        glucose_stream.clear();
                        found = true;
                        startEatingPeriod = prev_minute;
                        endEatingPeriod = curr_minute;
                    }
                }
            }

            if (found == true) { // if eating period found, calculate insulin amount, ignore all insulin timestamp within the eating period
                storage.set(GLUCOSE_STREAM,"");
                found = false;

                // calculate insulin amount
                String insulin_str = storage.get(INSULIN_STREAM).orElse("");
                List<String> insulin_lst = new ArrayList<String>();
                if (insulin_str != "") {
                    insulin_lst = Arrays.asList(insulin_str.split(":"));
                }
                List<Pair<Long, Double>> insulin_stream = new ArrayList<Pair<Long, Double>>();
                Double insulinAmount = 0.0;
                int insulinCount = 0;
                for (int i = 0; i < insulin_lst.size(); i++) {
                    String[] item = insulin_lst.get(i).split(",");
                    Pair<Long, Double> pair = new MutablePair<>(Long.parseLong(item[0]), Double.parseDouble(item[1]));
                    Double insulin_amount = pair.getValue();
                    Long insulin_timestamp = pair.getKey();
                    if (insulin_timestamp >= startEatingPeriod && insulin_timestamp <= startEatingPeriod + 15) { // if within 15-minute time period
                        insulinAmount += insulin_amount;
                        insulinCount += 1;
                    } else if (insulin_timestamp > endEatingPeriod) {
                        insulin_stream.add(pair);
                    }
                }
                System.out.println("Query 4: " + "Insulin occurrence: " + String.valueOf(insulinCount));
                String storeStr = "";
                for (int i = 0; i < insulin_stream.size(); i++) {
                    Pair<Long, Double> pair = insulin_stream.get(i);
                    if (i == 0) {
                        storeStr = String.valueOf(pair.getKey()) + "," + String.valueOf(pair.getValue());
                    } else {
                        storeStr += ":" + String.valueOf(pair.getKey()) + "," + String.valueOf(pair.getValue());
                    }
                }
                storage.set(INSULIN_STREAM,storeStr);
            } else { // if eating period not found, add the new glucose value to the database
                glucose_stream.add(new MutablePair<>(curr_minute, curr_glu));
                String storeStr = "";
                for (int i = 0; i < glucose_stream.size(); i++) {
                    Pair<Long, Double> pair = glucose_stream.get(i);
                    if (i == 0) {
                        storeStr = String.valueOf(pair.getKey()) + "," + String.valueOf(pair.getValue());
                    } else {
                        storeStr += ":" + String.valueOf(pair.getKey()) + "," + String.valueOf(pair.getValue());
                    }
                    storage.set(GLUCOSE_STREAM,storeStr);
                }
            }

            // for Query 4B, saving last arrived glucose
            storage.set(GLUCOSE_LAST,curr_glu);
        }

        else if (message.is(INSULIN_TYPE)) { // if INSULIN_TYPE, store the insulin data to database
            final Insulin insulin = message.as(INSULIN_TYPE);
            var storage = context.storage();
            String insulin_str = storage.get(INSULIN_STREAM).orElse("");
            if (insulin_str == "") {
                insulin_str = String.valueOf(insulin.getTimestamp()) + "," + String.valueOf(insulin.getDoseAmount());
            } else {
                insulin_str += ":" + String.valueOf(insulin.getTimestamp()) + "," + String.valueOf(insulin.getDoseAmount());
            }
            storage.set(INSULIN_STREAM,insulin_str);
            System.out.println("Query 4: Insulin Timestamp: " + insulin.getTimestamp() + ", Device ID: " + insulin.getDeviceID() + ", Patient ID: " + insulin.getPatientID() + ", dose_amount: " + insulin.getDoseAmount());
        }

        else if (message.is(FITBIT_TYPE)) { // if FITBIT_TYPE, identify “activity” period based on heart rate & compute glucose drop
            final Fitbit fitbit = message.as(FITBIT_TYPE);
            System.out.println("Query 4: Fitbit Timestamp: " + fitbit.getTimestamp() + ", Device ID: " + fitbit.getDeviceID() + ", Patient ID: " + fitbit.getPatientID() + ", METs: " + fitbit.getMETs() + ", Intensity: " + fitbit.getIntensity() + ", Steps: " + fitbit.getSteps() + ", Heart rate: " + fitbit.getHeartRate());
            var threshold_activity_heartrate = 70; // “activity” period based on heart rate
            var storage = context.storage();
            Double glucose = storage.get(GLUCOSE_LAST).orElse(0.0);
            Double last_glucose_activity = storage.get(GLUCOSE_ACTIVITY).orElse(0.0);
            Long last_glucose_activity_timestamp = storage.get(GLUCOSE_ACTIVITY_TIMESTAMP).orElse(0L);
            Double last_glucose_inactivity = storage.get(GLUCOSE_INACTIVITY).orElse(0.0);
            Long last_glucose_inactivity_timestamp = storage.get(GLUCOSE_INACTIVITY_TIMESTAMP).orElse(0L);

            if (Double.parseDouble(fitbit.getHeartRate()) < threshold_activity_heartrate) {
                Double glucose_inactivity = glucose - last_glucose_inactivity;
                Long time = Long.parseLong(fitbit.getTimestamp()) - last_glucose_inactivity_timestamp;
                if (glucose_inactivity < 0) {
                    System.out.println("Query 4: " + "Inactivity period: glucose level is " + String.valueOf(glucose) + ", with a " + String.valueOf(-1*glucose_inactivity) + " decrease within a time period of " + time);
                } else {
                    System.out.println("Query 4: " + "Inactivity period: glucose level is " + String.valueOf(glucose) + ", with a " + String.valueOf(glucose_inactivity) + " increase within a time period of " + time);
                }
                storage.set(GLUCOSE_INACTIVITY,glucose);
                storage.set(GLUCOSE_INACTIVITY_TIMESTAMP,Long.parseLong(fitbit.getTimestamp()));
            } else {
                Double glucose_activity = glucose - last_glucose_activity;
                Long time = Long.parseLong(fitbit.getTimestamp()) - last_glucose_activity_timestamp;
                if (glucose_activity < 0) {
                    System.out.println("Query 4: " + "Activity period: glucose level is " + String.valueOf(glucose) + " at " + Long.parseLong(fitbit.getTimestamp()) + ", with a " + String.valueOf(-1*glucose_activity) + " decrease within a time period of " + time);
                } else {
                    System.out.println("Query 4: " + "Activity period: glucose level is " + String.valueOf(glucose) + " at " + Long.parseLong(fitbit.getTimestamp()) + ", with a " + String.valueOf(glucose_activity) + " increase within a time period of " + time);
                }
                storage.set(GLUCOSE_ACTIVITY,glucose);
                storage.set(GLUCOSE_ACTIVITY_TIMESTAMP,Long.parseLong(fitbit.getTimestamp()));
            }
        }

        else {
            throw new IllegalArgumentException("Eating Period: Unexpected message type: " + message.valueTypeName());
        }

        return context.done();
    }
}
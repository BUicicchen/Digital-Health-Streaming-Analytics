package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/* Compare the efficiencies
    Query 2: sBP [0, 15], dBP [0, 15], glucose [0, 15]
    Query 3: Rise by 20%, individual rise is immediate, inter-rise period = 1
    Query 4: Rise by 50% within 10 minutes
*/

// Q2 - anomaly detection: To detect an anomaly, the query maintains a moving average, the mean of the averages
// across three consecutive windows. The query compares the current window average with the moving average
// and generates an alert if their difference is too high. Identify anomalies in BP and glucose measurements.
public class Query2 {
    // showStream = 0: only show blood pressure
    // showStream = 1: only show glucose
    // showStream = other number: show both
    protected static int showStream = 2;
    // default tumbling window size, i.e., number of tuples per window
    protected static int windowSize = 5;
    protected static int numWindowTrack = 3;
    protected static final Map<String, Float[]> bloodPressureAlert = new HashMap<String, Float[]>(){{
        put(TupBloodPressure.attrNameDBP, new Float[] {0F, 15.0F});
        put(TupBloodPressure.attrNameSBP, new Float[] {0F, 15.0F});
    }};
    protected static final Map<String, Float[]> glucoseAlert = new HashMap<String, Float[]>() {{
        put(TupGlucose.attrNameGlucose, new Float[]{0F, 15.0F});
    }};
    // whether the current avg participates in the computation of the moving avg
    protected static boolean isCurrentInMovingAvg = true;
    // data stream config here
    protected static boolean isOutOfOrder = true;
    protected static int outOfOrderNess = 10;
    protected static final int autoWatermarkInterval = 10;
    protected static final int sleepTime = 150;
    protected static final int parallelism = 1;
    protected static final boolean isSaveFile = false;
    // show runtime
    protected static final boolean showRuntime = false;
    // limit of total tuple numbers, null is infinity
    protected static final Long dataSizeLim = null;
    // the distribution types for the 2 data streams
    protected static final String distributionType1 = DataGenerator.distNameUniform;
    protected static final String distributionType2 = DataGenerator.distNameUniform;
    // number of patients
    protected static final int patientNum = 1;
    // enabling Kafka
    protected static final boolean isKafka = false;
    // job name
    protected static final String jobName = "Query 2 Exe";
    // main program
    public static void main(String[] args) throws Exception {
        // initialize the producer properties
        Properties properties;
        if (isKafka) {
            properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
        }
        // initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (isOutOfOrder) {env.getConfig().setAutoWatermarkInterval(outOfOrderNess);}
        else {
            outOfOrderNess = 0;
            env.getConfig().setAutoWatermarkInterval(autoWatermarkInterval);
        }
        // define the source
        if (showStream != 1) {
            // blood pressure data source
            DataStream<TupBloodPressure> streamBloodPressure = env
                    .addSource(new DataGenerator<TupBloodPressure>(
                            patientNum, TupBloodPressure.deviceName, distributionType1,
                            0, 1, false,
                            sleepTime, outOfOrderNess, dataSizeLim, isSaveFile
                    ))
                    .returns(TypeInformation.of(TupBloodPressure.class))
                    .setParallelism(parallelism);
            if (isOutOfOrder) {
                // in the case of tumbling window, out-of-order data can be automatically handled without timers
                streamBloodPressure = streamBloodPressure.assignTimestampsAndWatermarks(WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)));
            }
            else {
                streamBloodPressure = streamBloodPressure.assignTimestampsAndWatermarks(WatermarkStrategy
                        .forMonotonousTimestamps());
            }
            if (!isKafka) {
                streamBloodPressure.keyBy(eventBloodPressure -> eventBloodPressure.patientID)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                        .process(new Q2WindowFunction<>(numWindowTrack, bloodPressureAlert, isCurrentInMovingAvg))
                        .print()
                        .setParallelism(parallelism);
            }
            else {
                DataStream<String> streamKafka = streamBloodPressure
                        .keyBy(eventBloodPressure -> eventBloodPressure.patientID)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                        .process(new Q2WindowFunction<>(numWindowTrack, bloodPressureAlert, isCurrentInMovingAvg))
                        .setParallelism(parallelism);
                KafkaSink<String> sink = KafkaSink.<String>builder()
                        .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("Query 2")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                        )
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build();
                streamKafka.sinkTo(sink);
            }
        }
        if (showStream != 0) {
            // glucose data source
            DataStream<TupGlucose> streamGlucose = env
                    .addSource(new DataGenerator<TupGlucose>(
                            patientNum, TupGlucose.deviceName, distributionType2,
                            0, 1, false,
                            sleepTime, outOfOrderNess, dataSizeLim, isSaveFile))
                    .returns(TypeInformation.of(TupGlucose.class))
                    .setParallelism(parallelism);
            if (isOutOfOrder) {
                streamGlucose = streamGlucose.assignTimestampsAndWatermarks(WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)));
            }
            else {
                streamGlucose = streamGlucose.assignTimestampsAndWatermarks(WatermarkStrategy
                        .forMonotonousTimestamps());
            }
            if (!isKafka) {
                streamGlucose.keyBy(eventGlucose -> eventGlucose.patientID)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                        .process(new Q2WindowFunction<>(numWindowTrack, glucoseAlert, isCurrentInMovingAvg))
                        .print()
                        .setParallelism(parallelism);
            }
            else {
                DataStream<String> streamKafka = streamGlucose
                        .keyBy(eventGlucose -> eventGlucose.patientID)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                        .process(new Q2WindowFunction<>(numWindowTrack, glucoseAlert, isCurrentInMovingAvg))
                        .setParallelism(parallelism);
                KafkaSink<String> sink = KafkaSink.<String>builder()
                        .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("Query 2")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                        )
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build();
                streamKafka.sinkTo(sink);
            }
        }
        // Execute the program
        if (showRuntime) {
            long startTime = System.nanoTime();
            env.execute(jobName);
            long endTime = System.nanoTime();
            long runtime = endTime - startTime;
            System.out.println("Runtime: " + runtime / 1000000F / 1000F + " seconds");
        }
        else {
            env.execute(jobName);
        }
    }
}
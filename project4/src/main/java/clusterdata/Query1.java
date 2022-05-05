package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// flexible "session window"
// consecutive tuples with a certain attribute value greater than the threshold will be grouped into a "window"
// Q1 - compute the average Blood Pressure value over different activity periods, e.g. running, walking, sleeping.
public class Query1 {
    // if the value is within the threshold range, this tuple will be put in a custom "session window"
    protected static final Map<String,Float[]> sessionNamesMap = new HashMap<String,Float[]>() {{
        put(DataGenerator.sessionNameSleep, new Float[] {DataGenerator.heartRateSleepLow.floatValue(),
                DataGenerator.heartRateSleepHigh.floatValue()});
        put(DataGenerator.sessionNameWalk, new Float[] {DataGenerator.heartRateWalkLow.floatValue(),
                DataGenerator.heartRateWalkHigh.floatValue()});
        put(DataGenerator.sessionNameEat, new Float[] {DataGenerator.heartRateEatLow.floatValue(),
                DataGenerator.heartRateEatHigh.floatValue()});
        put(DataGenerator.sessionNameRun, new Float[] {DataGenerator.heartRateRunLow.floatValue(),
                DataGenerator.heartRateRunHigh.floatValue()});
        put(DataGenerator.sessionNameNA, null);  // the session name for those that do not belong to any actual session
    }};
    // we are interested in sBP and dBP of the blood pressure stream
    protected static final String[] interestedAttr = new String[] {TupBloodPressure.attrNameDBP,
                                                                   TupBloodPressure.attrNameSBP};
    // data stream config here
    protected static boolean isOutOfOrder = true;
    protected static int outOfOrderNess = 10;
    protected static final int autoWatermarkInterval = 10;
    protected static final int sleepTime = 150;
    protected static final int parallelism = 1;
    protected static final boolean isSaveFile = false;
    // the distribution types for the 2 data streams
    protected static final String distributionType1 = DataGenerator.distNameUniform;
    protected static final String distributionType2 = DataGenerator.distNameUniform;
    // enabling Kafka
    protected static final boolean isKafka = false;
    // number of patients
    protected static final int patientNum = 1;
    // show runtime
    protected static final boolean showRuntime = false;
    // limit of total tuple numbers, null is infinity
    protected static Long dataSizeLim = null;
    // job name
    protected static final String jobName = "Query 1 Execution";
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
        if (isOutOfOrder) {
            // handle the data generator issue
            if (dataSizeLim != null) {dataSizeLim += outOfOrderNess;}
            env.getConfig().setAutoWatermarkInterval(outOfOrderNess);
        }
        else {
            outOfOrderNess = 0;
            env.getConfig().setAutoWatermarkInterval(autoWatermarkInterval);
        }
        // define the source
        // the stream of smartwatch data
        DataStream<TupSmartwatch> streamSmartwatch = env
                .addSource(new DataGenerator<TupSmartwatch>(
                        patientNum, TupSmartwatch.deviceName, distributionType1,
                        0, 1, false,
                        sleepTime, outOfOrderNess, dataSizeLim, isSaveFile))
                .returns(TypeInformation.of(TupSmartwatch.class))
                .setParallelism(parallelism);
        // the stream of blood pressure data
        DataStream<TupBloodPressure> streamBloodPressure = env
                .addSource(new DataGenerator<TupBloodPressure>(
                        patientNum, TupBloodPressure.deviceName, distributionType2,
                        0, 1, false,
                        sleepTime, outOfOrderNess, dataSizeLim, isSaveFile))
                .returns(TypeInformation.of(TupBloodPressure.class))
                .setParallelism(parallelism);
        // out-of-order & in-order streams have different watermark strategies
        if (isOutOfOrder) {
            streamSmartwatch = streamSmartwatch.assignTimestampsAndWatermarks(WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)));
            streamBloodPressure = streamBloodPressure.assignTimestampsAndWatermarks(WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)));
        }
        else {
            streamSmartwatch = streamSmartwatch.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
            streamBloodPressure = streamBloodPressure.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
        }
        // convert the streams into keyed streams
        streamSmartwatch = streamSmartwatch.keyBy(eventSmartwatch -> eventSmartwatch.patientID);
        streamBloodPressure = streamBloodPressure.keyBy(eventBloodPressure -> eventBloodPressure.patientID);
        if (!isKafka) {
            // connect the 2 streams
            streamSmartwatch
                    .connect(streamBloodPressure)
                    .process(new Q1CoProcessFunction<>(sessionNamesMap, interestedAttr, 100, false))
                    .print()
                    .setParallelism(parallelism);
        }
        else {
            DataStream<String> streamKafka = streamSmartwatch
                    .connect(streamBloodPressure)
                    .process(new Q1CoProcessFunction<>(sessionNamesMap, interestedAttr, 100, false))
                    .setParallelism(parallelism);
            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("Query 1")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
            streamKafka.sinkTo(sink);
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

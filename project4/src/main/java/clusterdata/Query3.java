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
import java.util.Properties;

// Q3 - pattern detection: Detect a sequence of events and raise an alert, e.g., an
// increase in glucose followed by an increase in heart rate within a specified threshold
// in this context, it will be an increase in stream 1 followed by an increase in stream 2
public class Query3 {
    // query config here
    protected static Double riseRatioGlucose = 0.2;
    // the value needs to increase by more than the threshold within this particular period of time
    protected static final long risePeriodGlucose = 1;
    protected static Double riseRatioSmartwatch = 0.2;
    protected static final long risePeriodSmartwatch = 1;
    protected static final long interRisePeriod = 1;  // the period length in between the 2 rises
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
    protected static final String jobName = "Query 3 Exe";
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
        // stream for glucose data
        DataStream<TupGlucose> streamGlucose = env
                .addSource(new DataGenerator<TupGlucose>(
                        patientNum, TupGlucose.deviceName, distributionType1,
                        0, 1, false,
                        sleepTime, outOfOrderNess, dataSizeLim, isSaveFile))
                .returns(TypeInformation.of(TupGlucose.class))
                .setParallelism(parallelism);
        // stream for smartwatch data
        DataStream<TupSmartwatch> streamSmartwatch = env
                .addSource(new DataGenerator<TupSmartwatch>(
                        patientNum, TupSmartwatch.deviceName, distributionType2,
                        0, 1, false,
                        sleepTime, outOfOrderNess, dataSizeLim, isSaveFile))
                .returns(TypeInformation.of(TupSmartwatch.class))
                .setParallelism(parallelism);
        // out-of-order & in-order streams have different watermark strategies
        if (isOutOfOrder) {
            streamGlucose = streamGlucose.assignTimestampsAndWatermarks(WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)));
            streamSmartwatch = streamSmartwatch.assignTimestampsAndWatermarks(WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)));
            // convert the streams into keyed streams, use window to handle out-of-ordered data
            streamGlucose = streamGlucose.keyBy(eventGlucose -> eventGlucose.patientID)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(outOfOrderNess)))
                    .process(new QueryWindowQueue<>())
                    .keyBy(eventGlucose -> eventGlucose.patientID);
            streamSmartwatch = streamSmartwatch.keyBy(eventSmartwatch -> eventSmartwatch.patientID)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(outOfOrderNess)))
                    .process(new QueryWindowQueue<>())
                    .keyBy(eventSmartwatch -> eventSmartwatch.patientID);
        }
        else {  // convert the streams into keyed streams
            streamGlucose = streamGlucose
                    .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                    .keyBy(eventGlucose -> eventGlucose.patientID);
            streamSmartwatch = streamSmartwatch
                    .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                    .keyBy(eventSmartwatch -> eventSmartwatch.patientID);
        }
        if (!isKafka) {
            // connect the 2 streams
            streamGlucose
                    .connect(streamSmartwatch)
                    .process(new Q3CoProcessFunction(riseRatioGlucose, risePeriodGlucose, riseRatioSmartwatch,
                            risePeriodSmartwatch, interRisePeriod))
                    .print()
                    .setParallelism(parallelism);
        }
        else {
            DataStream<String> streamKafka = streamGlucose
                    .connect(streamSmartwatch)
                    .process(new Q3CoProcessFunction(riseRatioGlucose, risePeriodGlucose, riseRatioSmartwatch,
                            risePeriodSmartwatch, interRisePeriod))
                    .setParallelism(parallelism);
            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("Query 3")
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

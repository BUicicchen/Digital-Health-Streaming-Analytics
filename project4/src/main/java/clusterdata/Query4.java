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

// Q4 - Identify eating period: when glucose rises by x% within y minutes, where x, y are input parameters.
// A. Determine how often insulin is given within 15 minutes of the start of this period.
// B. Identify “activity” period
// e.g., based on heart rate or steps and compute how glucose drops compared to inactivity period.
public class Query4 {
    // query config here
    // config for A
    protected static final Double riseRatioX = 0.5;
    protected static final long risePeriodY = 4;
    protected static final long freqPeriod = 15;  // how often insulin is given within 15 minutes of the start of this period
    // config for B
    protected static final String[] interestedAttr = new String[] {TupGlucose.attrNameGlucose};
    // switch between 2 function options when connecting only the 1st & 3rd streams
    protected static final boolean useFlipFunc = false;
    // if the value is within the threshold range, this tuple will be put in a custom "session window"
    protected static final Map<String,Float[]> sessionNamesMap = new HashMap<String,Float[]>() {{
        put(DataGenerator.sessionNameActive, new Float[] {0.0F, 70.0F});
        put(DataGenerator.sessionNameInactive, new Float[] {70.0F, null});
        put(DataGenerator.sessionNameNA, null);  // the session name for those that do not belong to any actual session
    }};
    // data stream config here
    protected static boolean isOutOfOrder = true;
    protected static int outOfOrderNess = 10;
    protected static final int autoWatermarkInterval = 10;
    protected static final int sleepTime = 150;
    protected static final int parallelism = 1;
    protected static final boolean isSaveFile = false;
    // limit of total tuple numbers, null is infinity
    protected static Long dataSizeLim = null;
    // show runtime
    protected static final boolean showRuntime = false;
    // control which streams to be shown, 1: stream 1 & 2; 2: stream 1 & 3; 3: all the streams
    protected static final int showStream = 3;
    // the distribution types for the 3 data streams
    protected static final String distributionType1 = DataGenerator.distNameUniform;
    protected static final String distributionType2 = DataGenerator.distNameUniform;
    protected static final String distributionType3 = DataGenerator.distNameUniform;
    // number of patients
    protected static final int patientNum = 1;
    // job name
    protected static final String jobName = "Query 4 Exe";
    // enabling Kafka
    protected static final boolean isKafka = false;
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
        // stream 1: glucose
        DataStream<TupGlucose> streamGlucose = env
                .addSource(new DataGenerator<TupGlucose>(patientNum, TupGlucose.deviceName, distributionType1,
                        0, 1, false,
                           sleepTime, outOfOrderNess, dataSizeLim, isSaveFile))
                .returns(TypeInformation.of(TupGlucose.class))
                .setParallelism(parallelism);
        // stream 2: insulin
        // "how often" will be calculated in terms of avg
        DataStream<TupInsulin> streamInsulin = env
                .addSource(new DataGenerator<TupInsulin>(patientNum, TupInsulin.deviceName, distributionType2,
                           0, 1, false,
                           sleepTime, outOfOrderNess, dataSizeLim, isSaveFile))
                .returns(TypeInformation.of(TupInsulin.class))
                .setParallelism(parallelism);
        // stream 3: activity, indicated by Fitbit
        DataStream<TupFitbit> streamFitbit = env
                .addSource(new DataGenerator<TupFitbit>(patientNum, TupFitbit.deviceName, distributionType3,
                           0, 1, false,
                           sleepTime, outOfOrderNess, dataSizeLim, isSaveFile))
                .returns(TypeInformation.of(TupFitbit.class))
                .setParallelism(parallelism);
        // out-of-order & in-order streams have different watermark strategies
        if (isOutOfOrder) {
            streamGlucose = streamGlucose.assignTimestampsAndWatermarks(WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)));
            streamInsulin = streamInsulin.assignTimestampsAndWatermarks(WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)));
            // convert the streams into keyed streams, use window to handle out-of-ordered data
            streamGlucose = streamGlucose.keyBy(eventGlucose -> eventGlucose.patientID)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(outOfOrderNess)))
                    .process(new QueryWindowQueue<>())
                    .keyBy(eventGlucose -> eventGlucose.patientID);
            streamInsulin = streamInsulin.keyBy(eventInsulin -> eventInsulin.patientID)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(outOfOrderNess)))
                    .process(new QueryWindowQueue<>())
                    .keyBy(eventInsulin -> eventInsulin.patientID);
            streamFitbit = streamFitbit.assignTimestampsAndWatermarks(WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderNess)))
                    .keyBy(eventFitbit -> eventFitbit.patientID);
        }
        else {
            streamGlucose = streamGlucose
                    .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                    .keyBy(eventGlucose -> eventGlucose.patientID);
            streamInsulin = streamInsulin
                    .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                    .keyBy(eventInsulin -> eventInsulin.patientID);
            streamFitbit = streamFitbit
                    .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                    .keyBy(eventFitbit -> eventFitbit.patientID);
        }
        if (showStream == 1) {
            // connect the 1st & 2nd streams
            // when glucose rises by x% within y minutes, determine how often insulin is given
            if (!isKafka) {
                streamGlucose
                        .connect(streamInsulin)
                        .process(new Q4CoProcessFunction<String>(riseRatioX, risePeriodY, freqPeriod, true, dataSizeLim))
                        .returns(TypeInformation.of(String.class))
                        .print()
                        .setParallelism(parallelism);
            }
            else {
                DataStream<String> streamKafka = streamGlucose
                        .connect(streamInsulin)
                        .process(new Q4CoProcessFunction<String>(riseRatioX, risePeriodY, freqPeriod, true, dataSizeLim))
                        .returns(TypeInformation.of(String.class))
                        .setParallelism(parallelism);
                KafkaSink<String> sink = KafkaSink.<String>builder()
                        .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("Query 4 A")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                        )
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build();
                streamKafka.sinkTo(sink);
            }
        }
        else if (showStream == 2) {
            // connect the 1st & 3rd streams
            if (useFlipFunc) {
                // it can be demonstrated that both the original and flipped version work well
                // when only Fitbit and glucose are connected
                if (!isKafka) {
                    streamGlucose
                            .connect(streamFitbit)
                            .process(new Q1CoProcessFuncFlip<>(sessionNamesMap, interestedAttr, 100, true))
                            .print()
                            .setParallelism(parallelism);
                }
                else {
                    DataStream<String> streamKafka = streamGlucose
                            .connect(streamFitbit)
                            .process(new Q1CoProcessFuncFlip<>(sessionNamesMap, interestedAttr, 100, true))
                            .setParallelism(parallelism);
                    KafkaSink<String> sink = KafkaSink.<String>builder()
                            .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                    .setTopic("Query 4 B Flip")
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                            )
                            .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .build();
                    streamKafka.sinkTo(sink);
                }
            }
            else {
                if (!isKafka) {
                    streamFitbit
                            .connect(streamGlucose)
                            .process(new Q1CoProcessFunction<>(sessionNamesMap, interestedAttr, 100, true))
                            .print()
                            .setParallelism(parallelism);
                }
                else {
                    DataStream<String> streamKafka = streamFitbit
                            .connect(streamGlucose)
                            .process(new Q1CoProcessFunction<>(sessionNamesMap, interestedAttr, 100, true))
                            .setParallelism(parallelism);
                    KafkaSink<String> sink = KafkaSink.<String>builder()
                            .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                    .setTopic("Query 4 B")
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                            )
                            .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .build();
                    streamKafka.sinkTo(sink);
                }
            }
        }
        else {
            if (!isKafka) {
                // connect the 3 streams
                streamGlucose
                        .connect(streamInsulin)
                        .process(new Q4CoProcessFunction<Tuple2CustomQ4>(riseRatioX, risePeriodY, freqPeriod, false, dataSizeLim))
                        .returns(TypeInformation.of(Tuple2CustomQ4.class))
                        .keyBy(event -> event.dataTuple.patientID)
                        .connect(streamFitbit)
                        .process(new Q1CoProcessFuncFlip<>(sessionNamesMap, interestedAttr, 100, true))
                        .print()
                        .setParallelism(parallelism);
            }
            else {
                DataStream<String> streamKafka = streamGlucose
                        .connect(streamInsulin)
                        .process(new Q4CoProcessFunction<Tuple2CustomQ4>(riseRatioX, risePeriodY, freqPeriod, false, dataSizeLim))
                        .returns(TypeInformation.of(Tuple2CustomQ4.class))
                        .keyBy(event -> event.dataTuple.patientID)
                        .connect(streamFitbit)
                        .process(new Q1CoProcessFuncFlip<>(sessionNamesMap, interestedAttr, 100, true))
                        .setParallelism(parallelism);
                KafkaSink<String> sink = KafkaSink.<String>builder()
                        .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("Query 4 A & B")
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

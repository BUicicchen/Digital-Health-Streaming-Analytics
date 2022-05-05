package clusterdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaStream {
    public static void main(String[] args) throws Exception {
        // initialize the producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set up the data stream
        DataStream<String> streamSmartwatch = env
                .addSource(new DataGenerator<TupSmartwatch>(1, 1, "Smartwatch",
                          false, 150))
                .returns(TypeInformation.of(TupSmartwatch.class))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(eventSmartwatch -> eventSmartwatch.deviceID)
                .process(new KeyedProcessFunction<Integer, TupSmartwatch, String>() {
                    @Override
                    public void processElement(TupSmartwatch data, Context ctx, Collector<String> out) throws Exception {
                        out.collect(data.toString());
                    }
                });
        // write the filtered data to a Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("TupSmartwatch")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
        streamSmartwatch.sinkTo(sink);
        // execute program
        env.execute("Consume events from Kafka");
    }
}

package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import java.util.Properties;

/**
 * Write Wikipedia Edit Events with non-null summary to a Kafka topic.
 * Make sure to start Kafka and create the topic before running this application!
 */
public class FilteredEventsToKafka {

    public static void main(String[] args) throws Exception {

        // Kafka producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WikipediaEditEvent> edits = env
                .addSource(new WikipediaEditsSource());

        // Filter out events with negative byteDiff
        /* TODO: Filter out events with empty edit summary, hint: use edits.filter and check their ByteDiff size
         * TODO: Create map function afterwards that returns the string of a WikipediaEditEvent
         *
         * Example:
         * DataStream<String> filtered = edits.filter(...).map(...);
         */
        DataStream<String> filtered = edits.filter(new FilterFunction<WikipediaEditEvent>() {
            @Override
            public boolean filter(WikipediaEditEvent e) throws Exception {
                return e.getByteDiff() > 0;
            }
        }).map(new MapFunction<WikipediaEditEvent, String>() {
            @Override
            public String map(WikipediaEditEvent e) throws Exception {
                return e.toString();
            }
        });

        // write the filtered data to a Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("wiki-edits")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        filtered.sinkTo(sink);

        // run the pipeline
        env.execute("Filtered Events to Kafka");
    }
}
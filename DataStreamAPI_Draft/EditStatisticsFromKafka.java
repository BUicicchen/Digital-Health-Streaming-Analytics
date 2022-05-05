package org.apache.flink.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Write a Flink program that consumes events from the Kafka topic "wiki-edits",
 * computes the absolute number of byteDiff every 10s,
 * and prints the result to standard output.
 * <p>
 * TIP: See the following Flink documentation pages:
 * - Kafka consumer: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/kafka.html#kafka-consumer
 * - Windows: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/operators/windows.html
 */
public class EditStatisticsFromKafka {
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("topic.id", "wiki-edits");
        properties.setProperty("group.id", "test");

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a Kafka consumer
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setTopics(properties.getProperty("topic.id"))
                .setGroupId(properties.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaStream.print();

        // execute program
        env.execute("Consume events from Kafka");

    }
}

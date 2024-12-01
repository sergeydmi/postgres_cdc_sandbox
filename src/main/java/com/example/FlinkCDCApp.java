package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Build the SourceFunction for PostgreSQL CDC
        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("postgres") // Kubernetes service name for PostgreSQL
                .port(5432)
                .database("cdc_testdb") // Monitor 'cdc_testdb' database
                .schemaList("public")   // Monitor 'public' schema
                .tableList("public.cdc_test") // Monitor 'cdc_test' table
                .username("postgres")
                .password("postgres")
                .decodingPluginName("pgoutput") // Use 'pgoutput' decoding plugin
                .deserializer(new JsonDebeziumDeserializationSchema(true)) // Convert SourceRecord to JSON String
                .build();

        // Add the source to the environment
        DataStream<String> postgresStream = env.addSource(sourceFunction);

        // Configure Kafka Sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("cdc_test_topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        // Sink the data to Kafka
        postgresStream
                .sinkTo(kafkaSink)
                .setParallelism(1); // Use parallelism 1 to keep message ordering

        // Execute the Flink job
        env.execute("Flink CDC Postgres to Kafka");
    }
}

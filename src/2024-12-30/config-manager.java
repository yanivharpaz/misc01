package org.example.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigManager {
    private static final String CONFIG_FILE = "application.properties";
    private final Properties properties;

    public ConfigManager() {
        this(CONFIG_FILE);
    }

    public ConfigManager(String configFile) {
        properties = new Properties();
        loadProperties(configFile);
        loadEnvironmentOverrides();
    }

    private void loadProperties(String configFile) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (input == null) {
                System.err.println("Unable to find " + configFile);
                return;
            }
            properties.load(input);
        } catch (IOException e) {
            System.err.println("Error loading properties file: " + e.getMessage());
        }
    }

    private void loadEnvironmentOverrides() {
        // Override with environment variables if they exist
        mapEnvVariable("KAFKA_BOOTSTRAP_SERVERS", "kafka.bootstrap.servers");
        mapEnvVariable("KAFKA_GROUP_ID", "kafka.group.id");
        mapEnvVariable("KAFKA_SOURCE_TOPIC", "kafka.source.topic");
        mapEnvVariable("KAFKA_DLQ_TOPIC", "kafka.dlq.topic");
        mapEnvVariable("ELASTICSEARCH_HOST", "elasticsearch.host");
        mapEnvVariable("ELASTICSEARCH_PORT", "elasticsearch.port");
        mapEnvVariable("ELASTICSEARCH_SCHEME", "elasticsearch.scheme");
        mapEnvVariable("BATCH_SIZE", "batch.size");
        mapEnvVariable("BATCH_TIMEOUT_MS", "batch.timeout.ms");
        mapEnvVariable("INDEX_PREFIX", "index.prefix");
    }

    private void mapEnvVariable(String envVar, String propertyKey) {
        String value = System.getenv(envVar);
        if (value != null && !value.isEmpty()) {
            properties.setProperty(propertyKey, value);
        }
    }

    public String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    public String getKafkaGroupId() {
        return properties.getProperty("kafka.group.id", "kafka-to-elasticsearch-consumer");
    }

    public String getKafkaSourceTopic() {
        return properties.getProperty("kafka.source.topic", "my-topic");
    }

    public String getKafkaDlqTopic() {
        return properties.getProperty("kafka.dlq.topic", "my-topic-dlq");
    }

    public String getElasticsearchHost() {
        return properties.getProperty("elasticsearch.host", "localhost");
    }

    public int getElasticsearchPort() {
        return Integer.parseInt(properties.getProperty("elasticsearch.port", "9200"));
    }

    public String getElasticsearchScheme() {
        return properties.getProperty("elasticsearch.scheme", "http");
    }

    public int getBatchSize() {
        return Integer.parseInt(properties.getProperty("batch.size", "1000"));
    }

    public long getBatchTimeoutMs() {
        return Long.parseLong(properties.getProperty("batch.timeout.ms", "5000"));
    }

    public String getIndexPrefix() {
        return properties.getProperty("index.prefix", "prd_a_");
    }

    public Properties getKafkaConsumerProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", getKafkaBootstrapServers());
        kafkaProps.put("group.id", getKafkaGroupId());
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("auto.offset.reset", properties.getProperty("kafka.consumer.auto.offset.reset", "earliest"));
        kafkaProps.put("enable.auto.commit", properties.getProperty("kafka.consumer.enable.auto.commit", "false"));
        return kafkaProps;
    }

    public Properties getKafkaProducerProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", getKafkaBootstrapServers());
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", 3);
        return kafkaProps;
    }
}
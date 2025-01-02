package org.example.config;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.io.File;
import java.io.FileWriter;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ConfigManagerTest {
    
    @TempDir
    Path tempDir;
    
    private void createPropertiesFile(String filename, Properties props) throws Exception {
        File file = tempDir.resolve(filename).toFile();
        try (FileWriter writer = new FileWriter(file)) {
            props.store(writer, null);
        }
    }

    @Test
    void shouldLoadDefaultConfiguration() {
        ConfigManager config = new ConfigManager();
        assertEquals("localhost:9092", config.getKafkaBootstrapServers());
        assertEquals(9200, config.getElasticsearchPort());
    }

    @Test
    void shouldLoadDevConfiguration() {
        System.setProperty("app.environment", "dev");
        ConfigManager config = new ConfigManager();
        assertTrue(config.isDevEnvironment());
        assertFalse(config.isProdEnvironment());
        System.clearProperty("app.environment");
    }

    @Test
    void shouldLoadProdConfiguration() {
        System.setProperty("app.environment", "prod");
        ConfigManager config = new ConfigManager();
        assertTrue(config.isProdEnvironment());
        assertFalse(config.isDevEnvironment());
        System.clearProperty("app.environment");
    }

    @Test
    void shouldOverrideWithEnvironmentVariables() {
        // Using reflection to set environment variable
        try {
            setEnv("KAFKA_BOOTSTRAP_SERVERS", "test-kafka:9092");
            ConfigManager config = new ConfigManager();
            assertEquals("test-kafka:9092", config.getKafkaBootstrapServers());
        } finally {
            setEnv("KAFKA_BOOTSTRAP_SERVERS", null);
        }
    }

    @Test
    void shouldCreateValidKafkaConsumerProperties() {
        ConfigManager config = new ConfigManager();
        Properties kafkaProps = config.getKafkaConsumerProperties();
        
        assertNotNull(kafkaProps);
        assertEquals("localhost:9092", kafkaProps.getProperty("bootstrap.servers"));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", 
            kafkaProps.getProperty("key.deserializer"));
    }

    @Test
    void shouldCreateValidKafkaProducerProperties() {
        ConfigManager config = new ConfigManager();
        Properties kafkaProps = config.getKafkaProducerProperties();
        
        assertNotNull(kafkaProps);
        assertEquals("all", kafkaProps.getProperty("acks"));
        assertEquals("3", kafkaProps.getProperty("retries"));
    }

    @Test
    void shouldHandleInvalidEnvironment() {
        System.setProperty("app.environment", "invalid");
        ConfigManager config = new ConfigManager();
        // Should default to dev
        assertTrue(config.isDevEnvironment());
        System.clearProperty("app.environment");
    }

    @Test
    void shouldValidateBatchConfiguration() {
        ConfigManager config = new ConfigManager();
        assertTrue(config.getBatchSize() > 0);
        assertTrue(config.getBatchTimeoutMs() > 0);
    }

    @Test
    void shouldLoadConfigurationInCorrectOrder() throws Exception {
        // Create temp properties files
        Properties defaultProps = new Properties();
        defaultProps.setProperty("test.property", "default");
        createPropertiesFile("application.properties", defaultProps);

        Properties devProps = new Properties();
        devProps.setProperty("test.property", "dev");
        createPropertiesFile("application-dev.properties", devProps);

        // Set up system property
        System.setProperty("app.environment", "dev");
        
        // TODO: Add test implementation after modifying ConfigManager to accept custom config path
        
        System.clearProperty("app.environment");
    }

    // Helper method to set environment variables for testing
    private static void setEnv(String key, String value) {
        try {
            java.lang.reflect.Field field = System.getenv().getClass().getDeclaredField("m");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.Map<String, String> env = (java.util.Map<String, String>) field.get(System.getenv());
            if (value == null) {
                env.remove(key);
            } else {
                env.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to set environment variable", e);
        }
    }
}
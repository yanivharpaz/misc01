package org.example.config;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({MockitoExtension.class, SystemStubsExtension.class})
class ConfigManagerTest {

    @SystemStub
    private EnvironmentVariables environmentVariables;
    
    @Test
    void shouldLoadDefaultConfiguration() {
        ConfigManager config = new ConfigManager();
        assertEquals("localhost:9092", config.getKafkaBootstrapServers());
        assertEquals("kafka-to-elasticsearch-consumer", config.getKafkaGroupId());
    }

    @Test
    void shouldOverrideWithEnvironmentSpecificConfig() {
        try {
            System.setProperty("app.environment", "prod");
            ConfigManager config = new ConfigManager();
            assertTrue(config.isProdEnvironment());
        } finally {
            System.clearProperty("app.environment");
        }
    }

    @Test
    void shouldUseEnvironmentVariablesOverProperties() {
        String testServer = "test-server:9092";
        environmentVariables.set("KAFKA_BOOTSTRAP_SERVERS", testServer);
        
        ConfigManager config = new ConfigManager();
        assertEquals(testServer, config.getKafkaBootstrapServers());
    }

    @Test
    void shouldCreateValidKafkaConsumerProperties() {
        ConfigManager config = new ConfigManager();
        Properties kafkaProps = config.getKafkaConsumerProperties();
        
        assertNotNull(kafkaProps);
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", 
            kafkaProps.getProperty("key.deserializer"));
        assertEquals("false", kafkaProps.getProperty("enable.auto.commit"));
    }

    @Test
    void shouldCreateValidKafkaProducerProperties() {
        ConfigManager config = new ConfigManager();
        Properties kafkaProps = config.getKafkaProducerProperties();
        
        assertNotNull(kafkaProps);
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", 
            kafkaProps.getProperty("key.serializer"));
        assertEquals("all", kafkaProps.getProperty("acks"));
    }

    @Test
    void shouldHandleMissingConfigurationFile() {
        ConfigManager config = new ConfigManager("nonexistent");
        // Should use default values
        assertNotNull(config.getKafkaBootstrapServers());
        assertNotNull(config.getElasticsearchHost());
    }

    @Test
    void shouldReturnCorrectEnvironmentStatus() {
        ConfigManager devConfig = new ConfigManager(ConfigManager.Environment.DEV);
        assertTrue(devConfig.isDevEnvironment());
        assertFalse(devConfig.isProdEnvironment());

        ConfigManager prodConfig = new ConfigManager(ConfigManager.Environment.PROD);
        assertTrue(prodConfig.isProdEnvironment());
        assertFalse(prodConfig.isDevEnvironment());
    }

    @Test
    void shouldHandleInvalidEnvironmentName() {
        String invalidEnv = "invalid";
        environmentVariables.set("APP_ENVIRONMENT", invalidEnv);
        
        ConfigManager config = new ConfigManager();
        assertTrue(config.isDevEnvironment(), "Should default to DEV for invalid environment");
    }
}
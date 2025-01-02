package org.example.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class KafkaServiceIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.2.1")
    );

    private KafkaService kafkaService;
    private static final String SOURCE_TOPIC = "test-source-topic";
    private static final String DLQ_TOPIC = "test-dlq-topic";

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        createTopics();
        
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        consumerProps.put("group.id", "test-group");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", "false");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        producerProps.put("acks", "all");

        kafkaService = new KafkaService(consumerProps, producerProps, SOURCE_TOPIC, DLQ_TOPIC);
    }

    private void createTopics() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(Arrays.asList(
                new NewTopic(SOURCE_TOPIC, 1, (short) 1),
                new NewTopic(DLQ_TOPIC, 1, (short) 1)
            )).all().get();
        }
    }

    @Test
    void shouldConsumeAndProcessMessages() throws Exception {
        // Produce a test message
        try (Producer<String, String> producer = createTestProducer()) {
            producer.send(new ProducerRecord<>(SOURCE_TOPIC, "key", "value")).get();
        }

        // Start consuming
        kafkaService.subscribe();
        ConsumerRecords<String, String> records = kafkaService.pollRecords(Duration.ofSeconds(5));
        
        assertFalse(records.isEmpty());
        for (ConsumerRecord<String, String> record : records) {
            assertEquals("value", record.value());
            kafkaService.commitOffset(record);
        }
    }

    @Test
    void shouldSendMessagesToDlq() throws Exception {
        // Create a record that will be sent to DLQ
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            SOURCE_TOPIC, 0, 0L, "key", "value"
        );
        Exception error = new RuntimeException("Test error");

        // Send to DLQ
        kafkaService.sendToDlq(record, error);

        // Verify message in DLQ
        try (KafkaConsumer<String, String> dlqConsumer = createDlqConsumer()) {
            dlqConsumer.subscribe(Collections.singletonList(DLQ_TOPIC));
            ConsumerRecords<String, String> dlqRecords = dlqConsumer.poll(Duration.ofSeconds(5));
            
            assertFalse(dlqRecords.isEmpty());
            for (ConsumerRecord<String, String> dlqRecord : dlqRecords) {
                assertTrue(dlqRecord.value().contains("Test error"));
            }
        }
    }

    @Test
    void shouldHandleConsumerRebalancing() throws Exception {
        // Create two consumers in the same group
        KafkaService consumer1 = createKafkaService("test-group");
        KafkaService consumer2 = createKafkaService("test-group");

        // Subscribe both consumers
        consumer1.subscribe();
        consumer2.subscribe();

        // Produce some messages
        produceTestMessages(10);

        // Both consumers should receive messages
        verifyConsumerReceivesMessages(consumer1);
        verifyConsumerReceivesMessages(consumer2);
    }

    private KafkaService createKafkaService(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", groupId);
        return new KafkaService(props, props, SOURCE_TOPIC, DLQ_TOPIC);
    }

    private void produceTestMessages(int count) throws Exception {
        try (Producer<String, String> producer = createTestProducer()) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(SOURCE_TOPIC, 
                    "key-" + i, "value-" + i)).get();
            }
        }
    }

    private void verifyConsumerReceivesMessages(KafkaService consumer) {
        ConsumerRecords<String, String> records = consumer.pollRecords(Duration.ofSeconds(5));
        assertFalse(records.isEmpty());
    }

    private Producer<String, String> createTestProducer() {
        // Implementation to create test producer
        return null; // TODO: Implement
    }

    private KafkaConsumer<String, String> createDlqConsumer() {
        // Implementation to create DLQ consumer
        return null; // TODO: Implement
    }
}
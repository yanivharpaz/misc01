package org.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaServiceTest {

    @Mock
    private KafkaConsumer<String, String> mockConsumer;

    @Mock
    private KafkaProducer<String, String> mockProducer;

    private KafkaService kafkaService;
    private static final String SOURCE_TOPIC = "source-topic";
    private static final String DLQ_TOPIC = "dlq-topic";

    @BeforeEach
    void setUp() {
        kafkaService = new KafkaService(mockConsumer, mockProducer, SOURCE_TOPIC, DLQ_TOPIC);
    }

    @Test
    void shouldSubscribeToTopic() {
        kafkaService.subscribe();
        verify(mockConsumer).subscribe(Collections.singletonList(SOURCE_TOPIC));
    }

    @Test
    void shouldPollRecords() {
        Duration timeout = Duration.ofMillis(100);
        ConsumerRecords<String, String> expectedRecords = mock(ConsumerRecords.class);
        when(mockConsumer.poll(timeout)).thenReturn(expectedRecords);

        ConsumerRecords<String, String> actualRecords = kafkaService.pollRecords(timeout);
        
        assertSame(expectedRecords, actualRecords);
        verify(mockConsumer).poll(timeout);
    }

    @Test
    void shouldCommitOffsets() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            SOURCE_TOPIC, 0, 123L, "key", "value"
        );

        kafkaService.commitOffset(record);

        verify(mockConsumer).commitSync(any());
    }

    @Test
    void shouldHandleCommitFailure() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            SOURCE_TOPIC, 0, 123L, "key", "value"
        );

        doThrow(new RuntimeException("Commit failed"))
            .when(mockConsumer).commitSync(any());

        assertThrows(RuntimeException.class, () -> kafkaService.commitOffset(record));
    }

    @Test
    void shouldSendToDlq() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            SOURCE_TOPIC, 0, 123L, "key", "value"
        );
        Exception error = new RuntimeException("Test error");

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> future = mock(Future.class);
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(future.get()).thenReturn(metadata);
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(future);

        kafkaService.sendToDlq(record, error);

        verify(mockProducer).send(any(ProducerRecord.class));
    }

    @Test
    void shouldHandleDlqSendFailure() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            SOURCE_TOPIC, 0, 123L, "key", "value"
        );
        Exception error = new RuntimeException("Test error");

        when(mockProducer.send(any(ProducerRecord.class)))
            .thenThrow(new RuntimeException("DLQ send failed"));

        assertThrows(Exception.class, () -> kafkaService.sendToDlq(record, error));
    }

    @Test
    void shouldCloseResources() throws Exception {
        kafkaService.close();
        
        verify(mockConsumer).close();
        verify(mockProducer).close();
    }

    @Test
    void shouldHandleCloseFailure() {
        doThrow(new RuntimeException("Close failed"))
            .when(mockConsumer).close();

        assertThrows(Exception.class, () -> kafkaService.close());
    }
}
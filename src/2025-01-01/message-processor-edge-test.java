package org.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.bulk.BulkResponse;
import org.example.config.BatchConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class MessageProcessorEdgeTest {

    @Mock
    private KafkaService kafkaService;

    @Mock
    private ElasticsearchService elasticsearchService;

    private MessageProcessor processor;
    private BatchConfig batchConfig;

    @BeforeEach
    void setUp() {
        batchConfig = BatchConfig.builder()
            .withBatchSize(100)
            .withBatchTimeoutMs(1000)
            .build();
        processor = new MessageProcessor(kafkaService, elasticsearchService, batchConfig);
    }

    @Test
    void shouldHandleEmptyBatch() throws Exception {
        when(kafkaService.pollRecords(any(Duration.class)))
            .thenReturn(ConsumerRecords.empty());

        processor.processNextBatch();

        verify(elasticsearchService, never()).bulkIndex(any());
    }

    @Test
    void shouldHandleMaximumBatchSize() throws Exception {
        List<ConsumerRecord<String, String>> records = createLargeBatch(batchConfig.getBatchSize());
        when(kafkaService.pollRecords(any(Duration.class)))
            .thenReturn(createConsumerRecords(records))
            .thenReturn(ConsumerRecords.empty());
        when(elasticsearchService.bulkIndex(any())).thenReturn(createSuccessfulBulkResponse());

        processor.processNextBatch();

        verify(elasticsearchService, times(1)).bulkIndex(any());
    }

    @Test
    void shouldHandleTimeoutWithPartialBatch() throws Exception {
        List<ConsumerRecord<String, String>> records = 
            createLargeBatch(batchConfig.getBatchSize() / 2);
        when(kafkaService.pollRecords(any(Duration.class)))
            .thenReturn(createConsumerRecords(records))
            .thenReturn(ConsumerRecords.empty());
        when(elasticsearchService.bulkIndex(any())).thenReturn(createSuccessfulBulkResponse());

        Thread.sleep(batchConfig.getBatchTimeoutMs() + 100);
        processor.processNextBatch();

        verify(elasticsearchService, times(1)).bulkIndex(any());
    }

    @Test
    void shouldHandleSpecialCharactersInProductType() throws Exception {
        String messageWithSpecialChars = "{\"product_type\":\"widget#123@\",\"name\":\"test\"}";
        ConsumerRecord<String, String> record = createRecord(messageWithSpecialChars);
        
        when(kafkaService.pollRecords(any(Duration.class)))
            .thenReturn(createConsumerRecords(List.of(record)))
            .thenReturn(ConsumerRecords.empty());

        processor.processNextBatch();

        verify(kafkaService).sendToDlq(any(), any(IllegalArgumentException.class));
    }

    private List<ConsumerRecord<String, String>> createLargeBatch(int size) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            records.add(createRecord(
                String.format("{\"product_type\":\"widget\",\"name\":\"test-%d\"}", i)
            ));
        }
        return records;
    }

    private ConsumerRecord<String, String> createRecord(String value) {
        return new ConsumerRecord<>("test-topic", 0, 0L, "key", value);
    }

    private ConsumerRecords<String, String> createConsumerRecords(List<ConsumerRecord<String, String>> records) {
        // Implementation from previous helper methods
        return null; // TODO: Implement
    }

    private BulkResponse createSuccessfulBulkResponse() {
        // Implementation from previous helper methods
        return null; // TODO: Implement
    }
}
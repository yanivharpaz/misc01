package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.example.config.BatchConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class MessageProcessor {
    private final KafkaService kafkaService;
    private final ElasticsearchService elasticsearchService;
    private final BatchConfig batchConfig;
    private final ObjectMapper objectMapper;
    private final DocumentBatch currentBatch;
    private long lastBatchProcessTime;

    public MessageProcessor(KafkaService kafkaService, ElasticsearchService elasticsearchService, BatchConfig batchConfig) {
        this.kafkaService = kafkaService;
        this.elasticsearchService = elasticsearchService;
        this.batchConfig = batchConfig;
        this.objectMapper = new ObjectMapper();
        this.currentBatch = new DocumentBatch();
        this.lastBatchProcessTime = System.currentTimeMillis();
    }

    public void start() {
        kafkaService.subscribe();
        System.out.println("Started message processor with batch size: " + batchConfig.getBatchSize() + 
                          " and timeout: " + batchConfig.getBatchTimeoutMs() + "ms");

        while (true) {
            try {
                ConsumerRecords<String, String> records = kafkaService.pollRecords(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                // Check if we need to flush the batch
                if (shouldFlushBatch()) {
                    processBatch();
                }
            } catch (Exception e) {
                System.err.println("Error processing messages: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            JsonNode jsonNode = objectMapper.readTree(record.value());
            
            if (jsonNode.isArray()) {
                // Handle array of documents
                for (JsonNode node : jsonNode) {
                    addToCurrentBatch(record, node);
                }
            } else {
                // Handle single document
                addToCurrentBatch(record, jsonNode);
            }
        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
            try {
                kafkaService.sendToDlq(record, e);
            } catch (Exception dlqError) {
                System.err.println("Error sending to DLQ: " + dlqError.getMessage());
            }
        }
    }

    private void addToCurrentBatch(ConsumerRecord<String, String> record, JsonNode document) {
        String productType = document.has("product_type") ? 
            document.get("product_type").asText().toLowerCase() : "unknown";
        
        currentBatch.add(record, productType, document);

        if (currentBatch.size() >= batchConfig.getBatchSize()) {
            processBatch();
        }
    }

    private boolean shouldFlushBatch() {
        return !currentBatch.isEmpty() && 
               (currentBatch.size() >= batchConfig.getBatchSize() || 
                System.currentTimeMillis() - lastBatchProcessTime >= batchConfig.getBatchTimeoutMs());
    }

    private void processBatch() {
        if (currentBatch.isEmpty()) {
            return;
        }

        try {
            System.out.println("Processing batch of " + currentBatch.size() + " documents");
            BulkResponse bulkResponse = elasticsearchService.bulkIndex(currentBatch.getDocuments());
            
            // Handle the response and commit offsets
            handleBulkResponse(bulkResponse);
            
            // Update the last batch process time
            lastBatchProcessTime = System.currentTimeMillis();
        } catch (Exception e) {
            System.err.println("Error processing batch: " + e.getMessage());
            handleBatchError();
        } finally {
            currentBatch.clear();
        }
    }

    private void handleBulkResponse(BulkResponse bulkResponse) {
        List<ConsumerRecord<String, String>> recordsToCommit = new ArrayList<>();
        List<ConsumerRecord<String, String>> failedRecords = new ArrayList<>();

        for (int i = 0; i < bulkResponse.getItems().length; i++) {
            BulkItemResponse itemResponse = bulkResponse.getItems()[i];
            ConsumerRecord<String, String> record = currentBatch.getRecords().get(i);

            if (itemResponse.isFailed()) {
                failedRecords.add(record);
                System.err.println("Failed to index document: " + itemResponse.getFailureMessage());
            } else {
                recordsToCommit.add(record);
            }
        }

        // Send failed records to DLQ
        for (ConsumerRecord<String, String> failedRecord : failedRecords) {
            try {
                kafkaService.sendToDlq(failedRecord, 
                    new Exception("Failed to index document in batch"));
            } catch (Exception e) {
                System.err.println("Error sending to DLQ: " + e.getMessage());
            }
        }

        // Commit offsets for successful records
        for (ConsumerRecord<String, String> record : recordsToCommit) {
            kafkaService.commitOffset(record);
        }
    }

    private void handleBatchError() {
        // Send all records in the batch to DLQ
        for (ConsumerRecord<String, String> record : currentBatch.getRecords()) {
            try {
                kafkaService.sendToDlq(record, 
                    new Exception("Batch processing failed"));
            } catch (Exception e) {
                System.err.println("Error sending to DLQ: " + e.getMessage());
            }
        }
    }
}
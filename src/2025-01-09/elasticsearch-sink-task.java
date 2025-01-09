package org.example.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.config.ElasticsearchSinkConnectorConfig;
import org.example.service.ElasticsearchService;

import java.util.Collection;
import java.util.Map;

public class ElasticsearchSinkTask extends SinkTask {
    private ElasticsearchSinkConnectorConfig config;
    private ElasticsearchService elasticsearchService;
    private ObjectMapper objectMapper;
    private String dlqTopic;

    @Override
    public void start(Map<String, String> props) {
        config = new ElasticsearchSinkConnectorConfig(props);
        elasticsearchService = new ElasticsearchService(
            config.getString(ElasticsearchSinkConnectorConfig.ELASTICSEARCH_HOST),
            config.getInt(ElasticsearchSinkConnectorConfig.ELASTICSEARCH_PORT),
            config.getString(ElasticsearchSinkConnectorConfig.ELASTICSEARCH_SCHEME),
            config.getString(ElasticsearchSinkConnectorConfig.INDEX_PREFIX)
        );
        objectMapper = new ObjectMapper();
        dlqTopic = config.getString(ElasticsearchSinkConnectorConfig.DLQ_TOPIC);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            BulkRequest bulkRequest = new BulkRequest();
            
            for (SinkRecord record : records) {
                JsonNode jsonNode = objectMapper.readTree(record.value().toString());
                String productType = extractProductType(jsonNode);
                
                // Ensure index and alias exist
                elasticsearchService.ensureIndexAndAliasExist(productType);
                String aliasName = elasticsearchService.getAliasName(productType);

                // Add document to bulk request
                IndexRequest indexRequest = new IndexRequest(aliasName)
                    .source(record.value().toString(), XContentType.JSON);
                bulkRequest.add(indexRequest);
            }

            // Execute bulk request
            BulkResponse response = elasticsearchService.getClient()
                .bulk(bulkRequest, RequestOptions.DEFAULT);

            // Handle failures
            if (response.hasFailures() && !dlqTopic.isEmpty()) {
                handleFailures(response, records);
            }

        } catch (Exception e) {
            // If DLQ is configured, send all records to DLQ
            if (!dlqTopic.isEmpty()) {
                records.forEach(record -> sendToDlq(record, e));
            }
            throw new RuntimeException("Failed to write to Elasticsearch", e);
        }
    }

    private String extractProductType(JsonNode jsonNode) {
        if (!jsonNode.has("product_type")) {
            return "unknown";
        }
        return jsonNode.get("product_type").asText().toLowerCase();
    }

    private void handleFailures(BulkResponse response, Collection<SinkRecord> records) {
        // Implementation for handling individual record failures
        // and sending failed records to DLQ
    }

    private void sendToDlq(SinkRecord record, Exception error) {
        // Implementation for sending record to DLQ
    }

    @Override
    public void stop() {
        try {
            if (elasticsearchService != null) {
                elasticsearchService.close();
            }
        } catch (Exception e) {
            // Log error
        }
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
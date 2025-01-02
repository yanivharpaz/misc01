package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class ElasticsearchServiceIntegrationTest {

    @Container
    static ElasticsearchContainer elasticsearchContainer = new ElasticsearchContainer(
        "docker.elastic.co/elasticsearch/elasticsearch:7.9.3")
        .withExposedPorts(9200);

    private ElasticsearchService elasticsearchService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        elasticsearchService = new ElasticsearchService(
            elasticsearchContainer.getHost(),
            elasticsearchContainer.getFirstMappedPort(),
            "http",
            "test_"
        );
    }

    @Test
    void shouldCreateIndexAndAliasInRealElasticsearch() throws IOException {
        String productType = "widget";
        elasticsearchService.ensureIndexAndAliasExist(productType);
        
        // Verify index exists
        assertTrue(indexExists("test_widget_00001"));
        // Verify alias exists
        assertTrue(aliasExists("test_widget"));
    }

    @Test
    void shouldPerformBulkIndexing() throws IOException {
        // Prepare test documents
        List<DocumentBatch.Document> documents = createTestDocuments();
        
        // Ensure index exists
        elasticsearchService.ensureIndexAndAliasExist("widget");
        
        // Perform bulk indexing
        BulkResponse response = elasticsearchService.bulkIndex(documents);
        
        assertNotNull(response);
        assertFalse(response.hasFailures());
    }

    @Test
    void shouldHandleMultipleProductTypes() throws IOException {
        // Create indices for different product types
        elasticsearchService.ensureIndexAndAliasExist("widget");
        elasticsearchService.ensureIndexAndAliasExist("gadget");
        
        // Verify both indices and aliases exist
        assertTrue(indexExists("test_widget_00001"));
        assertTrue(indexExists("test_gadget_00001"));
        assertTrue(aliasExists("test_widget"));
        assertTrue(aliasExists("test_gadget"));
    }

    private List<DocumentBatch.Document> createTestDocuments() throws IOException {
        List<DocumentBatch.Document> documents = new ArrayList<>();
        String json = "{\"product_type\":\"widget\",\"name\":\"test widget\",\"price\":10.0}";
        JsonNode node = objectMapper.readTree(json);
        documents.add(new DocumentBatch.Document("widget", node));
        return documents;
    }

    private boolean indexExists(String indexName) throws IOException {
        RestHighLevelClient client = elasticsearchService.getClient();
        GetIndexRequest request = new GetIndexRequest(indexName);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    private boolean aliasExists(String aliasName) throws IOException {
        RestHighLevelClient client = elasticsearchService.getClient();
        return client.indices().existsAlias(new GetAliasesRequest(aliasName), RequestOptions.DEFAULT);
    }
}
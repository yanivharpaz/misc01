package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElasticsearchServiceTest {

    @Mock
    private RestHighLevelClient mockClient;

    @Mock
    private BulkResponse mockBulkResponse;

    private ElasticsearchService elasticsearchService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        elasticsearchService = new ElasticsearchService("localhost", 9200, "http", "test_");
        objectMapper = new ObjectMapper();
    }

    @Test
    void shouldCreateIndexIfNotExists() throws IOException {
        // Given
        when(mockClient.indices().exists(any(GetIndexRequest.class))).thenReturn(false);

        // When
        elasticsearchService.ensureIndexAndAliasExist("testType");

        // Then
        verify(mockClient.indices()).exists(any(GetIndexRequest.class));
        verify(mockClient.indices()).create(any());
    }

    @Test
    void shouldNotCreateIndexIfExists() throws IOException {
        // Given
        when(mockClient.indices().exists(any(GetIndexRequest.class))).thenReturn(true);

        // When
        elasticsearchService.ensureIndexAndAliasExist("testType");

        // Then
        verify(mockClient.indices()).exists(any(GetIndexRequest.class));
        verify(mockClient.indices(), never()).create(any());
    }

    @Test
    void shouldHandleBulkIndexing() throws IOException {
        // Given
        List<DocumentBatch.Document> documents = new ArrayList<>();
        String json = "{\"product_type\":\"test\",\"value\":\"test\"}";
        JsonNode jsonNode = objectMapper.readTree(json);
        documents.add(new DocumentBatch.Document("test", jsonNode));

        when(mockClient.bulk(any())).thenReturn(mockBulkResponse);
        when(mockBulkResponse.hasFailures()).thenReturn(false);

        // When
        BulkResponse response = elasticsearchService.bulkIndex(documents);

        // Then
        assertNotNull(response);
        assertFalse(response.hasFailures());
        verify(mockClient).bulk(any());
    }

    @Test
    void shouldHandleFailedBulkIndexing() throws IOException {
        // Given
        List<DocumentBatch.Document> documents = new ArrayList<>();
        String json = "{\"product_type\":\"test\",\"value\":\"test\"}";
        JsonNode jsonNode = objectMapper.readTree(json);
        documents.add(new DocumentBatch.Document("test", jsonNode));

        BulkItemResponse mockItemResponse = mock(BulkItemResponse.class);
        when(mockItemResponse.isFailed()).thenReturn(true);
        when(mockItemResponse.getFailureMessage()).thenReturn("Test failure");
        
        when(mockClient.bulk(any())).thenReturn(mockBulkResponse);
        when(mockBulkResponse.hasFailures()).thenReturn(true);
        when(mockBulkResponse.getItems()).thenReturn(new BulkItemResponse[]{mockItemResponse});

        // When
        BulkResponse response = elasticsearchService.bulkIndex(documents);

        // Then
        assertNotNull(response);
        assertTrue(response.hasFailures());
        verify(mockClient).bulk(any());
    }

    @Test
    void shouldCloseClientProperly() throws IOException {
        // When
        elasticsearchService.close();

        // Then
        verify(mockClient).close();
    }

    @Test
    void shouldHandleIndexCreationFailure() {
        // Given
        when(mockClient.indices().exists(any(GetIndexRequest.class)))
            .thenThrow(new RuntimeException("Test exception"));

        // When/Then
        assertThrows(IOException.class, () -> 
            elasticsearchService.ensureIndexAndAliasExist("testType"));
    }

    @Test
    void shouldGenerateCorrectIndexNames() throws IOException {
        // Given
        String productType = "widget";
        
        // When
        String indexName = elasticsearchService.getIndexName(productType);
        
        // Then
        assertTrue(indexName.startsWith("test_widget_"));
        assertTrue(indexName.matches("test_widget_\\d{5}"));
    }
}
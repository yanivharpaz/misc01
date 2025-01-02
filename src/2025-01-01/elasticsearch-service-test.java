package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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

    private ElasticsearchService elasticsearchService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        elasticsearchService = new ElasticsearchService(mockClient, "test_");
    }

    @Test
    void shouldEnsureIndexAndAliasExist() throws IOException {
        // Mock index does not exist
        when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(false);

        String productType = "widget";
        elasticsearchService.ensureIndexAndAliasExist(productType);

        // Verify index creation was attempted
        verify(mockClient.indices(), times(1)).create(any(), any(RequestOptions.class));
        // Verify alias creation was attempted
        verify(mockClient.getLowLevelClient(), times(1)).performRequest(any());
    }

    @Test
    void shouldReuseExistingIndex() throws IOException {
        // Mock index already exists
        when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);

        String productType = "widget";
        elasticsearchService.ensureIndexAndAliasExist(productType);

        // Verify no index creation was attempted
        verify(mockClient.indices(), never()).create(any(), any(RequestOptions.class));
    }

    @Test
    void shouldHandleIndexCreationError() throws IOException {
        when(mockClient.indices().exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(false);
        when(mockClient.indices().create(any(), any(RequestOptions.class)))
            .thenThrow(new IOException("Failed to create index"));

        String productType = "widget";
        assertThrows(IOException.class, () -> 
            elasticsearchService.ensureIndexAndAliasExist(productType));
    }

    @Test
    void shouldValidateProductType() {
        String invalidProductType = null;
        assertThrows(IllegalArgumentException.class, () ->
            elasticsearchService.ensureIndexAndAliasExist(invalidProductType));

        invalidProductType = "";
        assertThrows(IllegalArgumentException.class, () ->
            elasticsearchService.ensureIndexAndAliasExist(invalidProductType));

        invalidProductType = "invalid/product";
        assertThrows(IllegalArgumentException.class, () ->
            elasticsearchService.ensureIndexAndAliasExist(invalidProductType));
    }
}
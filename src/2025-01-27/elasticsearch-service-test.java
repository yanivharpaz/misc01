package org.example.service;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElasticsearchServiceTest {

    @Mock
    private RestHighLevelClient client;

    @Mock
    private IndicesClient indicesClient;

    @Mock
    private RestClient lowLevelClient;

    private ElasticsearchService elasticsearchService;

    @BeforeEach
    void setUp() {
        when(client.indices()).thenReturn(indicesClient);
        when(client.getLowLevelClient()).thenReturn(lowLevelClient);
        elasticsearchService = new ElasticsearchService(client);
    }

    @Test
    void ensureIndexAndAliasExist_CreatesNewIndexAndAlias() throws IOException {
        // Given
        String productType = "test";
        String expectedIndexName = "prd_a_test_00001";
        String expectedAliasName = "prd_a_test";

        // Mock index doesn't exist
        when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(false);

        // Mock create index response
        when(indicesClient.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(mock(CreateIndexResponse.class));

        // Mock alias creation response
        Response mockResponse = mock(Response.class);
        when(mockResponse.getStatusLine()).thenReturn(mock(StatusLine.class));
        when(mockResponse.getStatusLine().getStatusCode()).thenReturn(200);
        when(lowLevelClient.performRequest(any(Request.class))).thenReturn(mockResponse);

        // When
        elasticsearchService.indexDocument("""
            {
                "product_type": "test",
                "some_field": "some_value"
            }
            """);

        // Then
        verify(indicesClient).exists(argThat(request -> 
            request.indices()[0].equals(expectedIndexName)), 
            eq(RequestOptions.DEFAULT));

        verify(indicesClient).create(argThat(request ->
            request.index().equals(expectedIndexName)), 
            eq(RequestOptions.DEFAULT));

        verify(lowLevelClient).performRequest(argThat(request -> {
            try {
                String endpoint = request.getEndpoint();
                String entity = request.getEntity().toString();
                return endpoint.equals("/_aliases") &&
                       entity.contains(expectedIndexName) &&
                       entity.contains(expectedAliasName) &&
                       entity.contains("is_write_index");
            } catch (IOException e) {
                return false;
            }
        }));
    }

    @Test
    void ensureIndexAndAliasExist_UsesExistingIndexAndAlias() throws IOException {
        // Given
        String productType = "test";
        String expectedIndexName = "prd_a_test_00001";

        // Mock index exists
        when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(true);

        // Mock alias check response
        Response mockResponse = mock(Response.class);
        when(mockResponse.getStatusLine()).thenReturn(mock(StatusLine.class));
        when(mockResponse.getStatusLine().getStatusCode()).thenReturn(200);
        when(lowLevelClient.performRequest(any(Request.class))).thenReturn(mockResponse);

        // When
        elasticsearchService.indexDocument("""
            {
                "product_type": "test",
                "some_field": "some_value"
            }
            """);

        // Then
        verify(indicesClient).exists(argThat(request -> 
            request.indices()[0].equals(expectedIndexName)), 
            eq(RequestOptions.DEFAULT));

        // Verify that create index was not called
        verify(indicesClient, never()).create(any(CreateIndexRequest.class), any(RequestOptions.class));
    }
}

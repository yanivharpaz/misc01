package org.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.config.ElasticsearchConfig;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ElasticsearchService implements AutoCloseable {
    private final RestHighLevelClient client;
    private final Map<String, String> aliasCache;
    private final Map<String, Integer> indexCounters;

    public ElasticsearchService(ElasticsearchConfig config) {
        this.client = new RestHighLevelClient(
            RestClient.builder(new HttpHost(config.getHost(), config.getPort(), config.getScheme()))
        );
        this.aliasCache = new ConcurrentHashMap<>();
        this.indexCounters = new ConcurrentHashMap<>();
        initializeAliasCache();
    }

    // ... (keep existing methods for alias and index management)

    public BulkResponse bulkIndex(List<DocumentBatch.Document> documents) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        Map<String, String> productTypeAliases = new HashMap<>();

        // First ensure all necessary indices and aliases exist
        for (DocumentBatch.Document doc : documents) {
            String productType = doc.getProductType();
            if (!productTypeAliases.containsKey(productType)) {
                ensureIndexAndAliasExist(productType);
                productTypeAliases.put(productType, getAliasName(productType));
            }
        }

        // Add all documents to the bulk request
        for (DocumentBatch.Document doc : documents) {
            String aliasName = productTypeAliases.get(doc.getProductType());
            bulkRequest.add(new IndexRequest(aliasName, "_doc")
                .source(doc.getContent().toString(), XContentType.JSON));
        }

        return client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
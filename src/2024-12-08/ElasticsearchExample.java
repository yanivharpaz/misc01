package org.example;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ElasticsearchExample {

    public static void main(String[] args) {
        // Create the Elasticsearch client
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );

        try {
            // Create a sample document
            Map<String, Object> document = new HashMap<>();
            document.put("title", "Sample Document");
            document.put("description", "This is a test document");
            document.put("timestamp", System.currentTimeMillis());
            document.put("active", true);

            // Create an index request
            IndexRequest indexRequest = new IndexRequest(
                    "my_index",    // Index name
                    "_doc",        // Type name (deprecated in newer versions)
                    UUID.randomUUID().toString()  // Document ID
            );

            // Add the document to the request
            indexRequest.source(document, XContentType.JSON);

            // Execute the request
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

            // Print the response
            System.out.println("Document indexed successfully!");
            System.out.println("Index: " + response.getIndex());
            System.out.println("Type: " + response.getType());
            System.out.println("ID: " + response.getId());
            System.out.println("Version: " + response.getVersion());
            System.out.println("Result: " + response.getResult().name());

        } catch (IOException e) {
            System.err.println("Error while indexing document: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the client
            try {
                client.close();
            } catch (IOException e) {
                System.err.println("Error while closing client: " + e.getMessage());
            }
        }
    }
}
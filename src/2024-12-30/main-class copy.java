package org.example;

import org.example.config.ConfigManager;
import org.example.config.BatchConfig;
import org.example.service.ElasticsearchService;
import org.example.service.KafkaService;
import org.example.service.MessageProcessor;

public class KafkaToElasticsearchConsumer {
    public static void main(String[] args) {
        try {
            // Load configuration with environment
            ConfigManager config = new ConfigManager();
            
            // Log startup information
            System.out.println("Starting application in " + config.getActiveProfile() + " environment");
            if (config.isDevEnvironment()) {
                System.out.println("Development mode enabled - using development settings");
            }
            
            // Create services with configuration
            KafkaService kafkaService = new KafkaService(
                config.getKafkaConsumerProperties(),
                config.getKafkaProducerProperties(),
                config.getKafkaSourceTopic(),
                config.getKafkaDlqTopic()
            );

            ElasticsearchService esService = new ElasticsearchService(
                config.getElasticsearchHost(),
                config.getElasticsearchPort(),
                config.getElasticsearchScheme(),
                config.getIndexPrefix()
            );

            BatchConfig batchConfig = BatchConfig.builder()
                .withBatchSize(config.getBatchSize())
                .withBatchTimeoutMs(config.getBatchTimeoutMs())
                .build();

            // Create and start the message processor
            MessageProcessor processor = new MessageProcessor(kafkaService, esService, batchConfig);
            
            // Log configuration
            System.out.println("\nConfiguration Details:");
            System.out.println("----------------------");
            System.out.println("Environment: " + config.getActiveProfile().toUpperCase());
            System.out.println("Kafka Bootstrap Servers: " + config.getKafkaBootstrapServers());
            System.out.println("Kafka Source Topic: " + config.getKafkaSourceTopic());
            System.out.println("Kafka DLQ Topic: " + config.getKafkaDlqTopic());
            System.out.println("Elasticsearch Host: " + config.getElasticsearchHost() + ":" + config.getElasticsearchPort());
            System.out.println("Elasticsearch Scheme: " + config.getElasticsearchScheme());
            System.out.println("Index Prefix: " + config.getIndexPrefix());
            System.out.println("Batch Size: " + config.getBatchSize());
            System.out.println("Batch Timeout: " + config.getBatchTimeoutMs() + "ms");
            System.out.println("----------------------\n");
            
            // Start processing
            processor.start();
        } catch (Exception e) {
            System.err.println("Error starting application: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
package org.example;

import org.example.config.ConfigManager;
import org.example.config.BatchConfig;
import org.example.service.ElasticsearchService;
import org.example.service.KafkaService;
import org.example.service.MessageProcessor;

public class KafkaToElasticsearchConsumer {
    public static void main(String[] args) {
        try {
            // Load configuration
            ConfigManager config = new ConfigManager();
            
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
            System.out.println("Starting with configuration:");
            System.out.println("Kafka Bootstrap Servers: " + config.getKafkaBootstrapServers());
            System.out.println("Kafka Source Topic: " + config.getKafkaSourceTopic());
            System.out.println("Elasticsearch Host: " + config.getElasticsearchHost() + ":" + config.getElasticsearchPort());
            System.out.println("Batch Size: " + config.getBatchSize());
            System.out.println("Batch Timeout: " + config.getBatchTimeoutMs() + "ms");
            
            // Start processing
            processor.start();
        } catch (Exception e) {
            System.err.println("Error starting application: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
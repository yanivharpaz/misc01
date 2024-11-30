// Config.java
package com.example.kafka2elastic;

import lombok.Data;

@Data
public class Config {
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String kafkaGroupId;
    private final String elasticsearchHosts;
    private final String elasticsearchIndexPrefix;
    private final String elasticsearchAliasName;
    private final int elasticsearchPort;
    private final int cacheSize;

    public static Config defaultConfig() {
        return new Config(
            "localhost:9092",
            "your-topic",
            "elasticsearch-consumer-group",
            "localhost",
            "your-index-prefix",
            "your-alias-name",
            9200,
            1000
        );
    }
}

// KafkaConsumerWrapper.java
package com.example.kafka2elastic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class KafkaConsumerWrapper implements AutoCloseable {
    private final KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    public KafkaConsumerWrapper(Config config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getKafkaGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(config.getKafkaTopic()));
    }

    public void consume(Consumer<ConsumerRecord<String, String>> recordHandler) {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        recordHandler.accept(record);
                    } catch (Exception e) {
                        log.error("Error processing record: {}", record, e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error in Kafka consumer", e);
        }
    }

    public void stop() {
        running = false;
    }

    @Override
    public void close() {
        consumer.close();
    }
}

// ElasticsearchWrapper.java
package com.example.kafka2elastic;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.UUID;

@Slf4j
public class ElasticsearchWrapper implements AutoCloseable {
    private final RestHighLevelClient client;
    private final String indexPrefix;
    private final String aliasName;

    public ElasticsearchWrapper(Config config) {
        this.client = new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(config.getElasticsearchHosts(), 
                           config.getElasticsearchPort(), "http")
            )
        );
        this.indexPrefix = config.getElasticsearchIndexPrefix();
        this.aliasName = config.getElasticsearchAliasName();
    }

    public void ensureAliasExists() throws IOException {
        GetAliasesRequest getAliasRequest = new GetAliasesRequest(aliasName);
        boolean aliasExists = client.indices().existsAlias(getAliasRequest, RequestOptions.DEFAULT);

        if (!aliasExists) {
            String newIndexName = String.format("%s-%d-%s",
                indexPrefix,
                System.currentTimeMillis(),
                UUID.randomUUID().toString());

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(newIndexName);
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

            IndicesAliasesRequest aliasRequest = new IndicesAliasesRequest();
            IndicesAliasesRequest.AliasActions aliasAction = 
                IndicesAliasesRequest.AliasActions.add()
                    .index(newIndexName)
                    .alias(aliasName);
            aliasRequest.addAliasAction(aliasAction);

            client.indices().updateAliases(aliasRequest, RequestOptions.DEFAULT);
            log.info("Created new index {} with alias {}", newIndexName, aliasName);
        }
    }

    public void indexDocument(String id, String document) throws IOException {
        IndexRequest indexRequest = new IndexRequest(aliasName)
            .id(id)
            .source(document, XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}

// DocumentCache.java
package com.example.kafka2elastic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentCache {
    private final Cache<String, String> cache;

    public DocumentCache(Config config) {
        this.cache = Caffeine.newBuilder()
            .maximumSize(config.getCacheSize())
            .build();
    }

    public boolean shouldProcess(String key, String value) {
        String existingValue = cache.getIfPresent(key);
        if (existingValue != null && existingValue.equals(value)) {
            log.debug("Cache hit for key: {}", key);
            return false;
        }
        cache.put(key, value);
        return true;
    }

    public void invalidate(String key) {
        cache.invalidate(key);
    }
}

// KafkaToElasticService.java
package com.example.kafka2elastic;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class KafkaToElasticService implements AutoCloseable {
    private final KafkaConsumerWrapper kafkaConsumer;
    private final ElasticsearchWrapper elasticsearch;
    private final DocumentCache cache;

    public KafkaToElasticService(Config config) throws IOException {
        this.kafkaConsumer = new KafkaConsumerWrapper(config);
        this.elasticsearch = new ElasticsearchWrapper(config);
        this.cache = new DocumentCache(config);
        
        // Ensure alias exists before starting
        this.elasticsearch.ensureAliasExists();
    }

    public void start() {
        kafkaConsumer.consume(record -> {
            try {
                String key = record.key();
                String value = record.value();
                
                // Only process if not in cache or value has changed
                if (cache.shouldProcess(key, value)) {
                    elasticsearch.indexDocument(key, value);
                    log.debug("Indexed document with key: {}", key);
                }
            } catch (Exception e) {
                log.error("Error processing record", e);
            }
        });
    }

    public void stop() {
        kafkaConsumer.stop();
    }

    @Override
    public void close() throws Exception {
        kafkaConsumer.close();
        elasticsearch.close();
    }
}

// Main.java
package com.example.kafka2elastic;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
    public static void main(String[] args) {
        Config config = Config.defaultConfig();
        
        try (KafkaToElasticService service = new KafkaToElasticService(config)) {
            Runtime.getRuntime().addShutdownHook(new Thread(service::stop));
            service.start();
        } catch (Exception e) {
            log.error("Application failed", e);
            System.exit(1);
        }
    }
}

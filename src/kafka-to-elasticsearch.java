import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaToElasticsearchConsumer {
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "your-topic-name";
    private static final String KAFKA_GROUP_ID = "elasticsearch-consumer-group";
    private static final String ES_INDEX_PREFIX = "your-index-prefix";
    private static final String ES_ALIAS_NAME = "your-alias-name";
    
    public static void main(String[] args) {
        try (RestHighLevelClient esClient = createElasticsearchClient();
             KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer()) {
            
            // Check if alias exists, if not create index and alias
            ensureAliasExists(esClient);
            
            // Start consuming from Kafka
            kafkaConsumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
            
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    IndexRequest indexRequest = new IndexRequest(ES_ALIAS_NAME)
                        .source(record.value(), XContentType.JSON);
                    
                    esClient.index(indexRequest, RequestOptions.DEFAULT);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static RestHighLevelClient createElasticsearchClient() {
        return new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
    }
    
    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        return new KafkaConsumer<>(props);
    }
    
    private static void ensureAliasExists(RestHighLevelClient client) throws IOException {
        GetAliasesRequest getAliasRequest = new GetAliasesRequest(ES_ALIAS_NAME);
        boolean aliasExists = client.indices().existsAlias(getAliasRequest, RequestOptions.DEFAULT);
        
        if (!aliasExists) {
            // Create new index with timestamp and UUID to ensure uniqueness
            String newIndexName = String.format("%s-%d-%s",
                ES_INDEX_PREFIX,
                System.currentTimeMillis(),
                UUID.randomUUID().toString());
            
            // Create the index
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(newIndexName);
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            
            // Create the alias
            IndicesAliasesRequest aliasRequest = new IndicesAliasesRequest();
            IndicesAliasesRequest.AliasActions aliasAction = 
                IndicesAliasesRequest.AliasActions.add()
                    .index(newIndexName)
                    .alias(ES_ALIAS_NAME);
            aliasRequest.addAliasAction(aliasAction);
            
            client.indices().updateAliases(aliasRequest, RequestOptions.DEFAULT);
        }
    }
}

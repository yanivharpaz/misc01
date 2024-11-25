// KafkaProducerExample.java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerExample {
    private static final String TOPIC_NAME = "example-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Create producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Create a producer record
            for (int i = 0; i < 5; i++) {
                String key = "key-" + i;
                String value = "message-" + i;
                ProducerRecord<String, String> record = 
                    new ProducerRecord<>(TOPIC_NAME, key, value);

                // Send data asynchronously
                Future<RecordMetadata> future = producer.send(record);
                future.get(); // Wait for acknowledgment
                System.out.println("Sent message: (" + key + ", " + value + ")");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// KafkaConsumerExample.java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {
    private static final String TOPIC_NAME = "example-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "example-group";

    public static void main(String[] args) {
        // Create consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            // Poll for records
            while (true) {
                ConsumerRecords<String, String> records = 
                    consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: key = %s, value = %s, partition = %d, offset = %d%n",
                        record.key(),
                        record.value(),
                        record.partition(),
                        record.offset()
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    public static void main(String[] args) {
        // Configure consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Create the consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList("my-topic"));

            // Poll for records
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("Received Message:%n" +
                                        "Topic: %s%n" +
                                        "Partition: %d%n" +
                                        "Offset: %d%n" +
                                        "Key: %s%n" +
                                        "Value: %s%n%n",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value());

                        // Add your processing logic here
                    }
                } catch (Exception e) {
                    System.err.println("Error while consuming messages: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}

package com.abeek.kafka.tutorials.tutorial1;

import com.abeek.kafka.tutorials.avro.schemas.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumerDemo {
    public static void main(String[] args) {
        //Consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "java_avro_customer_consumer");
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Consumer<String, Customer> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("customer_avro"));

        while(true) {
            ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record: records) {
                System.out.println("Offset: " + record.offset() + ", Key: " + record.key() + ", Value: " + record.value());
            }
        }
    }
}

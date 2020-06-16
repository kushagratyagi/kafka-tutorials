package com.abeek.kafka.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        //1. create Producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2. create KafkaProducer
        KafkaProducer producer = new KafkaProducer(properties);

        //3. create ProducerRecord without key
        ProducerRecord<String, String> record1 =
                new ProducerRecord<String, String>("first_topic", "hello");
        //create ProducerRecord with key
        ProducerRecord<String, String> record2 =
                new ProducerRecord<String, String>("first_topic", "id_1", "hello_1");

        //4. send to kafka
        producer.send(record1);
        producer.send(record2);

        //5. flush and close
        producer.close();
    }
}

package com.abeek.kafka.tutorials.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbackDemo {

    private static Logger LOGGER = LoggerFactory.getLogger(ProducerWithCallbackDemo.class.getName());
    public static void main(String[] args) {
        //create Producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer producer = new KafkaProducer(properties);

        //create ProducerRecord with key
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "id_1", "hello_1");

        //send to kafka with callback
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    LOGGER.info("partition: " + recordMetadata.partition());
                    LOGGER.info("offset: " + recordMetadata.offset());
                    LOGGER.info("timestamp: " + recordMetadata.timestamp());
                } else {
                    LOGGER.error("error: " + e.getMessage());
                }
            }
        });

        //flush and close
        producer.close();
    }
}

package com.abeek.kafka.tutorials.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAssignSeekDemo {
    private static Logger LOGGER = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class.getName());
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.put(ConsumerConfig.GROUP_ID_CONFIG, "java_consumer"); //this is not required

        KafkaConsumer consumer = new KafkaConsumer(properties);

        TopicPartition partition = new TopicPartition("first_topic", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.seek(partition, 2);

        int messagesToRead = 3;
        int messagesRead = 0;
        boolean keepReading = true;

        while(keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record: records) {
                LOGGER.info("key: " + record.key() + ", value: " + record.value() + ", partition: " + record.partition());
                messagesRead++;
                if (messagesRead == messagesToRead) {
                    keepReading = false;
                    break;
                }
            }
        }
    }
}

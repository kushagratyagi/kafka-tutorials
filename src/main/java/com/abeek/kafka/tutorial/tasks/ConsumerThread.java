package com.abeek.kafka.tutorial.tasks;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {
    private Logger LOGGER = LoggerFactory.getLogger(ConsumerThread.class.getName());
    private CountDownLatch latch;
    private Properties properties;
    private KafkaConsumer consumer;

    public ConsumerThread(CountDownLatch latch) {
        this.properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "java_consumer");
        this.latch = latch;
        this.consumer = new KafkaConsumer(properties);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singleton("first_topic"));
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record: records) {
                    LOGGER.info("key: " + record.key());
                    LOGGER.info("value: " + record.value());
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("task=run, message=Received shutdown signal(WakeupException)");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}

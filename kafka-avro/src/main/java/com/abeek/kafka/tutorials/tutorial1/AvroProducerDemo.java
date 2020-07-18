package com.abeek.kafka.tutorials.tutorial1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import com.abeek.kafka.tutorials.avro.schemas.Customer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AvroProducerDemo {
    public static void main(String[] args) {
        //Producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);

        //record
        Customer customer = Customer.newBuilder()
                .setFirstName("Kushagra")
                .setLastName("Tyagi")
                .setAge(33)
                .setHeight(184.5f)
                .setWeight(81).build();

        String topic = "customer_avro";
        String key = String.valueOf((int)(Math.random() * (100000000 - 1)));
        ProducerRecord<String, Customer> record = new ProducerRecord<String, Customer>(topic, key, customer);

        producer.send(record, new Callback(){
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("key: " + key + ", offset: "+metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();
    }
}

package com.aksain.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleKafkaProducer {
    private static final SimpleKafkaProducer INSTANCE = new SimpleKafkaProducer();

    private final Producer<String, String> producer;

    private SimpleKafkaProducer() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka brokers in format host1:port1,host2:port2
        props.put("acks", "1"); // 0 for no acknowledgements, 1 for leader acknowledgement and -1 for all replica acknowledgements
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public static SimpleKafkaProducer getInstance() {
        return INSTANCE;
    }

    public void send(String topicName, String key, String value) {
        producer.send(new ProducerRecord<>(topicName, key, value));
    }
}

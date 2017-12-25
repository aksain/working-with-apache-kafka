package com.aksain.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class SimpleKafkaConsumer extends Thread {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final BiConsumer<String, String> messageConsumer;

    public SimpleKafkaConsumer(Collection<String> topics, BiConsumer<String, String> messageConsumer) {
        this.messageConsumer = messageConsumer;

        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka message brokers in format host1:port1,host2:port2
        props.put("group.id", "test"); // Consumer group
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(topics);
    }

    @Override
    public void run() {
        while(!isInterrupted()) {
            for(ConsumerRecord<String, String> consumerRecord : kafkaConsumer.poll(1000))  {
                messageConsumer.accept(consumerRecord.key(), consumerRecord.value());
            }
        }
    }
}
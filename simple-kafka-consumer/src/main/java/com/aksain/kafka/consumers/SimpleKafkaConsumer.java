package com.aksain.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * {@link SimpleKafkaConsumer} pulls messages from provided topics in Apache Kafka and calls specified BiConsumer
 * for every pulled message.
 */
public class SimpleKafkaConsumer extends Thread {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final BiConsumer<String, String> messageConsumer;

    /**
     *
     * @param topics to pull messages from
     * @param consumerGroupId to define group of this consumer
     * @param messageConsumer to call for each pulled message
     */
    public SimpleKafkaConsumer(
            Collection<String> topics, String consumerGroupId, BiConsumer<String, String> messageConsumer) {

        this.messageConsumer = messageConsumer;

        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka message brokers in format host1:port1,host2:port2
        props.put("group.id", consumerGroupId); // Consumer group
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(topics);
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            for (ConsumerRecord<String, String> consumerRecord : kafkaConsumer.poll(1000)) {
                messageConsumer.accept(consumerRecord.key(), consumerRecord.value());
            }
        }
    }
}
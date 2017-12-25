package com.aksain.kafka.main;

import com.aksain.kafka.consumers.SimpleKafkaConsumer;

import java.util.Arrays;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        final String topicName = "test-topic";

        new SimpleKafkaConsumer(
                Arrays.asList(topicName)
                , (key, value) -> System.out.println("Message Key: " + key + ", Value: " + value)
        ).start();

        // Produce messages using following command from Kafka home directory
        // ./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic
    }
}

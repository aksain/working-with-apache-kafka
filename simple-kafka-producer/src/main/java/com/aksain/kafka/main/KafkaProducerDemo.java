package com.aksain.kafka.main;

import com.aksain.kafka.producers.SimpleKafkaProducer;

/**
 * @author Amit Kumar
 */
public class KafkaProducerDemo {

    public static void main(String[] args) {
        final String topicName = "test-topic";

        // Get Producer instance in try with resource to get it closed automatically
        final SimpleKafkaProducer simpleKafkaProducer = SimpleKafkaProducer.getInstance();
        // Send 10 messages
        System.out.println("Sending 10 messages to Kafka...");
        for (int i = 0; i < 10; i++) {
            simpleKafkaProducer.send(topicName, "Key" + i, "Sample Message " + i);
        }

        // Release producer resources and commit any pending messages. In real cases, it will be done at
        // application shutdown. E.g. PreDestroy in Spring
        simpleKafkaProducer.close();

        System.out.println("Sent 10 messages to Kafka successfully!!!");

        // Consume these sent messages using following command from Kafka home directory
        // ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic
    }
}

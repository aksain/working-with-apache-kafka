package com.aksain.kafka.main;

import com.aksain.kafka.consumers.SimpleKafkaConsumer;

import java.util.Arrays;

/**
 * @author Amit Kumar
 */
public class KafkaConsumerDemo {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Usage java -jar com.aksain.kafka.main.KafkaConsumerDemo <topicnames> <consumergroupid>");
            System.exit(1);
        }

        final String[] topicNames = args[0].split("\\s,\\s");
        final String groupId = args[1];

        new SimpleKafkaConsumer(
                Arrays.asList(topicNames)
                , groupId
                , (key, value) -> System.out.println("Message Key: " + key + ", Value: " + value)
        ).start();

        // Produce messages using following command from Kafka home directory
        // ./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic
    }
}

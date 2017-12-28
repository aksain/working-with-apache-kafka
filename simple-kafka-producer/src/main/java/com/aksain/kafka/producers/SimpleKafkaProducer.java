package com.aksain.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * <code>{@link SimpleKafkaProducer}</code> exposes methods to send messages to Kafka. It is implemented based on
 * Singleton to avoid creation of multiple {@link KafkaProducer} instances.
 */
public class SimpleKafkaProducer {
    private static final SimpleKafkaProducer INSTANCE = new SimpleKafkaProducer();

    private final Producer<String, String> producer;

    private SimpleKafkaProducer() {
        // Set some properties, ideally these would come from properties file
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka brokers in format host1:port1,host2:port2
        props.put("acks", "1"); // 0 for no acknowledgements, 1 for leader acknowledgement and -1 for all replica acknowledgements
        props.put("linger.ms", "1"); // Frequency of message commits to Kafka
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    /**
     * Factory method to get instance.
     *
     * @return instance of {@link SimpleKafkaProducer}
     */
    public static SimpleKafkaProducer getInstance() {
        return INSTANCE;
    }

    /**
     * Sends message with input key and value to specified topic name.
     *
     * @param topicName name of topic to publish messages to
     * @param key key of message
     * @param value payload of message
     */
    public void send(String topicName, String key, String value) {
        producer.send(new ProducerRecord<>(topicName, key, value));
    }

    /**
     * Releases pool of buffer space that holds records that haven't yet been transmitted to the server as well as a
     * background I/O thread that is responsible for turning these records into requests and transmitting them to
     * the cluster.
     */
    public void close() {
        producer.close();
    }
}

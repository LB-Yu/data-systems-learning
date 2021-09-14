package org.apache.kafka.learning.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerExample {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-tx");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        try {
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("topic1", "A", "A-message"));
            producer.send(new ProducerRecord<>("topic1", "B", "B-message"));
            producer.send(new ProducerRecord<>("topic2", "A", "A-message"));
            producer.send(new ProducerRecord<>("topic2", "B", "B-message"));
            Thread.sleep(1000);
            double t = 1 / 0;
            producer.send(new ProducerRecord<>("topic1", "C", "C-messgae"));
            producer.send(new ProducerRecord<>("topic2", "C", "C-messgae"));
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            producer.abortTransaction();
        }
    }
}

package org.apache.flink.learning.tool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class KafkaPartitionProducer {

  public static void main(String[] args) {
    String topic = "test1";

    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, IdPartitioner.class.getCanonicalName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    Scanner scanner = new Scanner(System.in);
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      String[] items = line.split(",");
      String id = items[0];
      producer.send(new ProducerRecord<>(topic, id, line));
    }
    producer.close();
  }
}

package io.github.lb.kafka.examples.avro;

import io.github.lb.kafka.examples.KafkaProperties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class Producer extends Thread {

  private final KafkaProducer<Integer, byte[]> producer;
  private final String topic;
  private final Boolean isAsync;
  private final CountDownLatch latch;

  public Producer(final String topic,
                  final boolean isAsync,
                  final CountDownLatch latch) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroDemoProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    producer = new KafkaProducer<>(props);
    this.topic = topic;
    this.isAsync = isAsync;
    this.latch = latch;
  }

  @Override
  public void run() {
    Random random = new Random();
    UserGenericRecord userRecord = new UserGenericRecord();
    for (int i = 0; i < 50; i++) {
      User user = new User(i, "user" + i, random.nextInt() % 25);
      byte[] userBytes = userRecord.serialize(user);
      ProducerRecord<Integer, byte[]> producerRecord = new ProducerRecord<>(topic, 1, userBytes);
      if (isAsync) {
        producer.send(producerRecord, (recordMetadata, e) -> {
          // do nothing in call back
        });
      } else {
        producer.send(producerRecord);
      }
    }
    latch.countDown();
  }
}

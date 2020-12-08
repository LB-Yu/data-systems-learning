package io.github.lb.kafka.examples.avro;

import io.github.lb.kafka.examples.simple.KafkaProperties;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer extends ShutdownableThread {
  private final KafkaConsumer<Integer, byte[]> consumer;
  private final String topic;
  private final String groupId;
  private final CountDownLatch latch;

  public Consumer(final String topic,
                  final String groupId,
                  final Optional<String> instanceId,
                  final CountDownLatch latch) {
    super("KafkaConsumerExample", false);
    this.groupId = groupId;
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    consumer = new KafkaConsumer<>(props);
    this.topic = topic;
    this.latch = latch;
  }

  @Override
  public void doWork() {
    consumer.subscribe(Collections.singletonList(this.topic));
    UserGenericRecord userRecord = new UserGenericRecord();

    ConsumerRecords<Integer, byte[]> records = consumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<Integer, byte[]> record : records) {
      User user = userRecord.deserialize(record.value());
      System.out.println(user);
    }
  }
}

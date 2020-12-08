package io.github.lb.kafka.examples.avro;

import io.github.lb.kafka.examples.simple.KafkaProperties;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerProducerDemo {

  public static void main(String[] args) throws InterruptedException {
    boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
    CountDownLatch latch = new CountDownLatch(2);
    Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync, latch);
    producerThread.start();

    Consumer consumerThread = new Consumer(KafkaProperties.TOPIC, "DemoConsumer", Optional.empty(), latch);
    consumerThread.start();

    if (!latch.await(5, TimeUnit.MINUTES)) {
      throw new TimeoutException("Timeout after 5 minutes waiting for demo producer and consumer to finish");
    }

    System.out.println("All finished!");
  }
}

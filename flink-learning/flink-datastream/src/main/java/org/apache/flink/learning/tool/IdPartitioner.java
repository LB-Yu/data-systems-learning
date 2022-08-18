package org.apache.flink.learning.tool;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class IdPartitioner implements Partitioner {

  @Override
  public int partition(String topic,
                       Object key,
                       byte[] keyBytes,
                       Object value,
                       byte[] valueBytes,
                       Cluster cluster) {
    return Integer.parseInt(key.toString());
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
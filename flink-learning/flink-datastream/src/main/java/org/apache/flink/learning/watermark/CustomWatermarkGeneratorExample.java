package org.apache.flink.learning.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class CustomWatermarkGeneratorExample {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(1);

    KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("test1")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

//    DataStream<String> streamSource = env.fromSource(
//            source,
//            WatermarkStrategy.forGenerator(
//                    (ctx) -> new CustomWatermarkGenerator<>(Duration.ofSeconds(5))),
//            "kafka-source");
  }

  public static class VisitWatermarkGenerator implements WatermarkGenerator<Visit> {

    private final long outOfOrdernessMillis;
    private final Map<String, Long> maxTimePerServer = new HashMap<>(2);

    public VisitWatermarkGenerator(Duration maxOutOfOrderness) {
      this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
    }

    @Override
    public void onEvent(Visit event, long eventTimestamp, WatermarkOutput output) {
      String server = event.serverId;
      if (!maxTimePerServer.containsKey(server) || eventTimestamp > maxTimePerServer.get(server)) {
        maxTimePerServer.put(server, eventTimestamp);
      }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      Optional<Long> maxTimestamp  = maxTimePerServer.values().stream()
              .min(Comparator.comparingLong(Long::valueOf));
      maxTimestamp.ifPresent(
              t -> output.emitWatermark(new Watermark(t - outOfOrdernessMillis - 1)));
    }
  }

  public static class Visit {
    public String serverId;
    public String userId;
    public long eventTime;
  }
}

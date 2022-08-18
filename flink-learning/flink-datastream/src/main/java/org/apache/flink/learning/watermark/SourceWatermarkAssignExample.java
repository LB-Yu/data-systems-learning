package org.apache.flink.learning.watermark;

import cn.hutool.core.date.LocalDateTimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;

public class SourceWatermarkAssignExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(2);
    env.getConfig().setAutoWatermarkInterval(1);

    KafkaSource<Tuple3<Integer, String, Long>> source = KafkaSource
            .<Tuple3<Integer, String, Long>>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("test1")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new KafkaDeserializeSchema())
            .build();
    DataStream<Tuple3<Integer, String, Long>> streamSource = env.fromSource(
            source,
            WatermarkStrategy
                    .<Tuple3<Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((t, time) -> t.f2),
            "kafka-source");

    DataStream<Tuple3<Integer, String, Long>> windowStream = streamSource
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new WindowFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, String, Long>, Integer, TimeWindow>() {

              @Override
              public void apply(Integer key,
                                TimeWindow timeWindow,
                                Iterable<Tuple3<Integer, String, Long>> iterable,
                                Collector<Tuple3<Integer, String, Long>> collector) throws Exception {
                System.out.println("========");
                System.out.println("Window key: " + key);
                System.out.println("Window range: ["
                        + LocalDateTimeUtil.format(LocalDateTimeUtil.of(timeWindow.getStart()), "yyyy-MM-dd HH:mm:ss")
                        + ", "
                        + LocalDateTimeUtil.format(LocalDateTimeUtil.of(timeWindow.getEnd()), "yyyy-MM-dd HH:mm:ss")
                        + "]");
                System.out.println("Window items:");
                for (Tuple3<Integer, String, Long> t : iterable) {
                  System.out.println(t);
                  collector.collect(t);
                }
                System.out.println("========");
              }
            });

    windowStream
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new WindowFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, String, Long>, Integer, TimeWindow>() {

              @Override
              public void apply(Integer key,
                                TimeWindow timeWindow,
                                Iterable<Tuple3<Integer, String, Long>> iterable,
                                Collector<Tuple3<Integer, String, Long>> collector) throws Exception {
                System.out.println("========");
                System.out.println("Window key: " + key);
                System.out.println("Window range: ["
                        + LocalDateTimeUtil.format(LocalDateTimeUtil.of(timeWindow.getStart()), "yyyy-MM-dd HH:mm:ss")
                        + ", "
                        + LocalDateTimeUtil.format(LocalDateTimeUtil.of(timeWindow.getEnd()), "yyyy-MM-dd HH:mm:ss")
                        + "]");
                System.out.println("Window items:");
                for (Tuple3<Integer, String, Long> t : iterable) {
                  System.out.println(t);
                  collector.collect(t);
                }
                System.out.println("========");
              }
            });

    env.execute();
  }

  public static class KafkaDeserializeSchema implements DeserializationSchema<Tuple3<Integer, String, Long>> {

    @Override
    public Tuple3<Integer, String, Long> deserialize(byte[] bytes) throws IOException {
      String msg = new String(bytes);
      String[] items = msg.split(",");
      int id = Integer.parseInt(items[0]);
      String value = items[1];
      LocalDateTime localDateTime = LocalDateTimeUtil.parse(items[2], "yyyy-MM-dd HH:mm:ss");
      long timestamp = LocalDateTimeUtil.toEpochMilli(localDateTime);
      return new Tuple3<>(id, value, timestamp);
    }

    @Override
    public boolean isEndOfStream(Tuple3<Integer, String, Long> integerStringLongTuple3) {
      return false;
    }

    @Override
    public TypeInformation<Tuple3<Integer, String, Long>> getProducedType() {
      return TypeInformation.of(new TypeHint<Tuple3<Integer, String, Long>>() { });
    }
  }
}

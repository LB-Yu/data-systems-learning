package org.apache.flink.learning.broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class BroadCastExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> ds1 = env.fromElements("A", "B", "C");
    DataStream<String> ds2 = env.fromElements("B", "C", "D");
    final MapStateDescriptor<Integer, CustomObject<String>> broadcastDesc = new MapStateDescriptor<>(
            "broadcast-state",
            TypeInformation.of(Integer.class),
            TypeInformation.of(new TypeHint<CustomObject<String>>() { }));
    BroadcastStream<String> broadcastStream = ds2.broadcast(broadcastDesc);
    DataStream<Tuple2<String, String>> result = ds1.connect(broadcastStream).process(new BroadcastProcessFunction<String, String, Tuple2<String, String>>() {

      @Override
      public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Tuple2<String, String>> collector) throws Exception {
        ReadOnlyBroadcastState<Integer, CustomObject<String>> state = readOnlyContext.getBroadcastState(broadcastDesc);
        if (state.contains(0)) {
          CustomObject<String> set = state.get(0);
          if (set.contains(s)) {
            collector.collect(new Tuple2<>(s, s));
          }
        }
      }

      @Override
      public void processBroadcastElement(String s, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
        BroadcastState<Integer, CustomObject<String>> state = context.getBroadcastState(broadcastDesc);
        if (!state.contains(0)) {
          state.put(0, new CustomObject<>());
        }
        CustomObject<String> set = state.get(0);
        set.add(s);
      }
    });
//    result.print();

    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    FlinkKafkaProducer<Tuple2<String, String>> kafkaProducer = new FlinkKafkaProducer<>(
            "test", new SimpleSerializer(), props, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    result.addSink(kafkaProducer);

    env.execute();
  }

  public static class CustomObject<T> {
    private final List<T> values;

    public CustomObject() {
      values = new ArrayList<>();
    }

    public void add(T v) {
      values.add(v);
    }

    public boolean contains(T v) {
      return values.contains(v);
    }
  }

  public static class SimpleSerializer implements KafkaSerializationSchema<Tuple2<String, String>> {

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> t, @Nullable Long timestamp) {
      return new ProducerRecord<>("test", (t.f0 + "," + t.f1).getBytes(StandardCharsets.UTF_8));
    }
  }
}

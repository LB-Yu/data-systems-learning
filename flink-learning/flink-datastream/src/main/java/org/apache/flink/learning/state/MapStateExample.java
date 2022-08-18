package org.apache.flink.learning.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapStateExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(1);

    DataStream<Tuple4<Integer, String, Integer, Integer>> originStream = env
            .socketTextStream("localhost", 8899)
            .flatMap(new FlatMapFunction<String, Tuple4<Integer, String, Integer, Integer>>() {
              @Override
              public void flatMap(String s, Collector<Tuple4<Integer, String, Integer, Integer>> collector) throws Exception {
                try {
                  String[] items = s.split(",");
                  collector.collect(
                          new Tuple4<>(
                                  Integer.parseInt(items[0]),
                                  items[1],
                                  Integer.parseInt(items[2]),
                                  Integer.parseInt(items[3])));
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            });

    DataStream<Tuple4<Integer, String, Integer, Integer>> deduplicateStream = originStream
            .keyBy(t -> t.f0)
            .process(new DeduplicateProcessFunction());

    deduplicateStream.print();

    env.execute();
  }

  public static final class DeduplicateProcessFunction
          extends KeyedProcessFunction<Integer, Tuple4<Integer, String, Integer, Integer>,
          Tuple4<Integer, String, Integer, Integer>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicateProcessFunction.class);

    private MapState<String, RecordId> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
      StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
              .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
              .cleanupInRocksdbCompactFilter(10000)
              .build();

      MapStateDescriptor<String, RecordId> mapStateDescriptor = new MapStateDescriptor<>(
              "deduplicate-state",
              String.class, RecordId.class);

      mapStateDescriptor.enableTimeToLive(stateTtlConfig);

      mapState = this.getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(Tuple4<Integer, String, Integer, Integer> value,
                               Context ctx,
                               Collector<Tuple4<Integer, String, Integer, Integer>> out) throws Exception {
      if (mapState.contains(value.f1)) {
        RecordId recordId = mapState.get(value.f1);
        if (value.f2 > recordId.txId && value.f3 > recordId.id) {
          out.collect(value);
          mapState.put(value.f1, new RecordId(value.f2, value.f3));
        }
      } else {
        out.collect(value);
        mapState.put(value.f1, new RecordId(value.f2, value.f3));
      }
    }
  }

  public static class RecordId {
    public long txId;
    public long id;

    public RecordId(long txId, long id) {
      this.txId = txId;
      this.id = id;
    }
  }
}

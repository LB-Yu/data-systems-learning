package org.apache.flink.learning.watermark;

import cn.hutool.core.date.LocalDateTimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 * @author Yu Liebing
 * */
public class WatermarkAfterFilterExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(1);

    env.socketTextStream("localhost", 8899)
            .flatMap(new FlatMapFunction<String, Tuple2<Integer, Long>>() {
              @Override
              public void flatMap(String s, Collector<Tuple2<Integer, Long>> collector) throws Exception {
                try {
                  String[] items = s.split(",");
                  int id = Integer.parseInt(items[0]);
                  long ts = Long.parseLong(items[1]);
                  collector.collect(new Tuple2<>(id, ts));
                } catch (Exception e) {
                  System.out.println("Error");
                }
              }
            })
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Tuple2<Integer, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner((t, time) -> t.f1))
            .filter(t -> t.f0 == 1)
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new AllWindowFunction<Tuple2<Integer, Long>, Object, TimeWindow>() {
              @Override
              public void apply(TimeWindow timeWindow, Iterable<Tuple2<Integer, Long>> iterable, Collector<Object> collector) throws Exception {
                System.out.println("========");
                System.out.println("Window range: ["
                        + LocalDateTimeUtil.format(LocalDateTimeUtil.of(timeWindow.getStart()), "yyyy-MM-dd HH:mm:ss")
                        + ", "
                        + LocalDateTimeUtil.format(LocalDateTimeUtil.of(timeWindow.getEnd()), "yyyy-MM-dd HH:mm:ss")
                        + "]");
                System.out.println("Window items:");
                iterable.forEach(System.out::println);
              }
            });
    env.execute();
  }
}

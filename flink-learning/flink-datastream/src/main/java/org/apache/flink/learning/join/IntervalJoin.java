package org.apache.flink.learning.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.learning.utils.Order;
import org.apache.flink.learning.utils.OrderMapper;
import org.apache.flink.learning.utils.Shipment;
import org.apache.flink.learning.utils.ShipmentMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoin {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Order> order = env
            .socketTextStream("localhost", 8888)
            .flatMap(new OrderMapper())
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Order>forMonotonousTimestamps()
                    .withTimestampAssigner((event, time) -> event.getTimestamp()));

    DataStream<Shipment> shipment = env
            .socketTextStream("localhost", 9999)
            .flatMap(new ShipmentMapper())
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Shipment>forMonotonousTimestamps()
                    .withTimestampAssigner((event, time) -> event.getTimestamp()));

    order.keyBy(Order::getOrderId)
            .intervalJoin(shipment.keyBy(Shipment::getOrderId))
            .between(Time.seconds(0), Time.seconds(5))
            .process(new ProcessJoinFunction<Order, Shipment, Tuple7<String, String, String, Long, String, String, Long>>() {
              @Override
              public void processElement(Order o,
                                         Shipment s,
                                         Context context,
                                         Collector<Tuple7<String, String, String, Long, String, String, Long>> collector) throws Exception {
                collector.collect(new Tuple7<>(o.getOrderId(), o.getUserName(), o.getItem(), o.getTimestamp(),
                        s.getShipId(), s.getCompany(), s.getTimestamp()));
              }
            })
            .print();

    env.execute();
  }
}

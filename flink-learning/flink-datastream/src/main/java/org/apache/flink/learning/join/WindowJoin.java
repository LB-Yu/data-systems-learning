package org.apache.flink.learning.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.learning.utils.Order;
import org.apache.flink.learning.utils.OrderMapper;
import org.apache.flink.learning.utils.Shipment;
import org.apache.flink.learning.utils.ShipmentMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoin {

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

    order.join(shipment)
            .where(Order::getOrderId)
            .equalTo(Shipment::getOrderId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new JoinFunction<Order, Shipment, Tuple7<String, String, String, Long, String, String, Long>>() {
              @Override
              public Tuple7<String, String, String, Long, String, String, Long> join(Order o, Shipment s) throws Exception {
                return new Tuple7<>(o.getOrderId(), o.getUserName(), o.getItem(), o.getTimestamp(),
                        s.getShipId(), s.getCompany(), s.getTimestamp());
              }
            })
            .print();

    env.execute();
  }
}

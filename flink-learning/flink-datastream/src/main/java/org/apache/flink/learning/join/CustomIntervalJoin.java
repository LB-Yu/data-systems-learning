package org.apache.flink.learning.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.learning.utils.Order;
import org.apache.flink.learning.utils.OrderMapper;
import org.apache.flink.learning.utils.Shipment;
import org.apache.flink.learning.utils.ShipmentMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class CustomIntervalJoin {

  public static void main(String[] args) {
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

    order
            .connect(shipment)
            .keyBy(Order::getOrderId, Shipment::getShipId)
            .process(new CoProcessFunction<Order, Shipment, Object>() {
              @Override
              public void processElement1(Order order, CoProcessFunction<Order, Shipment, Object>.Context context, Collector<Object> collector) throws Exception {

              }

              @Override
              public void processElement2(Shipment shipment, CoProcessFunction<Order, Shipment, Object>.Context context, Collector<Object> collector) throws Exception {

              }
            });
  }
}

package org.apache.flink.learning.table.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class OrderMapper extends RichFlatMapFunction<String, Order> {

    private static Logger LOGGER = LoggerFactory.getLogger(OrderMapper.class);

    private DateTimeFormatter formatter;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void flatMap(String line, Collector<Order> collector) throws Exception {
        try {
            String[] items = line.split(",");
            String orderId = items[0];
            String userName = items[1];
            String item = items[2];
            long timestamp = LocalDateTime.parse(items[3], formatter).toInstant(ZoneOffset.of("+8")).toEpochMilli();
            collector.collect(new Order(orderId, userName, item, timestamp));
        } catch (Exception e) {
            LOGGER.error("Cannot parse line: {}", line);
        }
    }
}

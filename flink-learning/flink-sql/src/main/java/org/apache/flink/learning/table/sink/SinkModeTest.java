package org.apache.flink.learning.table.sink;

import org.apache.flink.learning.table.utils.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

public class SinkModeTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Order> dataStream =
                env.fromCollection(
                        Arrays.asList(
                                new Order("1", "Bob", "Shoes", System.currentTimeMillis()),
                                new Order("2", "Bob", "Shoes", System.currentTimeMillis()),
                                new Order("3", "Julia", "Hat", System.currentTimeMillis())));

        Table table = tEnv.fromDataStream(dataStream);

        Table result = tEnv.sqlQuery("SELECT count(*) from " + table);

        tEnv.toChangelogStream(result).print();

        env.execute();
    }
}

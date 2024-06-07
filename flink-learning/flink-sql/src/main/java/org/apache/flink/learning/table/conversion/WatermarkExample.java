package org.apache.flink.learning.table.conversion;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class WatermarkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE source (\n" +
                        "  id BIGINT,\n" +
                        "  price DECIMAL(32,2),\n" +
                        "  buyer STRING,\n" +
                        "  order_time TIMESTAMP(3),\n" +
                        "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'datagen',\n" +
                        "  'fields.id.min' = '1',\n" +
                        "  'fields.id.max' = '1000',\n" +
                        "  'fields.price.min' = '0',\n" +
                        "  'fields.price.max' = '10000',\n" +
                        "  'fields.buyer.length' = '5'\n" +
                        ")");
        Table table = tableEnv.sqlQuery("SELECT * FROM source");
        DataStream<Row> dataStream = tableEnv.toDataStream(table);

        Table resultTable = tableEnv.fromDataStream(dataStream,
                Schema.newBuilder()
                        .column("id", "BIGINT")
                        .column("price", "DECIMAL(32,2)")
                        .column("buyer", "STRING")
                        .column("order_time", "TIMESTAMP(3)")
                        .watermark("order_time", "SOURCE_WATERMARK()")
                        .build());
        resultTable.printSchema();
        tableEnv.createTemporaryView("result_table", resultTable);

        Table table1 = tableEnv.sqlQuery(
                "SELECT window_start, window_end, SUM(price)\n" +
                        "FROM TABLE(\n" +
                        "  TUMBLE(TABLE result_table, DESCRIPTOR(order_time), INTERVAL '10' SECONDS))\n" +
                        "GROUP BY window_start, window_end");
        table1.execute().print();

        env.execute();
    }
}

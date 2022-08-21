package org.apache.flink.learning.table.conversion;

import org.apacge.flink.common.join.JdbcRowLookupFunction;
import org.apacge.flink.common.join.RowLookupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class LookupJoinExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    // define source and result tables
    tableEnv.executeSql(
            "CREATE TABLE traj_point (\n" +
                    "  id BIGINT,\n" +
                    "  `user` STRING,\n" +
                    "  x FLOAT,\n" +
                    "  y FLOAT,\n" +
                    "  `time` TIMESTAMP(3)\n" +
                    ") WITH (\n" +
                    "  'connector' = 'datagen',\n" +
                    "  'fields.id.min' = '1',\n" +
                    "  'fields.id.max' = '100',\n" +
                    "  'fields.user.length' = '5',\n" +
                    "  'fields.x.min' = '0',\n" +
                    "  'fields.x.max' = '10',\n" +
                    "  'fields.y.min' = '0',\n" +
                    "  'fields.y.max' = '10'\n" +
                    ")");
    tableEnv.executeSql(
            "CREATE TABLE traj_point_with_restaurant (\n" +
                    "  id BIGINT,\n" +
                    "  `user` STRING,\n" +
                    "  x FLOAT,\n" +
                    "  y FLOAT,\n" +
                    "  `time` TIMESTAMP(3),\n" +
                    "  restaurant STRING\n" +
                    ") WITH (\n" +
                    "  'connector' = 'print'\n" +
                    ")");
    // get source table and convert to DataStream
    Table table = tableEnv.sqlQuery("SELECT * FROM traj_point");
    DataStream<Row> dataStream = tableEnv.toDataStream(table);
    // do lookup join with DataStream API
    JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
            .JdbcConnectionOptionsBuilder()
            .withDriverName("com.mysql.cj.jdbc.Driver")
            .withUrl("jdbc:mysql://localhost:3306/test")
            .withUsername("root")
            .withPassword("0407")
            .build();
    JdbcLookupOptions lookupOptions = new JdbcLookupOptions
            .Builder()
            .setCacheMaxSize(100)
            .setCacheExpireMs(10000)
            .setMaxRetryTimes(3)
            .setCacheMissingKey(false)
            .build();
    String lookupSQL =
            "SELECT name FROM restaurant_table " +
            "ORDER BY SQRT(POW(x - ?, 2) + POW(y - ?, 2)) LIMIT 1";
    JoinFunction<Row, Row, Row> joinFunction = Row::join;
    RowLookupFunction lookupFunction = new JdbcRowLookupFunction(
            connectionOptions,
            lookupOptions,
            lookupSQL,
            new int[] {2, 3},
            joinFunction,
            true);
    RowTypeInfo rowTypeInfo = new RowTypeInfo(
            new TypeInformation[] {
                    Types.LONG,
                    Types.STRING,
                    Types.FLOAT,
                    Types.FLOAT,
                    Types.LOCAL_DATE_TIME,
                    Types.STRING},
            new String[] {"id", "user", "x", "y", "time", "restaurant"});
    DataStream<Row> resultDataStream = dataStream.process(lookupFunction).returns(rowTypeInfo);
    // convert join result to Table
    Table resultTable = tableEnv.fromDataStream(resultDataStream);
    tableEnv.executeSql("INSERT INTO traj_point_with_restaurant SELECT * FROM " + resultTable);

    env.execute();
  }
}

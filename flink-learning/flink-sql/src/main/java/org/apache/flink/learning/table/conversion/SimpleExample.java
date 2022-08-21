package org.apache.flink.learning.table.conversion;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * A simple example shows how to convert between Table & DataStream
 * */
public class SimpleExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    DataStream<Row> dataStream = env.fromElements(
            Row.of("Alice", 12),
            Row.of("Bob", 10),
            Row.of("Alice", 100));

    RowTypeInfo rowTypeInfo = new RowTypeInfo(
            new TypeInformation<?>[] {Types.STRING, Types.INT},
            new String[] {"name", "score"});
    DataStream<Row> processedStream = dataStream.process(new ProcessFunction<Row, Row>() {
      @Override
      public void processElement(Row row,
                                 Context context,
                                 Collector<Row> collector) {
        collector.collect(row);
      }
    }).returns(rowTypeInfo);

    Table inputTable = tableEnv.fromChangelogStream(processedStream);
    inputTable.printSchema();

    env.execute();
  }
}

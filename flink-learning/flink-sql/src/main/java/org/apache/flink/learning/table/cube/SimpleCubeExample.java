package org.apache.flink.learning.table.cube;

import org.apache.flink.learning.udaf.GenericRecordAgg;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

import static org.apache.flink.learning.table.utils.Utils.createTempFile;

public class SimpleCubeExample {

    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporaryFunction("GenericRecordAgg", GenericRecordAgg.class);

        // user_id, product, count, price
        String contents =
                "1,A,1,10\n" +
                        "2,A,3,27\n" +
                        "3,B,4,30\n" +
                        "4,B,5,70\n" +
                        "5,A,1,10\n" +
                        "6,C,2,30";
        String path = createTempFile(contents);

        String source =
                "CREATE TABLE source (\n"
                        + "  user_id INT,\n"
                        + "  product STRING,\n"
                        + "  `count` STRING,\n"
                        + "  price STRING\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(source);

        String sql =
                "select\n" +
                        "\tdims, GenericRecordAgg(metricData)\n" +
                        "from (\n" +
                        "\tselect \n" +
                        "\t\tproduct as dims, \n" +
                        "\t\tconcat('SUM:', `count`, '|', 'SUM:', price) as metricData\n" +
                        "\tfrom source\n" +
                        ") group by dims;";

        tEnv.sqlQuery(sql).execute().print();
    }
}

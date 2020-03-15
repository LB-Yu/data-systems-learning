package org.apache.hadoop.hbase.client.meta;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Yu Liebing
 */
public class HBaseMeta extends Configured implements Tool {

  private static final String HBASE_META = "hbase:meta";

  @Override
  public int run(String[] args) throws Exception {
    Connection connection = ConnectionFactory.createConnection(getConf());
    Table metaTable = connection.getTable(TableName.valueOf(HBASE_META));

    Scan scan = new Scan();
    ResultScanner scanner = metaTable.getScanner(scan);
    for (Result result : scanner) {
      System.out.println(result);
    }

    metaTable.close();
    connection.close();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new HBaseMeta(), args);
  }
}

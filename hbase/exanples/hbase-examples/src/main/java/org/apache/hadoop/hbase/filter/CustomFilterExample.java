package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Yu Liebing
 */
public class CustomFilterExample extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Connection connection = ConnectionFactory.createConnection(getConf());
    Table table = connection.getTable(TableName.valueOf("filter_test"));
    Scan scan = new Scan();
    scan.setFilter(new CustomFilter("1506".getBytes(), "f2".getBytes(), "d".getBytes(), "12".getBytes()));
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      System.out.println(result);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new CustomFilterExample(), args);
  }
}

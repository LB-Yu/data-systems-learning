package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
    Connection connection = null;
    Table table = null;
    ResultScanner scanner = null;
    try {
      connection = ConnectionFactory.createConnection(getConf());
      table = connection.getTable(TableName.valueOf("filter_test"));
      Scan scan = new Scan();
      scan.setFilter(new CustomFilter("1506".getBytes(), "f2".getBytes(), "d".getBytes(), "12".getBytes()));
      scanner = table.getScanner(scan);
      for (Result result : scanner) {
        for (Cell cell : result.rawCells()) {
          System.out.println(cellToString(cell));
        }
      }
    } finally {
      if (scanner != null) scanner.close();
      if (table != null) table.close();
      if (connection != null) connection.close();
    }
    return 0;
  }

  private String cellToString(Cell cell) {
    StringBuilder sb = new StringBuilder();
    sb.append("row key: ").append(new String(CellUtil.cloneRow(cell))).append(", ");
    sb.append("family: ").append(new String(CellUtil.cloneFamily(cell))).append(", ");
    sb.append("qualifier: ").append(new String(CellUtil.cloneQualifier(cell))).append(", ");
    sb.append("value: ").append(new String(CellUtil.cloneValue(cell)));
    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new CustomFilterExample(), args);
  }
}

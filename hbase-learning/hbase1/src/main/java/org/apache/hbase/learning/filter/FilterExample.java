package org.apache.hbase.learning.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

import static org.apache.hbase.learning.filter.Util.cellToString;

public class FilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("filter_test"));
    Scan scan = new Scan();
    scan.setFilter(new CustomFilter("1506".getBytes(), "f2".getBytes(), "d".getBytes(), "12".getBytes()));
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      for (Cell cell : result.rawCells()) {
        System.out.println(cellToString(cell));
      }
    }
  }
}

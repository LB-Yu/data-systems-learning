package org.apache.hadoop.hbase.client.meta;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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

    try {
      Scan scan = new Scan();
      ResultScanner scanner = metaTable.getScanner(scan);
      for (Result result : scanner) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
          String rowKey = new String(CellUtil.cloneRow(cell));
          String family = new String(CellUtil.cloneFamily(cell));
          String qualifier = new String(CellUtil.cloneQualifier(cell));
          byte[] valueBytes = CellUtil.cloneValue(cell);
          String value;
          switch (qualifier) {
            case "regioninfo":
              HRegionInfo hRegionInfo = HRegionInfo.parseFrom(valueBytes);
              value = hRegionInfo.toString();
              break;
            case "seqnumDuringOpen":
            case "serverstartcode":
              value = Bytes.toLong(valueBytes) + "";
              break;
            default:
              value = new String(valueBytes);
              break;
          }
          System.out.println("[Row key: " + rowKey + "] " +
                  "[column family: " + family + "] " +
                  "[column qualifier: " + qualifier + "] " +
                  "[value: " + value + "]");
        }
        System.out.println();
      }
    } finally {
      metaTable.close();
      connection.close();
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new HBaseMeta(), args);
  }
}

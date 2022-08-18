package org.apache.hbase.learning.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestRowMutations {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("test"));

    byte[] rowKey = Bytes.toBytes("row1");
    byte[] cf = Bytes.toBytes("cf");
    byte[] q1 = Bytes.toBytes("q1");
    byte[] q2 = Bytes.toBytes("q2");
    byte[] v1 = Bytes.toBytes("v1");
    byte[] v2 = Bytes.toBytes("v2");

    Put put = new Put(rowKey);
    put.addColumn(cf, q1, v1);
    put.addColumn(cf, q2, v2);

    Delete delete = new Delete(rowKey);
    delete.addColumn(cf, q2);

    mutator.mutate(put);
    mutator.mutate(delete);

    mutator.flush();
    mutator.close();
  }
}

package org.apache.calcite.adapter.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTools {

    public static void createTestTable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("test");
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("F"));
        tableDesc.addFamily(columnDesc);
        admin.createTable(tableDesc);

        Table table = conn.getTable(tableName);

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("F"), Bytes.toBytes("a"), Bytes.toBytes(1));
        put1.addColumn(Bytes.toBytes("F"), Bytes.toBytes("b"), Bytes.toBytes("hello"));
        table.put(put1);

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("F"), Bytes.toBytes("a"), Bytes.toBytes(2));
        put2.addColumn(Bytes.toBytes("F"), Bytes.toBytes("b"), Bytes.toBytes("world"));
        table.put(put2);

        table.close();
        conn.close();
    }

    public static void main(String[] args) throws IOException {
        createTestTable();
    }
}

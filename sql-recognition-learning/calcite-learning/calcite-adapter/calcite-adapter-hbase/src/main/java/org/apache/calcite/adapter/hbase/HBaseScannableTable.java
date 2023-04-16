package org.apache.calcite.adapter.hbase;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseScannableTable extends HBaseTable implements ScannableTable {

    public HBaseScannableTable(Table table, String fieldsInfo) {
        super(table, fieldsInfo);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                try {
                    Scan scan = new Scan();
                    ResultScanner scanner = table.getScanner(scan);
                    return new HBaseEnumerator(
                            scanner,
                            getRowType(new JavaTypeFactoryImpl()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}

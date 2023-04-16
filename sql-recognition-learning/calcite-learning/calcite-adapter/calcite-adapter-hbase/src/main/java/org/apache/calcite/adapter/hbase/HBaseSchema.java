package org.apache.calcite.adapter.hbase;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class HBaseSchema extends AbstractSchema {

    private final Source schemaSource;
    private final Connection connection;

    public HBaseSchema(String schemaPath) throws IOException {
        File schemaFile = new File(schemaPath);
        schemaSource = Sources.of(schemaFile);

        Configuration conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        try {
            CSVReader csvReader = new CSVReader(schemaSource.reader(), '|');
            String[] item;
            while ((item = csvReader.readNext()) != null) {
                if (item.length == 2) {
                    String tableName = item[0].toUpperCase();
                    String fieldsInfo = item[1];
                    org.apache.hadoop.hbase.client.Table table =
                            connection.getTable(TableName.valueOf(tableName));
                    builder.put(tableName, new HBaseScannableTable(table, fieldsInfo));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return builder.build();
    }
}

package org.apache.calcite.adapter.hbase;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.CsvEnumerator;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class HBaseTranslatableTable
        extends HBaseTable
        implements QueryableTable, TranslatableTable {

    public HBaseTranslatableTable(Table table, String fieldsInfo) {
        super(table, fieldsInfo);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    public Enumerable<Object> project(DataContext root, String[] columnNames) {
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                try {
                    Scan scan = new Scan();
                    byte[][] columnPrefix = new byte[columnNames.length][];
                    for (int i = 0; i < columnNames.length; ++i) {
                        columnPrefix[i] = columnNames[i].getBytes();
                    }
                    MultipleColumnPrefixFilter columnFilter =
                            new MultipleColumnPrefixFilter(columnPrefix);
                    scan.setFilter(columnFilter);
                    ResultScanner scanner = table.getScanner(scan);
                    return new HBaseEnumerator<>(scanner, getProjectRowType(columnNames));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private RelDataType getProjectRowType(String[] columnNames) {
        RelDataTypeFactory.FieldInfoBuilder builder = new JavaTypeFactoryImpl().builder();

        RelDataType rowType = getRowType(new JavaTypeFactoryImpl());
        Set<String> projectNames = new HashSet<>(Arrays.asList(columnNames));
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            RelDataTypeField field = rowType.getFieldList().get(i);
            if (projectNames.contains(field.getName())) {
                builder.add(field.getName(), field.getType());
            }
        }
        return builder.build();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final int fieldCount = relOptTable.getRowType().getFieldCount();
        final int[] fields = CsvEnumerator.identityList(fieldCount);
        return new HBaseTableScan(
                context.getCluster(),
                relOptTable,
                this,
                fields);
    }
}

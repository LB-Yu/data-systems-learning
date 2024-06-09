package org.apache.calcite.adapter.hbase;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hbase.client.Table;
import org.immutables.value.Value;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public abstract class HBaseTable
        extends AbstractQueryableTable
        implements TranslatableTable {

    protected final Table table;
    private final String fieldsInfo;

    protected RelDataType relDataType;

    public HBaseTable(Table table, String fieldsInfo) {
        super(Object[].class);
        this.table = table;
        this.fieldsInfo = fieldsInfo;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (relDataType != null) {
            return relDataType;
        }

        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        String[] strings = fieldsInfo.split(",");
        for (String string : strings) {
            final String name;
            final RelDataType fieldType;
            final int colon = string.indexOf(':');
            if (colon >= 0) {
                name = string.substring(0, colon);
                String typeString = string.substring(colon + 1);
                fieldType = typeFactory.createSqlType(SqlTypeName.valueOf(typeString));
                if (fieldType == null) {
                    System.out.println("WARNING: Found unknown type: " + typeString
                            + " for column: " + name
                            + ". Will assume the type of column is string");
                }
            } else {
                name = string;
                fieldType = null;
            }
            final RelDataType type;
            if (fieldType == null) {
                type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
            } else {
                type = fieldType;
            }
            names.add(name);
            types.add(type);
        }
        relDataType = typeFactory.createStructType(Pair.zip(names, types));
        return relDataType;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new HBaseQueryable<>(queryProvider, schema, this, tableName);
    }

    public Enumerable<Object> query() {
        return null;
    }

    @Override
    public RelNode toRel(
            RelOptTable.ToRelContext context,
            RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new HBaseTableScan(cluster, relOptTable, this, null);
    }

    public static class HBaseQueryable<T> extends AbstractTableQueryable<T> {

        public HBaseQueryable(
                QueryProvider queryProvider,
                SchemaPlus schema,
                HBaseTable table,
                String tableName) {
            super(queryProvider, schema, table, tableName);
        }

        @Override
        public Enumerator<T> enumerator() {
            return null;
        }
    }
}

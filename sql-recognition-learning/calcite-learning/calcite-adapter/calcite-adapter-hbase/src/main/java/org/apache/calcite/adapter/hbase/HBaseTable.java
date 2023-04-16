package org.apache.calcite.adapter.hbase;

import org.apache.calcite.adapter.file.CsvFieldType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hbase.client.Table;

import java.util.ArrayList;
import java.util.List;

public abstract class HBaseTable extends AbstractTable {

    protected final Table table;
    private final String fieldsInfo;

    protected RelDataType relDataType;

    public HBaseTable(Table table, String fieldsInfo) {
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
            final CsvFieldType fieldType;
            final int colon = string.indexOf(':');
            if (colon >= 0) {
                name = string.substring(0, colon);
                String typeString = string.substring(colon + 1);
                fieldType = CsvFieldType.of(typeString);
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
                type = fieldType.toType((JavaTypeFactory) typeFactory);
            }
            names.add(name);
            types.add(type);
        }
        relDataType = typeFactory.createStructType(Pair.zip(names, types));
        return relDataType;
    }
}

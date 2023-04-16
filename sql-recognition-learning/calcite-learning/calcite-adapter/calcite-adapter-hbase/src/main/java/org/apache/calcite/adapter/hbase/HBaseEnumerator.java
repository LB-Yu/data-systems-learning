package org.apache.calcite.adapter.hbase;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseEnumerator implements Enumerator<Object[]> {

    private final ResultScanner scanner;
    private final RelDataType relDataType;

    private Result current;

    public HBaseEnumerator(ResultScanner scanner, RelDataType relDataType) {
        this.scanner = scanner;
        this.relDataType = relDataType;
    }

    @Override
    public Object[] current() {
        if (current != null) {
            return convertRow(current);
        }
        return null;
    }

    @Override
    public boolean moveNext() {
        try {
            current = scanner.next();
            return (current != null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        scanner.close();
    }

    private Object[] convertRow(Result result) {
        int fieldCount = relDataType.getFieldCount();
        Object[] row = new Object[fieldCount];

        List<RelDataTypeField> fieldList = relDataType.getFieldList();
        for (int i = 0; i < fieldCount; ++i) {
            RelDataTypeField field = fieldList.get(i);

            String name = field.getName();
            SqlTypeName type = field.getType().getSqlTypeName();
            switch (type) {
                case INTEGER:
                    row[i] = Bytes.toInt(getColumn(result, name));
                    break;
                case VARCHAR:
                default:
                    row[i] = Bytes.toString(getColumn(result, name));
                    break;
            }

        }
        return row;
    }

    private byte[] getColumn(Result result, String qualifier) {
        return result.getValue(Bytes.toBytes("F"), Bytes.toBytes(qualifier));
    }
}

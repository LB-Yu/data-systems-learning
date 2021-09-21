package org.apache.calcite.example.memory;

import org.apache.calcite.example.memory.util.DateFormat;
import org.apache.calcite.linq4j.Enumerator;

import java.math.BigDecimal;
import java.util.List;

public class MemoryEnumerator<E> implements Enumerator<E> {
    private List<List<String>> data = null;
    private int currentIndex = -1;
    private RowConverter<E> rowConvert;
    private List<String> columnTypes;

    public MemoryEnumerator(int[] fields, List<String> types, List<List<String>> data) {
        this.data = data;
        this.columnTypes = types;
        rowConvert = (RowConverter<E>) new ArrayRowConverter(fields);
    }

    abstract static class RowConverter<E> {
        abstract E convertRow(List<String> rows, List<String> columnTypes);
    }

    static class ArrayRowConverter extends RowConverter<Object[]> {
        private int[] fields;

        public ArrayRowConverter(int[] fields) {
            this.fields = fields;
        }

        @Override
        Object[] convertRow(List<String> rows, List<String> columnTypes) {
            Object[] objects = new Object[fields.length];
            int i = 0 ;
            for(int field : this.fields) {
                objects[i ++] = convertOptiqCellValue(rows.get(field), columnTypes.get(field));
            }
            return objects;
        }
    }

    public void close() {
        // TODO Auto-generated method stub

    }

    public E current() {
        List<String> line = data.get(currentIndex);
        return rowConvert.convertRow(line, this.columnTypes);
    }

    public boolean moveNext() {
        return ++ currentIndex < data.size();
    }

    public void reset() {
        // TODO Auto-generated method stub

    }


    public static Object convertOptiqCellValue(String strValue, String dataType) {
        if (strValue == null)
            return null;

        if ((strValue.equals("") || strValue.equals("\\N")) && !dataType.equals("string"))
            return null;

        // TODO use data type enum instead of string comparison
        if ("date".equals(dataType)) {
            // convert epoch time
        	/*
            Date dateValue = DateFormat.stringToDate(strValue); // NOTE: forces GMT timezone
            long millis = dateValue.getTime();
            long days = millis / (1000 * 3600 * 24);
            return Integer.valueOf((int) days); // Optiq expects Integer instead of Long. by honma
            */
            return DateFormat.stringToDate(strValue);
        } else if ("tinyint".equals(dataType)) {
            return Byte.valueOf(strValue);
        } else if ("short".equals(dataType) || "smallint".equals(dataType)) {
            return Short.valueOf(strValue);
        } else if ("integer".equals(dataType)) {
            return Integer.valueOf(strValue);
        } else if ("long".equals(dataType) || "bigint".equals(dataType)) {
            return Long.valueOf(strValue);
        } else if ("double".equals(dataType)) {
            return Double.valueOf(strValue);
        } else if ("decimal".equals(dataType)) {
            return new BigDecimal(strValue);
        } else if ("timestamp".equals(dataType)) {
            return Long.valueOf(DateFormat.stringToMillis(strValue));
        } else if ("float".equals(dataType)) {
            return Float.valueOf(strValue);
        } else if ("boolean".equals(dataType)) {
            return Boolean.valueOf(strValue);
        } else {
            return strValue;
        }
    }
}

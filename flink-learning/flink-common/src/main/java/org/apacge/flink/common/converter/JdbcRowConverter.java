package org.apacge.flink.common.converter;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcRowConverter implements Serializable {

    public Row toInternal(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int arity = resultSetMetaData.getColumnCount();
        Row row = new Row(arity);
        for (int i = 0; i < arity; ++i) {
            row.setField(i, resultSet.getObject(i + 1));
        }
        return row;
    }

    PreparedStatement toExternal(Row row, PreparedStatement preparedStatement) {
        return null;
    }
}

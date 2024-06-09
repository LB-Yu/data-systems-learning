package org.apache.calcite.adapter.hbase;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.util.Sources;

import java.sql.*;
import java.util.Properties;

public class HBaseExample {

    public static void main(String[] args) throws Exception {
        String sql = "select b from test";

        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", Sources.of(HBaseExample.class.getResource("/model.json")).file().getAbsolutePath());
            info.put(CalciteConnectionProperty.UNQUOTED_CASING.toString(), Casing.UNCHANGED.toString());
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
                    Object v = resultSet.getObject(i);
                    sb.append(v).append(",");
                }
                sb.setLength(sb.length() - 1);
                System.out.println(sb);
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}

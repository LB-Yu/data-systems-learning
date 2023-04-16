package org.apache.calcite.adapter.hbase;

import org.apache.calcite.util.Sources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class HBaseExample {

    public static void main(String[] args) throws Exception {
        String sql = "select * from test";

        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", Sources.of(HBaseExample.class.getResource("/model.json")).file().getAbsolutePath());
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String a = resultSet.getString(1);
                String b = resultSet.getString(2);
                System.out.println(a + ", " + b);
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

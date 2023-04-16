package org.apache.calcite.avatica.example.simple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Client {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        Properties prop = new Properties();
        prop.put("serialization", "protobuf");
        try (Connection conn =
                     DriverManager.getConnection(
                             "jdbc:avatica:remote:url=http://localhost:5888",
                             prop)) {

            final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery("SELECT * FROM student_x");
            while (rs.next()) {
                System.out.println(rs.getInt(1));
            }
        }
    }
}

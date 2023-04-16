package org.apache.calcite.avatica.example.simple;

import java.sql.*;
import java.util.Properties;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.*;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.jdbc.*;

public class CustomAvaticaServer {

    public static void main(String[] args) throws Exception {
//        // Setup MySQL and PostgreSQL connections
//        Connection mysqlConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/my_database", "root", "password");
//        Connection postgresConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/my_database", "postgres", "password");
//
//        // Create Avatica JDBC drivers for MySQL and PostgreSQL
//        Driver mysqlDriver = new Driver();
//        Driver postgresDriver = new Driver();
//
//        // Create a factory that returns the appropriate JDBC driver based on the requested URL
//        AvaticaFactory factory = new AvaticaFactory() {
//            public ConnectionInfo parseConnectionUrl(String url, Properties info) throws SQLException {
//                if (url.contains("mysql")) {
//                    return new ConnectionInfo("jdbc:mysql://localhost:3306/my_database", info);
//                } else if (url.contains("postgresql")) {
//                    return new ConnectionInfo("jdbc:postgresql://localhost:5432/my_database", info);
//                } else {
//                    throw new SQLException("Unsupported database type: " + url);
//                }
//            }
//
//            public DriverVersion createDriverVersion() {
//                return new DriverVersion("1.0", "Custom Avatica Server");
//            }
//
//            public Meta createMeta(AvaticaConnection connection) {
//                return new JdbcMeta(connection);
//            }
//        };
//
//        // Create Avatica server configuration
//        ServerConfiguration configuration = new ServerConfigurationBuilder()
//                .withFactory(factory)
//                .withPort(8765)
//                .build();
//
//        // Create Avatica server and start it
//        Server server = new Server(configuration);
//        server.start();
//
//        // Register MySQL and PostgreSQL connections with the Avatica server
//        server.addConnection(mysqlDriver.connect("jdbc:avatica:remote:url=http://localhost:8765;serialization=protobuf", new Properties()));
//        server.addConnection(postgresDriver.connect("jdbc:avatica:remote:url=http://localhost:8765;serialization=protobuf", new Properties()));
//
//        // Wait for the server to stop
//        server.join();
//
//        // Close MySQL and PostgreSQL connections
//        mysqlConnection.close();
//        postgresConnection.close();
    }
}

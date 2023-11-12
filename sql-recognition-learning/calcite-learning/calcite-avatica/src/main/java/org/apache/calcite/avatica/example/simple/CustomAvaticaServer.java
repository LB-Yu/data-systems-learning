package org.apache.calcite.avatica.example.simple;

import java.sql.*;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.Driver;
import org.eclipse.jetty.server.Server;

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
//            @Override
//            public int getJdbcMajorVersion() {
//                return 0;
//            }
//
//            @Override
//            public int getJdbcMinorVersion() {
//                return 0;
//            }
//
//            @Override
//            public AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) throws SQLException {
//                return null;
//            }
//
//            @Override
//            public AvaticaStatement newStatement(AvaticaConnection connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
//                return null;
//            }
//
//            @Override
//            public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h, Meta.Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
//                return null;
//            }
//
//            @Override
//            public AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state, Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame) throws SQLException {
//                return null;
//            }
//
//            @Override
//            public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
//                return null;
//            }
//
//            @Override
//            public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, Meta.Signature signature) throws SQLException {
//                return null;
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

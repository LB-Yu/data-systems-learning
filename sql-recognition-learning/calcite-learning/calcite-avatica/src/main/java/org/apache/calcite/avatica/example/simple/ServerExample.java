package org.apache.calcite.avatica.example.simple;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HttpServer;

public class ServerExample {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://localhost:3306/test";
        final JdbcMeta meta = new JdbcMeta(url, "root", "0407");
        final Service service = new LocalService(meta);
        final HttpServer server = new HttpServer.Builder<>()
                .withPort(5888)
                .withHandler(service, Driver.Serialization.PROTOBUF)
                .build();
        server.start();
        server.join();
    }
}

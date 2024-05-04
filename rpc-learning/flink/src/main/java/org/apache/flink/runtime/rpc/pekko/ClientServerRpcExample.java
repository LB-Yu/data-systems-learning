package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Optional;

public class ClientServerRpcExample {

    private static RpcService rpcService1;
    private static RpcService rpcService2;

    public static void open() throws Exception {
        rpcService1 =
                PekkoRpcServiceUtils.createRemoteRpcService(
                        new Configuration(),
                        "localhost",
                        "0",
                        null,
                        Optional.empty());
        rpcService2 =
                PekkoRpcServiceUtils.createRemoteRpcService(
                        new Configuration(),
                        "localhost",
                        "0",
                        null,
                        Optional.empty());
    }

    public static void close() throws Exception {
        if (rpcService1 != null) {
            rpcService1.closeAsync().get();
        }
        if (rpcService2 != null) {
            rpcService2.closeAsync().get();
        }
    }

    public static void main(String[] args) throws Exception {
        open();

        HelloRpcEndpoint helloRpcEndpoint = new HelloRpcEndpoint(rpcService1);
        helloRpcEndpoint.start();

        HelloGateway helloGateway =
                rpcService2.connect(helloRpcEndpoint.getAddress(), HelloGateway.class).get();
        String result = helloGateway.hello();
        System.out.println(result);

        close();
    }
}

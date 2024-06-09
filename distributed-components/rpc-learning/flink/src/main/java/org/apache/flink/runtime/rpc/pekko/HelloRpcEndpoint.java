package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

public class HelloRpcEndpoint extends RpcEndpoint implements HelloGateway {

    public HelloRpcEndpoint(RpcService rpcService) {
        super(rpcService);
    }

    @Override
    public String hello() {
        return "Hello";
    }
}

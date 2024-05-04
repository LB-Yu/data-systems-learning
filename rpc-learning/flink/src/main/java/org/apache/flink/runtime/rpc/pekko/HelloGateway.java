package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.RpcGateway;

public interface HelloGateway extends RpcGateway {

    String hello();
}

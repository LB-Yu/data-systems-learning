package org.apache.zookeeper.learning.watcher;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class WatcherExample {

    public static void main(String[] args) throws Exception {
        String connectionString = "localhost:2181";
        ExponentialBackoffRetry retryPolicy =
                new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();

        String workerPath = "/test/listener/remoteNode";
        String subWorkerPath = "/test/listener/remoteNode/id-";

        Stat stat = client.checkExists().forPath(workerPath);
        if (stat == null) {
            client.create().creatingParentsIfNeeded().forPath(workerPath);
        }

        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("Received watched event: " + watchedEvent);
            }
        };
        byte[] content = client.getData().usingWatcher(watcher).forPath(workerPath);
        System.out.println("Content: " + new String(content));

        client.setData().forPath(workerPath, "1".getBytes());
        client.setData().forPath(workerPath, "2".getBytes());

        client.close();
    }
}

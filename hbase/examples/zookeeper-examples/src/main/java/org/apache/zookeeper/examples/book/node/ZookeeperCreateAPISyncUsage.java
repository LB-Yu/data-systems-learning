package org.apache.zookeeper.examples.book.node;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 */
public class ZookeeperCreateAPISyncUsage implements Watcher {
  private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    ZooKeeper zooKeeper = new ZooKeeper(
            "localhost:2181",
            5000,
            new ZookeeperCreateAPISyncUsage());
    connectedSemaphore.await();
    String path1 = zooKeeper.create(
            "/zk-test-ephemeral-",
            "".getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL);
    System.out.println("Success create znode: " + path1);

    String path2 = zooKeeper.create(
            "/zk-test-ephemeral-",
            "".getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
    System.out.println("Success create znode: " + path2);
  }

  public void process(WatchedEvent watchedEvent) {
    if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
      connectedSemaphore.countDown();
    }
  }
}

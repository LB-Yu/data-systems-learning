package org.apache.zookeeper.examples.book.session;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 */
public class ZookeeperConstructorUsageWithSidPasswd implements Watcher {
  private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

  public static void main(String[] args) throws IOException, InterruptedException {
    ZooKeeper zooKeeper = new ZooKeeper(
            "localhost:2181",
            5000,
            new ZookeeperConstructorUsageWithSidPasswd());
    connectedSemaphore.await();
    long sessionId = zooKeeper.getSessionId();
    byte[] passwd = zooKeeper.getSessionPasswd();

    // use illegal sessionId and sessionPasswd
    zooKeeper = new ZooKeeper(
            "localhost:2181",
            5000,
            new ZookeeperConstructorUsageWithSidPasswd(),
            1L,
            "test".getBytes());

    // use correct sessionId and sessionPasswd
    zooKeeper = new ZooKeeper(
            "localhost:2181",
            5000,
            new ZookeeperConstructorUsageWithSidPasswd(),
            sessionId,
            passwd);
    Thread.sleep(Integer.MAX_VALUE);
  }

  public void process(WatchedEvent watchedEvent) {
    System.out.println("Receive watched event: " + watchedEvent);
    if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
      connectedSemaphore.countDown();
    }
  }
}

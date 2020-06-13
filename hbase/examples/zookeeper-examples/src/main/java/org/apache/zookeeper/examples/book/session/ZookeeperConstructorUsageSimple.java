package org.apache.zookeeper.examples.book.session;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 */
public class ZookeeperConstructorUsageSimple implements Watcher {
  private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

  public static void main(String[] args) throws IOException {
    ZooKeeper zooKeeper = new ZooKeeper(
            "localhost:2181",
            5000,
            new ZookeeperConstructorUsageSimple());
    System.out.println(zooKeeper.getState());
    try {
      connectedSemaphore.await();
    } catch (InterruptedException e) { }
    System.out.println("Zookeeper session established.");
  }

  public void process(WatchedEvent watchedEvent) {
    System.out.println("Receive watch event: " + watchedEvent);
    if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
      connectedSemaphore.countDown();
    }
  }
}

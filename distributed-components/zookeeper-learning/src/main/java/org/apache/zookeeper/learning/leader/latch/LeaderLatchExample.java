package org.apache.zookeeper.learning.leader.latch;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LeaderLatchExample {

    private static final String PATH = "/examples/leader";

    private static final Integer CLIENT_COUNT = 5;

    public static void main(String[] args) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(CLIENT_COUNT);

        for (int i = 0; i < CLIENT_COUNT ; i++) {
            final int index = i;
            service.submit(() -> {
                try {
                    schedule(index);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        Thread.sleep(30 * 1000);
        service.shutdownNow();
    }

    private static void schedule(int thread) throws Exception {
        CuratorFramework client = getClient(thread);

        LeaderLatch latch = new LeaderLatch(client, PATH, String.valueOf(thread));

        latch.addListener(new LeaderLatchListener() {

            @Override
            public void notLeader() {
                System.out.println("Client [" + thread + "] I am the follower !");
            }

            @Override
            public void isLeader() {
                System.out.println("Client [" + thread + "] I am the leader !");
            }
        });

        latch.start();

        Thread.sleep(2 * (thread + 5) * 1000);

        latch.close(LeaderLatch.CloseMode.NOTIFY_LEADER);
        client.close();
        System.out.println("Client [" + latch.getId() + "] Server closed...");
    }

    private static CuratorFramework getClient(final int thread) {
        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(1000000)
                .connectionTimeoutMs(3000)
                .retryPolicy(rp)
                .build();
        client.start();
        System.out.println("Client [" + thread + "] Server connected...");
        return client;
    }
}

package com.niuzj.test;

import com.niuzj.util.CuratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Curator实现集群leader选举
 */
public class CuratorLeaderTest {

    private String path = "/zk-leader";

    @Test
    public void test01() throws InterruptedException {
        LeaderSelectorListener listener = new LeaderSelectorListener() {

            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                System.out.println(Thread.currentThread().getName() + "成为leader");
                Thread.sleep(5000L);
                System.out.println(Thread.currentThread().getName() + "退出leader");
            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            }

        };

        new Thread(() -> {
            registerListener(listener);
        }, "t1").start();

        new Thread(() -> {
            registerListener(listener);
        }, "t2").start();

        new Thread(() -> {
            registerListener(listener);
        }, "t3").start();

        Thread.sleep(Long.MAX_VALUE);

    }

    /**
     * 随机从候选着中选出一台作为leader，选中之后除非调用close()释放leadship，否则其他的后选择无法成为leader。其中spark使用的就是这种方法。
     * 参考:https://blog.csdn.net/wo541075754/article/details/70216046
     */
    @Test
    public void test02() {
        List<LeaderLatch> leaderLatches = new ArrayList<>();
        List<CuratorFramework> clients = new ArrayList<>();

        try {
            for (int i = 0; i < 10; i++) {
                CuratorFramework client = CuratorUtil.getNewClient();
                client.start();
                clients.add(client);
                final LeaderLatch latch = new LeaderLatch(client, path, String.valueOf(i));
                latch.addListener(new LeaderLatchListener() {

                    //成为leader时调用
                    @Override
                    public void isLeader() {
                        System.out.println(latch.getId() + " is leader");
                        //id小于5时放弃leader身份
                        if (Integer.parseInt(latch.getId()) < 5) {
                            try {
                                latch.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    @Override
                    public void notLeader() {
                        System.out.println(latch.getId() + " is not leader");

                    }
                });
                latch.start();
                leaderLatches.add(latch);
            }
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }

            for (LeaderLatch leaderLatch : leaderLatches) {
                CloseableUtils.closeQuietly(leaderLatch);
            }

        }


    }

    private void registerListener(LeaderSelectorListener listener) {
        CuratorFramework client = CuratorUtil.getNewClient();
        client.start();
        try {
            new EnsurePath(path).ensure(client.getZookeeperClient());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LeaderSelector selector = new LeaderSelector(client, path, listener);
        //设置
        selector.autoRequeue();
        selector.start();
    }
}

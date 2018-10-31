package com.niuzj.test;

import com.niuzj.util.CuratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Curator实现集群leader选举
 * 参考:https://blog.csdn.net/wo541075754/article/details/70216046
 */
public class CuratorLeaderTest {

    private String path = "/zk-leader";

    /**
     * leader latch方式
     * 随机从候选着中选出一台作为leader，选中之后除非调用close()释放leadship，否则其他的后选择无法成为leader。其中spark使用的就是这种方法。
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

    /**
     * leader election
     * 多个客户端轮流成为leader
     */
    @Test
    public void test03() {
        CountDownLatch lock = new CountDownLatch(1);
        List<LeaderSelector> selectors = new ArrayList<>();
        List<CuratorFramework> clients = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                final int id = i;
                CuratorFramework client = CuratorUtil.getNewClient();
                client.start();
                clients.add(client);
                LeaderSelector selector = new LeaderSelector(client, path, new LeaderSelectorListener() {

                    @Override
                    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                        Thread.sleep(3000L);
                        System.out.println(id + " is leader");
                    }

                    @Override
                    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                    }
                });
                selector.autoRequeue();
                selector.start();
                selectors.add(selector);
            }
            lock.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }

            for (LeaderSelector selector : selectors) {
                CloseableUtils.closeQuietly(selector);
            }
        }
    }
}

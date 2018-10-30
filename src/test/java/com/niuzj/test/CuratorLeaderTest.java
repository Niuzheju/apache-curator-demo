package com.niuzj.test;

import com.niuzj.util.CuratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.EnsurePath;
import org.junit.Test;

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

    private void registerListener(LeaderSelectorListener listener){
        CuratorFramework client = CuratorUtil.getNewClient();
        client.start();
        try {
            new EnsurePath(path).ensure(client.getZookeeperClient());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LeaderSelector selector = new LeaderSelector(client, path, listener);
        selector.autoRequeue();
        selector.start();
    }
}

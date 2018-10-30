package com.niuzj.test;

import com.niuzj.util.CuratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.Before;
import org.junit.Test;

import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 * curator分布式锁使用
 * 以一个节点作为锁
 */
public class CuratorLockTest {

    private String path = "/zk-test";

    private CuratorFramework client;

    @Before
    public void before() {
        client = CuratorUtil.getNewClient();
        client.start();
    }

    @Test
    public void test01() throws InterruptedException {
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                doWithLock();
            }
        }, "t1").start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                doWithLock();
            }
        }, "t2").start();

        Thread.sleep(Long.MAX_VALUE);

    }

    private void doWithLock() {
        InterProcessMutex lock = new InterProcessMutex(client, path);
        try {
            //获取锁
            if (lock.acquire(2000L, TimeUnit.MILLISECONDS)) {
                System.out.println(Thread.currentThread().getName() + "获取锁");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                //判断当前线程释放持有锁
                if (lock.isAcquiredInThisProcess()) {
                    System.out.println(Thread.currentThread().getName() + "释放锁");
                    //释放锁
                    lock.release();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }


}

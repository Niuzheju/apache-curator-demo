package com.niuzj.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Apache Curator测试
 * 参考博客https://www.cnblogs.com/seaspring/p/5536338.html
 */
public class CuratorTest {

    private String host = "192.168.70.80:2181";

    private CuratorFramework client;

    @Before
    public void before() {
        client = CuratorFrameworkFactory.newClient(host, new RetryNTimes(10, 5000));
        client.start();
    }

    //创建一个节点,持久化,如果父节点不存在创建父节点
    @Test
    public void test01() throws Exception {
        client.create().creatingParentsIfNeeded().forPath("/zyq/nzj", "nzj".getBytes());
    }

    //获取一个节点的所有子节点
    @Test
    public void test02() throws Exception {
        List<String> list = client.getChildren().forPath("/zyq");
        System.out.println(list);
    }

    //获取一个节点的数据
    @Test
    public void test03() throws Exception {
        byte[] bytes = client.getData().forPath("/zyq");
        System.out.println(new String(bytes));
    }

    //修改节点数据
    @Test
    public void test04() throws Exception {
        client.setData().forPath("/zyq", "zyq".getBytes());
        System.out.println(new String(client.getData().forPath("/zyq")));
    }

    //删除节点,如果存在子节点,删除所有子节点,然后删除该节点
    @Test
    public void test05() throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath("/zyq");
        //判断节点是否存在
        System.out.println(client.checkExists().forPath("/zyq"));
    }

    @Test
    public void test06() throws Exception {
        /*
         * 监视一个节点下1）孩子结点的创建 2）删除 3）以及结点数据的更新
         * 不包含节点自己的变化
         */
        PathChildrenCache watcher = new PathChildrenCache(client, "/zyq", true);
        watcher.getListenable().addListener((client, event) -> {
            ChildData data = event.getData();
            //节点path
            System.out.println(data.getPath());
            //发生改变操作的类型
            System.out.println(event.getType());
            //发生改变的节点的数据,如果是添加则是添加新节点的数据, 如果是删除则是删除节点的数据, 如果是更新数据, 则是更新后的数据
            System.out.println(new String(data.getData()));
        });
        watcher.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        Thread.sleep(Long.MAX_VALUE);

    }

}

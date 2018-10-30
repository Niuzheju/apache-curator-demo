package com.niuzj.test;

import com.niuzj.util.CuratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Apache Curator测试, 基本操作
 * 参考博客https://www.cnblogs.com/seaspring/p/5536338.html
 */
public class CuratorTest {

    private CuratorFramework client;

    @Before
    public void before() {
        client = CuratorUtil.getNewClient();
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
        client.delete().deletingChildrenIfNeeded().forPath("/zk-test");
        //判断节点是否存在
        System.out.println(client.checkExists().forPath("/zk-test"));
    }

    @Test
    public void test06() throws Exception {
        /*
         * 监视一个节点下1）孩子结点的创建 2）删除 3）以及结点数据的更新
         * 不包含节点自己的变化
         * cacheData:是否缓存数据到本地
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

    //监听一个节点的更新,删除,新建
    @Test
    public void test07() throws Exception {
        final String path = "/zyq";
        NodeCache watcher = new NodeCache(client, path);
        watcher.getListenable().addListener(() -> {
            ChildData data = watcher.getCurrentData();
            //为null则不存在
//            if (data == null){
//                System.out.println(path + "已不存在");
//                return;
//            }
//            System.out.println("data is " + new String(data.getData()));
//            System.out.println(data.getPath());
            //////////////////////////////////第二种判断方式////////////////////////////////////
            if (client.checkExists().forPath(path) == null){
                System.out.println(path + "已不存在");
                return;
            }
            System.out.println(new String(client.getData().forPath(path)));

        });
        watcher.start();
        Thread.sleep(Long.MAX_VALUE);
    }

    /*
        是NodeCache和PathChildrenCache的结合体
        既能监听该节点的更新, 删除, 创建
        又能监听该节点的子节点的更新, 删除, 创建
     */

    @Test
    public void test08() throws Exception {
        String path = "/zyq";
        TreeCache watcher = new TreeCache(client, path);
        watcher.getListenable().addListener((client, event) -> {
            ChildData data = event.getData();
            if (data == null){
                return;
            }
            TreeCacheEvent.Type type = event.getType();
            System.out.println(type);
            System.out.println(data.getPath());
            System.out.println(type.equals(TreeCacheEvent.Type.NODE_REMOVED) ? "" : new String(data.getData()));
        });
        watcher.start();
        Thread.sleep(Long.MAX_VALUE);
    }

}

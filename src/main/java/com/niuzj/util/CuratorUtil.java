package com.niuzj.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.ResourceBundle;

public final class CuratorUtil {

    private static CuratorFramework client;

    static{
        ResourceBundle bundle = ResourceBundle.getBundle("zookeeper");
        String host = bundle.getString("host");
        client = CuratorFrameworkFactory.newClient(host, new RetryNTimes(10, 5000));
    }

    public static CuratorFramework getNewClient(String host){
        if (host == null || "".equals(host)){
            return client;
        }
        return CuratorFrameworkFactory.newClient(host, new RetryNTimes(10, 5000));
    }

    public static CuratorFramework getNewClient(){
        return client;
    }
}

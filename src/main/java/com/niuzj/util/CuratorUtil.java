package com.niuzj.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.ResourceBundle;

public final class CuratorUtil {

    private static CuratorFramework client;

    static{
        client = newInstance();
    }

    public static CuratorFramework getClient(String host){
        if (host == null || "".equals(host)){
            return client;
        }
        return CuratorFrameworkFactory.newClient(host, new RetryNTimes(10, 1000));
    }

    public static CuratorFramework getClient(){
        return client;
    }

    public static CuratorFramework getNewClient(){
        return newInstance();
    }

    private static CuratorFramework newInstance(){
        ResourceBundle bundle = ResourceBundle.getBundle("zookeeper");
        String host = bundle.getString("host");
        client = CuratorFrameworkFactory.newClient(host, new RetryNTimes(10, 1000));
        return client;
    }
}

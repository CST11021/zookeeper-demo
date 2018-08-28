package com.whz.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @Author: wanghz
 * @Date: 2018/8/28 下午7:58
 */
public class ZkClientUtil {

    private static String connectString = "localhost:2181";

    /** 初始sleep时间 */
    private static int baseSleepTimeMs = 1000;

    /** 最大重试次数 */
    private static int maxRetries = 3;

    /** 会话超时时间 */
    private static int sessionTimeoutMs = 5000;

    /** 连接的超时时间 */
    private static int connectionTimeoutMs = 3000;

    /**
     * 使用curator创建一个ZooKeeper客户端
     */
    public static CuratorFramework createSession() {

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs,
                new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries));
        client.start();
        return client;
    }

    /**
     * 使用Fluent风格的API接口来创建一个ZooKeeper客户端
     */
    public static CuratorFramework createSessionByFluent() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                        .connectString(connectString)
                        .sessionTimeoutMs(sessionTimeoutMs)
                        .connectionTimeoutMs(connectionTimeoutMs)
                        .retryPolicy(new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries))
                        .build();
        client.start();
        return client;
    }

    /**
     * 使用curator来创建一个含隔离命名空间的ZooKeeper客户端
     * @param namespace 命名空间
     */
    public static CuratorFramework createSession(String namespace) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                        .connectString("localhost:2181")
                        .sessionTimeoutMs(5000)
                        .retryPolicy(retryPolicy)
                        .namespace(namespace)
                        .build();
        client.start();
        return client;
    }

    /**
     * 只能删除叶子节点
     */
    public static void deleteLeafNode(CuratorFramework client, String path) throws Exception {
        client.delete().forPath(path);
    }

    /**
     * 删除一个节点，并递归删除其所有子节点
     */
    public static void deleteChildrenNode(CuratorFramework client, String path) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }

    /**
     * 删除一个节点，强制指定版本进行删除
     */
    public static void deleteWithVersion(CuratorFramework client, String path, int version) throws Exception {
        client.delete().withVersion(version).forPath(path);
    }

    /**
     * 删除一个节点，强制保证删除。注意，guaranteed()接口是一个保障措施，只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到节
     * 点删除成功。
     *
     * guaranteed()方法：正如该接口的官方文档中所注明的，在ZooKeeper客户端使用过程中，可能会碰到这样的问题：客户端执行一个删除节点操作，
     * 但是由于一些网络原因，导致删除操作失败。对于这个异常，在有些场景中是致命的，如“Master选举”——在这个场景中，ZooKeeper客户端通常是通
     * 过节点的创建与删除来实现的。针对这个问题，Curator中引入了一种重试机制：如果我们调用了guaranteed()方法，那么当客户端碰到上面这些网
     * 络异常的时候，会记录下这次失败的删除操作，只要客户端会话有效，那么其就会在后台反复重试，直到节点删除成功。通过这样的措施，就可以保证
     * 节点删除操作一定会生效。
     */
    public static void deleteWithGuarantee(CuratorFramework client, String path) throws Exception {
        client.delete().guaranteed().forPath(path);
    }

}

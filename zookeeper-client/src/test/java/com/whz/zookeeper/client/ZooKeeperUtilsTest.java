package com.whz.zookeeper.client;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author: wanghz
 * @Date: 2020/1/9 10:19 AM
 */
public class ZooKeeperUtilsTest {

    @Test
    public void syncCreateZooKeeper() throws IOException, InterruptedException {
        ZooKeeper zooKeeper = ZooKeeperUtils.syncCreateZooKeeper();
        System.out.println();
    }

    @Test
    public void syncCreateZooKeeper2() throws IOException, InterruptedException {
        ZooKeeper zookeeper = ZooKeeperUtils.syncCreateZooKeeper();
        long sessionId = zookeeper.getSessionId();
        byte[] passwd  = zookeeper.getSessionPasswd();

        // 使用错误的 sessionId and sessionPassWd
        zookeeper = ZooKeeperUtils.syncCreateZooKeeper(1L, "test".getBytes());

        // 使用正确的 sessionId and sessionPassWd
        zookeeper = ZooKeeperUtils.syncCreateZooKeeper(sessionId, passwd);
        Thread.sleep(Integer.MAX_VALUE);
    }

}

package com.whz.zookeeper.curator.curatortest;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

import java.io.File;

/**
 * 为了便于开发人员进行Zookeeper的开发与测试，Curator提供了一种启动简易Zookeeper服务的方法——TestingServer。TestingServer允许开发人
 * 员非常方便地启动一个标准的Zookeeper服务器，并以此来进行一系列的单元测试。TestingServer在Curator的test包中
 */
public class TestingServer_Sample {
	static String path = "/zookeeper";
	public static void main(String[] args) throws Exception {
		TestingServer server = new TestingServer(2181,new File("/home/admin/zk-book-data"));
		
		CuratorFramework client = CuratorFrameworkFactory.builder()
	            .connectString(server.getConnectString())
	            .sessionTimeoutMs(5000)
	            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
	            .build();
        client.start();
        System.out.println( client.getChildren().forPath( path ));
        server.close();
	}
}
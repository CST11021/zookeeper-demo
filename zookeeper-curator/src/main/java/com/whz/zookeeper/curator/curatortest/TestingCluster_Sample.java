package com.whz.zookeeper.curator.curatortest;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;

/**
 * TestingCluster是一个可以模拟Zookeeper机器环境的Curator工具类，能够便于开发人员在本地模拟由n台机器组成的机器环境。下面我们通过模拟一
 * 个由3台机器组成的Zookeeper集群的场景来了解TestingCluster工具类的使用
 */
public class TestingCluster_Sample {

	public static void main(String[] args) throws Exception {
		TestingCluster cluster = new TestingCluster(3);
		cluster.start();
		Thread.sleep(2000);
		
		TestingZooKeeperServer leader = null;
		for(TestingZooKeeperServer zs : cluster.getServers()){
			System.out.print(zs.getInstanceSpec().getServerId()+"-");
			System.out.print(zs.getQuorumPeer().getServerState()+"-");  
			System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());
			if( zs.getQuorumPeer().getServerState().equals( "leading" )){
				leader = zs;
			}
		}
        leader.kill();
        System.out.println( "--After leader kill:" );
        for(TestingZooKeeperServer zs : cluster.getServers()){
			System.out.print(zs.getInstanceSpec().getServerId()+"-");
			System.out.print(zs.getQuorumPeer().getServerState()+"-");  
			System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());
		}
        cluster.stop();
	}
}
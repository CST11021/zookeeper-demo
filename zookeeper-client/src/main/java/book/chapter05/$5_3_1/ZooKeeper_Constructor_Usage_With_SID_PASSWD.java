package book.chapter05.$5_3_1;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * Java API -> 创建连接 -> 创建一个最基本的ZooKeeper对象实例，复用sessionId和session passwd
 *
 * 通过sessionId和sessionPasswd的目的是为了复用会话，以维持之前会话的有效性
 *
 * 从输出结果我们可以看到，第一次使用了错误的sessionId和sessionPasswd来创建ZooKeeper客户端的实例，结果客户端接收到了服务
 * 端的Expired事件通知；而第二次则使用正确的sessionId和sessionPasswd来创建客户端的实例，结果连接成功。
 *
 */
public class ZooKeeper_Constructor_Usage_With_SID_PASSWD implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws Exception{
        ZooKeeper zookeeper = new ZooKeeper(
                "localhost:2181",
				5000,
				new ZooKeeper_Constructor_Usage_With_SID_PASSWD());
        connectedSemaphore.await();
        long sessionId = zookeeper.getSessionId();
        byte[] passwd  = zookeeper.getSessionPasswd();
        
        // 使用错误的 sessionId and sessionPassWd
        zookeeper = new ZooKeeper("localhost:2181",
				5000,
				new ZooKeeper_Constructor_Usage_With_SID_PASSWD(),
				1L,
				"test".getBytes());

        // 使用正确的 sessionId and sessionPassWd
        zookeeper = new ZooKeeper("localhost:2181",
				5000,
				new ZooKeeper_Constructor_Usage_With_SID_PASSWD(),
				sessionId,
				passwd);
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event：" + event);
        if (KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        }
    }
}


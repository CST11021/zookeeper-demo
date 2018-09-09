import com.whz.zookeeper.curator.ZkClientUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: wanghz
 * @Date: 2018/9/8 下午1:35
 */
public class ZkClientUtilTest {

    public static CuratorFramework zkClient;

    @BeforeClass
    public static void init() {
        zkClient = ZkClientUtil.createSession();
    }

    @AfterClass
    public static void destory() {
        zkClient.close();
    }

    @Test
    public void createTest1() throws Exception {
        ZkClientUtil.create(zkClient, "/whz");
    }

    @Test
    public void createTest2() throws Exception {
        ZkClientUtil.create(zkClient, "/whz/test1", "测试数据".getBytes(), CreateMode.PERSISTENT);
        ZkClientUtil.create(zkClient, "/whz/ccc", "测试数据".getBytes(), CreateMode.PERSISTENT);
        ZkClientUtil.create(zkClient, "/whz/bbb", "测试数据".getBytes(), CreateMode.PERSISTENT);
        ZkClientUtil.create(zkClient, "/whz/aaa", "测试数据".getBytes(), CreateMode.PERSISTENT);

        ZkClientUtil.create(zkClient, "/whz/bbb-sequential", "测试数据".getBytes(), CreateMode.PERSISTENT_SEQUENTIAL);
        ZkClientUtil.create(zkClient, "/whz/ccc-sequential", "测试数据".getBytes(), CreateMode.PERSISTENT_SEQUENTIAL);
        ZkClientUtil.create(zkClient, "/whz/aaa-sequential", "测试数据".getBytes(), CreateMode.PERSISTENT_SEQUENTIAL);
    }

    @Test
    public void getChildrenTest() throws Exception {
        List<String> children = ZkClientUtil.getChildren(zkClient, "/whz");
        System.out.println(children);
    }

    @Test
    public void createTest3() throws Exception {
        ZkClientUtil.create(zkClient, "/whz/test2", "测试数据".getBytes(), CreateMode.EPHEMERAL);
    }

    @Test
    public void create() throws Exception {
        CountDownLatch semaphore = new CountDownLatch(2);
        ExecutorService tp = Executors.newFixedThreadPool(2);

        // 1、使用自定义的线程池服务
        ZkClientUtil.creatingInBackground(zkClient, "/whz/testBackground", "测试数据".getBytes(), CreateMode.EPHEMERAL,
                new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println("event[code: " + event.getResultCode() + ", type: " + event.getType() + "]");
                        System.out.println("Thread of processResult: " + Thread.currentThread().getName());
                        semaphore.countDown();
                    }
                }, tp);

        // 2、没有传入自定义的Executor，默认使用Zookeeper的EventThread来处理
        ZkClientUtil.creatingInBackground(zkClient, "/whz/testBackground", "测试数据".getBytes(), CreateMode.EPHEMERAL,
                new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println("event[code: " + event.getResultCode() + ", type: " + event.getType() + "]");
                        System.out.println("Thread of processResult: " + Thread.currentThread().getName());
                        semaphore.countDown();
                    }
                });

        semaphore.await();
    }

    @Test
    public void read1() throws Exception {
        byte[] data = ZkClientUtil.read(zkClient, "/whz");
        System.out.println(new String(data));
    }

    @Test
    public void read2() throws Exception {
        byte[] data = ZkClientUtil.read(zkClient, "/whz/test1");
        System.out.println(new String(data));
    }

}

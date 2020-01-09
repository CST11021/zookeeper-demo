package com.whz.zookeeper.client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Author: wanghz
 * @Date: 2020/1/9 10:04 AM
 */
public class ZooKeeperUtils {

    /**
     * 指Zookeeper服务器列表，由英文状态逗号分开的host:port字符串组成，每一个都代表一台Zookeeper机器，
     * 例如：192.168.1.1：2181,192.168.1.2：2181,192.168.1.3：2181
     */
    private static String connectString = "localhost:2181";

    /**
     * 会话超时时间，单位为毫秒，默认是60000ms（一分钟）
     */
    private static int sessionTimeoutMs = 5000;

    /**
     * 使用同步的方式创建zk，注意使用该方式如果连不上zk，可能无限阻塞
     *
     * @return
     * @throws IOException
     */
    public static ZooKeeper syncCreateZooKeeper() throws IOException, InterruptedException {

        // 使用闭锁的方式实现信号量功能
        CountDownLatch connectedSemaphore = new CountDownLatch(1);

        ZooKeeper zookeeper = new ZooKeeper(connectString, sessionTimeoutMs, new IWatcher(connectedSemaphore));

        // 线程进行等待，直到闭锁事件（即客户端成功连接上zk，并创建了session）发生
        connectedSemaphore.await();
        return zookeeper;
    }

    /**
     * 使用同步并且制定sessionId和passwd的方式创建zk，注意使用该方式如果连不上zk，可能无限阻塞
     *
     * @param sessionId     zk的SessionId
     * @param passwd        密码
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    public static ZooKeeper syncCreateZooKeeper(long sessionId, byte[] passwd) throws InterruptedException, IOException {

        CountDownLatch connectedSemaphore = new CountDownLatch(1);

        ZooKeeper zookeeper = new ZooKeeper(
                connectString,
                sessionTimeoutMs,
                new IWatcher(connectedSemaphore),
                sessionId,
                passwd);

        connectedSemaphore.await();
        return zookeeper;
    }

    /**
     * 同步创建一个节点，默认该节点无权限控制
     *
     * @param path          节点路径
     * @param data          节点数据
     * @param mode          节点类型，关于节点类型请参考{@link CreateMode}源码
     * @param zooKeeper     zk客户端
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String syncCreateNode(String path, byte[] data, CreateMode mode, ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        return syncCreateNode(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode, zooKeeper);
    }

    /**
     * 同步创建一个节点
     *
     * @param path          节点路径
     * @param data          节点数据
     * @param aclList       ACL权限列表
     * @param mode          节点类型，关于节点类型请参考{@link CreateMode}源码
     * @param zooKeeper     zk客户端
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String syncCreateNode(String path, byte[] data, List<ACL> aclList, CreateMode mode, ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        String nodePath = zooKeeper.create(path, data, aclList, mode);
        System.out.println("Success create znode: " + nodePath);
        return nodePath;
    }

    /**
     * 异步创建一个节点，默认该节点无权限控制
     *
     * @param path          节点路径
     * @param data          节点数据
     * @param mode          节点类型，关于节点类型请参考{@link CreateMode}源码
     * @param context       用于传递一个对象，可以在回调方法执行的时候使用，通常是放一个上下文信息
     * @param zooKeeper     zk客户端
     * @throws IOException
     * @throws InterruptedException
     */
    public static void asyncCreateNode(String path, byte[] data, CreateMode mode, Object context, ZooKeeper zooKeeper) throws IOException, InterruptedException {
        asyncCreateNode(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode, context, zooKeeper);
    }

    /**
     * 异步创建一个节点
     *
     * @param path          节点路径
     * @param data          节点数据
     * @param aclList       ACL权限列表
     * @param mode          节点类型，关于节点类型请参考{@link CreateMode}源码
     * @param context       用于传递一个对象，可以在回调方法执行的时候使用，通常是放一个上下文信息
     * @param zooKeeper     zk客户端
     * @throws IOException
     * @throws InterruptedException
     */
    public static void asyncCreateNode(String path, byte[] data, List<ACL> aclList, CreateMode mode, Object context, ZooKeeper zooKeeper) throws IOException, InterruptedException {
        zooKeeper.create(path, data, aclList, mode, new IStringCallback(), context);
    }

    /**
     * 同步删除节点
     *
     * @param path          节点路径
     * @param zooKeeper     zk客户端
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void syncDeleteNode(String path, ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        syncDeleteNode(path, -1, zooKeeper);
    }

    /**
     * 同步删除指定版本的节点
     *
     * @param path          节点路径
     * @param version       版本
     * @param zooKeeper     zk客户端
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void syncDeleteNode(String path, int version, ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        zooKeeper.delete(path, version);
    }

    /**
     * 通过异步回调的方式获取节点数据
     *
     * @param path              节点路径
     * @param dataCallback      回调接口
     * @param zooKeeper         zk客户端
     */
    public static void asyncGetData(String path, AsyncCallback.DataCallback dataCallback, ZooKeeper zooKeeper) {
        zooKeeper.getData(path, true, dataCallback, null);
    }

    /**
     * 使用同步的方式获取节点数据
     *
     * @param path              节点路径
     * @param zooKeeper         zk客户端
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static byte[] syncGetData(String path, ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        return zooKeeper.getData(path, true, new Stat());
    }

    /**
     * 使用闭锁的方式实现
     */
    static class IWatcher implements Watcher {

        private CountDownLatch connectedSemaphore;

        public IWatcher(CountDownLatch connectedSemaphore) {
            this.connectedSemaphore = connectedSemaphore;
        }

        /**
         * 在收到服务端发来的SyncConnected事件之后，解除主程序CountDownLatch上的等待阻塞。
         *
         * 注意，ZooKeeper客户端和服务端会话的建立是一个异步的过程，也就是说在程序中，构造方法会在处理完客户端初始化工作后立
         * 即返回，在大多数情况下，此时并没有真正建立好一个可用的会话，在会话的生命周期中处于“CONNECTION”的状态。
         *
         * 当该会话真正创建完毕后，ZooKeeper服务端会向会话对应的客户端发送一个事件通知，以告知客户端，客户端只有在获取这个通
         * 知之后，才算真正建立了会话。
         *
         * 该构造方法内部实现了与ZooKeeper服务器之间的TCP连接创建，负责维护客户端会话的生命周期。
         *
         * @param event
         */
        @Override
        public void process(WatchedEvent event) {
            System.out.println("receive watched event：" + event);
            if (Event.KeeperState.SyncConnected == event.getState()) {
                connectedSemaphore.countDown();
                System.out.println("create session success!");
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                System.out.println("node data changed, path = " + event.getPath());
            }
        }
    }

    /**
     * 使用异步接口创建节点：
     *
     *      使用异步方式创建时，用户需要实现AsynCallback.StringCallback()接口即可。
     *      AsynCallback包含：
     *          StatCallback、
     *          DataCallback、
     *          ACLCallback、
     *          ChildrenCallback、
     *          Children2Callback、
     *          StringCallback、
     *          VoidCallback
     *          七种不同的回调接口，用户可以在不同的异步接口中实现不同的接口。
     *
     *      和同步接口方法最大的区别在于，节点的创建过程（包括网络通信和服务端的节点创建过程）是异步的。并且，在同步接口调用过程中，
     *      我们需要关注接口抛出异常的可能；但是在异步接口中，接口本身是不会抛出异常的，所有的异常都会在回调函数中通过ResultCode（响应码）来体现。
     */
    static class IStringCallback implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int resultCode, String path, Object ctx, String name) {
            System.out.println("Create path result: [" + resultCode + ", " + path + ", " + ctx + ", real path name: " + name);
        }

    }

}

package com.whz.zookeeper.curator;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 *
 * 说明：此类只封装了一些比较常用的操作方法，仅作为学习参考，具体开发过程中推荐使用Fluent风格的客户端API框架进行编码会比较灵活
 *
 * @Author: wanghz
 * @Date: 2018/8/28 下午7:58
 */
public class ZkClientUtil {

    /**
     * 指Zookeeper服务器列表，由英文状态逗号分开的host:port字符串组成，每一个都代表一台Zookeeper机器，
     * 例如：192.168.1.1：2181,192.168.1.2：2181,192.168.1.3：2181
     */
    private static String connectString = "localhost:2181";

    /**
     * 初始sleep时间
     */
    private static int baseSleepTimeMs = 1000;

    /**
     * 最大重试次数
     */
    private static int maxRetries = 3;

    /**
     * 会话超时时间，单位为毫秒，默认是60000ms（一分钟）
     */
    private static int sessionTimeoutMs = 5000;

    /**
     * 连接的超时时间，单位为毫秒，默认是15000ms
     */
    private static int connectionTimeoutMs = 3000;

    /**
     * 重试策略。默认主要有四种实现，分别是：Exponential BackoffRetry、RetryNTimes、RetryOneTime、RetryUntilElapsed
     * 这里可以通过实现RetryPolicy接口来实现自定义的重试策略，在RetryPolicy接口中只定义了一个方法：
     * boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper);
     * <p>
     * 参数说明：
     * retryCount：已经重试的次数。如果是对此重试，那么该参数为0
     * elapsedTimeMs：从第一次重试开始已经花费的时间，单位为毫秒
     * sleeper：用于sleep指定时间。Curator建议不要使用Thread.sleep来进行sleep操作
     */
    private static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);


    // ----------------
    // 客户端创建
    // ----------------


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
     * 使用curator来创建一个含隔离命名空间的ZooKeeper客户端：为了实现不同的Zookeeper业务之间的隔离，往往会为每个业务分配一个独立的命名
     * 空间，即指定一个Zookeeper跟路径。如果指定了命名空间，该客户端对Zookeeper上数据节点的任何操作，都是基于该相对目录进行的
     *
     * @param namespace 命名空间
     */
    public static CuratorFramework createSession(String namespace) {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMs)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .build();
        client.start();
        return client;
    }

    // --------------------------------
    // 执行以下方法前，请先创建客户端
    // --------------------------------

    // 创建节点
    // 注意：由于在Zookeeper中规定了所有非叶子节点必须为持久节点，调用上面这个API之后，只有path参数对应的数据节点是临时节点，其父节点均为持久节点。

    /**
     * 创建一个持久节点，初始内容为空
     *
     * @param client Curator客户端
     * @param path   节点路径
     * @throws Exception
     */
    public static void create(CuratorFramework client, String path) throws Exception {
        if (client == null) {
            throw new Exception("client is null");
        }
        if (StringUtils.isBlank(path)) {
            throw new Exception("path is bank");
        }
        client.create().forPath(path);
    }

    /**
     * 创建一个持久节点，初始内容为空
     *
     * @param client Curator客户端
     * @param path   节点路径
     * @throws Exception
     */
    public static void create(CuratorFramework client, String path, byte[] data, CreateMode mode) throws Exception {
        if (client == null) {
            throw new Exception("client is null");
        }
        if (StringUtils.isBlank(path)) {
            throw new Exception("path is bank");
        }
        if (mode == null) {
            throw new Exception("mode is null");
        }
        client.create().withMode(mode).forPath(path, data);
    }

    /**
     * 在创建节点的时，可能会经常碰到NoNodeException异常，其中一个可能的原因就是试图对一个不存在的父节点创建子节点。因此，开发人员不得不
     * 在每次创建节点之前，都判断一下该父节点是否存在，在使用Curator的creatingParentsIfNeeded接口，Curator就能够自动地递归创建所有需
     * 要的父节点。
     *
     * @param client Curator客户端
     * @param path   节点路径
     * @param data   节点数据
     * @param mode   节点类型，默认为持久节点，一般有四种节点类型：持久节点、有序持久的节点、临时节点和有序的临时节点
     * @throws Exception
     */
    public static void creatingParentsIfNeeded(CuratorFramework client, String path, byte[] data, CreateMode mode) throws Exception {
        client.create().creatingParentsIfNeeded().withMode(mode).forPath(path, data);
    }

    /**
     * @param client Curator客户端
     * @param path   节点路径
     * @throws Exception
     */
    public static void creatingParentsIfNeeded(CuratorFramework client, String path) throws Exception {
        client.create().creatingParentsIfNeeded().forPath(path);
    }

    /**
     * 使用异步的方式创建节点
     *
     * BackgroundCallback接口：
     * BackgroundCallback接口只有一个processResult方法，该方法会再操作完成后被异步调用，CuratorEvent类是processResult()方法的一个
     * 入参，它定义了Zookeeper服务端发送到客户端的一系列事件参数，其中比较重要的有事件类型和响应码两个参数：
     *
     * 事件类型（CuratorEventType）：表示此次操作的操作类型，枚举值包括：CREATE,DELETE,EXISTS,GET_DATA,SET_DATA等
     * 响应码（int）：响应码用于标识事件的结果状态，所有响应码都被定义在 KeeperException#Code 类中，比较常见的响应码有：
     * 0（OK：接口调用成功）
     * -4（ConnectionLoss：客户端与服务端连接断开）
     * -110（NodeExists：指定节点已存在）
     * -112（SessionExpired：会话过期）等
     *
     * @param client
     * @param path
     * @param data
     * @param mode
     * @param callback
     * @throws Exception
     */
    public static void creatingInBackground(CuratorFramework client, String path, byte[] data, CreateMode mode, BackgroundCallback callback) throws Exception {
        client.create().creatingParentsIfNeeded().withMode(mode).inBackground(callback).forPath(path, data);
    }

    /**
     * executor入参说明：在Zookeeper中，所有异步通知事件处理都是由EventThread这个线程来处理的，EventThread线程用于串行处理所有的事件
     * 通知。EventThread的"串行处理机制"在绝大部分应用场景下能够保证对事件处理的顺序性，但这个特性也有其弊端，就是一旦碰上一个复杂的处理
     * 单元就会消耗过长的处理时间，从而影响对其他事件的处理。因此，inBackground接口允许用户传入一个Executor实例，这样就可以把那些比较复杂
     * 事件处理放到一个专门的线程池中去。
     *
     * @param client
     * @param path
     * @param data
     * @param mode
     * @param callback
     * @param executor
     * @throws Exception
     */
    public static void creatingInBackground(CuratorFramework client, String path, byte[] data, CreateMode mode, BackgroundCallback callback, Executor executor) throws Exception {
        client.create().creatingParentsIfNeeded().withMode(mode).inBackground(callback, executor).forPath(path, data);
    }

    // 读取节点

    /**
     * 读取节点的数据内容
     *
     * @param client
     * @param path
     * @return
     * @throws Exception
     */
    public static byte[] read(CuratorFramework client, String path) throws Exception {
        return client.getData().forPath(path);
    }

    /**
     * 读取节点的数据内容，同时获取到该节点的stat信息，通过传入一个旧的stat变量的方式来存储服务端返回的最新的节点状态信息
     *
     * @param client
     * @param path
     * @param stat
     * @return
     * @throws Exception
     */
    public static byte[] read(CuratorFramework client, String path, Stat stat) throws Exception {
        return client.getData().storingStatIn(stat).forPath(path);
    }

    // 更新节点

    /**
     *
     * 更新节点数据，该方法是非线程安全，推荐使用{@link #updateByCAS(CuratorFramework, String, byte[])}线程安全的方法
     *
     * @param client
     * @param path
     * @param data
     * @return 返回更新后的stat
     * @throws Exception
     */
    public static Stat update(CuratorFramework client, String path, byte[] data) throws Exception {
        return client.setData().forPath(path, data);
    }

    /**
     * 使用乐观锁的方式更新节点数据
     * @param client
     * @param path
     * @param data
     * @return 返回更新后的stat
     * @throws Exception
     */
    public static Stat updateByCAS(CuratorFramework client, String path, byte[] data) throws Exception {
        Stat stat = getCurrentStat(client, path);
        return update(client, path, data, stat.getVersion());
    }

    /**
     * 更新一个节点的数据内容，强制指定版本进行更新
     * 注意：withVersion接口就是用来实现CAS的，version通常从一个旧的stat对象中获取到的
     *
     * @param client
     * @param path
     * @param data
     * @param version
     * @return 返回更新后的stat
     * @throws Exception
     */
    public static Stat update(CuratorFramework client, String path, byte[] data, int version) throws Exception {
        return client.setData().withVersion(version).forPath(path, data);
    }

    // 删除节点

    /**
     * 删除一个节点，强制保证删除。注意，guaranteed()接口是一个保障措施，只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到节
     * 点删除成功。
     * <p>
     * guaranteed()方法：正如该接口的官方文档中所注明的，在ZooKeeper客户端使用过程中，可能会碰到这样的问题：客户端执行一个删除节点操作，
     * 但是由于一些网络原因，导致删除操作失败。对于这个异常，在有些场景中是致命的，如“Master选举”——在这个场景中，ZooKeeper客户端通常是通
     * 过节点的创建与删除来实现的。针对这个问题，Curator中引入了一种重试机制：如果我们调用了guaranteed()方法，那么当客户端碰到上面这些网
     * 络异常的时候，会记录下这次失败的删除操作，只要客户端会话有效，那么其就会在后台反复重试，直到节点删除成功。通过这样的措施，就可以保证
     * 节点删除操作一定会生效。
     *
     * @param client
     * @param path
     * @throws Exception
     */
    public static void deleteWithGuarantee(CuratorFramework client, String path) throws Exception {
        client.delete().guaranteed().forPath(path);
    }

    /**
     * 只能删除叶子节点
     *
     * @param client
     * @param path
     * @throws Exception
     */
    public static void deleteLeafNode(CuratorFramework client, String path) throws Exception {
        client.delete().forPath(path);
    }

    /**
     * 删除一个节点，并递归删除其所有子节点
     *
     * @param client
     * @param path
     * @throws Exception
     */
    public static void deletingChildrenIfNeeded(CuratorFramework client, String path) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }

    /**
     * 删除一个节点，强制指定版本进行删除
     *
     * @param client
     * @param path
     * @param version
     * @throws Exception
     */
    public static void deleteWithVersion(CuratorFramework client, String path, int version) throws Exception {
        client.delete().withVersion(version).forPath(path);
    }

    /**
     * 获取孩子节点
     *
     * @param client
     * @param path
     * @return
     * @throws Exception
     */
    public static List<String> getChildren(CuratorFramework client, String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    // 获取当前指定节点的Stat

    /**
     * 获取当前指定节点的Stat
     * @param client
     * @param path
     * @return
     * @throws Exception
     */
    public static Stat getCurrentStat(CuratorFramework client, String path) throws Exception {
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath(path);
        return stat;
    }



    // 节点事件监听

    /**
     * NodeCache不仅可以用于监听数据节点的内容变更，也能监听指定节点是否存在。如果原本节点不存在，那么Cache就会在节点被创建后触发
     * NodeCacheListener，但是，如果该数据节点被删除，那么Curator就无法触发NodeCacheListener监听的方法了
     * @param client        Curator客户端
     * @param path          要监听的节点路径
     * @param listener      监听器
     * @throws Exception
     */
    public static void setNodeListener(CuratorFramework client, String path, NodeListener listener) throws Exception {
        final NodeCache cache = new NodeCache(client,path,false);
        // 默认为false，这里设备为true时，NodeCache在第一次启动的时候就会立刻从Zookeeper上读取对应的节点的数据内容，并保存在Cache中
        cache.start(true);
        cache.getListenable().addListener(new NodeCacheListener() {

            @Override
            public void nodeChanged() throws Exception {
                listener.nodeChanged(cache);
            }
        });

    }

    /**
     *
     * @param client
     * @param path
     * @param listener
     * @throws Exception
     */
    public static void setChildrenListener(CuratorFramework client, String path, PathChildrenCacheListener listener) throws Exception {

        PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(listener);

    }

    /**
     * 监听子节点变更：当指定节点的子节点发生变化时，就会调用{@link PathChildrenCacheListener#childEvent(CuratorFramework, PathChildrenCacheEvent)}
     * 方法，{@link PathChildrenCacheEvent}中定义了所有的事件类型，主要包括新增子节点、子节点数据变更和子节点删除等
     *
     * 注意：
     * 1、该方法只监听指定节点的子节点，指定的节点本身的变更，并没有调用监听方法；
     * 2、和其他Zookeeper客户端一样，Curator也无法对二级子节点进行事件监听。也就说，如果使用{@link PathChildrenCache}对"/zk-book"
     * 进行监听，那么当"/zk-book/c1/c2"节点被创建或删除的时候，是无法触发子节点变更事件的。
     *
     * @param client
     * @param path
     * @param executor      通过线程池来异步处理事件通知
     * @param cacheData     用于配置是否把节点的内容数据缓存起来，如果配置为true，那么客户端在接收到节点列表变更的同时，也能获取到节点
     *                      的数据内容；如果配置为false，则无法获取到节点的数据内容
     * @param listener      监听接口
     * @throws Exception
     */
    public static void setChildrenListener(CuratorFramework client, String path, boolean cacheData, ExecutorService executor, PathChildrenCacheListener listener) throws Exception {

        PathChildrenCache cache = new PathChildrenCache(client, path, cacheData, false, executor);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(listener);

    }


    /**
     * 对Curator事件监听的封装
     */
    public abstract static class NodeListener implements NodeCacheListener {

        NodeCache cache;

        @Override
        public void nodeChanged() throws Exception {
            nodeChanged(cache);
        }

        public abstract void nodeChanged(NodeCache cache) throws Exception;

    }

}

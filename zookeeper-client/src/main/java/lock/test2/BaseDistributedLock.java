package lock.test2;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BaseDistributedLock {

    private static final Integer MAX_RETRY_COUNT = 10;

    private final ZkClient client;

    /** 所有锁节点的根节点 */
    private final String basePath;

    /** 表示锁节点的前缀 */
    private final String lockName;

    /** path = ${basePath}/${lockName} */
    private final String path;


    public BaseDistributedLock(ZkClient client, String path, String lockName) {
        this.client = client;
        this.basePath = path;
        this.lockName = lockName;
        this.path = path.concat("/").concat(lockName);
    }

    /**
     * 释放锁
     *
     * @param lockPath
     * @throws Exception
     */
    protected void releaseLock(String lockPath) throws Exception {
        deleteOurPath(lockPath);
    }

    /**
     * 请求获取一个锁，如果返回值不为空，说明成功获取了一个锁
     *
     * @param time
     * @param unit
     * @return
     * @throws Exception
     */
    protected String attemptLock(long time, TimeUnit unit) throws Exception {
        final long startMillis = System.currentTimeMillis();
        final Long millisToWait = (unit != null) ? unit.toMillis(time) : null;

        // 表示锁节点的路径
        String ourPath = null;
        // 表示是否获取到了锁
        boolean hasTheLock = false;
        // 表示当前尝试创建所节点的次数
        int retryCount = 0;

        // 网络闪断需要重试一试
        boolean isDone = false;
        while (!isDone) {
            isDone = true;

            try {
                // createLockNode用于在locker（basePath持久节点）下创建客户端要获取锁的[临时]顺序节点
                ourPath = createLockNode(client, path);
                /**
                 * 该方法用于判断自己是否获取到了锁，即自己创建的顺序节点在locker的所有子节点中是否最小
                 * 如果没有获取到锁，则等待其它客户端锁的释放，并且稍后重试直到获取到锁或者超时
                 */
                hasTheLock = waitToLock(startMillis, millisToWait, ourPath);

            } catch (ZkNoNodeException e) {
                // 如果创建失败了，则重试
                retryCount++;
                if (retryCount < MAX_RETRY_COUNT) {
                    isDone = false;
                } else {
                    throw e;
                }
            }
        }

        // 如果获取到了锁，就返回锁节点的路径
        if (hasTheLock) {
            return ourPath;
        }

        return null;
    }

    /**
     * 创建一个临时的顺序节点，该节点表示一个锁
     *
     * @param client
     * @param path
     * @return
     * @throws Exception
     */
    private String createLockNode(ZkClient client, String path) throws Exception {
        return client.createEphemeralSequential(path, null);
    }

    /**
     * 删除锁节点
     *
     * @param ourPath
     * @throws Exception
     */
    private void deleteOurPath(String ourPath) throws Exception {
        client.delete(ourPath);
    }

    /**
     * 获取锁的核心方法
     *
     * @param startMillis       表示开始要去获取锁的时间
     * @param millisToWait      表示客户端的获取锁超时时间
     * @param ourPath           表示锁节点的路径
     * @return
     * @throws Exception
     */
    private boolean waitToLock(long startMillis, Long millisToWait, String ourPath) throws Exception {

        // 表示是否已经获取到了锁
        boolean haveTheLock = false;
        boolean doDelete = false;

        try {
            while (!haveTheLock) {
                // 该方法实现获取locker节点下的所有顺序节点，并且从小到大排序
                List<String> children = getSortedChildren();
                // 获取所节点的节点名称
                String sequenceNodeName = ourPath.substring(basePath.length() + 1);

                // 计算刚才客户端创建的顺序节点在locker的所有子节点中排序位置，如果是排序为0，则表示获取到了锁
                int ourIndex = children.indexOf(sequenceNodeName);

                // 如果在getSortedChildren中没有找到之前创建的[临时]顺序节点，这表示可能由于网络闪断而导致
                // Zookeeper认为连接断开而删除了我们创建的节点，此时需要抛出异常，让上一级去处理上一级的做法是捕获该异常，并且执行重试指定的次数 见后面的 attemptLock方法
                if (ourIndex < 0) {
                    throw new ZkNoNodeException("节点没有找到: " + sequenceNodeName);
                }

                // 如果当前客户端创建的节点在locker子节点列表中位置大于0，表示其它客户端已经获取了锁，此时当前客户端需要等待其它客户端释放锁，
                boolean isGetTheLock = ourIndex == 0;
                if (isGetTheLock) {
                    haveTheLock = true;
                }

                // 如何判断其它客户端是否已经释放了锁？从子节点列表中获取到比自己次小的哪个节点，并对其建立监听，如果次小的节点被删除了，
                // 则表示当前客户端的节点应该是最小的了，所以使用CountDownLatch来实现等待
                final CountDownLatch latch = new CountDownLatch(1);
                String pathToWatch = isGetTheLock ? null : children.get(ourIndex - 1);
                String previousSequencePath = basePath.concat("/").concat(pathToWatch);
                final IZkDataListener previousListener = new IZkDataListener() {
                    //次小节点删除事件发生时，让countDownLatch结束等待, 此时还需要重新让程序回到while，重新判断一次！
                    @Override
                    public void handleDataDeleted(String dataPath) throws Exception {
                        latch.countDown();
                    }
                    @Override
                    public void handleDataChange(String dataPath, Object data) throws Exception {
                    }
                };

                try {
                    // 监听前一个顺序节点，如果节点不存在会出现异常
                    client.subscribeDataChanges(previousSequencePath, previousListener);

                    if (millisToWait != null) {
                        long currentTime = System.currentTimeMillis();
                        millisToWait -= currentTime - startMillis;
                        startMillis = currentTime;

                        // millisToWait <= 0 说明锁超时了
                        if (millisToWait <= 0) {
                            // timed out - delete our node
                            doDelete = true;
                            break;
                        }

                        latch.await(millisToWait, TimeUnit.MICROSECONDS);
                    } else {
                        latch.await();
                    }

                } catch (ZkNoNodeException e) {
                    //ignore
                } finally {
                    client.unsubscribeDataChanges(previousSequencePath, previousListener);
                }
            }

        } catch (Exception e) {
            //发生异常需要删除节点
            doDelete = true;
            throw e;
        } finally {
            // 如果需要删除节点
            if (doDelete) {
                deleteOurPath(ourPath);
            }
        }
        return haveTheLock;
    }

    /**
     * 获取所有的子节点，并从小到大排序
     *
     * @return
     * @throws Exception
     */
    private List<String> getSortedChildren() throws Exception {
        try {
            List<String> children = client.getChildren(basePath);
            Collections.sort(children, new Comparator<String>() {
                @Override
                public int compare(String lhs, String rhs) {
                    return getLockNodeNumber(lhs, lockName).compareTo(getLockNodeNumber(rhs, lockName));
                }
            });
            return children;
        } catch (ZkNoNodeException e) {
            client.createPersistent(basePath, true);
            return getSortedChildren();
        }
    }

    /**
     * 获取所节点的序号，例如，入参："lock/node0001", "node"
     * 返回：0001
     *
     * @param str
     * @param lockName
     * @return
     */
    private String getLockNodeNumber(String str, String lockName) {
        int index = str.lastIndexOf(lockName);
        if (index >= 0) {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }
}
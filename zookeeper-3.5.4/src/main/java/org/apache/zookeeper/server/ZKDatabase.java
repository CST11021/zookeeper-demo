/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.PlayBackListener;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * ZKDatabase是zk的内存数据库，负责管理zk的所有会话记录以及DataTree和事务日志的存储，
 * 这个类维护zookeeper服务器状态的内存数据库，包括会话、数据树和提交的日志，它是在从磁盘读取日志和快照之后启动的。
 */
public class ZKDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(ZKDatabase.class);

    /** 用于标记该ZKDatabase是否从事务日志重加载并初始化 */
    volatile private boolean initialized = false;

    public static final double DEFAULT_SNAPSHOT_SIZE_FACTOR = 0.33;
    /** 如果txnlog大小超过快照大小的1/3，则默认值为使用快照 */
    public static final String SNAPSHOT_SIZE_FACTOR = "zookeeper.snapshotSizeFactor";
    /** 对应{@link #SNAPSHOT_SIZE_FACTOR}配置的值，或是{@link #DEFAULT_SNAPSHOT_SIZE_FACTOR}默认的常量值 */
    private double snapshotSizeFactor;

    /** zk内存数据存储的核心，也是一个树形结构 */
    protected DataTree dataTree;
    /** Map<会话ID，会话过期时间> */
    protected ConcurrentHashMap<Long, Integer> sessionsWithTimeouts;

    /** 事务日志文件和快照数据文件 */
    protected FileTxnSnapLog snapLog;
    /** Leader中缓存队列commitedLog中最小Proposal的ZXID、maxCommittedLog：表示Leader缓存队列commitedLog中Proposal的最大ZXID */
    protected long minCommittedLog, maxCommittedLog;
    /** 对于committedLog中的提案的数量限制，当提案大于500时，会移除最小的一个提案 */
    public static final int commitLogCount = 500;
    protected static int commitLogBuffer = 700;
    /** 每次DataTree做一次操作后(添加、更新、删除节点等操作)，提交一个Proposal保存到该集合中，用于后续同步给follower节点 */
    protected LinkedList<Proposal> committedLog = new LinkedList<Proposal>();
    /** 用于控制{@link #committedLog}的读写锁，读操作的锁叫共享锁，写操作的锁叫排他锁。就是遇见写锁就需互斥。那么以此可得出读读共享，写写互斥，读写互斥，写读互斥 */
    protected ReentrantReadWriteLock logLock = new ReentrantReadWriteLock();
    /**
     * PlayBackListener监听器主要用来接收事务应用过程中的回调。在ZooKeeper数据恢复后期，会有一个事务订正的过程，在这个过程中，会回调PlayBackListener监听器来进行对应的数据订正。
     */
    private final PlayBackListener commitProposalPlaybackListener = new PlayBackListener() {
        public void onTxnLoaded(TxnHeader hdr, Record txn){
            //  添加一个事务请求
            addCommittedProposal(hdr, txn);
        }
    };

    /**
     * 这个zk数据库对应一个FileTxnSnapLog，FileTxnSnapLog 和 ZKDatabase 之间存在一对一的关系
     *
     * @param snapLog ZKDatabase对应的
     */
    public ZKDatabase(FileTxnSnapLog snapLog) {
        dataTree = new DataTree();
        sessionsWithTimeouts = new ConcurrentHashMap<Long, Integer>();
        this.snapLog = snapLog;

        try {
            snapshotSizeFactor = Double.parseDouble(System.getProperty(SNAPSHOT_SIZE_FACTOR, Double.toString(DEFAULT_SNAPSHOT_SIZE_FACTOR)));
            if (snapshotSizeFactor > 1) {
                snapshotSizeFactor = DEFAULT_SNAPSHOT_SIZE_FACTOR;
                LOG.warn("The configured {} is invalid, going to use the default {}", SNAPSHOT_SIZE_FACTOR, DEFAULT_SNAPSHOT_SIZE_FACTOR);
            }
        } catch (NumberFormatException e) {
            LOG.error("Error parsing {}, using default value {}", SNAPSHOT_SIZE_FACTOR, DEFAULT_SNAPSHOT_SIZE_FACTOR);
            snapshotSizeFactor = DEFAULT_SNAPSHOT_SIZE_FACTOR;
        }
        LOG.info("{} = {}", SNAPSHOT_SIZE_FACTOR, snapshotSizeFactor);
    }

    /**
     * 将数据从磁盘加载到内存中，并将事物添加到内存中的提交日志中
     *
     * @return 返回磁盘上最后一个有效的zxid
     * @throws IOException
     */
    public long loadDataBase() throws IOException {
        long zxid = snapLog.restore(dataTree, sessionsWithTimeouts, commitProposalPlaybackListener);
        initialized = true;
        return zxid;
    }


    // 事务日志相关操作

    /**
     * 每次DataTree做一次操作后(添加、更新、删除节点等操作)，提交一个Proposal保存到{@link #committedLog}集合中，
     * 后续同步给follower节点时，会调用该方法，获取相关的操作
     *
     * @return
     */
    public synchronized List<Proposal> getCommittedLog() {
        ReadLock rl = logLock.readLock();
        // 如果这个线程还没有锁，就复制一个committedLog，getReadHoldCount()方法：查询当前线程在此锁上保持的重入读取锁数量
        if(logLock.getReadHoldCount() <=0) {
            try {
                rl.lock();
                return new LinkedList<Proposal>(this.committedLog);
            } finally {
                rl.unlock();
            }
        }
        return this.committedLog;
    }
    /**
     * DataTree做一次操作后(添加、更新、删除节点等操作)，都会调用该方法，提交一个Proposal，用于后续同步给follower节点
     *
     * @param hdr   事务头
     * @param txn   请求提
     */
    private void addCommittedProposal(TxnHeader hdr, Record txn) {
        Request r = new Request(0, hdr.getCxid(), hdr.getType(), hdr, txn, hdr.getZxid());
        addCommittedProposal(r);
    }
    /**
     * DataTree做一次操作后(添加、更新、删除节点等操作)，都会调用该方法，提交一个Proposal，用于后续同步给follower节点
     *
     * @param request committed request
     */
    public void addCommittedProposal(Request request) {
        // 获取一些写锁，防止并发写操作
        WriteLock wl = logLock.writeLock();
        try {
            wl.lock();
            // 当Proposal数量 > 500时，移除第一个提案，并记录内存中最小的事务ID
            if (committedLog.size() > commitLogCount) {
                committedLog.removeFirst();
                minCommittedLog = committedLog.getFirst().packet.getZxid();
            }

            if (committedLog.isEmpty()) {
                minCommittedLog = request.zxid;
                maxCommittedLog = request.zxid;
            }

            // 往committedLog添加一个Proposal
            byte[] data = SerializeUtils.serializeRequest(request);
            QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);
            Proposal p = new Proposal();
            p.packet = pp;
            p.request = request;
            committedLog.add(p);

            maxCommittedLog = p.packet.getZxid();
        } finally {
            wl.unlock();
        }
    }
    /**
     * 将事务日志恢复到内存中
     *
     * @return 返回事务日志中，最后一个有效的zxid（即最大一个事务ID）
     * @throws IOException
     */
    public long fastForwardDataBase() throws IOException {
        long zxid = snapLog.fastForwardFromEdits(dataTree, sessionsWithTimeouts, commitProposalPlaybackListener);
        initialized = true;
        return zxid;
    }
    /**
     * snapshotSizeFactor >= 0时返回ture
     *
     * @return
     */
    public boolean isTxnLogSyncEnabled() {
        boolean enabled = snapshotSizeFactor >= 0;
        if (enabled) {
            LOG.info("On disk txn sync enabled with snapshotSizeFactor " + snapshotSizeFactor);
        } else {
            LOG.info("On disk txn sync disabled");
        }
        return enabled;
    }

    /**
     * 计算最新的快照文件大小
     * @return
     */
    public long calculateTxnLogSizeLimit() {
        long snapSize = 0;
        try {
            // 快照目录中的最新快照
            File snapFile = snapLog.findMostRecentSnapshot();
            if (snapFile != null) {
                snapSize = snapFile.length();
            }
        } catch (IOException e) {
            LOG.error("Unable to get size of most recent snapshot");
        }
        return (long) (snapSize * snapshotSizeFactor);
    }
    /**
     * Get proposals from txnlog. Only packet part of proposal is populated.
     *
     * @param startZxid the starting zxid of the proposal
     * @param sizeLimit maximum on-disk size of txnlog to fetch 0 is unlimited, negative value means disable.
     * @return list of proposal (request part of each proposal is null)
     */
    public Iterator<Proposal> getProposalsFromTxnLog(long startZxid, long sizeLimit) {
        if (sizeLimit < 0) {
            LOG.debug("Negative size limit - retrieving proposal via txnlog is disabled");
            return TxnLogProposalIterator.EMPTY_ITERATOR;
        }

        TxnIterator itr = null;
        try {

            // 开始从给定的zxid读取之后的所有事务
            itr = snapLog.readTxnLog(startZxid, false);

            // If we cannot guarantee that this is strictly the starting txn after a given zxid, we should fail.
            if ((itr.getHeader() != null) && (itr.getHeader().getZxid() > startZxid)) {
                LOG.warn("Unable to find proposals from txnlog for zxid: " + startZxid);
                itr.close();
                return TxnLogProposalIterator.EMPTY_ITERATOR;
            }

            if (sizeLimit > 0) {
                long txnSize = itr.getStorageSize();
                if (txnSize > sizeLimit) {
                    LOG.info("Txnlog size: " + txnSize + " exceeds sizeLimit: " + sizeLimit);
                    itr.close();
                    return TxnLogProposalIterator.EMPTY_ITERATOR;
                }
            }
        } catch (IOException e) {
            LOG.error("Unable to read txnlog from disk", e);
            try {
                if (itr != null) {
                    itr.close();
                }
            } catch (IOException ioe) {
                LOG.warn("Error closing file iterator", ioe);
            }
            return TxnLogProposalIterator.EMPTY_ITERATOR;
        }
        return new TxnLogProposalIterator(itr);
    }
    /**
     * 截断该事务ID之后的事务日志，即将大于该zxid的事务日志都删除掉
     *
     * @param zxid the zxid to truncate zk database to
     * @return 如果能够截断日志，则返回true，否则false
     * @throws IOException
     */
    public boolean truncateLog(long zxid) throws IOException {
        clear();
        // truncate the log
        boolean truncated = snapLog.truncateLog(zxid);
        if (!truncated) {
            return false;
        }
        loadDataBase();
        return true;
    }
    /**
     * 追加请求到事务日志中
     *
     * @param si the request to append
     * @return true if the append was succesfull and false if not
     */
    public boolean append(Request si) throws IOException {
        return this.snapLog.append(si);
    }
    /**
     * 回滚日志事务
     */
    public void rollLog() throws IOException {
        this.snapLog.rollLog();
    }
    /**
     * commit to the underlying transaction log
     * @throws IOException
     */
    public void commit() throws IOException {
        this.snapLog.commit();
    }
    /**
     * close this database. free the resources
     * @throws IOException
     */
    public void close() throws IOException {
        this.snapLog.close();
    }
    /**
     * 清除zkdatabase。
     * 开发人员请注意，清除方法清除了zkdatabase中的所有数据结构。
     */
    public void clear() {
        minCommittedLog = 0;
        maxCommittedLog = 0;
        /* to be safe we just create a new
         * datatree.
         */
        dataTree = new DataTree();
        sessionsWithTimeouts.clear();
        WriteLock lock = logLock.writeLock();
        try {
            lock.lock();
            committedLog.clear();
        } finally {
            lock.unlock();
        }
        initialized = false;
    }



    // 直接委托给dataTree的相关方法

    /**
     * 初始化dataTree中"/zookeeper/config"的节点信息
     *
     * @param qv
     */
    public synchronized void initConfigInZKDatabase(QuorumVerifier qv) {
        // only happens during tests
        if (qv == null) return;

        try {
            if (this.dataTree.getNode(ZooDefs.CONFIG_NODE) == null) {
                // should only happen during upgrade
                LOG.warn("configuration znode missing (should only happen during upgrade), creating the node");
                this.dataTree.addConfigNode();
            }
            this.dataTree.setData(ZooDefs.CONFIG_NODE, qv.toString().getBytes(), -1, qv.getVersion(), Time.currentWallTime());
        } catch (NoNodeException e) {
            System.out.println("configuration node missing - should not happen");
        }
    }

    /**
     * 将ia反序列化为DataTree
     *
     * @param ia the input archive you want to deserialize from
     * @throws IOException
     */
    public void deserializeSnapshot(InputArchive ia) throws IOException {
        clear();
        SerializeUtils.deserializeSnapshot(getDataTree(), ia, getSessionWithTimeOuts());
        initialized = true;
    }
    /**
     * 将datatree序列化输出
     *
     * @param oa the output archive to which the snapshot needs to be serialized
     * @throws IOException
     * @throws InterruptedException
     */
    public void serializeSnapshot(OutputArchive oa) throws IOException, InterruptedException {
        SerializeUtils.serializeSnapshot(getDataTree(), oa, getSessionWithTimeOuts());
    }
    /**
     * 从数据树获取最后一个处理过的事务ID
     *
     * @return the last processed zxid of a datatree
     */
    public long getDataTreeLastProcessedZxid() {
        return dataTree.lastProcessedZxid;
    }
    /**
     * 返回dataTree中的所有session
     *
     * @return the data tree sessions
     */
    public Collection<Long> getSessions() {
        return dataTree.getSessions();
    }
    /**
     * 获取节点的权限
     *
     * @param n
     * @return
     */
    public List<ACL> aclForNode(DataNode n) {
        return dataTree.getACL(n);
    }
    /**
     * 从dataTree移除一个ServerCnxn的连接实例
     *
     * @param cnxn the cnxn to remove from the datatree
     */
    public void removeCnxn(ServerCnxn cnxn) {
        dataTree.removeCnxn(cnxn);
    }
    /**
     * kill一个session
     *
     * @param sessionId     会话ID
     * @param zxid          kill该会话的事务ID
     */
    public void killSession(long sessionId, long zxid) {
        dataTree.killSession(sessionId, zxid);
    }
    /**
     * dump出DataTree中所有的临时节点
     *
     * write a text dump of all the ephemerals in the datatree
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        dataTree.dumpEphemerals(pwriter);
    }
    /**
     * 返回所有的临时节点Map<sesseionId, 临时节点集合>
     *
     * @return
     */
    public Map<Long, Set<String>> getEphemerals() {
        return dataTree.getEphemerals();
    }
    /**
     * 统计DataTree中的节点数量
     *
     * @return the node count of datatree
     */
    public int getNodeCount() {
        return dataTree.getNodeCount();
    }
    /**
     * 根据sessionId获取临时节点
     *
     * @param sessionId the session id for which paths match to
     * @return the paths for a session id
     */
    public Set<String> getEphemerals(long sessionId) {
        return dataTree.getEphemerals(sessionId);
    }
    /**
     * 设置操作dataTree的最后一个事务ID
     *
     * @param zxid the last processed zxid in the datatree
     */
    public void setlastProcessedZxid(long zxid) {
        dataTree.lastProcessedZxid = zxid;
    }
    /**
     * 处理操作DataTree的请求，比如查询节点数据，添加/删除节点等操作
     *
     * @param hdr the txnheader for the txn
     * @param txn the transaction that needs to be processed
     * @return the result of processing the transaction on this
     * datatree/zkdatabase
     */
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
        return dataTree.processTxn(hdr, txn);
    }
    /**
     * 获取节点的Stat信息
     *
     * @param path the path for which stat is to be done
     * @param serverCnxn the servercnxn attached to this request
     * @return the stat of this node
     * @throws KeeperException.NoNodeException
     */
    public Stat statNode(String path, ServerCnxn serverCnxn) throws KeeperException.NoNodeException {
        return dataTree.statNode(path, serverCnxn);
    }
    /**
     * 根据路径获取一个数据节点
     *
     * @param path the path to lookup
     * @return the datanode for getting the path
     */
    public DataNode getNode(String path) {
      return dataTree.getNode(path);
    }
    /**
     * 获取指定节点的数据
     *
     * @param path the path being queried
     * @param stat the stat for this path
     * @param watcher the watcher function
     * @return
     * @throws KeeperException.NoNodeException
     */
    public byte[] getData(String path, Stat stat, Watcher watcher) throws KeeperException.NoNodeException {
        return dataTree.getData(path, stat, watcher);
    }
    /**
     * 设置watcher监听
     *
     * @param relativeZxid the relative zxid that client has seen
     * @param dataWatches the data watches the client wants to reset
     * @param existWatches the exists watches the client wants to reset
     * @param childWatches the child watches the client wants to reset
     * @param watcher the watcher function
     */
    public void setWatches(long relativeZxid, List<String> dataWatches, List<String> existWatches, List<String> childWatches, Watcher watcher) {
        dataTree.setWatches(relativeZxid, dataWatches, existWatches, childWatches, watcher);
    }
    /**
     * 获取指定节点的权限
     *
     * @param path the path to query for acl
     * @param stat the stat for the node
     * @return the acl list for this path
     * @throws NoNodeException
     */
    public List<ACL> getACL(String path, Stat stat) throws NoNodeException {
        return dataTree.getACL(path, stat);
    }
    /**
     * 获取孩子节点
     *
     * @param path the path of the node
     * @param stat the stat of the node
     * @param watcher the watcher function for this path
     * @return the list of children for this path
     * @throws KeeperException.NoNodeException
     */
    public List<String> getChildren(String path, Stat stat, Watcher watcher) throws KeeperException.NoNodeException {
        return dataTree.getChildren(path, stat, watcher);
    }
    /**
     * 判断是否为zk管理员所拥有的特殊节点
     *
     * @param path the input path
     * @return true if path is special and false if not
     */
    public boolean isSpecialPath(String path) {
        return dataTree.isSpecialPath(path);
    }
    /**
     * 获取有设置权限的节点数量
     *
     * @return the acl size of the datatree
     */
    public int getAclSize() {
        return dataTree.aclCacheSize();
    }
    /**
     * 判断节点的监听是否存在
     *
     * @param path      node to check watcher existence
     * @param type      type of watcher
     * @param watcher   watcher function
     */
    public boolean containsWatcher(String path, WatcherType type, Watcher watcher) {
        return dataTree.containsWatcher(path, type, watcher);
    }
    /**
     * 移除监听
     *
     * @param path node to remove watches from
     * @param type type of watcher to remove
     * @param watcher watcher function to remove
     */
    public boolean removeWatch(String path, WatcherType type, Watcher watcher) {
        return dataTree.removeWatch(path, type, watcher);
    }



    // getter ...

    /**
     * checks to see if the zk database has been
     * initialized or not.
     * @return true if zk database is initialized and false if not
     */
    public boolean isInitialized() {
        return initialized;
    }
    /**
     * the datatree for this zkdatabase
     * @return the datatree for this zkdatabase
     */
    public DataTree getDataTree() {
        return this.dataTree;
    }
    /**
     * the committed log for this zk database
     * @return the committed log for this zkdatabase
     */
    public long getmaxCommittedLog() {
        return maxCommittedLog;
    }
    /**
     * the minimum committed transaction log
     * available in memory
     * @return the minimum committed transaction
     * log available in memory
     */
    public long getminCommittedLog() {
        return minCommittedLog;
    }
    /**
     * Get the lock that controls the committedLog. If you want to get the pointer to the committedLog, you need
     * to use this lock to acquire a read lock before calling getCommittedLog()
     * @return the lock that controls the committed log
     */
    public ReentrantReadWriteLock getLogLock() {
        return logLock;
    }
    public ConcurrentHashMap<Long, Integer> getSessionWithTimeOuts() {
        return sessionsWithTimeouts;
    }
    /**
     * Use for unit testing, so we can turn this feature on/off
     * @param snapshotSizeFactor Set to minus value to turn this off.
     */
    public void setSnapshotSizeFactor(double snapshotSizeFactor) {
        this.snapshotSizeFactor = snapshotSizeFactor;
    }


}

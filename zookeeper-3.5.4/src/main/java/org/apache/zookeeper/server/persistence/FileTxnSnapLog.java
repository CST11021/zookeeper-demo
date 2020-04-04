/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.persistence;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FileTxnSnapLog是Zookeeper上层服务器和底层数据存储之间的对接层，提供了一系列操作数据文件的接口，如事务日志文件和快照数据文件。
 * Zookeeper根据zoo.cfg文件中解析出的快照数据目录dataDir和事务日志目录dataLogDir来创建FileTxnSnapLog。
 *
 * 实际对节点进行操作后会通过该类将数据保存到文件中
 */
public class FileTxnSnapLog {

    private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);

    public final static int VERSION = 2;

    public final static String version = "version-";
    /** 表示是否自动创建dataDir目录，默认为true，否则zk启动前需要事先创建的事务日志文件目录，否则抛出{@link DatadirException}异常 */
    public static final String ZOOKEEPER_DATADIR_AUTOCREATE = "zookeeper.datadir.autocreate";
    /** 表示{@link #ZOOKEEPER_DATADIR_AUTOCREATE}的默认值 */
    public static final String ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT = "true";

    /** 包含事务日志的direcotry */
    private final File dataDir;
    /** 包含快照目录的目录 */
    private final File snapDir;
    /** 表示zk事务日志文件，对应{@link #dataDir}目录 */
    private TxnLog txnLog;
    /** 表示zk服务器的内存储快照文件，对应{@link #snapDir}目录 */
    private SnapShot snapLog;

    /**
     * 构造器需要指定datadir 和 snapdir
     *
     * @param dataDir 事务日志文件的目录
     * @param snapDir zk服务器的内存储快照文件的目录
     */
    public FileTxnSnapLog(File dataDir, File snapDir) throws IOException {
        LOG.debug("Opening datadir:{} snapDir:{}", dataDir, snapDir);

        // 事务日志文件：${dataDir}/version-2
        this.dataDir = new File(dataDir, version + VERSION);
        // 快照文件：${snapDir}/version-2
        this.snapDir = new File(snapDir, version + VERSION);

        // by default create snap/log dirs, but otherwise complain instead See ZOOKEEPER-1161 for more details
        boolean enableAutocreate = Boolean.valueOf(System.getProperty(ZOOKEEPER_DATADIR_AUTOCREATE, ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT));



        // 检查是否存在dataDir目录，不存在则创建一个目录
        if (!this.dataDir.exists()) {
            if (!enableAutocreate) {
                throw new DatadirException("Missing data directory " + this.dataDir
                        + ", automatic data directory creation is disabled (" + ZOOKEEPER_DATADIR_AUTOCREATE
                        + " is false). Please create this directory manually.");
            }

            if (!this.dataDir.mkdirs()) {
                throw new DatadirException("Unable to create data directory " + this.dataDir);
            }
        }
        // 检查dataDir是否可写
        if (!this.dataDir.canWrite()) {
            throw new DatadirException("Cannot write to data directory " + this.dataDir);
        }



        // 检查是否存在 snapDir 目录，不存在则创建一个目录
        if (!this.snapDir.exists()) {
            // by default create this directory, but otherwise complain instead
            // See ZOOKEEPER-1161 for more details
            if (!enableAutocreate) {
                throw new DatadirException("Missing snap directory " + this.snapDir
                        + ", automatic data directory creation is disabled (" + ZOOKEEPER_DATADIR_AUTOCREATE
                        + " is false). Please create this directory manually.");
            }

            if (!this.snapDir.mkdirs()) {
                throw new DatadirException("Unable to create snap directory " + this.snapDir);
            }
        }
        // 检查 snapDir 是否可写
        if (!this.snapDir.canWrite()) {
            throw new DatadirException("Cannot write to snap directory " + this.snapDir);
        }

        // 如果是两个不同的目录，请检查事务日志和快照目录的内容
        if (!this.dataDir.getPath().equals(this.snapDir.getPath())) {
            // 检查事务日志目录，看看服务启动前是否清理历史快照和log文件，如果没清理则报错
            checkLogDir();
            // 检查快照目录，看看服务启动前是否清理历史快照文件，如果没清理则报错
            checkSnapDir();
        }

        txnLog = new FileTxnLog(this.dataDir);
        snapLog = new FileSnap(this.snapDir);
    }

    /**
     * 检查事务日志目录，看看服务启动前是否清理历史快照和log文件，如果没清理则报错
     *
     * @throws LogDirContentCheckException
     */
    private void checkLogDir() throws LogDirContentCheckException {

        // 过滤出所有"snapshot."开头的文件
        File[] files = this.dataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isSnapshotFileName(name);
            }
        });


        if (files != null && files.length > 0) {
            // 日志目录有快照文件。检查dataLogDir和dataDir配置是否正确。
            throw new LogDirContentCheckException("Log directory has snapshot files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    /**
     * 检查快照目录，看看服务启动前是否清理历史快照文件，如果没清理则报错
     *
     * @throws SnapDirContentCheckException
     */
    private void checkSnapDir() throws SnapDirContentCheckException {
        File[] files = this.snapDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isLogFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new SnapDirContentCheckException("Snapshot directory has log files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }




    /**
     * 该函数从快照和事务日志中读取数据后恢复服务器数据库
     *
     * @param dt        要恢复的数据树
     * @param sessions  要恢复的会话，Map<会话ID，会话过期时间>
     * @param listener  the playback listener to run on the
     * database restoration
     * @return the highest zxid restored
     * @throws IOException
     */
    public long restore(DataTree dt, Map<Long, Integer> sessions, PlayBackListener listener) throws IOException {
        // 从最近的快照中对数据树进行反序列化
        long deserializeResult = snapLog.deserialize(dt, sessions);
        // lastProcessedZxid+1从事务日志文件txnLog读取事务操作
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        if (-1L == deserializeResult) {
            /* 这意味着我们无法找到任何快照，因此我们需要初始化一个空数据库(reported in ZOOKEEPER-2325) */
            if (txnLog.getLastLoggedZxid() != -1) {
                throw new IOException("No snapshot found, but there are log entries. Something is broken!");
            }

            // 初始化一个空数据库：将数据树和会话保存到快照文件中
            save(dt, (ConcurrentHashMap<Long, Integer>) sessions);
            // 返回一个零的zxid，因为我们的数据库是空的
            return 0;
        }
        return fastForwardFromEdits(dt, sessions, listener);
    }

    /**
     * 获取datatree中最大的事务ID，然后从事务日志中恢复该日志中>该事务ID之后的事务操作
     *
     * @param dt        将事务日志文件中操作日志恢复到对应的DataTree
     * @param sessions  保存恢复的session
     * @param listener  the playback listener to run on the database transactions.
     * @return 返回事务日志中，最后一个有效的zxid（即最大一个事务ID）
     * @throws IOException
     */
    public long fastForwardFromEdits(DataTree dt, Map<Long, Integer> sessions, PlayBackListener listener) throws IOException {
        // 从内存树获取最新的事务ID
        TxnIterator itr = txnLog.read(dt.lastProcessedZxid + 1);
        long highestZxid = dt.lastProcessedZxid;
        TxnHeader hdr;
        try {
            while (true) {
                // 迭代器在初始化时指向第一个有效的txn
                hdr = itr.getHeader();
                if (hdr == null) {
                    // 如果日志是空的话，就会返回当前内存树最大的事务ID
                    return dt.lastProcessedZxid;
                }

                // 正常情况下，zk的事务日志会比内存树的最大事务ID大
                if (hdr.getZxid() < highestZxid && highestZxid != 0) {
                    LOG.error("{}(highestZxid) > {}(next log) for type {}", highestZxid, hdr.getZxid(), hdr.getType());
                } else {
                    highestZxid = hdr.getZxid();
                }

                try {
                    processTransaction(hdr, dt, sessions, itr.getTxn());
                } catch (KeeperException.NoNodeException e) {
                    throw new IOException("Failed to process transaction type: " + hdr.getType() + " error: " + e.getMessage(), e);
                }

                listener.onTxnLoaded(hdr, itr.getTxn());
                if (!itr.next())
                    break;
            }
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
        return highestZxid;
    }

    /**
     * 开始从给定的zxid读取之后的所有事务
     *
     * @param zxid starting zxid
     * @return TxnIterator
     * @throws IOException
     */
    public TxnIterator readTxnLog(long zxid) throws IOException {
        return readTxnLog(zxid, true);
    }

    /**
     * 开始从给定的zxid读取之后的所有事务
     *
     * @param zxid starting zxid
     * @param fastForward true if the iterator should be fast forwarded to point
     *        to the txn of a given zxid, else the iterator will point to the
     *        starting txn of a txnlog that may contain txn of a given zxid
     * @return TxnIterator
     * @throws IOException
     */
    public TxnIterator readTxnLog(long zxid, boolean fastForward) throws IOException {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.read(zxid, fastForward);
    }

    /**
     * 恢复事务事务日志里的一个事务操作
     *
     * @param hdr       要回恢复操作的事务头
     * @param dt        恢复到对应的DataTree
     * @param sessions  用于保存恢复的会话
     * @param txn       要恢复的事务体
     */
    public void processTransaction(TxnHeader hdr, DataTree dt, Map<Long, Integer> sessions, Record txn) throws KeeperException.NoNodeException {
        ProcessTxnResult rc;
        switch (hdr.getType()) {
            case OpCode.createSession:
                sessions.put(hdr.getClientId(), ((CreateSessionTxn) txn).getTimeOut());

                if (LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "playLog --- create session in log: 0x"
                            + Long.toHexString(hdr.getClientId()) + " with timeout: " + ((CreateSessionTxn) txn).getTimeOut());
                }

                // 给dataTree一个机会来同步它的lastProcessedZxid
                rc = dt.processTxn(hdr, txn);
                break;
            case OpCode.closeSession:
                sessions.remove(hdr.getClientId());
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "playLog --- close session in log: 0x" + Long.toHexString(hdr.getClientId()));
                }
                rc = dt.processTxn(hdr, txn);
                break;
            default:
                rc = dt.processTxn(hdr, txn);
        }

        /**
         * Snapshots are lazily created. So when a snapshot is in progress,
         * there is a chance for later transactions to make into the
         * snapshot. Then when the snapshot is restored, NONODE/NODEEXISTS
         * errors could occur. It should be safe to ignore these.
         */
        if (rc.err != Code.OK.intValue()) {
            LOG.debug("Ignoring processTxn failure hdr: {}, error: {}, path: {}", hdr.getType(), rc.err, rc.path);
        }
    }

    /**
     * the last logged zxid on the transaction logs
     * @return the last logged zxid
     */
    public long getLastLoggedZxid() {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.getLastLoggedZxid();
    }

    /**
     * 将数据树和会话保存到快照文件中
     *
     * @param dataTree              要序列化到磁盘上的数据树
     * @param sessionsWithTimeouts  要序列化到磁盘上的会话超时
     * @throws IOException
     */
    public void save(DataTree dataTree, ConcurrentHashMap<Long, Integer> sessionsWithTimeouts) throws IOException {
        long lastZxid = dataTree.lastProcessedZxid;
        File snapshotFile = new File(snapDir, Util.makeSnapshotName(lastZxid));
        LOG.info("Snapshotting: 0x{} to {}", Long.toHexString(lastZxid), snapshotFile);
        snapLog.serialize(dataTree, sessionsWithTimeouts, snapshotFile);

    }

    /**
     * 截断该事务ID之后的事务日志，即将大于该zxid的事务日志都删除掉
     *
     * @param zxid the zxid to truncate the logs to
     * @return 如果能够截断日志，则返回true，否则false
     * @throws IOException
     */
    public boolean truncateLog(long zxid) throws IOException {
        // close the existing txnLog and snapLog
        close();

        // truncate it
        FileTxnLog truncLog = new FileTxnLog(dataDir);
        boolean truncated = truncLog.truncate(zxid);
        truncLog.close();

        // re-open the txnLog and snapLog
        // I'd rather just close/reopen this object itself, however that 
        // would have a big impact outside ZKDatabase as there are other
        // objects holding a reference to this object.
        txnLog = new FileTxnLog(dataDir);
        snapLog = new FileSnap(snapDir);

        return truncated;
    }

    /**
     * 快照目录中的最新快照
     *
     * @return the file that contains the most
     * recent snapshot
     * @throws IOException
     */
    public File findMostRecentSnapshot() throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findMostRecentSnapshot();
    }

    /**
     * the n most recent snapshots
     * @param n the number of recent snapshots
     * @return the list of n most recent snapshots, with
     * the most recent in front
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findNRecentSnapshots(n);
    }

    /**
     * get the snapshot logs which may contain transactions newer than the given zxid.
     * This includes logs with starting zxid greater than given zxid, as well as the
     * newest transaction log with starting zxid less than given zxid.  The latter log
     * file may contain transactions beyond given zxid.
     * @param zxid the zxid that contains logs greater than
     * zxid
     * @return
     */
    public File[] getSnapshotLogs(long zxid) {
        return FileTxnLog.getLogFiles(dataDir.listFiles(), zxid);
    }

    /**
     * 追加请求到事务日志中
     *
     * @param si the request to be appended
     * returns true iff something appended, otw false
     * @throws IOException
     */
    public boolean append(Request si) throws IOException {
        return txnLog.append(si.getHdr(), si.getTxn());
    }

    /**
     * 提交日志事务
     *
     * @throws IOException
     */
    public void commit() throws IOException {
        txnLog.commit();
    }

    /**
     * 事务日志的同步时间(以毫秒为单位)
     *
     * @return elapsed sync time of transaction log commit in milliseconds
     */
    public long getTxnLogElapsedSyncTime() {
        return txnLog.getTxnLogSyncElapsedTime();
    }

    /**
     * 回滚日志事务
     *
     * @throws IOException
     */
    public void rollLog() throws IOException {
        txnLog.rollLog();
    }

    /**
     * close the transaction log files
     * @throws IOException
     */
    public void close() throws IOException {
        txnLog.close();
        snapLog.close();
    }


    // getter ...

    public File getDataDir() {
        return this.dataDir;
    }
    public File getSnapDir() {
        return this.snapDir;
    }



    /**
     * 这个监听器帮助调用restore的外部api在数据恢复时收集信息。
     * PlayBackListener监听器主要用来接收事务应用过程中的回调。在ZooKeeper数据恢复后期，会有一个事务订正的过程，在这个过程中，会回调PlayBackListener监听器来进行对应的数据订正。
     */
    public interface PlayBackListener {
        void onTxnLoaded(TxnHeader hdr, Record rec);
    }

    @SuppressWarnings("serial")
    public static class DatadirException extends IOException {
        public DatadirException(String msg) {
            super(msg);
        }

        public DatadirException(String msg, Exception e) {
            super(msg, e);
        }
    }

    @SuppressWarnings("serial")
    public static class LogDirContentCheckException extends DatadirException {
        public LogDirContentCheckException(String msg) {
            super(msg);
        }
    }

    @SuppressWarnings("serial")
    public static class SnapDirContentCheckException extends DatadirException {
        public SnapDirContentCheckException(String msg) {
            super(msg);
        }
    }
}

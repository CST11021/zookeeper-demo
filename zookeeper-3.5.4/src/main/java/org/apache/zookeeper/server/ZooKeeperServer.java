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

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.RequestProcessor.RequestProcessorException;
import org.apache.zookeeper.server.ServerCnxn.CloseRequestException;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 这个类实现了一个简单的独立ZooKeeperServer。它设置了以下的请求处理器链来处理请求:
 * PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor
 */
public class ZooKeeperServer implements SessionExpirer, ServerStats.Provider {

    protected static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(ZooKeeperServer.class);

        Environment.logEnv("Server environment:", LOG);
    }

    /** JMXBean */
    protected ZooKeeperServerBean jmxServerBean;
    /** JMXBean */
    protected DataTreeBean jmxDataTreeBean;

    /**
     * 默认的tickTime为3秒
     */
    public static final int DEFAULT_TICK_TIME = 3000;
    /**
     * 参数tickTime用于配置zk中最小时间单元的长度，很多运行时的时间间隔都是使用tickTime的倍数来表示的。
     * 例如，zk中会话的最小超时时间默认是2*tickTime。(心跳基本时间单位，毫秒级，ZK基本上所有的时间都是这个时间的整数倍)。
     */
    protected int tickTime = DEFAULT_TICK_TIME;
    /** 值为-1表示未设置，使用默认值，最小的客户端session超时时间 */
    protected int minSessionTimeout = -1;
    /** 值为-1表示未设置，使用默认值，最小的客户端session超时时间 */
    protected int maxSessionTimeout = -1;
    /** zk用来跟踪和管理session */
    protected SessionTracker sessionTracker;
    /** zk数据快照及事务日志文件加载器 */
    private FileTxnSnapLog txnLogFactory = null;
    /** zk内存数据库 */
    private ZKDatabase zkDb;
    /** 表示当前zk处理的客户端请求的事务id，当接收到请求时该值+1 */
    private final AtomicLong hzxid = new AtomicLong(0);
    public final static Exception ok = new Exception("No prob");
    /** 客户端的请求到服务端后需要经过一系列的处理器来处理，这些处理器组成一条处理器链，该对象表示第一个处理器，根据不同的服务类型（比如follow、leader和learn）而不同 */
    protected RequestProcessor firstProcessor;
    /** 实例化一个ZooKeeperServer后，默认是初始化状态 */
    protected volatile State state = State.INITIAL;

    /**
     * 表示zk服务器状态枚举
     */
    protected enum State {
        /** 表示zk服务器正在初始化中 */
        INITIAL,
        /** 表示zk服务器正在运行 */
        RUNNING,
        /** 表示zk服务器正在shutdown */
        SHUTDOWN,
        /** 表示zk服务器error */
        ERROR
    }

    /**
     * This is the secret that we use to generate passwords, for the moment it
     * is more of a sanity check.
     */
    static final private long superSecret = 0XB3415C00L;

    /**
     * 客户端的请求处理完成后会将requestsInProcess+1
     */
    private final AtomicInteger requestsInProcess = new AtomicInteger(0);
    /** 用于存放刚进行更改还没有同步到ZKDatabase中的节点信息 */
    final List<ChangeRecord> outstandingChanges = new ArrayList<ChangeRecord>();
    /**
     * 1、用于存放刚进行更改还没有同步到ZKDatabase中的节点信息
     * 2、根据节点的三层缓存模型，在获取节点信息时，会首先从outstandingChangesForPath中获取信息，当没有找到对应的节点信息时，再通过zkDb::getNode获取
     * 3、注意：必须在outstandingChanges锁下访问此数据结构
     */
    final HashMap<String, ChangeRecord> outstandingChangesForPath = new HashMap<String, ChangeRecord>();

    protected ServerCnxnFactory serverCnxnFactory;
    protected ServerCnxnFactory secureServerCnxnFactory;

    /** zk服务器运行时的统计器 */
    private final ServerStats serverStats;
    /** ZK服务监听：用于通知服务器某些关键线程已经停止，它通常发生在发生致命错误时。 */
    private final ZooKeeperServerListener listener;
    /** ZooKeeper服务器关闭处理程序，用于处理错误或关闭服务器状态转换，从而释放相关的关闭锁存器。 */
    private ZooKeeperServerShutdownHandler zkShutdownHandler;
    private volatile int createSessionTrackerServerId = 1;



    /**
     * Creates a ZooKeeperServer instance. Nothing is setup, use the setX
     * methods to prepare the instance (eg datadir, datalogdir, ticktime,
     * builder, etc...)
     *
     * @throws IOException
     */
    public ZooKeeperServer() {
        serverStats = new ServerStats(this);
        listener = new ZooKeeperServerListenerImpl(this);
    }
    /**
     *
     * @param txnLogFactory
     * @param tickTime
     * @throws IOException
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory, int tickTime) throws IOException {
        this(txnLogFactory, tickTime, -1, -1, new ZKDatabase(txnLogFactory));
    }
    /**
     * 创建一个ZooKeeperServer实例。它会设置所有内容，但是在调用run()之前不会真正开始监听客户机。
     *
     * @param txnLogFactory
     * @param tickTime
     * @param minSessionTimeout
     * @param maxSessionTimeout
     * @param zkDb
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory, int tickTime, int minSessionTimeout, int maxSessionTimeout, ZKDatabase zkDb) {
        serverStats = new ServerStats(this);
        this.txnLogFactory = txnLogFactory;
        this.zkDb = zkDb;
        this.tickTime = tickTime;
        setMinSessionTimeout(minSessionTimeout);
        setMaxSessionTimeout(maxSessionTimeout);
        listener = new ZooKeeperServerListenerImpl(this);
        LOG.info("Created server with tickTime " + tickTime
                + " minSessionTimeout " + getMinSessionTimeout()
                + " maxSessionTimeout " + getMaxSessionTimeout()
                + " datadir " + txnLogFactory.getDataDir()
                + " snapdir " + txnLogFactory.getSnapDir());
    }
    /**
     * This constructor is for backward compatibility with the existing unit
     * test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public ZooKeeperServer(File snapDir, File logDir, int tickTime) throws IOException {
        this(new FileTxnSnapLog(snapDir, logDir), tickTime);
    }
    /**
     * Default constructor, relies on the config for its argument values
     *
     * @throws IOException
     */
    public ZooKeeperServer(FileTxnSnapLog txnLogFactory) throws IOException {
        this(txnLogFactory, DEFAULT_TICK_TIME, -1, -1, new ZKDatabase(txnLogFactory));
    }

    /**
     * 启动zk服务
     */
    public synchronized void startup() {
        // 实例化sessionTracker
        if (sessionTracker == null) {
            createSessionTracker();
        }

        // 启动SessionTracker
        startSessionTracker();

        // 启动RequestProcessor
        setupRequestProcessors();

        // 注册到JMX
        registerJMX();

        // 设置状态
        setState(State.RUNNING);

        // 发布通知
        notifyAll();
    }

    // sessionTracker
    /**
     * 创建一个 SessionTrackerImpl，zk服务启动的时候会调用该方法
     */
    protected void createSessionTracker() {
        sessionTracker = new SessionTrackerImpl(
                this,
                zkDb.getSessionWithTimeOuts(),
                tickTime,
                createSessionTrackerServerId,
                getZooKeeperServerListener());
    }
    /**
     * 启动sessionTracker
     */
    protected void startSessionTracker() {
        ((SessionTrackerImpl)sessionTracker).start();
    }
    /**
     * 启动RequestProcessor
     */
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        RequestProcessor syncProcessor = new SyncRequestProcessor(this, finalProcessor);
        ((SyncRequestProcessor)syncProcessor).start();
        firstProcessor = new PrepRequestProcessor(this, syncProcessor);
        ((PrepRequestProcessor)firstProcessor).start();
    }
    /**
     * 注册到JMX
     */
    protected void registerJMX() {
        // register with JMX
        try {
            jmxServerBean = new ZooKeeperServerBean(this);
            MBeanRegistry.getInstance().register(jmxServerBean, null);

            try {
                jmxDataTreeBean = new DataTreeBean(zkDb.getDataTree());
                MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
                jmxDataTreeBean = null;
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }


    /**
     * 从zk快照文件中加载节点数据
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void startdata() throws IOException, InterruptedException {
        //check to see if zkDb is not null
        if (zkDb == null) {
            zkDb = new ZKDatabase(this.txnLogFactory);
        }

        if (!zkDb.isInitialized()) {
            loadData();
        }
    }

    void removeCnxn(ServerCnxn cnxn) {
        zkDb.removeCnxn(cnxn);
    }

    public void dumpConf(PrintWriter pwriter) {
        pwriter.print("clientPort=");
        pwriter.println(getClientPort());
        pwriter.print("secureClientPort=");
        pwriter.println(getSecureClientPort());
        pwriter.print("dataDir=");
        pwriter.println(zkDb.snapLog.getSnapDir().getAbsolutePath());
        pwriter.print("dataDirSize=");
        pwriter.println(getDataDirSize());
        pwriter.print("dataLogDir=");
        pwriter.println(zkDb.snapLog.getDataDir().getAbsolutePath());
        pwriter.print("dataLogSize=");
        pwriter.println(getLogDirSize());
        pwriter.print("tickTime=");
        pwriter.println(getTickTime());
        pwriter.print("maxClientCnxns=");
        pwriter.println(getMaxClientCnxnsPerHost());
        pwriter.print("minSessionTimeout=");
        pwriter.println(getMinSessionTimeout());
        pwriter.print("maxSessionTimeout=");
        pwriter.println(getMaxSessionTimeout());

        pwriter.print("serverId=");
        pwriter.println(getServerId());
    }

    public ZooKeeperServerConf getConf() {
        return new ZooKeeperServerConf
            (getClientPort(),
             zkDb.snapLog.getSnapDir().getAbsolutePath(),
             zkDb.snapLog.getDataDir().getAbsolutePath(),
             getTickTime(),
             serverCnxnFactory.getMaxClientCnxnsPerHost(),
             getMinSessionTimeout(),
             getMaxSessionTimeout(),
             getServerId());
    }

    /**
     * 从快照文件恢复会话和数据
     */
    public void loadData() throws IOException, InterruptedException {
        /**
         * When a new leader starts executing Leader#lead, it 
         * invokes this method. The database, however, has been
         * initialized before running leader election so that
         * the server could pick its zxid for its initial vote.
         * It does it by invoking QuorumPeer#getLastLoggedZxid.
         * Consequently, we don't need to initialize it once more
         * and avoid the penalty of loading it a second time. Not 
         * reloading it is particularly important for applications
         * that host a large database.
         * 
         * The following if block checks whether the database has
         * been initialized or not. Note that this method is
         * invoked by at least one other method: 
         * ZooKeeperServer#startdata.
         *  
         * See ZOOKEEPER-1642 for more detail.
         */
        if(zkDb.isInitialized()){
            setZxid(zkDb.getDataTreeLastProcessedZxid());
        } else {
            setZxid(zkDb.loadDataBase());
        }
        
        // Clean up dead sessions
        LinkedList<Long> deadSessions = new LinkedList<Long>();
        for (Long session : zkDb.getSessions()) {
            if (zkDb.getSessionWithTimeOuts().get(session) == null) {
                deadSessions.add(session);
            }
        }

        for (long session : deadSessions) {
            // XXX: Is lastProcessedZxid really the best thing to use?
            killSession(session, zkDb.getDataTreeLastProcessedZxid());
        }

        // Make a clean snapshot
        takeSnapshot();
    }

    /**
     * 将数据树和会话保存到快照文件中
     */
    public void takeSnapshot(){
        try {
            txnLogFactory.save(zkDb.getDataTree(), zkDb.getSessionWithTimeOuts());
        } catch (IOException e) {
            LOG.error("Severe unrecoverable error, exiting", e);
            // 这是一个严重的错误，我们无法恢复，所以我们需要退出
            System.exit(10);
        }
    }

    /**
     * 获取zk的快照文件数量
     *
     * @return
     */
    @Override
    public long getDataDirSize() {
        if (zkDb == null) {
            return 0L;
        }

        File path = zkDb.snapLog.getDataDir();
        return getDirSize(path);
    }

    /**
     * 获取zk事务日志文件数量
     *
     * @return
     */
    @Override
    public long getLogDirSize() {
        if (zkDb == null) {
            return 0L;
        }
        File path = zkDb.snapLog.getSnapDir();
        return getDirSize(path);
    }

    /**
     * 返回对应文件夹中的文件数量
     *
     * @param file
     * @return
     */
    private long getDirSize(File file) {
        long size = 0L;
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    size += getDirSize(f);
                }
            }
        } else {
            size = file.length();
        }
        return size;
    }

    /**
     * 返回当前处理的请求的事务id
     *
     * @return
     */
    public long getZxid() {
        return hzxid.get();
    }

    /**
     * 表示处理请求的事务id，{@link PrepRequestProcessor}处理请求的时候会调用该方法
     *
     * @return
     */
    long getNextZxid() {
        return hzxid.incrementAndGet();
    }

    public void setZxid(long zxid) {
        hzxid.set(zxid);
    }


    // session

    /**
     * kill Session
     *
     * @param sessionId
     * @param zxid
     */
    protected void killSession(long sessionId, long zxid) {
        zkDb.killSession(sessionId, zxid);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "ZooKeeperServer --- killSession: 0x" + Long.toHexString(sessionId));
        }
        if (sessionTracker != null) {
            sessionTracker.removeSession(sessionId);
        }
    }
    /**
     * 使session过期
     *
     * @param session
     */
    public void expire(Session session) {
        long sessionId = session.getSessionId();
        LOG.info("Expiring session 0x" + Long.toHexString(sessionId) + ", timeout of " + session.getTimeout() + "ms exceeded");
        close(sessionId);
    }
    /**
     * 根据sessionId关闭session
     *
     * @param sessionId
     */
    private void close(long sessionId) {
        Request si = new Request(null, sessionId, 0, OpCode.closeSession, null, null);
        setLocalSessionFlag(si);
        submitRequest(si);
    }
    /**
     * 根据sessionId关闭session
     *
     * @param sessionId
     */
    public void closeSession(long sessionId) {
        LOG.info("Closing session 0x" + Long.toHexString(sessionId));

        // we do not want to wait for a session close. send it as soon as we
        // detect it!
        close(sessionId);
    }
    public void closeSession(ServerCnxn cnxn, RequestHeader requestHeader) {
        closeSession(cnxn.getSessionId());
    }


    /**
     * ServerCnxn处理来自客户端的请求时会调用该方法，检查当前请求的session是否有效
     *
     * @param cnxn
     * @throws MissingSessionException
     */
    void touch(ServerCnxn cnxn) throws MissingSessionException {
        if (cnxn == null) {
            return;
        }

        long id = cnxn.getSessionId();
        int to = cnxn.getSessionTimeout();
        // 检查
        if (!sessionTracker.touchSession(id, to)) {
            throw new MissingSessionException("No session with sessionid 0x" + Long.toHexString(id) + " exists, probably expired and removed");
        }
    }



    // state and shutdown
    /**
     * Sets the state of ZooKeeper server. After changing the state, it notifies
     * the server state change to a registered shutdown handler, if any.
     * <p>
     * The following are the server state transitions:
     * <li>During startup the server will be in the INITIAL state.</li>
     * <li>After successfully starting, the server sets the state to RUNNING.
     * </li>
     * <li>The server transitions to the ERROR state if it hits an internal
     * error. {@link ZooKeeperServerListenerImpl} notifies any critical resource
     * error events, e.g., SyncRequestProcessor not being able to write a txn to
     * disk.</li>
     * <li>During shutdown the server sets the state to SHUTDOWN, which
     * corresponds to the server not running.</li>
     *
     * @param state new server state.
     */
    protected void setState(State state) {
        this.state = state;
        // Notify server state changes to the registered shutdown handler, if any.
        if (zkShutdownHandler != null) {
            zkShutdownHandler.handle(state);
        } else {
            LOG.debug("ZKShutdownHandler is not registered, so ZooKeeper server " + "won't take any action on ERROR or SHUTDOWN server state changes");
        }
    }
    /**
     * 根据zk状态判断是否可以关闭zk服务
     *
     * @return true if the server is running or server hits an error, false otherwise.
     */
    protected boolean canShutdown() {
        return state == State.RUNNING || state == State.ERROR;
    }
    /**
     * zk服务是否正在运行中
     *
     * @return true if the server is running, false otherwise.
     */
    public boolean isRunning() {
        return state == State.RUNNING;
    }
    /**
     * shutdown Zk服务
     */
    public void shutdown() {
        shutdown(false);
    }
    /**
     * shutdown Zk服务
     *
     * @param fullyShutDown true if another server using the same database will not replace this one in the same process
     */
    public synchronized void shutdown(boolean fullyShutDown) {
        // 如果已经关闭了，则不需在关闭
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        LOG.info("shutting down");

        // 设置zk状态为SHUTDOWN
        setState(State.SHUTDOWN);

        // Since sessionTracker and syncThreads poll we just have to set running to false and they will detect it during the poll interval.
        if (sessionTracker != null) {
            sessionTracker.shutdown();
        }
        if (firstProcessor != null) {
            firstProcessor.shutdown();
        }

        if (zkDb != null) {
            if (fullyShutDown) {
                zkDb.clear();
            } else {
                // else there is no need to clear the database
                //  * When a new quorum is established we can still apply the diff on top of the same zkDb data
                //  * If we fetch a new snapshot from leader, the zkDb will be cleared anyway before loading the snapshot
                try {
                    // 这将快速将数据库转发到最新记录的事务
                    zkDb.fastForwardDataBase();
                } catch (IOException e) {
                    LOG.error("Error updating DB", e);
                    zkDb.clear();
                }
            }
        }

        // 注销JMX
        unregisterJMX();
    }
    /**
     * 注销JMX
     */
    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
        jmxDataTreeBean = null;
    }


    /**
     * 客户端的请求处理完成后，会调用该方法，将requestsInProcess+1
     */
    public void incInProcess() {
        requestsInProcess.incrementAndGet();
    }
    public void decInProcess() {
        requestsInProcess.decrementAndGet();
    }
    public int getInProcess() {
        return requestsInProcess.get();
    }




    byte[] generatePasswd(long id) {
        Random r = new Random(id ^ superSecret);
        byte p[] = new byte[16];
        r.nextBytes(p);
        return p;
    }

    protected boolean checkPasswd(long sessionId, byte[] passwd) {
        return sessionId != 0
                && Arrays.equals(passwd, generatePasswd(sessionId));
    }

    long createSession(ServerCnxn cnxn, byte passwd[], int timeout) {
        if (passwd == null) {
            // Possible since it's just deserialized from a packet on the wire.
            passwd = new byte[0];
        }
        long sessionId = sessionTracker.createSession(timeout);
        Random r = new Random(sessionId ^ superSecret);
        r.nextBytes(passwd);
        ByteBuffer to = ByteBuffer.allocate(4);
        to.putInt(timeout);
        cnxn.setSessionId(sessionId);
        Request si = new Request(cnxn, sessionId, 0, OpCode.createSession, to, null);
        setLocalSessionFlag(si);
        submitRequest(si);
        return sessionId;
    }

    /**
     * set the owner of this session as owner
     * @param id the session id
     * @param owner the owner of the session
     * @throws SessionExpiredException
     */
    public void setOwner(long id, Object owner) throws SessionExpiredException {
        sessionTracker.setOwner(id, owner);
    }

    protected void revalidateSession(ServerCnxn cnxn, long sessionId, int sessionTimeout) throws IOException {
        boolean rc = sessionTracker.touchSession(sessionId, sessionTimeout);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId) +
                            " is valid: " + rc);
        }
        finishSessionInit(cnxn, rc);
    }

    public void reopenSession(ServerCnxn cnxn, long sessionId, byte[] passwd, int sessionTimeout) throws IOException {
        if (checkPasswd(sessionId, passwd)) {
            revalidateSession(cnxn, sessionId, sessionTimeout);
        } else {
            LOG.warn("Incorrect password from " + cnxn.getRemoteSocketAddress()
                    + " for session 0x" + Long.toHexString(sessionId));
            finishSessionInit(cnxn, false);
        }
    }

    public void finishSessionInit(ServerCnxn cnxn, boolean valid) {
        // register with JMX
        try {
            if (valid) {
                if (serverCnxnFactory != null && serverCnxnFactory.cnxns.contains(cnxn)) {
                    serverCnxnFactory.registerConnection(cnxn);
                } else if (secureServerCnxnFactory != null && secureServerCnxnFactory.cnxns.contains(cnxn)) {
                    secureServerCnxnFactory.registerConnection(cnxn);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
        }

        try {
            ConnectResponse rsp = new ConnectResponse(0, valid ? cnxn.getSessionTimeout()
                    : 0, valid ? cnxn.getSessionId() : 0, // send 0 if session is no
                            // longer valid
                            valid ? generatePasswd(cnxn.getSessionId()) : new byte[16]);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            bos.writeInt(-1, "len");
            rsp.serialize(bos, "connect");
            if (!cnxn.isOldClient) {
                bos.writeBool(
                        this instanceof ReadOnlyZooKeeperServer, "readOnly");
            }
            baos.close();
            ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
            bb.putInt(bb.remaining() - 4).rewind();
            cnxn.sendBuffer(bb);

            if (valid) {
                LOG.info("Established session 0x"
                        + Long.toHexString(cnxn.getSessionId())
                        + " with negotiated timeout " + cnxn.getSessionTimeout()
                        + " for client "
                        + cnxn.getRemoteSocketAddress());
                cnxn.enableRecv();
            } else {

                LOG.info("Invalid session 0x"
                        + Long.toHexString(cnxn.getSessionId())
                        + " for client "
                        + cnxn.getRemoteSocketAddress()
                        + ", probably expired");
                cnxn.sendBuffer(ServerCnxnFactory.closeConn);
            }

        } catch (Exception e) {
            LOG.warn("Exception while establishing session, closing", e);
            cnxn.close();
        }
    }

    /**
     * 返回用于标记唯一的服务器ID
     *
     * @return
     */
    public long getServerId() {
        return 0;
    }

    /**
     * If the underlying Zookeeper server support local session, this method
     * will set a isLocalSession to true if a request is associated with
     * a local session.
     *
     * @param si
     */
    protected void setLocalSessionFlag(Request si) {
    }

    /**
     * 提交来自客户端的请求，并处理请求
     *
     * @param si
     */
    public void submitRequest(Request si) {
        if (firstProcessor == null) {
            synchronized (this) {
                try {
                    // 由于所有的请求都被传递给请求处理器，所以它应该等待请求处理器链的建立，建立完成后，将状态将更新为运行。
                    while (state == State.INITIAL) {
                        wait(1000);
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected interruption", e);
                }
                if (firstProcessor == null || state != State.RUNNING) {
                    throw new RuntimeException("Not started");
                }
            }
        }

        try {
            // 检查当前请求的session是否有效
            touch(si.cnxn);
            // 检查请求的类型是否有效
            boolean validpacket = Request.isValid(si.type);
            if (validpacket) {
                firstProcessor.processRequest(si);
                if (si.cnxn != null) {
                    // 客户端的请求处理完成后，会调用该方法，将requestsInProcess+1
                    incInProcess();
                }
            } else {
                LOG.warn("Received packet at server of unknown type " + si.type);
                new UnimplementedRequestProcessor().processRequest(si);
            }
        } catch (MissingSessionException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping request: " + e.getMessage());
            }
        } catch (RequestProcessorException e) {
            LOG.error("Unable to process request:" + e.getMessage(), e);
        }
    }

    public static int getSnapCount() {
        String sc = System.getProperty("zookeeper.snapCount");
        try {
            int snapCount = Integer.parseInt(sc);

            // snapCount must be 2 or more. See org.apache.zookeeper.server.SyncRequestProcessor
            if( snapCount < 2 ) {
                LOG.warn("SnapCount should be 2 or more. Now, snapCount is reset to 2");
                snapCount = 2;
            }
            return snapCount;
        } catch (Exception e) {
            return 100000;
        }
    }

    public int getGlobalOutstandingLimit() {
        String sc = System.getProperty("zookeeper.globalOutstandingLimit");
        int limit;
        try {
            limit = Integer.parseInt(sc);
        } catch (Exception e) {
            limit = 1000;
        }
        return limit;
    }

    /**
     * 获取最后操作DataTree的事务ID
     *
     * @return
     */
    public long getLastProcessedZxid() {
        return zkDb.getDataTreeLastProcessedZxid();
    }

    /**
     * 返回队列中尚未处理的请求，这些请求尚未被处理
     */
    public long getOutstandingRequests() {
        return getInProcess();
    }

    /**
     * 返回此服务器上活动的客户端连接的总数
     */
    public int getNumAliveConnections() {
        int numAliveConnections = 0;

        if (serverCnxnFactory != null) {
            numAliveConnections += serverCnxnFactory.getNumAliveConnections();
        }

        if (secureServerCnxnFactory != null) {
            numAliveConnections += secureServerCnxnFactory.getNumAliveConnections();
        }

        return numAliveConnections;
    }

    /**
     * trunccate the log to get in sync with others
     * if in a quorum
     * @param zxid the zxid that it needs to get in sync
     * with others
     * @throws IOException
     */
    public void truncateLog(long zxid) throws IOException {
        this.zkDb.truncateLog(zxid);
    }

    /** Maximum number of connections allowed from particular host (ip) */
    public int getMaxClientCnxnsPerHost() {
        if (serverCnxnFactory != null) {
            return serverCnxnFactory.getMaxClientCnxnsPerHost();
        }
        if (secureServerCnxnFactory != null) {
            return secureServerCnxnFactory.getMaxClientCnxnsPerHost();
        }
        return -1;
    }



    /**
     * Returns the elapsed sync of time of transaction log in milliseconds.
     */
    public long getTxnLogElapsedSyncTime() {
        return txnLogFactory.getTxnLogElapsedSyncTime();
    }

    public String getState() {
        return "standalone";
    }

    public void dumpEphemerals(PrintWriter pwriter) {
        zkDb.dumpEphemerals(pwriter);
    }

    public Map<Long, Set<String>> getEphemerals() {
        return zkDb.getEphemerals();
    }

    /**
     * 读取请求的数据到incomingBuffer
     *
     * @param cnxn
     * @param incomingBuffer
     * @throws IOException
     */
    public void processConnectRequest(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException {
        BinaryInputArchive bia = BinaryInputArchive.getArchive(new ByteBufferInputStream(incomingBuffer));
        ConnectRequest connReq = new ConnectRequest();
        connReq.deserialize(bia, "connect");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Session establishment request from client "
                    + cnxn.getRemoteSocketAddress()
                    + " client's lastZxid is 0x"
                    + Long.toHexString(connReq.getLastZxidSeen()));
        }
        boolean readOnly = false;
        try {
            readOnly = bia.readBool("readOnly");
            cnxn.isOldClient = false;
        } catch (IOException e) {
            // this is ok -- just a packet from an old client which
            // doesn't contain readOnly field
            LOG.warn("Connection request from old client "
                    + cnxn.getRemoteSocketAddress()
                    + "; will be dropped if server is in r-o mode");
        }
        if (!readOnly && this instanceof ReadOnlyZooKeeperServer) {
            String msg = "Refusing session request for not-read-only client "
                + cnxn.getRemoteSocketAddress();
            LOG.info(msg);
            throw new CloseRequestException(msg);
        }
        if (connReq.getLastZxidSeen() > zkDb.dataTree.lastProcessedZxid) {
            String msg = "Refusing session request for client "
                + cnxn.getRemoteSocketAddress()
                + " as it has seen zxid 0x"
                + Long.toHexString(connReq.getLastZxidSeen())
                + " our last zxid is 0x"
                + Long.toHexString(getZKDatabase().getDataTreeLastProcessedZxid())
                + " client must try another server";

            LOG.info(msg);
            throw new CloseRequestException(msg);
        }
        int sessionTimeout = connReq.getTimeOut();
        byte passwd[] = connReq.getPasswd();
        int minSessionTimeout = getMinSessionTimeout();
        if (sessionTimeout < minSessionTimeout) {
            sessionTimeout = minSessionTimeout;
        }
        int maxSessionTimeout = getMaxSessionTimeout();
        if (sessionTimeout > maxSessionTimeout) {
            sessionTimeout = maxSessionTimeout;
        }
        cnxn.setSessionTimeout(sessionTimeout);
        // We don't want to receive any packets until we are sure that the
        // session is setup
        cnxn.disableRecv();
        long sessionId = connReq.getSessionId();
        if (sessionId == 0) {
            LOG.info("Client attempting to establish new session at "
                    + cnxn.getRemoteSocketAddress());
            createSession(cnxn, passwd, sessionTimeout);
        } else {
            long clientSessionId = connReq.getSessionId();
            LOG.info("Client attempting to renew session 0x"
                    + Long.toHexString(clientSessionId)
                    + " at " + cnxn.getRemoteSocketAddress());
            if (serverCnxnFactory != null) {
                serverCnxnFactory.closeSession(sessionId);
            }
            if (secureServerCnxnFactory != null) {
                secureServerCnxnFactory.closeSession(sessionId);
            }
            cnxn.setSessionId(sessionId);
            reopenSession(cnxn, sessionId, passwd, sessionTimeout);
        }
    }

    public boolean shouldThrottle(long outStandingCount) {
        if (getGlobalOutstandingLimit() < getInProcess()) {
            return outStandingCount > 0;
        }
        return false;
    }

    /**
     * 处理来自客户端的请求包
     *
     * @param cnxn
     * @param incomingBuffer
     * @throws IOException
     */
    public void processPacket(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException {
        // 将来自客户端的请求数据反序列为RequestHeader对象
        InputStream bais = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bia = BinaryInputArchive.getArchive(bais);
        RequestHeader h = new RequestHeader();
        h.deserialize(bia, "header");
        incomingBuffer = incomingBuffer.slice();

        // 如果是授权的请求，对应客户端的AddAuthCommand命令
        if (h.getType() == OpCode.auth) {
            LOG.info("got auth packet " + cnxn.getRemoteSocketAddress());

            // 将字节转为AuthPacket对象
            AuthPacket authPacket = new AuthPacket();
            ByteBufferInputStream.byteBuffer2Record(incomingBuffer, authPacket);

            // 例如：addauth digest whz:123456 命令对应的scheme = digest，data = whz:123456
            String scheme = authPacket.getScheme();
            AuthenticationProvider ap = ProviderRegistry.getProvider(scheme);
            Code authReturn = KeeperException.Code.AUTHFAILED;

            if(ap != null) {
                try {
                    authReturn = ap.handleAuthentication(cnxn, authPacket.getAuth());
                } catch(RuntimeException e) {
                    LOG.warn("Caught runtime exception from AuthenticationProvider: " + scheme + " due to " + e);
                    authReturn = KeeperException.Code.AUTHFAILED;
                }
            }

            // 如果认证成功
            if (authReturn == KeeperException.Code.OK) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Authentication succeeded for scheme: " + scheme);
                }
                LOG.info("auth success " + cnxn.getRemoteSocketAddress());
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0, KeeperException.Code.OK.intValue());
                // 向客户端发送一个响应包
                cnxn.sendResponse(rh, null, null);
            }
            // 认证失败
            else {
                if (ap == null) {
                    LOG.warn("No authentication provider for scheme: " + scheme + " has " + ProviderRegistry.listProviders());
                } else {
                    LOG.warn("Authentication failed for scheme: " + scheme);
                }
                // send a response...
                ReplyHeader rh = new ReplyHeader(h.getXid(), 0, KeeperException.Code.AUTHFAILED.intValue());
                cnxn.sendResponse(rh, null, null);
                // ... and close connection
                cnxn.sendBuffer(ServerCnxnFactory.closeConn);
                cnxn.disableRecv();
            }
            return;
        }

        else if (h.getType() == OpCode.sasl) {
            Record rsp = processSasl(incomingBuffer,cnxn);
            ReplyHeader rh = new ReplyHeader(h.getXid(), 0, KeeperException.Code.OK.intValue());
            // not sure about 3rd arg..what is it?
            cnxn.sendResponse(rh,rsp, "response");
            return;
        }

        // zk客户端的大部分请求都会走该方法
        else {
            // 统计请求的数量+1
            cnxn.incrOutstandingRequests(h);
            // 根据客户端的请求数据创建一个Request对象
            Request si = new Request(cnxn, cnxn.getSessionId(), h.getXid(), h.getType(), incomingBuffer, cnxn.getAuthInfo());
            si.setOwner(ServerCnxn.me);
            // 始终将来自客户机的包视为可能的本地请求。
            setLocalSessionFlag(si);
            // 提交请求
            submitRequest(si);
            return;
        }
    }

    private Record processSasl(ByteBuffer incomingBuffer, ServerCnxn cnxn) throws IOException {
        LOG.debug("Responding to client SASL token.");
        GetSASLRequest clientTokenRecord = new GetSASLRequest();
        ByteBufferInputStream.byteBuffer2Record(incomingBuffer,clientTokenRecord);
        byte[] clientToken = clientTokenRecord.getToken();
        LOG.debug("Size of client SASL token: " + clientToken.length);
        byte[] responseToken = null;
        try {
            ZooKeeperSaslServer saslServer  = cnxn.zooKeeperSaslServer;
            try {
                // note that clientToken might be empty (clientToken.length == 0):
                // if using the DIGEST-MD5 mechanism, clientToken will be empty at the beginning of the
                // SASL negotiation process.
                responseToken = saslServer.evaluateResponse(clientToken);
                if (saslServer.isComplete()) {
                    String authorizationID = saslServer.getAuthorizationID();
                    LOG.info("adding SASL authorization for authorizationID: " + authorizationID);
                    cnxn.addAuthInfo(new Id("sasl",authorizationID));
                    if (System.getProperty("zookeeper.superUser") != null && 
                        authorizationID.equals(System.getProperty("zookeeper.superUser"))) {
                        cnxn.addAuthInfo(new Id("super", ""));
                    }
                }
            }
            catch (SaslException e) {
                LOG.warn("Client failed to SASL authenticate: " + e, e);
                if ((System.getProperty("zookeeper.allowSaslFailedClients") != null)
                  &&
                  (System.getProperty("zookeeper.allowSaslFailedClients").equals("true"))) {
                    LOG.warn("Maintaining client connection despite SASL authentication failure.");
                } else {
                    LOG.warn("Closing client connection due to SASL authentication failure.");
                    cnxn.close();
                }
            }
        }
        catch (NullPointerException e) {
            LOG.error("cnxn.saslServer is null: cnxn object did not initialize its saslServer properly.");
        }
        if (responseToken != null) {
            LOG.debug("Size of server SASL response: " + responseToken.length);
        }
        // wrap SASL response token to client inside a Response object.
        return new SetSASLResponse(responseToken);
    }

    // entry point for quorum/Learner.java
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
        return processTxn(null, hdr, txn);
    }

    // entry point for FinalRequestProcessor.java
    public ProcessTxnResult processTxn(Request request) {
        return processTxn(request, request.getHdr(), request.getTxn());
    }

    private ProcessTxnResult processTxn(Request request, TxnHeader hdr, Record txn) {
        ProcessTxnResult rc;
        int opCode = request != null ? request.type : hdr.getType();
        long sessionId = request != null ? request.sessionId : hdr.getClientId();
        if (hdr != null) {
            rc = getZKDatabase().processTxn(hdr, txn);
        } else {
            rc = new ProcessTxnResult();
        }
        if (opCode == OpCode.createSession) {
            if (hdr != null && txn instanceof CreateSessionTxn) {
                CreateSessionTxn cst = (CreateSessionTxn) txn;
                sessionTracker.addGlobalSession(sessionId, cst.getTimeOut());
            } else if (request != null && request.isLocalSession()) {
                request.request.rewind();
                int timeout = request.request.getInt();
                request.request.rewind();
                sessionTracker.addSession(request.sessionId, timeout);
            } else {
                LOG.warn("*****>>>>> Got "
                        + txn.getClass() + " "
                        + txn.toString());
            }
        } else if (opCode == OpCode.closeSession) {
            sessionTracker.removeSession(sessionId);
        }
        return rc;
    }

    public Map<Long, Set<Long>> getSessionExpiryMap() {
        return sessionTracker.getSessionExpiryMap();
    }

    /**
     * This method is used to register the ZooKeeperServerShutdownHandler to get
     * server's error or shutdown state change notifications.
     * {@link ZooKeeperServerShutdownHandler#handle(State)} will be called for
     * every server state changes {@link #setState(State)}.
     *
     * @param zkShutdownHandler shutdown handler
     */
    void registerServerShutdownHandler(ZooKeeperServerShutdownHandler zkShutdownHandler) {
        this.zkShutdownHandler = zkShutdownHandler;
    }



    // getter and setter...

    public void setServerCnxnFactory(ServerCnxnFactory factory) {
        serverCnxnFactory = factory;
    }
    public ServerCnxnFactory getServerCnxnFactory() {
        return serverCnxnFactory;
    }
    public void setSecureServerCnxnFactory(ServerCnxnFactory factory) {
        secureServerCnxnFactory = factory;
    }
    public ZKDatabase getZKDatabase() {
        return this.zkDb;
    }
    public void setZKDatabase(ZKDatabase zkDb) {
        this.zkDb = zkDb;
    }
    public int getTickTime() {
        return tickTime;
    }
    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }
    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }
    public void setMinSessionTimeout(int min) {
        this.minSessionTimeout = min == -1 ? tickTime * 2 : min;
        LOG.info("minSessionTimeout set to {}", this.minSessionTimeout);
    }
    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }
    public void setMaxSessionTimeout(int max) {
        this.maxSessionTimeout = max == -1 ? tickTime * 20 : max;
        LOG.info("maxSessionTimeout set to {}", this.maxSessionTimeout);
    }
    public int getClientPort() {
        return serverCnxnFactory != null ? serverCnxnFactory.getLocalPort() : -1;
    }
    public int getSecureClientPort() {
        return secureServerCnxnFactory != null ? secureServerCnxnFactory.getLocalPort() : -1;
    }
    public void setTxnLogFactory(FileTxnSnapLog txnLog) {
        this.txnLogFactory = txnLog;
    }
    public FileTxnSnapLog getTxnLogFactory() {
        return this.txnLogFactory;
    }
    public ZooKeeperServerListener getZooKeeperServerListener() {
        return listener;
    }
    /**
     * Change the server ID used by {@link #createSessionTracker()}. Must be called prior to
     * {@link #startup()} being called
     *
     * @param newId ID to use
     */
    public void setCreateSessionTrackerServerId(int newId) {
        createSessionTrackerServerId = newId;
    }
    public ServerStats serverStats() {
        return serverStats;
    }
    public SessionTracker getSessionTracker() {
        return sessionTracker;
    }



    /**
     * 该结构用于促进PrepRP和FinalRP之间的信息共享。
     */
    static class ChangeRecord {

        /** 表示操作节点的事务id */
        long zxid;
        /** 节点路径 */
        String path;
        /** 节点的状态 */
        StatPersisted stat;
        /** 孩子节点数量 */
        int childCount;
        /** 节点的权限 */
        List<ACL> acl;

        ChangeRecord(long zxid, String path, StatPersisted stat, int childCount, List<ACL> acl) {
            this.zxid = zxid;
            this.path = path;
            this.stat = stat;
            this.childCount = childCount;
            this.acl = acl;
        }

        /**
         * 复制一个ChangeRecord对象
         *
         * @param zxid
         * @return
         */
        ChangeRecord duplicate(long zxid) {
            StatPersisted stat = new StatPersisted();
            if (this.stat != null) {
                DataTree.copyStatPersisted(this.stat, stat);
            }
            return new ChangeRecord(zxid, path, stat, childCount, acl == null ? new ArrayList<ACL>() : new ArrayList<ACL>(acl));
        }
    }

    public static class MissingSessionException extends IOException {
        private static final long serialVersionUID = 7467414635467261007L;

        public MissingSessionException(String msg) {
            super(msg);
        }
    }
}

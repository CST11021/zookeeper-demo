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
package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.common.AtomicFileWritingIdiom;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.WriterStatement;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.admin.AdminServerFactory;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.auth.*;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这个服务器有三种状态:
 * 1、Leader election：每个服务器将选举一个Leader(一开始提出自己是Leader)。
 * 2、Follower：服务器将与leader同步并复制任何事务
 * 3、Leader：服务器将处理请求并转发给关注者。大多数追随者必须在请求被接受之前将其记录下来。
 *
 *
 * 这个类就是zookeeper的Leader选举的启动类，负责创建选举算法，zk数据恢复，启动leader选举等
 *
 *
 * This class will setup a datagram socket that will always respond with its view of the current leader. The response will take the form of:
 *
 * <pre>
 * int xid;
 *
 * long myid;
 *
 * long leader_id;
 *
 * long leader_zxid;
 * </pre>
 *
 * The request for the current leader will consist solely of an xid: int xid;
 */
public class QuorumPeer extends ZooKeeperThread implements QuorumStats.Provider {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeer.class);

    /**
     * ServerState记录当前server所处的状态
     */
    public enum ServerState {
        // LOOKING：寻找Leader状态，当服务器处于该状态时，它会认为当前集群中没有Leader，因此需要进入Leader选举状态。
        // FOLLOWING：跟随者状态，表明当前服务器角色是Follower。
        // LEADING：领导者状态，表明当前服务器角色是Leader。
        // OBSERVING：观察者状态，表明当前服务器角色是Observer。
        LOOKING, FOLLOWING, LEADING, OBSERVING;
    }
    /**
     * server类型
     */
    public enum LearnerType {
        /** 能够参与一致性的投票，以及选举leader */
        PARTICIPANT,
        /** 表示观察者，不参与投票 */
        OBSERVER;
    }

    // 常量

    /** 表示observer，为了使观察者没有标识符，我们需要一个通用的标识符，至少对于QuorumCnxManager是这样，该常量作为这种通用标识符的值 */
    static final long OBSERVER_ID = Long.MAX_VALUE;
    public static final String FLE_TIME_UNIT = "MS";
    /**
     * The syncEnabled can also be set via a system property.
     */
    public static final String SYNC_ENABLED = "zookeeper.observer.syncEnabled";
    // TODO: need to tune the default value of thread size
    private static final int QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE = 20;
    public static final String CURRENT_EPOCH_FILENAME = "currentEpoch";
    public static final String ACCEPTED_EPOCH_FILENAME = "acceptedEpoch";

    // JMX

    private QuorumBean jmxQuorumBean;
    LocalPeerBean jmxLocalPeerBean;
    private Map<Long, RemotePeerBean> jmxRemotePeerBean;
    LeaderElectionBean jmxLeaderElectionBean;

    // 组件

    /** ZKDatabase是quorumpeer的顶级成员，它将在以后实例化的所有zookeeperservers中使用。而且，它只在启动时创建一次，只有在来自leader的消息被截断的情况下才会被丢弃 */
    private ZKDatabase zkDb;
    /** 用于与客户端建立TCP长连接 */
    ServerCnxnFactory cnxnFactory;
    /** 用于与客户端建立TCP长连接 */
    ServerCnxnFactory secureCnxnFactory;
    /** 供了一系列操作事务日志文件和快照数据文件的接口 */
    private FileTxnSnapLog logFactory = null;

    // 服务节点状态

    /** 表示该选举成员当前的状态 */
    private final QuorumStats quorumStats;
    /**
     * Default value of peer is participant
     */
    private LearnerType learnerType = LearnerType.PARTICIPANT;
    /** 表示当前的服务节点状态 */
    private ServerState state = ServerState.LOOKING;

    // 选举算法

    /** 选举算法类型编号，对应不同的electionAlg */
    private int electionType;
    /** 选举算法 */
    Election electionAlg;






    /** 选举leader时的网络IO管理器 */
    private QuorumCnxManager qcm;
    /** QuorumServer记录每个sid机器的地址，server间通信端口，以及server间选举leader端口属性 */
    QuorumAuthServer authServer;
    QuorumAuthLearner authLearner;



    /**
     * Record leader election time
     * fle = fast leader election
     */
    public long start_fle, end_fle;

    private String configFilename = null;
    public int getQuorumSize() {
        return getVotingView().size();
    }
    /** last committed quorum verifier */
    public QuorumVerifier quorumVerifier;
    /** last proposed quorum verifier */
    public QuorumVerifier lastSeenQuorumVerifier = null;
    /** Lock object that guard access to quorumVerifier and lastSeenQuorumVerifier. */
    final Object QV_LOCK = new Object();
    /** 当前机器myid(sid) */
    private long myid;
    /**
     * 表示当前该QuorumPeer认为的Leader，及当前的投票结果
     */
    volatile private Vote currentVote;
    private volatile boolean running = true;
    /** 每一秒的毫秒数, 参考：{@link QuorumPeerConfig#tickTime} */
    protected int tickTime;
    /** 此仲裁中的学习者是否应创建新的本地会话，默认情况下为False，以保留现有行为 */
    protected boolean localSessionsEnabled = false;
    /**
     * Whether learners in this quorum should upgrade local sessions to
     * global. Only matters if local sessions are enabled.
     */
    protected boolean localSessionsUpgradingEnabled = true;
    /** 参考：{@link QuorumPeerConfig#minSessionTimeout} */
    protected int minSessionTimeout = -1;
    /** 参考：{@link QuorumPeerConfig#maxSessionTimeout} */
    protected int maxSessionTimeout = -1;
    /** 初始同步阶段可以接受的节拍数，初始化阶段，learner和leader的通信读取超时时间，最长为initLimit*tickTime，参考：{@link QuorumPeerConfig#initLimit} */
    protected int initLimit;
    /** 在发送请求和获得确认之间可以传递的节拍数，在初始化阶段之后的请求阶段Learner和Leader通信的读取超时时间，参考：{@link QuorumPeerConfig#syncLimit} */
    protected int syncLimit;
    /**
     * Enables/Disables sync request processor. This option is enabled
     * by default and is to be used with observers.
     */
    protected boolean syncEnabled = true;
    /** 当前所处的ticktime周期数 */
    protected AtomicInteger tick = new AtomicInteger();
    /**
     * Whether or not to listen on all IPs for the two quorum ports
     * (broadcast and fast leader election).
     */
    protected boolean quorumListenOnAllIPs = false;
    /**
     * Keeps time taken for leader election in milliseconds. Sets the value to
     * this variable only after the completion of leader election.
     */
    private long electionTimeTaken = -1;
    /**
     * Enable/Disables quorum authentication using sasl. Defaulting to false.
     */
    protected boolean quorumSaslEnableAuth;
    /**
     * If this is false, quorum peer server will accept another quorum peer client
     * connection even if the authentication did not succeed. This can be used while
     * upgrading ZooKeeper server. Defaulting to false (required).
     */
    protected boolean quorumServerSaslAuthRequired;
    /**
     * If this is false, quorum peer learner will talk to quorum peer server
     * without authentication. This can be used while upgrading ZooKeeper
     * server. Defaulting to false (required).
     */
    protected boolean quorumLearnerSaslAuthRequired;
    /**
     * Kerberos quorum service principal. Defaulting to 'zkquorum/localhost'.
     */
    protected String quorumServicePrincipal;
    /**
     * Quorum learner login context name in jaas-conf file to read the kerberos
     * security details. Defaulting to 'QuorumLearner'.
     */
    protected String quorumLearnerLoginContext;
    /**
     * Quorum server login context name in jaas-conf file to read the kerberos
     * security details. Defaulting to 'QuorumServer'.
     */
    protected String quorumServerLoginContext;

    /**
     * The maximum number of threads to allow in the connectionExecutors thread
     * pool which will be used to initiate quorum server connections.
     */
    protected int quorumCnxnThreadsSize = QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE;
    private long acceptedEpoch = -1;
    private long currentEpoch = -1;


    // indicates that a reconfig just committed
    private boolean reconfigFlag = false;
    DatagramSocket udpSocket;
    /** 当前机器的地址 */
    private InetSocketAddress myQuorumAddr;
    private InetSocketAddress myElectionAddr = null;
    private InetSocketAddress myClientAddr = null;




    /**
     * 启动jetty服务器，用于通过调用url执行zookeeper命令，这个类封装了一个Jetty服务器来运行命令。
     *
     * 给定默认设置，启动ZooKeeper服务器并访问，
     * http://<hostname>:8080/commands，用于链接到所有已注册的命令。参照：http://<hostname>:8080/commands/<commandname>将执行相关的命令并在响应体中返回结果。
     * 命令的任何关键字参数都用URL参数指定(例如，http://localhost:8080/commands/set_trace_mask?traceMask=306)。
     */
    AdminServer adminServer;
    ResponderThread responder;

    public Follower follower;
    public Leader leader;
    public Observer observer;

    boolean shuttingDownLE = false;





    public QuorumPeer() throws SaslException {
        super("QuorumPeer");
        quorumStats = new QuorumStats(this);
        jmxRemotePeerBean = new HashMap<Long, RemotePeerBean>();
        adminServer = AdminServerFactory.createAdminServer();
        initialize();
    }
    /**
     * For backward compatibility purposes, we instantiate QuorumMaj by default.
     */
    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir, File dataLogDir, int electionType, long myid, int tickTime, int initLimit, int syncLimit, ServerCnxnFactory cnxnFactory) throws IOException {
        this(quorumPeers, dataDir, dataLogDir, electionType, myid, tickTime, initLimit, syncLimit, false, cnxnFactory, new QuorumMaj(quorumPeers));
    }
    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir, File dataLogDir, int electionType, long myid, int tickTime, int initLimit, int syncLimit, boolean quorumListenOnAllIPs, ServerCnxnFactory cnxnFactory, QuorumVerifier quorumConfig) throws IOException {
        this();
        this.cnxnFactory = cnxnFactory;
        this.electionType = electionType;
        this.myid = myid;
        this.tickTime = tickTime;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
        this.quorumListenOnAllIPs = quorumListenOnAllIPs;
        this.logFactory = new FileTxnSnapLog(dataLogDir, dataDir);
        this.zkDb = new ZKDatabase(this.logFactory);
        if (quorumConfig == null) quorumConfig = new QuorumMaj(quorumPeers);
        setQuorumVerifier(quorumConfig, false);
        adminServer = AdminServerFactory.createAdminServer();
    }
    /**
     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File snapDir, File logDir, int clientPort, int electionAlg, long myid, int tickTime, int initLimit, int syncLimit) throws IOException {
        this(quorumPeers, snapDir, logDir, electionAlg, myid, tickTime, initLimit, syncLimit, false,
                ServerCnxnFactory.createFactory(getClientAddress(quorumPeers, myid, clientPort), -1),
                new QuorumMaj(quorumPeers));
    }
    /**
     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File snapDir, File logDir, int clientPort, int electionAlg, long myid, int tickTime, int initLimit, int syncLimit, QuorumVerifier quorumConfig) throws IOException {
        this(quorumPeers, snapDir, logDir, electionAlg, myid, tickTime, initLimit, syncLimit, false,
                ServerCnxnFactory.createFactory(getClientAddress(quorumPeers, myid, clientPort), -1), quorumConfig);
    }

    /**
     * 初始化QuorumAuthServer和QuorumAuthLearner
     *
     * @throws SaslException
     */
    public void initialize() throws SaslException {
        // init quorum auth server & learner
        if (isQuorumSaslAuthEnabled()) {
            Set<String> authzHosts = new HashSet<String>();
            for (QuorumServer qs : getView().values()) {
                authzHosts.add(qs.hostname);
            }
            authServer = new SaslQuorumAuthServer(isQuorumServerSaslAuthRequired(), quorumServerLoginContext, authzHosts);
            authLearner = new SaslQuorumAuthLearner(isQuorumLearnerSaslAuthRequired(), quorumServicePrincipal, quorumLearnerLoginContext);
        } else {
            // 不是sasl不需要身份认证
            authServer = new NullQuorumAuthServer();
            authLearner = new NullQuorumAuthLearner();
        }
    }

    public static QuorumPeer testingQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }
    /**
     * Resolves hostname for a given server ID.
     *
     * This method resolves hostname for a given server ID in both quorumVerifer
     * and lastSeenQuorumVerifier. If the server ID matches the local server ID,
     * it also updates myQuorumAddr and myElectionAddr.
     */
    public void recreateSocketAddresses(long id) {
        QuorumVerifier qv = getQuorumVerifier();
        if (qv != null) {
            QuorumServer qs = qv.getAllMembers().get(id);
            if (qs != null) {
                qs.recreateSocketAddresses();
                if (id == getId()) {
                    setQuorumAddress(qs.addr);
                    setElectionAddress(qs.electionAddr);
                }
            }
        }
        qv = getLastSeenQuorumVerifier();
        if (qv != null) {
            QuorumServer qs = qv.getAllMembers().get(id);
            if (qs != null) {
                qs.recreateSocketAddresses();
            }
        }
    }



    @Override
    public synchronized void start() {
        if (!getView().containsKey(myid)) {
            throw new RuntimeException("My id " + myid + " not in the peer list");
        }

        // 加载zk内存数据库
        loadDataBase();
        // startServerCnxnFactory
        startServerCnxnFactory();
        try {
            adminServer.start();
        } catch (AdminServerException e) {
            LOG.warn("Problem starting AdminServer", e);
            System.out.println(e);
        }
        // 开始leader选举
        startLeaderElection();
        super.start();
    }

    /**
     * 将数据从磁盘加载到内存中，并将事物添加到内存中的提交日志中
     */
    private void loadDataBase() {
        try {
            zkDb.loadDataBase();

            // load the epochs
            long lastProcessedZxid = zkDb.getDataTree().lastProcessedZxid;
            long epochOfZxid = ZxidUtils.getEpochFromZxid(lastProcessedZxid);
            try {
                currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
            } catch (FileNotFoundException e) {
                // pick a reasonable epoch number
                // this should only happen once when moving to a
                // new code version
                currentEpoch = epochOfZxid;
                LOG.info(CURRENT_EPOCH_FILENAME + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation", currentEpoch);
                writeLongToFile(CURRENT_EPOCH_FILENAME, currentEpoch);
            }

            if (epochOfZxid > currentEpoch) {
                throw new IOException("The current epoch, " + ZxidUtils.zxidToString(currentEpoch) + ", is older than the last zxid, " + lastProcessedZxid);
            }

            try {
                acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
            } catch (FileNotFoundException e) {
                // pick a reasonable epoch number
                // this should only happen once when moving to a
                // new code version
                acceptedEpoch = epochOfZxid;
                LOG.info(ACCEPTED_EPOCH_FILENAME + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation", acceptedEpoch);
                writeLongToFile(ACCEPTED_EPOCH_FILENAME, acceptedEpoch);
            }

            if (acceptedEpoch < currentEpoch) {
                throw new IOException("The accepted epoch, " + ZxidUtils.zxidToString(acceptedEpoch) + " is less than the current epoch, " + ZxidUtils.zxidToString(currentEpoch));
            }
        } catch (IOException ie) {
            LOG.error("Unable to load database on disk", ie);
            throw new RuntimeException("Unable to run quorum server ", ie);
        }
    }

    synchronized public void stopLeaderElection() {
        responder.running = false;
        responder.interrupt();
    }

    /**
     * 开始leader选举
     */
    synchronized public void startLeaderElection() {
        try {
            if (getPeerState() == ServerState.LOOKING) {
                currentVote = new Vote(myid, getLastLoggedZxid(), getCurrentEpoch());
            }
        } catch (IOException e) {
            RuntimeException re = new RuntimeException(e.getMessage());
            re.setStackTrace(e.getStackTrace());
            throw re;
        }

        // 确定选举算法
        if (electionType == 0) {
            try {
                udpSocket = new DatagramSocket(myQuorumAddr.getPort());
                responder = new ResponderThread();
                responder.start();
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
        this.electionAlg = createElectionAlgorithm(electionType);
    }

    /**
     * Count the number of nodes in the map that could be followers.
     * @param peers
     * @return The number of followers in the map
     */
    protected static int countParticipants(Map<Long, QuorumServer> peers) {
        int count = 0;
        for (QuorumServer q : peers.values()) {
            if (q.type == LearnerType.PARTICIPANT) {
                count++;
            }
        }
        return count;
    }

    private static InetSocketAddress getClientAddress(Map<Long, QuorumServer> quorumPeers, long myid, int clientPort) throws IOException {
        QuorumServer quorumServer = quorumPeers.get(myid);
        if (null == quorumServer) {
            throw new IOException("No QuorumServer correspoding to myid " + myid);
        }
        if (null == quorumServer.clientAddr) {
            return new InetSocketAddress(clientPort);
        }
        if (quorumServer.clientAddr.getPort() != clientPort) {
            throw new IOException("QuorumServer port " + quorumServer.clientAddr.getPort()
                    + " does not match with given port " + clientPort);
        }
        return quorumServer.clientAddr;
    }

    /**
     * returns the highest zxid that this host has seen
     *
     * @return the highest zxid for this host
     */
    public long getLastLoggedZxid() {
        if (!zkDb.isInitialized()) {
            loadDataBase();
        }
        return zkDb.getDataTreeLastProcessedZxid();
    }

    protected Follower makeFollower(FileTxnSnapLog logFactory) throws IOException {
        return new Follower(this, new FollowerZooKeeperServer(logFactory, this, this.zkDb));
    }

    protected Leader makeLeader(FileTxnSnapLog logFactory) throws IOException {
        return new Leader(this, new LeaderZooKeeperServer(logFactory, this, this.zkDb));
    }

    protected Observer makeObserver(FileTxnSnapLog logFactory) throws IOException {
        return new Observer(this, new ObserverZooKeeperServer(logFactory, this, this.zkDb));
    }

    /**
     * 根据选举算法类型编号确定选举算法
     *
     * @param electionAlgorithm
     * @return
     */
    @SuppressWarnings("deprecation")
    protected Election createElectionAlgorithm(int electionAlgorithm) {
        Election le = null;

        //TODO: use a factory rather than a switch
        switch (electionAlgorithm) {
            case 0:
                le = new LeaderElection(this);
                break;
            case 1:
                le = new AuthFastLeaderElection(this);
                break;
            case 2:
                le = new AuthFastLeaderElection(this, true);
                break;
            case 3:
                qcm = createCnxnManager();
                QuorumCnxManager.Listener listener = qcm.listener;
                if (listener != null) {
                    listener.start();
                    FastLeaderElection fle = new FastLeaderElection(this, qcm);
                    fle.start();
                    le = fle;
                } else {
                    LOG.error("Null listener when initializing cnx manager");
                }
                break;
            default:
                assert false;
        }
        return le;
    }

    @SuppressWarnings("deprecation")
    protected Election makeLEStrategy() {
        LOG.debug("Initializing leader election protocol...");
        if (getElectionType() == 0) {
            electionAlg = new LeaderElection(this);
        }
        return electionAlg;
    }

    @Override
    public void run() {
        // 更新线程名称
        updateThreadName();

        LOG.debug("Starting quorum peer");
        try {
            jmxQuorumBean = new QuorumBean(this);
            MBeanRegistry.getInstance().register(jmxQuorumBean, null);


            for (QuorumServer s : getView().values()) {
                ZKMBeanInfo p;
                if (getId() == s.id) {
                    p = jmxLocalPeerBean = new LocalPeerBean(this);
                    try {
                        MBeanRegistry.getInstance().register(p, jmxQuorumBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                        jmxLocalPeerBean = null;
                    }
                } else {
                    RemotePeerBean rBean = new RemotePeerBean(s);
                    try {
                        MBeanRegistry.getInstance().register(rBean, jmxQuorumBean);
                        jmxRemotePeerBean.put(s.id, rBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxQuorumBean = null;
        }

        try {
            while (running) {
                switch (getPeerState()) {
                    case LOOKING:
                        LOG.info("LOOKING");

                        if (Boolean.getBoolean("readonlymode.enabled")) {
                            LOG.info("Attempting to start ReadOnlyZooKeeperServer");

                            // Create read-only server but don't start it immediately
                            final ReadOnlyZooKeeperServer roZk =
                                    new ReadOnlyZooKeeperServer(logFactory, this, this.zkDb);

                            // Instead of starting roZk immediately, wait some grace
                            // period before we decide we're partitioned.
                            //
                            // Thread is used here because otherwise it would require
                            // changes in each of election strategy classes which is
                            // unnecessary code coupling.
                            Thread roZkMgr = new Thread() {
                                public void run() {
                                    try {
                                        // lower-bound grace period to 2 secs
                                        sleep(Math.max(2000, tickTime));
                                        if (ServerState.LOOKING.equals(getPeerState())) {
                                            roZk.startup();
                                        }
                                    } catch (InterruptedException e) {
                                        LOG.info("Interrupted while attempting to start ReadOnlyZooKeeperServer, not started");
                                    } catch (Exception e) {
                                        LOG.error("FAILED to start ReadOnlyZooKeeperServer", e);
                                    }
                                }
                            };
                            try {
                                roZkMgr.start();
                                reconfigFlagClear();
                                if (shuttingDownLE) {
                                    shuttingDownLE = false;
                                    startLeaderElection();
                                }
                                setCurrentVote(makeLEStrategy().lookForLeader());
                            } catch (Exception e) {
                                LOG.warn("Unexpected exception", e);
                                setPeerState(ServerState.LOOKING);
                            } finally {
                                // If the thread is in the the grace period, interrupt
                                // to come out of waiting.
                                roZkMgr.interrupt();
                                roZk.shutdown();
                            }
                        } else {
                            try {
                                reconfigFlagClear();
                                if (shuttingDownLE) {
                                    shuttingDownLE = false;
                                    startLeaderElection();
                                }
                                setCurrentVote(makeLEStrategy().lookForLeader());
                            } catch (Exception e) {
                                LOG.warn("Unexpected exception", e);
                                setPeerState(ServerState.LOOKING);
                            }
                        }
                        break;
                    case OBSERVING:
                        try {
                            LOG.info("OBSERVING");
                            setObserver(makeObserver(logFactory));
                            observer.observeLeader();
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            observer.shutdown();
                            setObserver(null);
                            updateServerState();
                        }
                        break;
                    case FOLLOWING:
                        try {
                            LOG.info("FOLLOWING");
                            setFollower(makeFollower(logFactory));
                            follower.followLeader();
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            follower.shutdown();
                            setFollower(null);
                            updateServerState();
                        }
                        break;
                    case LEADING:
                        LOG.info("LEADING");
                        try {
                            setLeader(makeLeader(logFactory));
                            leader.lead();
                            setLeader(null);
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            if (leader != null) {
                                leader.shutdown("Forcing shutdown");
                                setLeader(null);
                            }
                            updateServerState();
                        }
                        break;
                }
                start_fle = Time.currentElapsedTime();
            }
        } finally {
            LOG.warn("QuorumPeer main thread exited");
            MBeanRegistry instance = MBeanRegistry.getInstance();
            instance.unregister(jmxQuorumBean);
            instance.unregister(jmxLocalPeerBean);

            for (RemotePeerBean remotePeerBean : jmxRemotePeerBean.values()) {
                instance.unregister(remotePeerBean);
            }

            jmxQuorumBean = null;
            jmxLocalPeerBean = null;
            jmxRemotePeerBean = null;
        }
    }

    private synchronized void updateServerState() {
        if (!reconfigFlag) {
            setPeerState(ServerState.LOOKING);
            LOG.warn("PeerState set to LOOKING");
            return;
        }

        if (getId() == getCurrentVote().getId()) {
            setPeerState(ServerState.LEADING);
            LOG.debug("PeerState set to LEADING");
        } else if (getLearnerType() == LearnerType.PARTICIPANT) {
            setPeerState(ServerState.FOLLOWING);
            LOG.debug("PeerState set to FOLLOWING");
        } else if (getLearnerType() == LearnerType.OBSERVER) {
            setPeerState(ServerState.OBSERVING);
            LOG.debug("PeerState set to OBSERVER");
        } else { // currently shouldn't happen since there are only 2 learner types
            setPeerState(ServerState.LOOKING);
            LOG.debug("Shouldn't be here");
        }
        reconfigFlag = false;
    }

    public void shutdown() {
        running = false;
        if (leader != null) {
            leader.shutdown("quorum Peer shutdown");
        }
        if (follower != null) {
            follower.shutdown();
        }
        shutdownServerCnxnFactory();
        if (udpSocket != null) {
            udpSocket.close();
        }

        try {
            adminServer.shutdown();
        } catch (AdminServerException e) {
            LOG.warn("Problem stopping AdminServer", e);
        }

        if (getElectionAlg() != null) {
            this.interrupt();
            getElectionAlg().shutdown();
        }
        try {
            zkDb.close();
        } catch (IOException ie) {
            LOG.warn("Error closing logs ", ie);
        }
    }

    /**
     * A 'view' is a node's current opinion of the membership of the entire ensemble.
     */
    public Map<Long, QuorumPeer.QuorumServer> getView() {
        return Collections.unmodifiableMap(getQuorumVerifier().getAllMembers());
    }

    /**
     * Observers are not contained in this view, only nodes with
     * PeerType=PARTICIPANT.
     */
    public Map<Long, QuorumPeer.QuorumServer> getVotingView() {
        return getQuorumVerifier().getVotingMembers();
    }

    /**
     * Returns only observers, no followers.
     */
    public Map<Long, QuorumPeer.QuorumServer> getObservingView() {
        return getQuorumVerifier().getObservingMembers();
    }

    public synchronized Set<Long> getCurrentAndNextConfigVoters() {
        Set<Long> voterIds = new HashSet<Long>(getQuorumVerifier()
                .getVotingMembers().keySet());
        if (getLastSeenQuorumVerifier() != null) {
            voterIds.addAll(getLastSeenQuorumVerifier().getVotingMembers()
                    .keySet());
        }
        return voterIds;
    }

    /**
     * Check if a node is in the current view. With static membership, the
     * result of this check will never change; only when dynamic membership
     * is introduced will this be more useful.
     */
    public boolean viewContains(Long sid) {
        return this.getView().containsKey(sid);
    }


    // Provider接口

    /**
     * 目前只被 QuorumStats 使用
     */
    public String[] getQuorumPeers() {
        List<String> l = new ArrayList<String>();
        synchronized (this) {
            if (leader != null) {
                for (LearnerHandler fh : leader.getLearners()) {
                    if (fh.getSocket() != null) {
                        String s = fh.getSocket().getRemoteSocketAddress().toString();
                        if (leader.isLearnerSynced(fh))
                            s += "*";
                        l.add(s);
                    }
                }
            } else if (follower != null) {
                l.add(follower.sock.getRemoteSocketAddress().toString());
            }
        }

        // 将List转为array
        return l.toArray(new String[0]);
    }
    /**
     * 返回服务器的选举状态
     *
     * @return
     */
    public String getServerState() {
        switch (getPeerState()) {
            case LOOKING:
                return QuorumStats.Provider.LOOKING_STATE;
            case LEADING:
                return QuorumStats.Provider.LEADING_STATE;
            case FOLLOWING:
                return QuorumStats.Provider.FOLLOWING_STATE;
            case OBSERVING:
                return QuorumStats.Provider.OBSERVING_STATE;
        }
        return QuorumStats.Provider.UNKNOWN_STATE;
    }




    public QuorumVerifier configFromString(String s) throws IOException, ConfigException {
        Properties props = new Properties();
        props.load(new StringReader(s));
        return QuorumPeerConfig.parseDynamicConfig(props, electionType, false, false);
    }

    /**
     * 返回最后提交配置的QuorumVerifier对象
     */
    public QuorumVerifier getQuorumVerifier() {
        synchronized (QV_LOCK) {
            return quorumVerifier;
        }
    }

    /**
     * Return QuorumVerifier object for the last proposed configuration.
     */
    public QuorumVerifier getLastSeenQuorumVerifier() {
        synchronized (QV_LOCK) {
            return lastSeenQuorumVerifier;
        }
    }

    private void connectNewPeers() {
        synchronized (QV_LOCK) {
            if (qcm != null && quorumVerifier != null && lastSeenQuorumVerifier != null) {
                Map<Long, QuorumServer> committedView = quorumVerifier.getAllMembers();
                for (Entry<Long, QuorumServer> e : lastSeenQuorumVerifier.getAllMembers().entrySet()) {
                    if (e.getKey() != getId() && !committedView.containsKey(e.getKey()))
                        qcm.connectOne(e.getKey());
                }
            }
        }
    }

    public synchronized void restartLeaderElection(QuorumVerifier qvOLD, QuorumVerifier qvNEW) {
        if (qvOLD == null || !qvOLD.equals(qvNEW)) {
            LOG.warn("Restarting Leader Election");
            getElectionAlg().shutdown();
            shuttingDownLE = false;
            startLeaderElection();
        }
    }

    public String getNextDynamicConfigFilename() {
        if (configFilename == null) {
            LOG.warn("configFilename is null! This should only happen in tests.");
            return null;
        }
        return configFilename + QuorumPeerConfig.nextDynamicConfigFileSuffix;
    }

    public void setLastSeenQuorumVerifier(QuorumVerifier qv, boolean writeToDisk) {
        synchronized (QV_LOCK) {
            if (lastSeenQuorumVerifier != null && lastSeenQuorumVerifier.getVersion() > qv.getVersion()) {
                LOG.error("setLastSeenQuorumVerifier called with stale config " + qv.getVersion() +
                        ". Current version: " + quorumVerifier.getVersion());

            }
            // assuming that a version uniquely identifies a configuration, so if
            // version is the same, nothing to do here.
            if (lastSeenQuorumVerifier != null &&
                    lastSeenQuorumVerifier.getVersion() == qv.getVersion()) {
                return;
            }
            lastSeenQuorumVerifier = qv;
            connectNewPeers();
            if (writeToDisk) {
                try {
                    String fileName = getNextDynamicConfigFilename();
                    if (fileName != null) {
                        QuorumPeerConfig.writeDynamicConfig(fileName, qv, true);
                    }
                } catch (IOException e) {
                    LOG.error("Error writing next dynamic config file to disk: ", e.getMessage());
                }
            }
        }
    }

    public QuorumVerifier setQuorumVerifier(QuorumVerifier qv, boolean writeToDisk) {
        synchronized (QV_LOCK) {
            if ((quorumVerifier != null) && (quorumVerifier.getVersion() >= qv.getVersion())) {
                // this is normal. For example - server found out about new config through FastLeaderElection gossiping
                // and then got the same config in UPTODATE message so its already known
                LOG.debug(getId() + " setQuorumVerifier called with known or old config " + qv.getVersion() +
                        ". Current version: " + quorumVerifier.getVersion());
                return quorumVerifier;
            }
            QuorumVerifier prevQV = quorumVerifier;
            quorumVerifier = qv;
            if (lastSeenQuorumVerifier == null || (qv.getVersion() > lastSeenQuorumVerifier.getVersion()))
                lastSeenQuorumVerifier = qv;

            if (writeToDisk) {
                // some tests initialize QuorumPeer without a static config file
                if (configFilename != null) {
                    try {
                        String dynamicConfigFilename = makeDynamicConfigFilename(
                                qv.getVersion());
                        QuorumPeerConfig.writeDynamicConfig(
                                dynamicConfigFilename, qv, false);
                        QuorumPeerConfig.editStaticConfig(configFilename,
                                dynamicConfigFilename,
                                needEraseClientInfoFromStaticConfig());
                    } catch (IOException e) {
                        LOG.error("Error closing file: ", e.getMessage());
                    }
                } else {
                    LOG.info("writeToDisk == true but configFilename == null");
                }
            }

            if (qv.getVersion() == lastSeenQuorumVerifier.getVersion()) {
                QuorumPeerConfig.deleteFile(getNextDynamicConfigFilename());
            }
            QuorumServer qs = qv.getAllMembers().get(getId());
            if (qs != null) {
                setQuorumAddress(qs.addr);
                setElectionAddress(qs.electionAddr);
                setClientAddress(qs.clientAddr);
            }
            return prevQV;
        }
    }

    private String makeDynamicConfigFilename(long version) {
        return configFilename + ".dynamic." + Long.toHexString(version);
    }

    private boolean needEraseClientInfoFromStaticConfig() {
        QuorumServer server = quorumVerifier.getAllMembers().get(getId());
        return (server != null && server.clientAddr != null);
    }

    synchronized public ZooKeeperServer getActiveServer() {
        if (leader != null)
            return leader.zk;
        else if (follower != null)
            return follower.zk;
        else if (observer != null)
            return observer.zk;
        return null;
    }

    /**
     * startServerCnxnFactory
     */
    private void startServerCnxnFactory() {
        if (cnxnFactory != null) {
            cnxnFactory.start();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.start();
        }
    }

    private void shutdownServerCnxnFactory() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.shutdown();
        }
    }

    // Leader and learner will control the zookeeper server and pass it into QuorumPeer.
    public void setZooKeeperServer(ZooKeeperServer zks) {
        if (cnxnFactory != null) {
            cnxnFactory.setZooKeeperServer(zks);
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.setZooKeeperServer(zks);
        }
    }

    public void closeAllConnections() {
        if (cnxnFactory != null) {
            cnxnFactory.closeAll();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.closeAll();
        }
    }

    /**
     * 初始化dataTree中"/zookeeper/config"的节点信息
     */
    public synchronized void initConfigInZKDatabase() {
        if (zkDb != null) zkDb.initConfigInZKDatabase(getQuorumVerifier());
    }

    private long readLongFromFile(String name) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = "";
        try {
            line = br.readLine();
            return Long.parseLong(line);
        } catch (NumberFormatException e) {
            throw new IOException("Found " + line + " in " + file);
        } finally {
            br.close();
        }
    }

    /**
     * Write a long value to disk atomically. Either succeeds or an exception
     * is thrown.
     * @param name file name to write the long to
     * @param value the long value to write to the named file
     * @throws IOException if the file cannot be written atomically
     */
    private void writeLongToFile(String name, final long value) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        new AtomicFileWritingIdiom(file, new WriterStatement() {
            @Override
            public void write(Writer bw) throws IOException {
                bw.write(Long.toString(value));
            }
        });
    }

    public boolean processReconfig(QuorumVerifier qv, Long suggestedLeaderId, Long zxid, boolean restartLE) {
        if (!QuorumPeerConfig.isReconfigEnabled()) {
            LOG.debug("Reconfig feature is disabled, skip reconfig processing.");
            return false;
        }

        InetSocketAddress oldClientAddr = getClientAddress();

        // update last committed quorum verifier, write the new config to disk
        // and restart leader election if config changed.
        QuorumVerifier prevQV = setQuorumVerifier(qv, true);

        // There is no log record for the initial config, thus after syncing
        // with leader
        // /zookeeper/config is empty! it is also possible that last committed
        // config is propagated during leader election
        // without the propagation the corresponding log records.
        // so we should explicitly do this (this is not necessary when we're
        // already a Follower/Observer, only
        // for Learner):
        initConfigInZKDatabase();

        if (prevQV.getVersion() < qv.getVersion() && !prevQV.equals(qv)) {
            Map<Long, QuorumServer> newMembers = qv.getAllMembers();
            updateRemotePeerMXBeans(newMembers);
            if (restartLE) restartLeaderElection(prevQV, qv);

            QuorumServer myNewQS = newMembers.get(getId());
            if (myNewQS != null && myNewQS.clientAddr != null
                    && !myNewQS.clientAddr.equals(oldClientAddr)) {
                cnxnFactory.reconfigure(myNewQS.clientAddr);
                updateThreadName();
            }

            boolean roleChange = updateLearnerType(qv);
            boolean leaderChange = false;
            if (suggestedLeaderId != null) {
                // zxid should be non-null too
                leaderChange = updateVote(suggestedLeaderId, zxid);
            } else {
                long currentLeaderId = getCurrentVote().getId();
                QuorumServer myleaderInCurQV = prevQV.getVotingMembers().get(currentLeaderId);
                QuorumServer myleaderInNewQV = qv.getVotingMembers().get(currentLeaderId);
                leaderChange = (myleaderInCurQV == null || myleaderInCurQV.addr == null ||
                        myleaderInNewQV == null || !myleaderInCurQV.addr.equals(myleaderInNewQV.addr));
                // we don't have a designated leader - need to go into leader
                // election
                reconfigFlagClear();
            }

            if (roleChange || leaderChange) {
                return true;
            }
        }
        return false;

    }

    private void updateRemotePeerMXBeans(Map<Long, QuorumServer> newMembers) {
        Set<Long> existingMembers = new HashSet<Long>(newMembers.keySet());
        existingMembers.retainAll(jmxRemotePeerBean.keySet());
        for (Long id : existingMembers) {
            RemotePeerBean rBean = jmxRemotePeerBean.get(id);
            rBean.setQuorumServer(newMembers.get(id));
        }

        Set<Long> joiningMembers = new HashSet<Long>(newMembers.keySet());
        joiningMembers.removeAll(jmxRemotePeerBean.keySet());
        joiningMembers.remove(getId()); // remove self as it is local bean
        for (Long id : joiningMembers) {
            QuorumServer qs = newMembers.get(id);
            RemotePeerBean rBean = new RemotePeerBean(qs);
            try {
                MBeanRegistry.getInstance().register(rBean, jmxQuorumBean);
                jmxRemotePeerBean.put(qs.id, rBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
        }

        Set<Long> leavingMembers = new HashSet<Long>(jmxRemotePeerBean.keySet());
        leavingMembers.removeAll(newMembers.keySet());
        for (Long id : leavingMembers) {
            RemotePeerBean rBean = jmxRemotePeerBean.remove(id);
            try {
                MBeanRegistry.getInstance().unregister(rBean);
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
        }
    }

    private boolean updateLearnerType(QuorumVerifier newQV) {
        //check if I'm an observer in new config
        if (newQV.getObservingMembers().containsKey(getId())) {
            if (getLearnerType() != LearnerType.OBSERVER) {
                setLearnerType(LearnerType.OBSERVER);
                LOG.info("Becoming an observer");
                reconfigFlagSet();
                return true;
            } else {
                return false;
            }
        } else if (newQV.getVotingMembers().containsKey(getId())) {
            if (getLearnerType() != LearnerType.PARTICIPANT) {
                setLearnerType(LearnerType.PARTICIPANT);
                LOG.info("Becoming a voting participant");
                reconfigFlagSet();
                return true;
            } else {
                return false;
            }
        }
        // I'm not in the view
        if (getLearnerType() != LearnerType.PARTICIPANT) {
            setLearnerType(LearnerType.PARTICIPANT);
            LOG.info("Becoming a non-voting participant");
            reconfigFlagSet();
            return true;
        }
        return false;
    }

    private boolean updateVote(long designatedLeader, long zxid) {
        Vote currentVote = getCurrentVote();
        if (currentVote != null && designatedLeader != currentVote.getId()) {
            setCurrentVote(new Vote(designatedLeader, zxid));
            reconfigFlagSet();
            LOG.warn("Suggested leader: " + designatedLeader);
            return true;
        }
        return false;
    }

    /**
     * Updates leader election info to avoid inconsistencies when
     * a new server tries to join the ensemble.
     *
     * see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
     */
    protected void updateElectionVote(long newEpoch) {
        Vote currentVote = getCurrentVote();
        if (currentVote != null) {
            setCurrentVote(new Vote(currentVote.getId(),
                    currentVote.getZxid(),
                    currentVote.getElectionEpoch(),
                    newEpoch,
                    currentVote.getState()));
        }
    }

    private void updateThreadName() {
        String plain = cnxnFactory != null ?
                cnxnFactory.getLocalAddress() != null ? cnxnFactory.getLocalAddress().toString() : "disabled"
                : "disabled";
        String secure = secureCnxnFactory != null ? secureCnxnFactory.getLocalAddress().toString() : "disabled";
        // 设置线程名称
        setName(String.format("QuorumPeer[myid=%d](plain=%s)(secure=%s)", getId(), plain, secure));
    }

    /**
     * 实例化一个QuorumCnxManager（选举leader时的网络IO管理器）
     *
     * @return
     */
    public QuorumCnxManager createCnxnManager() {
        return new QuorumCnxManager(this,
                this.getId(),
                this.getView(),
                this.authServer,
                this.authLearner,
                this.tickTime * this.syncLimit,
                this.getQuorumListenOnAllIPs(),
                this.quorumCnxnThreadsSize,
                this.isQuorumSaslAuthEnabled());
    }

    public long getCurrentEpoch() throws IOException {
        if (currentEpoch == -1) {
            currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
        }
        return currentEpoch;
    }
    public long getAcceptedEpoch() throws IOException {
        if (acceptedEpoch == -1) {
            acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
        }
        return acceptedEpoch;
    }






    // getter and setter ...

    boolean isQuorumSaslAuthEnabled() {
        return quorumSaslEnableAuth;
    }
    private boolean isQuorumServerSaslAuthRequired() {
        return quorumServerSaslAuthRequired;
    }
    private boolean isQuorumLearnerSaslAuthRequired() {
        return quorumLearnerSaslAuthRequired;
    }
    /**
     * Sets the time taken for leader election in milliseconds.
     *
     * @param electionTimeTaken time taken for leader election
     */
    void setElectionTimeTaken(long electionTimeTaken) {
        this.electionTimeTaken = electionTimeTaken;
    }
    /**
     * @return the time taken for leader election in milliseconds.
     */
    long getElectionTimeTaken() {
        return electionTimeTaken;
    }
    void setQuorumServerSaslRequired(boolean serverSaslRequired) {
        quorumServerSaslAuthRequired = serverSaslRequired;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED, serverSaslRequired);
    }
    void setQuorumLearnerSaslRequired(boolean learnerSaslRequired) {
        quorumLearnerSaslAuthRequired = learnerSaslRequired;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED, learnerSaslRequired);
    }
    void setQuorumSaslEnabled(boolean enableAuth) {
        quorumSaslEnableAuth = enableAuth;
        if (!quorumSaslEnableAuth) {
            LOG.info("QuorumPeer communication is not secured!");
        } else {
            LOG.info("{} set to {}", QuorumAuth.QUORUM_SASL_AUTH_ENABLED, enableAuth);
        }
    }
    void setQuorumServicePrincipal(String servicePrincipal) {
        quorumServicePrincipal = servicePrincipal;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL, quorumServicePrincipal);
    }
    void setQuorumLearnerLoginContext(String learnerContext) {
        quorumLearnerLoginContext = learnerContext;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT, quorumLearnerLoginContext);
    }
    void setQuorumServerLoginContext(String serverContext) {
        quorumServerLoginContext = serverContext;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT, quorumServerLoginContext);
    }
    void setQuorumCnxnThreadsSize(int qCnxnThreadsSize) {
        if (qCnxnThreadsSize > QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE) {
            quorumCnxnThreadsSize = qCnxnThreadsSize;
        }
        LOG.info("quorum.cnxn.threads.size set to {}", quorumCnxnThreadsSize);
    }
    public void setCurrentEpoch(long e) throws IOException {
        currentEpoch = e;
        writeLongToFile(CURRENT_EPOCH_FILENAME, e);

    }
    public void setAcceptedEpoch(long e) throws IOException {
        acceptedEpoch = e;
        writeLongToFile(ACCEPTED_EPOCH_FILENAME, e);
    }
    public int getClientPort() {
        if (cnxnFactory != null) {
            return cnxnFactory.getLocalPort();
        }
        return -1;
    }
    public void setTxnFactory(FileTxnSnapLog factory) {
        this.logFactory = factory;
    }
    public FileTxnSnapLog getTxnFactory() {
        return this.logFactory;
    }
    /**
     * set zk database for this node
     * @param database
     */
    public void setZKDatabase(ZKDatabase database) {
        this.zkDb = database;
    }
    protected ZKDatabase getZkDb() {
        return zkDb;
    }
    public boolean isRunning() {
        return running;
    }
    /**
     * get reference to QuorumCnxManager
     */
    public QuorumCnxManager getQuorumCnxManager() {
        return qcm;
    }
    /**
     * Return syncEnabled.
     *
     * @return
     */
    public boolean getSyncEnabled() {
        if (System.getProperty(SYNC_ENABLED) != null) {
            LOG.info(SYNC_ENABLED + "=" + Boolean.getBoolean(SYNC_ENABLED));
            return Boolean.getBoolean(SYNC_ENABLED);
        } else {
            return syncEnabled;
        }
    }
    /**
     * Set syncEnabled.
     *
     * @param syncEnabled
     */
    public void setSyncEnabled(boolean syncEnabled) {
        this.syncEnabled = syncEnabled;
    }
    /**
     * Gets the election type
     */
    public int getElectionType() {
        return electionType;
    }
    /**
     * Sets the election type
     */
    public void setElectionType(int electionType) {
        this.electionType = electionType;
    }
    public boolean getQuorumListenOnAllIPs() {
        return quorumListenOnAllIPs;
    }
    public void setQuorumListenOnAllIPs(boolean quorumListenOnAllIPs) {
        this.quorumListenOnAllIPs = quorumListenOnAllIPs;
    }
    public void setCnxnFactory(ServerCnxnFactory cnxnFactory) {
        this.cnxnFactory = cnxnFactory;
    }
    public void setSecureCnxnFactory(ServerCnxnFactory secureCnxnFactory) {
        this.secureCnxnFactory = secureCnxnFactory;
    }
    /**
     * set the id of this quorum peer.
     */
    public void setMyid(long myid) {
        this.myid = myid;
    }
    /**
     * Get the number of milliseconds of each tick
     */
    public int getTickTime() {
        return tickTime;
    }
    /**
     * Set the number of milliseconds of each tick
     */
    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }
    /** Maximum number of connections allowed from particular host (ip) */
    public int getMaxClientCnxnsPerHost() {
        if (cnxnFactory != null) {
            return cnxnFactory.getMaxClientCnxnsPerHost();
        }
        if (secureCnxnFactory != null) {
            return secureCnxnFactory.getMaxClientCnxnsPerHost();
        }
        return -1;
    }
    /** Whether local sessions are enabled */
    public boolean areLocalSessionsEnabled() {
        return localSessionsEnabled;
    }
    /** Whether to enable local sessions */
    public void enableLocalSessions(boolean flag) {
        LOG.info("Local sessions " + (flag ? "enabled" : "disabled"));
        localSessionsEnabled = flag;
    }
    /** Whether local sessions are allowed to upgrade to global sessions */
    public boolean isLocalSessionsUpgradingEnabled() {
        return localSessionsUpgradingEnabled;
    }
    /** Whether to allow local sessions to upgrade to global sessions */
    public void enableLocalSessionsUpgrading(boolean flag) {
        LOG.info("Local session upgrading " + (flag ? "enabled" : "disabled"));
        localSessionsUpgradingEnabled = flag;
    }
    /** minimum session timeout in milliseconds */
    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }
    /** minimum session timeout in milliseconds */
    public void setMinSessionTimeout(int min) {
        LOG.info("minSessionTimeout set to " + min);
        this.minSessionTimeout = min;
    }
    /** maximum session timeout in milliseconds */
    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }
    /** maximum session timeout in milliseconds */
    public void setMaxSessionTimeout(int max) {
        LOG.info("maxSessionTimeout set to " + max);
        this.maxSessionTimeout = max;
    }
    /**
     * Get the number of ticks that the initial synchronization phase can take
     */
    public int getInitLimit() {
        return initLimit;
    }
    /**
     * Set the number of ticks that the initial synchronization phase can take
     */
    public void setInitLimit(int initLimit) {
        LOG.info("initLimit set to " + initLimit);
        this.initLimit = initLimit;
    }
    /**
     * Get the current tick
     */
    public int getTick() {
        return tick.get();
    }
    public InetSocketAddress getQuorumAddress() {
        synchronized (QV_LOCK) {
            return myQuorumAddr;
        }
    }
    public void setQuorumAddress(InetSocketAddress addr) {
        synchronized (QV_LOCK) {
            myQuorumAddr = addr;
        }
    }
    public InetSocketAddress getElectionAddress() {
        synchronized (QV_LOCK) {
            return myElectionAddr;
        }
    }
    public void setElectionAddress(InetSocketAddress addr) {
        synchronized (QV_LOCK) {
            myElectionAddr = addr;
        }
    }
    public InetSocketAddress getClientAddress() {
        synchronized (QV_LOCK) {
            return myClientAddr;
        }
    }
    public void setClientAddress(InetSocketAddress addr) {
        synchronized (QV_LOCK) {
            myClientAddr = addr;
        }
    }
    /**
     * Get an instance of LeaderElection
     */
    public Election getElectionAlg() {
        return electionAlg;
    }
    /**
     * Get the synclimit
     */
    public int getSyncLimit() {
        return syncLimit;
    }
    /**
     * Set the synclimit
     */
    public void setSyncLimit(int syncLimit) {
        this.syncLimit = syncLimit;
    }
    public synchronized void setPeerState(ServerState newState) {
        state = newState;
    }
    public synchronized void reconfigFlagSet() {
        reconfigFlag = true;
    }
    public synchronized void reconfigFlagClear() {
        reconfigFlag = false;
    }
    public synchronized boolean isReconfigStateChange() {
        return reconfigFlag;
    }
    public synchronized ServerState getPeerState() {
        return state;
    }
    public synchronized Vote getCurrentVote() {
        return currentVote;
    }
    public synchronized void setCurrentVote(Vote v) {
        currentVote = v;
    }
    public LearnerType getLearnerType() {
        return learnerType;
    }
    /**
     * Sets the LearnerType
     */
    public void setLearnerType(LearnerType p) {
        learnerType = p;
    }
    protected synchronized void setConfigFileName(String s) {
        configFilename = s;
    }
    /**
     * get the id of this quorum peer.
     */
    public long getId() {
        return myid;
    }
    QuorumStats quorumStats() {
        return quorumStats;
    }
    synchronized protected void setLeader(Leader newLeader) {
        leader = newLeader;
    }
    synchronized protected void setFollower(Follower newFollower) {
        follower = newFollower;
    }
    synchronized protected void setObserver(Observer newObserver) {
        observer = newObserver;
    }


    /**
     * 这个类就是记录每一个zk配置中server的主要信息
     */
    public static class QuorumServer {

        public InetSocketAddress addr = null;

        /** 选举之前，各server参与选举的port */
        public InetSocketAddress electionAddr = null;

        public InetSocketAddress clientAddr = null;

        /** 机器的serverId */
        public long id;

        public String hostname;

        /** server类型，默认PARTICIPANT表示能够参与一致性的投票，以及选举leader */
        public LearnerType type = LearnerType.PARTICIPANT;

        private List<InetSocketAddress> myAddrs;

        private static final String wrongFormat = " does not have the form server_config or server_config;client_config" +
                " where server_config is host:port:port or host:port:port:type and client_config is port or host:port";

        public QuorumServer(long id, InetSocketAddress addr, InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
            this(id, addr, electionAddr, clientAddr, LearnerType.PARTICIPANT);
        }
        public QuorumServer(long id, InetSocketAddress addr, InetSocketAddress electionAddr) {
            this(id, addr, electionAddr, (InetSocketAddress) null, LearnerType.PARTICIPANT);
        }
        public QuorumServer(long id, InetSocketAddress addr) {
            this(id, addr, (InetSocketAddress) null, (InetSocketAddress) null, LearnerType.PARTICIPANT);
        }
        public QuorumServer(long sid, String addressStr) throws ConfigException {
            // LOG.warn("sid = " + sid + " addressStr = " + addressStr);
            this.id = sid;
            String serverClientParts[] = addressStr.split(";");
            String serverParts[] = splitWithLeadingHostname(serverClientParts[0]);
            if ((serverClientParts.length > 2) || (serverParts.length < 3)
                    || (serverParts.length > 4)) {
                throw new ConfigException(addressStr + wrongFormat);
            }

            if (serverClientParts.length == 2) {
                //LOG.warn("ClientParts: " + serverClientParts[1]);
                String clientParts[] = splitWithLeadingHostname(serverClientParts[1]);
                if (clientParts.length > 2) {
                    throw new ConfigException(addressStr + wrongFormat);
                }

                // is client_config a host:port or just a port
                hostname = (clientParts.length == 2) ? clientParts[0] : "0.0.0.0";
                try {
                    clientAddr = new InetSocketAddress(hostname,
                            Integer.parseInt(clientParts[clientParts.length - 1]));
                    //LOG.warn("Set clientAddr to " + clientAddr);
                } catch (NumberFormatException e) {
                    throw new ConfigException("Address unresolved: " + hostname + ":" + clientParts[clientParts.length - 1]);
                }
            }

            // server_config should be either host:port:port or host:port:port:type
            try {
                addr = new InetSocketAddress(serverParts[0], Integer.parseInt(serverParts[1]));
            } catch (NumberFormatException e) {
                throw new ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[1]);
            }
            try {
                electionAddr = new InetSocketAddress(serverParts[0], Integer.parseInt(serverParts[2]));
            } catch (NumberFormatException e) {
                throw new ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[2]);
            }

            if (serverParts.length == 4) {
                setType(serverParts[3]);
            }

            this.hostname = serverParts[0];

            setMyAddrs();
        }
        public QuorumServer(long id, InetSocketAddress addr, InetSocketAddress electionAddr, LearnerType type) {
            this(id, addr, electionAddr, (InetSocketAddress) null, type);
        }
        public QuorumServer(long id, InetSocketAddress addr, InetSocketAddress electionAddr, InetSocketAddress clientAddr, LearnerType type) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = electionAddr;
            this.type = type;
            this.clientAddr = clientAddr;

            setMyAddrs();
        }

        /**
         * Performs a DNS lookup for server address and election address.
         *
         * If the DNS lookup fails, this.addr and electionAddr remain
         * unmodified.
         */
        public void recreateSocketAddresses() {
            if (this.addr == null) {
                LOG.warn("Server address has not been initialized");
                return;
            }
            if (this.electionAddr == null) {
                LOG.warn("Election address has not been initialized");
                return;
            }
            String host = this.addr.getHostString();
            InetAddress address = null;
            try {
                address = InetAddress.getByName(host);
            } catch (UnknownHostException ex) {
                LOG.warn("Failed to resolve address: {}", host, ex);
                return;
            }
            LOG.debug("Resolved address for {}: {}", host, address);
            int port = this.addr.getPort();
            this.addr = new InetSocketAddress(address, port);
            port = this.electionAddr.getPort();
            this.electionAddr = new InetSocketAddress(address, port);
        }

        private void setType(String s) throws ConfigException {
            if (s.toLowerCase().equals("observer")) {
                type = LearnerType.OBSERVER;
            } else if (s.toLowerCase().equals("participant")) {
                type = LearnerType.PARTICIPANT;
            } else {
                throw new ConfigException("Unrecognised peertype: " + s);
            }
        }

        private static String[] splitWithLeadingHostname(String s) throws ConfigException {
            /* Does it start with an IPv6 literal? */
            if (s.startsWith("[")) {
                int i = s.indexOf("]:");
                if (i < 0) {
                    throw new ConfigException(s + " starts with '[' but has no matching ']:'");
                }

                String[] sa = s.substring(i + 2).split(":");
                String[] nsa = new String[sa.length + 1];
                nsa[0] = s.substring(1, i);
                System.arraycopy(sa, 0, nsa, 1, sa.length);

                return nsa;
            } else {
                return s.split(":");
            }
        }

        private void setMyAddrs() {
            this.myAddrs = new ArrayList<InetSocketAddress>();
            this.myAddrs.add(this.addr);
            this.myAddrs.add(this.clientAddr);
            this.myAddrs.add(this.electionAddr);
            this.myAddrs = excludedSpecialAddresses(this.myAddrs);
        }

        private static String delimitedHostString(InetSocketAddress addr) {
            String host = addr.getHostString();
            if (host.contains(":")) {
                return "[" + host + "]";
            } else {
                return host;
            }
        }

        private boolean checkAddressesEqual(InetSocketAddress addr1, InetSocketAddress addr2) {
            if ((addr1 == null && addr2 != null) ||
                    (addr1 != null && addr2 == null) ||
                    (addr1 != null && addr2 != null && !addr1.equals(addr2))) return false;
            return true;
        }

        public void checkAddressDuplicate(QuorumServer s) throws BadArgumentsException {
            List<InetSocketAddress> otherAddrs = new ArrayList<InetSocketAddress>();
            otherAddrs.add(s.addr);
            otherAddrs.add(s.clientAddr);
            otherAddrs.add(s.electionAddr);
            otherAddrs = excludedSpecialAddresses(otherAddrs);

            for (InetSocketAddress my : this.myAddrs) {

                for (InetSocketAddress other : otherAddrs) {
                    if (my.equals(other)) {
                        String error = String.format("%s of server.%d conflicts %s of server.%d", my, this.id, other, s.id);
                        throw new BadArgumentsException(error);
                    }
                }
            }
        }

        private List<InetSocketAddress> excludedSpecialAddresses(List<InetSocketAddress> addrs) {
            List<InetSocketAddress> included = new ArrayList<InetSocketAddress>();
            InetAddress wcAddr = new InetSocketAddress(0).getAddress();

            for (InetSocketAddress addr : addrs) {
                if (addr == null) {
                    continue;
                }
                InetAddress inetaddr = addr.getAddress();

                if (inetaddr == null ||
                        inetaddr.equals(wcAddr) || // wildCard address(0.0.0.0)
                        inetaddr.isLoopbackAddress()) { // loopback address(localhost/127.0.0.1)
                    continue;
                }
                included.add(addr);
            }
            return included;
        }

        public String toString() {
            StringWriter sw = new StringWriter();
            //addr should never be null, but just to make sure
            if (addr != null) {
                sw.append(delimitedHostString(addr));
                sw.append(":");
                sw.append(String.valueOf(addr.getPort()));
            }
            if (electionAddr != null) {
                sw.append(":");
                sw.append(String.valueOf(electionAddr.getPort()));
            }
            if (type == LearnerType.OBSERVER) sw.append(":observer");
            else if (type == LearnerType.PARTICIPANT) sw.append(":participant");
            if (clientAddr != null) {
                sw.append(";");
                sw.append(delimitedHostString(clientAddr));
                sw.append(":");
                sw.append(String.valueOf(clientAddr.getPort()));
            }
            return sw.toString();
        }

        public int hashCode() {
            assert false : "hashCode not designed";
            return 42; // any arbitrary constant will do
        }

        public boolean equals(Object o) {
            if (!(o instanceof QuorumServer)) return false;
            QuorumServer qs = (QuorumServer) o;
            if ((qs.id != id) || (qs.type != type)) return false;
            if (!checkAddressesEqual(addr, qs.addr)) return false;
            if (!checkAddressesEqual(electionAddr, qs.electionAddr)) return false;
            if (!checkAddressesEqual(clientAddr, qs.clientAddr)) return false;
            return true;
        }
    }
    /**
     * @deprecated As of release 3.4.0, this class has been deprecated, since
     * it is used with one of the udp-based versions of leader election, which
     * we are also deprecating.
     *
     * This class simply responds to requests for the current leader of this
     * node.
     * <p>
     * The request contains just an xid generated by the requestor.
     * <p>
     * The response has the xid, the id of this server, the id of the leader,
     * and the zxid of the leader.
     *
     *
     */
    @Deprecated
    class ResponderThread extends ZooKeeperThread {
        ResponderThread() {
            super("ResponderThread");
        }

        volatile boolean running = true;

        @Override
        public void run() {
            try {
                byte b[] = new byte[36];
                ByteBuffer responseBuffer = ByteBuffer.wrap(b);
                DatagramPacket packet = new DatagramPacket(b, b.length);
                while (running) {
                    udpSocket.receive(packet);
                    if (packet.getLength() != 4) {
                        LOG.warn("Got more than just an xid! Len = "
                                + packet.getLength());
                    } else {
                        responseBuffer.clear();
                        responseBuffer.getInt(); // Skip the xid
                        responseBuffer.putLong(myid);
                        Vote current = getCurrentVote();
                        switch (getPeerState()) {
                            case LOOKING:
                                responseBuffer.putLong(current.getId());
                                responseBuffer.putLong(current.getZxid());
                                break;
                            case LEADING:
                                responseBuffer.putLong(myid);
                                try {
                                    long proposed;
                                    synchronized (leader) {
                                        proposed = leader.lastProposed;
                                    }
                                    responseBuffer.putLong(proposed);
                                } catch (NullPointerException npe) {
                                    // This can happen in state transitions,
                                    // just ignore the request
                                }
                                break;
                            case FOLLOWING:
                                responseBuffer.putLong(current.getId());
                                try {
                                    responseBuffer.putLong(follower.getZxid());
                                } catch (NullPointerException npe) {
                                    // This can happen in state transitions,
                                    // just ignore the request
                                }
                                break;
                            case OBSERVING:
                                // Do nothing, Observers keep themselves to
                                // themselves.
                                break;
                        }
                        packet.setData(b);
                        udpSocket.send(packet);
                    }
                    packet.setLength(b.length);
                }
            } catch (RuntimeException e) {
                LOG.warn("Unexpected runtime exception in ResponderThread", e);
            } catch (IOException e) {
                LOG.warn("Unexpected IO exception in ResponderThread", e);
            } finally {
                LOG.warn("QuorumPeer responder thread exited");
            }
        }
    }
}

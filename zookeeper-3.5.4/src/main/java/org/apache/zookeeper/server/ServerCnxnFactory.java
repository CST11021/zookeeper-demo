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

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.Login;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.auth.SaslServerCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用于创建ServerCnxn的工厂
 */
public abstract class ServerCnxnFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ServerCnxnFactory.class);
    /** 表示一个配置项，用于配置zk使用的ServerCnxnFactory的实现类的全限定类名，没有配置默认使用NIOServerCnxnFactory */
    public static final String ZOOKEEPER_SERVER_CNXN_FACTORY = "zookeeper.serverCnxnFactory";
    /** The buffer will cause the connection to be close when we do a send. */
    static final ByteBuffer closeConn = ByteBuffer.allocate(0);

    public Login login;
    /** 告诉这个ServerCnxnFactory上是否启用了SSL */
    protected boolean secure;
    /** 表示zk连接的zk服务器 */
    protected ZooKeeperServer zkServer;
    protected SaslServerCallbackHandler saslServerCallbackHandler;
    /** 维护ServerCnxn实例对应的MBean */
    private final ConcurrentHashMap<ServerCnxn, ConnectionBean> connectionBeans = new ConcurrentHashMap<ServerCnxn, ConnectionBean>();
    /** 用于保存当前客户端连接的到zk服务的ServerCnxn实例 */
    protected final Set<ServerCnxn> cnxns = Collections.newSetFromMap(new ConcurrentHashMap<ServerCnxn, Boolean>());



    static public ServerCnxnFactory createFactory() throws IOException {
        // 获取ServerCnxnFactory的实现类名，默认为：NIOServerCnxnFactory
        String serverCnxnFactoryName = System.getProperty(ZOOKEEPER_SERVER_CNXN_FACTORY);
        if (serverCnxnFactoryName == null) {
            serverCnxnFactoryName = NIOServerCnxnFactory.class.getName();
        }

        try {
            ServerCnxnFactory serverCnxnFactory = (ServerCnxnFactory) Class.forName(serverCnxnFactoryName).getDeclaredConstructor().newInstance();
            LOG.info("Using {} as server connection factory", serverCnxnFactoryName);
            return serverCnxnFactory;
        } catch (Exception e) {
            IOException ioe = new IOException("Couldn't instantiate " + serverCnxnFactoryName);
            ioe.initCause(e);
            throw ioe;
        }
    }
    static public ServerCnxnFactory createFactory(int clientPort, int maxClientCnxns) throws IOException {
        return createFactory(new InetSocketAddress(clientPort), maxClientCnxns);
    }
    static public ServerCnxnFactory createFactory(InetSocketAddress addr, int maxClientCnxns) throws IOException {
        ServerCnxnFactory factory = createFactory();
        factory.configure(addr, maxClientCnxns);
        return factory;
    }


    /**
     * 获取连接的端口，一般为2181
     *
     * @return
     */
    public abstract int getLocalPort();

    /**
     * 获取当前的连接实例
     *
     * @return
     */
    public abstract Iterable<ServerCnxn> getConnections();

    /**
     * 获取当前连接alive的实例个数
     *
     * @return
     */
    public int getNumAliveConnections() {
        return cnxns.size();
    }

    /**
     * 获取连接的zk服务节点实例
     *
     * @return
     */
    public ZooKeeperServer getZooKeeperServer() {
        return zkServer;
    }

    /**
     * 根据sessionId关闭session
     *
     * @return true if the cnxn that contains the sessionId exists in this ServerCnxnFactory and it's closed. Otherwise false.
     */
    public abstract boolean closeSession(long sessionId);

    /**
     * zk设置
     *
     * @param addr      设置通道连接的客户端的IP及端口
     * @param maxcc     控制最大客户端连接数
     * @throws IOException
     */
    public void configure(InetSocketAddress addr, int maxcc) throws IOException {
        configure(addr, maxcc, false);
    }

    /**
     * zk设置
     *
     * @param addr      设置通道连接的客户端的zk服务IP及端口
     * @param maxcc     控制最大客户端连接数
     * @param secure    是否启用了SSL
     * @throws IOException
     */
    public abstract void configure(InetSocketAddress addr, int maxcc, boolean secure) throws IOException;

    /**
     * 设置通道连接的客户端的IP及端口
     *
     * @param addr
     */
    public abstract void reconfigure(InetSocketAddress addr);

    /**
     * 获取zk服务允许的最大连接数
     *
     * @return
     */
    public abstract int getMaxClientCnxnsPerHost();

    /**
     * 设置zk服务允许的最大连接数
     *
     * @param max
     */
    public abstract void setMaxClientCnxnsPerHost(int max);

    /**
     * 是否启用了SSL
     *
     * @return
     */
    public boolean isSecure() {
        return secure;
    }

    /**
     *
     */
    public abstract void start();

    /**
     * 启动zk服务实例
     *
     * @param zkServer
     * @throws IOException
     * @throws InterruptedException
     */
    public void startup(ZooKeeperServer zkServer) throws IOException, InterruptedException {
        startup(zkServer, true);
    }

    /**
     * 启动zk服务实例
     *
     * @param zkServer
     * @param startServer
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void startup(ZooKeeperServer zkServer, boolean startServer) throws IOException, InterruptedException;

    /**
     * 主线程执行完成后才执行该方法
     *
     * @throws InterruptedException
     */
    public abstract void join() throws InterruptedException;

    /**
     * shutdown服务
     */
    public abstract void shutdown();

    /**
     * 设置客户端连接的ZooKeeperServer实例
     *
     * @param zks
     */
    final public void setZooKeeperServer(ZooKeeperServer zks) {
        this.zkServer = zks;
        if (zks != null) {
            // 是否启用了SSL
            if (secure) {
                zks.setSecureServerCnxnFactory(this);
            } else {
                zks.setServerCnxnFactory(this);
            }
        }
    }

    /**
     * close所有ServerCnxn实例，即关闭所有连接
     */
    public abstract void closeAll();

    /**
     * 获取客户端的address
     *
     * @return
     */
    public abstract InetSocketAddress getLocalAddress();

    /**
     * reset所有的ServerCnxn实例的统计信息
     */
    public abstract void resetAllConnectionStats();

    /**
     * 获取所有ServerCnxn实例的连接信息
     *
     * @param brief 为true时，返回
     *             info.put("session_id", getSessionId());
     *             info.put("last_operation", getLastOperation());
     *             info.put("established", getEstablished());
     *             info.put("session_timeout", getSessionTimeout());
     *             info.put("last_cxid", getLastCxid());
     *             info.put("last_zxid", getLastZxid());
     *             info.put("last_response_time", getLastResponseTime());
     *             info.put("last_latency", getLastLatency());
     *             info.put("min_latency", getMinLatency());
     *             info.put("avg_latency", getAvgLatency());
     *             info.put("max_latency", getMaxLatency());
     *             以上详细信息
     * @return
     */
    public abstract Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief);

    /**
     * 从JMX 中注销ServerCnxn实例对应的MBean
     *
     * @param serverCnxn
     */
    public void unregisterConnection(ServerCnxn serverCnxn) {
        ConnectionBean jmxConnectionBean = connectionBeans.remove(serverCnxn);
        if (jmxConnectionBean != null){
            MBeanRegistry.getInstance().unregister(jmxConnectionBean);
        }
    }

    /**
     * 注册ServerCnxn实例对应的MBean到JMX
     *
     * @param serverCnxn
     */
    public void registerConnection(ServerCnxn serverCnxn) {
        if (zkServer != null) {
            ConnectionBean jmxConnectionBean = new ConnectionBean(serverCnxn, zkServer);
            try {
                MBeanRegistry.getInstance().register(jmxConnectionBean, zkServer.jmxServerBean);
                connectionBeans.put(serverCnxn, jmxConnectionBean);
            } catch (JMException e) {
                LOG.warn("Could not register connection", e);
            }
        }

    }

    /**
     * 如果使用的Sasl登录，需要额外的配置
     *
     * Initialize the server SASL if specified.
     *
     * If the user has specified a "ZooKeeperServer.LOGIN_CONTEXT_NAME_KEY"
     * or a jaas.conf using "java.security.auth.login.config"
     * the authentication is required and an exception is raised.
     * Otherwise no authentication is configured and no exception is raised.
     *
     * @throws IOException if jaas.conf is missing or there's an error in it.
     */
    protected void configureSaslLogin() throws IOException {
        String serverSection = System.getProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY,
                                                  ZooKeeperSaslServer.DEFAULT_LOGIN_CONTEXT_NAME);

        // Note that 'Configuration' here refers to javax.security.auth.login.Configuration.
        AppConfigurationEntry entries[] = null;
        SecurityException securityException = null;
        try {
            entries = Configuration.getConfiguration().getAppConfigurationEntry(serverSection);
        } catch (SecurityException e) {
            // handle below: might be harmless if the user doesn't intend to use JAAS authentication.
            securityException = e;
        }

        // No entries in jaas.conf
        // If there's a configuration exception fetching the jaas section and
        // the user has required sasl by specifying a LOGIN_CONTEXT_NAME_KEY or a jaas file
        // we throw an exception otherwise we continue without authentication.
        if (entries == null) {
            String jaasFile = System.getProperty(Environment.JAAS_CONF_KEY);
            String loginContextName = System.getProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY);
            if (securityException != null && (loginContextName != null || jaasFile != null)) {
                String errorMessage = "No JAAS configuration section named '" + serverSection +  "' was found";
                if (jaasFile != null) {
                    errorMessage += "in '" + jaasFile + "'.";
                }
                if (loginContextName != null) {
                    errorMessage += " But " + ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY + " was set.";
                }
                LOG.error(errorMessage);
                throw new IOException(errorMessage);
            }
            return;
        }

        // jaas.conf entry available
        try {
            saslServerCallbackHandler = new SaslServerCallbackHandler(Configuration.getConfiguration());
            login = new Login(serverSection, saslServerCallbackHandler, new ZKConfig() );
            login.startThreadIfNeeded();
        } catch (LoginException e) {
            throw new IOException("Could not configure server because SASL configuration did not allow the "
              + " ZooKeeper server to authenticate itself properly: " + e);
        }
    }
}

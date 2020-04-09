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

package org.apache.zookeeper;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class manages the socket i/o for the client. ClientCnxn maintains a list
 * of available servers to connect to and "transparently" switches servers it is
 * connected to as needed.
 *
 * ClientCnxn类中有SendThread和EventThread两个线程，SendThread负责io（发送和接收），EventThread负责事件处理：
 *
 * 前者是一个I/O线程，主要负责ZooKeeper客户端和服务器之间的网络I/O通信；
 * 后者是一个事件线程，主要负责服务端事件进行处理。
 *
 */
public class ClientCnxn {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxn.class);

    /** ZOOKEEPER-706: If a session has a large number of watches set then
     * attempting to re-establish those watches after a connection loss may
     * fail due to the SetWatches request exceeding the server's configured
     * jute.maxBuffer value. To avoid this we instead split the watch
     * re-establishement across multiple SetWatches calls. This constant
     * controls the size of each call. It is set to 128kB to be conservative
     * with respect to the server's 1MB default for jute.maxBuffer.
     */
    private static final int SET_WATCHES_MAX_LENGTH = 128 * 1024;


    // 核心组件

    /**
     * 客户端核心线程，其内部又包含两个线程，即SendThread和EventThread。前者是一个I/O线程，主要负责ZooKeeper客户端和服务器之间的网络I/O通信；后者是一个事件线程，主要负责服务端事件进行处理。
     */
    final SendThread sendThread;
    /**
     * 客户端核心线程，其内部又包含两个线程，即SendThread和EventThread。前者是一个I/O线程，主要负责ZooKeeper客户端和服务器之间的网络I/O通信；后者是一个事件线程，主要负责服务端事件进行处理。
     */
    final EventThread eventThread;
    /** zk客户端 */
    private final ZooKeeper zooKeeper;
    /**
     * A set of ZooKeeper hosts this client could connect to.
     */
    private final HostProvider hostProvider;


    /** 认证的用户 */
    private final CopyOnWriteArraySet<AuthData> authInfo = new CopyOnWriteArraySet<AuthData>();

    /** Pending队列是为了存储那些已经从客户端发送到服务端的，但是需要等待服务端响应的Packet集合。*/
    private final LinkedList<Packet> pendingQueue = new LinkedList<Packet>();
    /** Outgoing队列是一个请求发送队列，专门用于存储那些需要发送到服务端的Packet集合 */
    private final LinkedBlockingDeque<Packet> outgoingQueue = new LinkedBlockingDeque<Packet>();


    /**
     * Set to true when close is called. Latches the connection such that we
     * don't attempt to re-connect to the server if in the middle of closing the
     * connection (client sends session disconnect to server as part of close
     * operation)
     */
    private volatile boolean closing = false;
    /** 表示当前客户端与zk服务连接状态 */
    volatile States state = States.NOT_CONNECTED;

    private int connectTimeout;
    private int readTimeout;


    /** 客户端的Watcher管理器 */
    private final ClientWatchManager watcher;

    // 会话

    /** 表示与服务端建立连接后生成的sessionId，{@link SendThread#onConnected(int, long, byte[], boolean)} */
    private long sessionId;
    /** sessionPasswd */
    private byte sessionPasswd[] = new byte[16];
    /** session超时时间 */
    private final int sessionTimeout;
    /**
     * The timeout in ms the client negotiated with the server.
     * This is the "real" timeout, not the timeout request by the client (which may have been increased/decreased by the server which applies bounds to this value.
     */
    private volatile int negotiatedSessionTimeout;

    /**
     * 如果为真，则允许连接进入r-o模式。
     * 除其他数据外，此字段的值在会话创建握手期间发送。
     * 如果连接另一端的服务器被分区，它将只接受只读客户端。
     */
    private boolean readOnly;
    /**
     * 表示zk集群的根目录，例如，192.168.1.1:2181,192.168.1.2:2181,192.168.1.3:2181/zk-book，这样就指定了该客户端连接上ZooKeeper服务器之后，
     * 所有对ZooKeeper的操作，都会基于这个根目录。例如，客户端对/foo/bar的操作，都会指向节点的操作，都会基于这个根目录，例如，客户端对/foo/bar的操作，
     * 都会指向节点/zk-book/foo/bar——这个目录也叫Chroot，即客户端隔离命名空间。
     */
    final String chrootPath;

    /**
     * Is set to true when a connection to a r/w server is established for the
     * first time; never changed afterwards.
     * <p>
     * Is used to handle situations when client without sessionId connects to a
     * read-only server. Such client receives "fake" sessionId from read-only
     * server, but this sessionId is invalid for other servers. So when such
     * client finds a r/w server, it sends 0 instead of fake sessionId during
     * connection handshake and establishes new, valid session.
     * <p>
     * If this field is false (which implies we haven't seen r/w server before)
     * then non-zero sessionId is fake, otherwise it is valid.
     */
    volatile boolean seenRwServerBefore = false;
    public ZooKeeperSaslClient zooKeeperSaslClient;
    /** zk客户端配置 */
    private final ZKClientConfig clientConfig;
    private Object eventOfDeath = new Object();
    private volatile long lastZxid;
    /** 当数据包从outgoingQueue发送到服务器时，该xid++ */
    private int xid = 1;




    // 构造器

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath - the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider
     *                the list of ZooKeeper servers to connect to
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param clientCnxnSocket
     *                the socket implementation used (e.g. NIO/Netty)
     * @param canBeReadOnly
     *                whether the connection is allowed to go to read-only
     *                mode in case of partitioning
     * @throws IOException
     */
    public ClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper, ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket, boolean canBeReadOnly) throws IOException {
        this(chrootPath, hostProvider, sessionTimeout, zooKeeper, watcher, clientCnxnSocket, 0, new byte[16], canBeReadOnly);
    }
    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath - the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider
     *                the list of ZooKeeper servers to connect to
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param clientCnxnSocket
     *                the socket implementation used (e.g. NIO/Netty)
     * @param sessionId session id if re-establishing session
     * @param sessionPasswd session passwd if re-establishing session
     * @param canBeReadOnly
     *                whether the connection is allowed to go to read-only
     *                mode in case of partitioning
     * @throws IOException
     */
    public ClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper, ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket, long sessionId, byte[] sessionPasswd, boolean canBeReadOnly) {
        this.zooKeeper = zooKeeper;
        this.watcher = watcher;
        this.sessionId = sessionId;
        this.sessionPasswd = sessionPasswd;
        this.sessionTimeout = sessionTimeout;
        this.hostProvider = hostProvider;
        this.chrootPath = chrootPath;

        connectTimeout = sessionTimeout / hostProvider.size();
        readTimeout = sessionTimeout * 2 / 3;
        readOnly = canBeReadOnly;

        sendThread = new SendThread(clientCnxnSocket);
        eventThread = new EventThread();
        this.clientConfig = zooKeeper.getClientConfig();
    }


    /**
     * 客户端连接zk时，会调用该方法，开始sendThread和eventThread线程
     */
    public void start() {
        sendThread.start();
        eventThread.start();
    }

    /**
     * 设置线程名称的前缀标识：
     * Guard against creating "-EventThread-EventThread-EventThread-..." thread names when ZooKeeper object is being created from within a watcher.
     * See ZOOKEEPER-795 for details.
     */
    private static String makeThreadName(String suffix) {
        String name = Thread.currentThread().getName().replaceAll("-EventThread", "");
        return name + suffix;
    }

    /**
     *
     * @param p
     */
    private void finishPacket(Packet p) {
        // 如果zk的影响头带有错误，则根据错误吗注册一个监听
        int err = p.replyHeader.getErr();
        if (p.watchRegistration != null) {
            p.watchRegistration.register(err);
        }

        // Add all the removed watch events to the event queue, so that the clients will be notified with 'Data/Child WatchRemoved' event type.
        if (p.watchDeregistration != null) {
            Map<EventType, Set<Watcher>> materializedWatchers = null;
            try {
                materializedWatchers = p.watchDeregistration.unregister(err);
                for (Entry<EventType, Set<Watcher>> entry : materializedWatchers.entrySet()) {
                    Set<Watcher> watchers = entry.getValue();
                    if (watchers.size() > 0) {
                        queueEvent(p.watchDeregistration.getClientPath(), err, watchers, entry.getKey());
                        // ignore connectionloss when removing from local session
                        p.replyHeader.setErr(Code.OK.intValue());
                    }
                }
            } catch (KeeperException.NoWatcherException nwe) {
                p.replyHeader.setErr(nwe.code().intValue());
            } catch (KeeperException ke) {
                p.replyHeader.setErr(ke.code().intValue());
            }
        }

        // 如果请求有设置回调，则触发通知
        if (p.cb == null) {
            synchronized (p) {
                p.finished = true;
                p.notifyAll();
            }
        } else {
            p.finished = true;
            eventThread.queuePacket(p);
        }
    }

    void queueEvent(String clientPath, int err, Set<Watcher> materializedWatchers, EventType eventType) {
        KeeperState sessionState = KeeperState.SyncConnected;
        if (KeeperException.Code.SESSIONEXPIRED.intValue() == err || KeeperException.Code.CONNECTIONLOSS.intValue() == err) {
            sessionState = Event.KeeperState.Disconnected;
        }
        WatchedEvent event = new WatchedEvent(eventType, sessionState, clientPath);
        eventThread.queueEvent(event, materializedWatchers);
    }

    void queueCallback(AsyncCallback cb, int rc, String path, Object ctx) {
        eventThread.queueCallback(cb, rc, path, ctx);
    }

    /**
     * 当请求时，zk服务断开或者是关闭等不正常的状态，则给响应头设置对应的错误编码
     *
     * @param p 请求信息
     */
    private void conLossPacket(Packet p) {
        if (p.replyHeader == null) {
            return;
        }

        switch (state) {
            case AUTH_FAILED:
                p.replyHeader.setErr(KeeperException.Code.AUTHFAILED.intValue());
                break;
            case CLOSED:
                p.replyHeader.setErr(KeeperException.Code.SESSIONEXPIRED.intValue());
                break;
            default:
                p.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
        }
        finishPacket(p);
    }

    /**
     * Shutdown the send/event threads. This method should not be called
     * directly - rather it should be called as part of close operation. This
     * method is primarily here to allow the tests to verify disconnection
     * behavior.
     */
    public void disconnect() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Disconnecting client for session: 0x"
                    + Long.toHexString(getSessionId()));
        }

        sendThread.close();
        eventThread.queueEventOfDeath();
        if (zooKeeperSaslClient != null) {
            zooKeeperSaslClient.shutdown();
        }
    }

    /**
     * Close the connection, which includes; send session disconnect to the
     * server, shutdown the send/event threads.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing client for session: 0x"
                    + Long.toHexString(getSessionId()));
        }

        try {
            RequestHeader h = new RequestHeader();
            h.setType(ZooDefs.OpCode.closeSession);

            submitRequest(h, null, null, null);
        } catch (InterruptedException e) {
            // ignore, close the send/event threads
        } finally {
            disconnect();
        }
    }

    /**
     * 当数据包从outgoingQueue发送到服务器时，ClientCnxnNIO::doIO()在外部调用getXid()。因此，getXid()必须是公共的。
     */
    synchronized public int getXid() {
        return xid++;
    }


    // 提交请求

    /**
     * 提交一个客户端请求，并返回相应头
     *
     * @param h                     请求头
     * @param request               请求体
     * @param response              响应体
     * @param watchRegistration     监听器和对应的节点路径
     * @return
     * @throws InterruptedException
     */
    public ReplyHeader submitRequest(RequestHeader h, Record request, Record response, WatchRegistration watchRegistration) throws InterruptedException {
        return submitRequest(h, request, response, watchRegistration, null);
    }
    /**
     * 提交一个客户端请求，并返回相应头
     *
     * @param h                     请求头
     * @param request               请求体
     * @param response              响应体
     * @param watchRegistration     监听器和对应的节点路径
     * @param watchDeregistration
     * @return
     * @throws InterruptedException
     */
    public ReplyHeader submitRequest(RequestHeader h, Record request, Record response, WatchRegistration watchRegistration, WatchDeregistration watchDeregistration) throws InterruptedException {
        ReplyHeader r = new ReplyHeader();
        Packet packet = queuePacket(h, r, request, response, null, null, null, null, watchRegistration, watchDeregistration);
        synchronized (packet) {
            while (!packet.finished) {
                // 一直等待，直到packet被处理完成
                packet.wait();
            }
        }
        return r;
    }

    public void saslCompleted() {
        sendThread.getClientCnxnSocket().saslCompleted();
    }

    /**
     * 发送请求包
     *
     * @param request
     * @param response
     * @param cb
     * @param opCode
     * @throws IOException
     */
    public void sendPacket(Record request, Record response, AsyncCallback cb, int opCode) throws IOException {
        // 现在就生成Xid，因为它将被立即发送，方法是调用下面的sendThread.sendPacket()。
        int xid = getXid();
        RequestHeader h = new RequestHeader();
        h.setXid(xid);
        h.setType(opCode);

        ReplyHeader r = new ReplyHeader();
        r.setXid(xid);

        Packet p = new Packet(h, r, request, response, null, false);
        p.cb = cb;
        sendThread.sendPacket(p);
    }

    /**
     * 根据请求后、请求体等参数构建一个Packet，并将Packet添加到outgoingQueue队列中，返回该请求包的引用，当该请求包被服务端正常处理完成后，客户端会将{@link Packet#finished}设置为true
     *
     * @param h
     * @param r
     * @param request
     * @param response
     * @param cb
     * @param clientPath
     * @param serverPath
     * @param ctx
     * @param watchRegistration
     * @return
     */
    public Packet queuePacket(RequestHeader h, ReplyHeader r, Record request, Record response, AsyncCallback cb, String clientPath, String serverPath, Object ctx, WatchRegistration watchRegistration) {
        return queuePacket(h, r, request, response, cb, clientPath, serverPath, ctx, watchRegistration, null);
    }

    /**
     * 根据请求后、请求体等参数构建一个Packet，并将Packet添加到outgoingQueue队列中，返回该请求包的引用，当该请求包被服务端正常处理完成后，客户端会将{@link Packet#finished}设置为true
     *
     * @param h
     * @param r
     * @param request
     * @param response
     * @param cb
     * @param clientPath
     * @param serverPath
     * @param ctx
     * @param watchRegistration
     * @param watchDeregistration
     * @return
     */
    public Packet queuePacket(RequestHeader h, ReplyHeader r, Record request, Record response, AsyncCallback cb, String clientPath, String serverPath, Object ctx, WatchRegistration watchRegistration, WatchDeregistration watchDeregistration) {
        Packet packet = null;

        // Note that we do not generate the Xid for the packet yet.
        // It is generated later at send-time, by an implementation of ClientCnxnSocket::doIO(), where the packet is actually sent.
        packet = new Packet(h, r, request, response, watchRegistration);
        packet.cb = cb;
        packet.ctx = ctx;
        packet.clientPath = clientPath;
        packet.serverPath = serverPath;
        packet.watchDeregistration = watchDeregistration;

        // The synchronized block here is for two purpose:
        // 1. synchronize with the final cleanup() in SendThread.run() to avoid race
        // 2. synchronized against each packet. So if a closeSession packet is added, later packet will be notified.
        synchronized (state) {
            // 如果客户端与服务端连接状态不正常，或者客户端被关闭，则给packet的响应头设置对应的错误编码
            if (!state.isAlive() || closing) {
                conLossPacket(packet);
            } else {
                // 如果客户端要求关闭会话，则将其标记为关闭，并将包添加到outgoingQueue队列中
                if (h.getType() == OpCode.closeSession) {
                    closing = true;
                }
                outgoingQueue.add(packet);
            }
        }
        sendThread.getClientCnxnSocket().packetAdded();
        return packet;
    }

    /**
     * 将指定的认证用户信息添加到此连接，
     *
     * @param scheme
     * @param auth
     */
    public void addAuthInfo(String scheme, byte auth[]) {
        if (!state.isAlive()) {
            return;
        }
        authInfo.add(new AuthData(scheme, auth));
        queuePacket(new RequestHeader(-4, OpCode.auth), null, new AuthPacket(0, scheme, auth),
                null, null, null, null, null, null);
    }






    // getter ...

    public long getSessionId() {
        return sessionId;
    }
    public byte[] getSessionPasswd() {
        return sessionPasswd;
    }
    public int getSessionTimeout() {
        return negotiatedSessionTimeout;
    }
    States getState() {
        return state;
    }
    public long getLastZxid() {
        return lastZxid;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        SocketAddress local = sendThread.getClientCnxnSocket().getLocalSocketAddress();
        SocketAddress remote = sendThread.getClientCnxnSocket().getRemoteSocketAddress();
        sb
                .append("sessionid:0x").append(Long.toHexString(getSessionId()))
                .append(" local:").append(local)
                .append(" remoteserver:").append(remote)
                .append(" lastZxid:").append(lastZxid)
                .append(" xid:").append(xid)
                .append(" sent:").append(sendThread.getClientCnxnSocket().getSentCount())
                .append(" recv:").append(sendThread.getClientCnxnSocket().getRecvCount())
                .append(" queuedpkts:").append(outgoingQueue.size())
                .append(" pendingresp:").append(pendingQueue.size())
                .append(" queuedevents:").append(eventThread.waitingEvents.size());

        return sb.toString();
    }



    // 内部类

    /**
     * 表示一个认证的用户，例如：addauth digest whz:123456 命令对应的scheme = digest，data = whz:123456
     */
    static class AuthData {
        AuthData(String scheme, byte data[]) {
            this.scheme = scheme;
            this.data = data;
        }

        String scheme;

        byte data[];
    }
    /**
     * ClientCnxn是ZooKeeper客户端的核心工作累，负责维护客户端与服务端之间的网络连接并进行一系列网络通信。我们来看看ClientCnxn内部的工作原理。
     *
     * Packet是ClientCnxn内部定义的一个对协议层的封装，作为ZooKeeper中请求与响应的载体。
     * Packet中包含了最基本的请求头（requestHeader）、响应头（replyHeader）、请求体（request）、响应体（response）、节点路径（clientPath/serverPath）和注册的Watcher（watchRegistration）等信息。
     * 针对Packet中这么多的属性，读者可能疑惑它们是否都会在客户端和服务器之间进行网络传输？答案是是否定的。Packet的createBB()方法负责对象Packet对象进行序列化，最终生成可用于底层网络传输的ByteBuffer对象。在这个过程中，只会将requestHeader、request和readOnly三个属性进行序列化，其余属性都保存在客户端的上下文中，不会进行与服务端之间的网络传输。
     */
    static class Packet {
        /** 请求头 */
        RequestHeader requestHeader;
        /** 响应头 */
        ReplyHeader replyHeader;
        /** 请求体 */
        Record request;
        /** 响应体 */
        Record response;

        ByteBuffer bb;

        /** Client's view of the path (may differ due to chroot) **/
        String clientPath;
        /** Servers's view of the path (may differ due to chroot) **/
        String serverPath;

        /**
         * 表示该请求Packet是否被处理完成，Packet由客户端创建，并发送给zk服务器，服务端处理完后会将返回对应的replyHeader和response，
         * 然后客户端的{@link EventThread}线程会将对应的事件和监听放入{@link EventThread#waitingEvents}队列中，然后等待被处理，
         * 当处理完后，会将该值设置为true，表示该请求被处理完成
         */
        boolean finished;

        /** 表示客户端的回调 */
        AsyncCallback cb;

        Object ctx;

        /** 表示客户端发起请求时，注册的监听监听器 */
        WatchRegistration watchRegistration;

        public boolean readOnly;

        WatchDeregistration watchDeregistration;

        /** Convenience ctor */
        Packet(RequestHeader requestHeader, ReplyHeader replyHeader, Record request, Record response, WatchRegistration watchRegistration) {
            this(requestHeader, replyHeader, request, response,
                    watchRegistration, false);
        }

        Packet(RequestHeader requestHeader, ReplyHeader replyHeader, Record request, Record response, WatchRegistration watchRegistration, boolean readOnly) {

            this.requestHeader = requestHeader;
            this.replyHeader = replyHeader;
            this.request = request;
            this.response = response;
            this.readOnly = readOnly;
            this.watchRegistration = watchRegistration;
        }

        /**
         * Packet的createBB()方法负责对象Packet对象进行序列化，最终生成可用于底层网络传输的ByteBuffer对象。
         * 在这个过程中，只会将requestHeader、request和readOnly三个属性进行序列化，其余属性都保存在客户端的上下文中，不会进行与服务端之间的网络传输。
         */
        public void createBB() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                boa.writeInt(-1, "len"); // We'll fill this in later
                if (requestHeader != null) {
                    requestHeader.serialize(boa, "header");
                }
                if (request instanceof ConnectRequest) {
                    request.serialize(boa, "connect");
                    // append "am-I-allowed-to-be-readonly" flag
                    boa.writeBool(readOnly, "readOnly");
                } else if (request != null) {
                    request.serialize(boa, "request");
                }
                baos.close();
                this.bb = ByteBuffer.wrap(baos.toByteArray());
                this.bb.putInt(this.bb.capacity() - 4);
                this.bb.rewind();
            } catch (IOException e) {
                LOG.warn("Ignoring unexpected exception", e);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("clientPath:" + clientPath);
            sb.append(" serverPath:" + serverPath);
            sb.append(" finished:" + finished);

            sb.append(" header:: " + requestHeader);
            sb.append(" replyHeader:: " + replyHeader);
            sb.append(" request:: " + request);
            sb.append(" response:: " + response);

            // jute toString is horrible, remove unnecessary newlines
            return sb.toString().replaceAll("\r*\n+", " ");
        }
    }
    static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -5438877188796231422L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        @Override
        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }

    /**
     * 封装事件和监听该事件的所有监听器
     */
    private static class WatcherSetEventPair {
        /** 监听{@link #event} 事件的watcher*/
        private final Set<Watcher> watchers;
        /** 被监听的事件 */
        private final WatchedEvent event;

        public WatcherSetEventPair(Set<Watcher> watchers, WatchedEvent event) {
            this.watchers = watchers;
            this.event = event;
        }
    }

    private static class LocalCallback {
        private final AsyncCallback cb;
        private final int rc;
        private final String path;
        private final Object ctx;

        public LocalCallback(AsyncCallback cb, int rc, String path, Object ctx) {
            this.cb = cb;
            this.rc = rc;
            this.path = path;
            this.ctx = ctx;
        }
    }
    private static class SessionTimeoutException extends IOException {
        private static final long serialVersionUID = 824482094072071178L;

        public SessionTimeoutException(String msg) {
            super(msg);
        }
    }
    private static class SessionExpiredException extends IOException {
        private static final long serialVersionUID = -1388816932076193249L;

        public SessionExpiredException(String msg) {
            super(msg);
        }
    }
    private static class RWServerFoundException extends IOException {
        private static final long serialVersionUID = 90431199887158758L;

        public RWServerFoundException(String msg) {
            super(msg);
        }
    }

    /**
     * SendThread是客户端ClientCnxn内部一个核心的I/O调度线程，用于管理客户端和服务端之间的所有网络I/O操作。在zk客户端的实际运行过程中，
     * 一方面，SendThread维护了客户端与服务端之间的会话生命周期，其通过在一定的周期频率内向服务器发送一个Ping包来实现心跳检测。
     * 同时，在会话周期内，如果客户端与服务端之间出现TCP连接断开的情况，那么就会自动透明化地完成重连接操作。
     *
     * 另一方面，SendThread管理了客户端所有的请求发送和相应操作，器将上传客户端API操作转换成相应的请求协议并发送到服务端，
     * 并完成对同步调用的返回和异步调用的回调。同时，SendThread还负责将来自服务端的事件传递给EventThread去处理。
     */
    class SendThread extends ZooKeeperThread {

        private final static int minPingRwTimeout = 100;
        private final static int maxPingRwTimeout = 60000;
        private static final String RETRY_CONN_MSG = ", closing socket connection and attempting reconnect";
        /** 表示最后一次发送ping的时间 */
        private long lastPingSentNs;
        /** 用于与zk服务端建立TCP长连接的socket，主要有NIO和Netty两个实现 */
        private final ClientCnxnSocket clientCnxnSocket;
        private Random r = new Random(System.nanoTime());
        /** 表示该线程是本次执行时，是否第一连接zk服务 */
        private boolean isFirstConnect = true;
        private InetSocketAddress rwServerAddress = null;
        private int pingRwTimeout = minPingRwTimeout;
        /** 当且仅当ZooKeeperSaslClient的构造函数抛出LoginException时，将其设置为true:参见下面的startConnect()。*/
        private boolean saslLoginFailed = false;

        SendThread(ClientCnxnSocket clientCnxnSocket) {
            super(makeThreadName("-SendThread()"));
            // 线程的初始状态是连接中的状态
            state = States.CONNECTING;
            this.clientCnxnSocket = clientCnxnSocket;
            // 通过setDaemon(true)来设置线程为“守护线程”；将一个用户线程设置为守护线程的方式是在 线程对象创建 之前 用线程对象的setDaemon方法。
            setDaemon(true);
        }

        @Override
        public void run() {
            // 设置发送请求的线程，sessionId和保存发送请求的队列
            clientCnxnSocket.introduce(this, sessionId, outgoingQueue);
            // 设置请求时间
            clientCnxnSocket.updateNow();
            // 更新lastSend和lastHeard为当前时间
            clientCnxnSocket.updateLastSendAndHeard();

            // 该值 = zk配置的connectTimeout - socket发起请求后到等待服务器响应的时间，如果是负数说明连接超时了
            int to;
            long lastPingRwServer = Time.currentElapsedTime();
            // 10 seconds
            final int MAX_SEND_PING_INTERVAL = 10000;
            InetSocketAddress serverAddress = null;

            // 连接是否正常，连接失败或者连接认证失败
            while (state.isAlive()) {
                try {
                    // 处理客户端还没连接上zk服务的情况
                    if (!clientCnxnSocket.isConnected()) {
                        // don't re-establish connection if we are closing
                        if (closing) {
                            break;
                        }

                        if (rwServerAddress != null) {
                            serverAddress = rwServerAddress;
                            rwServerAddress = null;
                        } else {
                            // 从hostProvider随机获取一个zk服务地址
                            serverAddress = hostProvider.next(1000);
                        }

                        // 确定要连接的zk服务器后，开始连接zk服务
                        startConnect(serverAddress);
                        // 更新最后连接的时间和心跳时间
                        clientCnxnSocket.updateLastSendAndHeard();
                    }

                    // 如果连上了zk
                    if (state.isConnected()) {

                        // determine whether we need to send an AuthFailed event.
                        if (zooKeeperSaslClient != null) {
                            boolean sendAuthEvent = false;
                            if (zooKeeperSaslClient.getSaslState() == ZooKeeperSaslClient.SaslState.INITIAL) {
                                try {
                                    zooKeeperSaslClient.initialize(ClientCnxn.this);
                                } catch (SaslException e) {
                                    LOG.error("SASL authentication with Zookeeper Quorum member failed: " + e);
                                    state = States.AUTH_FAILED;
                                    sendAuthEvent = true;
                                }
                            }
                            KeeperState authState = zooKeeperSaslClient.getKeeperState();
                            if (authState != null) {
                                if (authState == KeeperState.AuthFailed) {
                                    // An authentication error occurred during authentication with the Zookeeper Server.
                                    state = States.AUTH_FAILED;
                                    sendAuthEvent = true;
                                } else {
                                    if (authState == KeeperState.SaslAuthenticated) {
                                        sendAuthEvent = true;
                                    }
                                }
                            }

                            if (sendAuthEvent == true) {
                                eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, authState, null));
                            }
                        }

                        // 计算超时时间
                        to = readTimeout - clientCnxnSocket.getIdleRecv();
                    } else {
                        to = connectTimeout - clientCnxnSocket.getIdleRecv();
                    }

                    // 小于等于0说明超时了
                    if (to <= 0) {
                        String warnInfo;
                        warnInfo = "Client session timed out, have not heard from server in "
                                + clientCnxnSocket.getIdleRecv() + "ms" + " for sessionid 0x" + Long.toHexString(sessionId);
                        LOG.warn(warnInfo);
                        throw new SessionTimeoutException(warnInfo);
                    }

                    // 处理连接上zk服务的情况
                    if (state.isConnected()) {
                        //1000(1秒)是为了防止丢失竞态条件而发送的第二个ping，也要确保在readTimeout很小的时候不要发送太多的ping
                        int timeToNextPing = readTimeout / 2 - clientCnxnSocket.getIdleSend() - ((clientCnxnSocket.getIdleSend() > 1000) ? 1000 : 0);
                        // 在MAX_SEND_PING_INTERVAL内发送一个ping请求，要么是时间到期，要么是没有发送包
                        if (timeToNextPing <= 0 || clientCnxnSocket.getIdleSend() > MAX_SEND_PING_INTERVAL) {
                            sendPing();
                            clientCnxnSocket.updateLastSend();
                        } else {
                            if (timeToNextPing < to) {
                                to = timeToNextPing;
                            }
                        }
                    }

                    // 如果我们处于只读模式，请寻找读/写服务器
                    if (state == States.CONNECTEDREADONLY) {
                        long now = Time.currentElapsedTime();
                        int idlePingRwServer = (int) (now - lastPingRwServer);
                        if (idlePingRwServer >= pingRwTimeout) {
                            lastPingRwServer = now;
                            idlePingRwServer = 0;
                            pingRwTimeout = Math.min(2 * pingRwTimeout, maxPingRwTimeout);
                            pingRwServer();
                        }
                        to = Math.min(to, pingRwTimeout - idlePingRwServer);
                    }

                    clientCnxnSocket.doTransport(to, pendingQueue, ClientCnxn.this);
                } catch (Throwable e) {
                    // 客户是否正在关闭
                    if (closing) {
                        if (LOG.isDebugEnabled()) {
                            // closing so this is expected
                            LOG.debug("An exception was thrown while closing send thread for session 0x"
                                    + Long.toHexString(getSessionId()) + " : " + e.getMessage());
                        }
                        break;
                    } else {
                        // this is ugly, you have a better way speak up
                        if (e instanceof SessionExpiredException) {
                            LOG.info(e.getMessage() + ", closing socket connection");
                        } else if (e instanceof SessionTimeoutException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof EndOfStreamException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof RWServerFoundException) {
                            LOG.info(e.getMessage());
                        } else if (e instanceof SocketException) {
                            LOG.info("Socket error occurred: {}: {}", serverAddress, e.getMessage());
                        } else {
                            LOG.warn("Session 0x{} for server {}, unexpected error{}", Long.toHexString(getSessionId()),
                                    serverAddress, RETRY_CONN_MSG, e);
                        }

                        // 指定到这里，可能仍然有新包追加到outgoingQueue。
                        // 它们将在下一次连接中处理，或者在关闭时清除。
                        cleanup();
                        if (state.isAlive()) {
                            eventThread.queueEvent(new WatchedEvent(Event.EventType.None, Event.KeeperState.Disconnected, null));
                        }
                        clientCnxnSocket.updateNow();
                        clientCnxnSocket.updateLastSendAndHeard();
                    }
                }
            }

            // 1、socket不再监听事件; 2、给剩余的相应的包的响应头设置对应的错误编码，并将包清掉
            synchronized (state) {
                // When it comes to this point, it guarantees that later queued packet to outgoingQueue will be notified of death.
                cleanup();
            }

            // 关闭sock监听
            clientCnxnSocket.close();

            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None, Event.KeeperState.Disconnected, null));
            }
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(), "SendThread exited loop for session: 0x" + Long.toHexString(getSessionId()));
        }

        /**
         * 读取服务端返回的数据
         *
         * @param incomingBuffer
         * @throws IOException
         */
        void readResponse(ByteBuffer incomingBuffer) throws IOException {
            ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
            BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
            ReplyHeader replyHdr = new ReplyHeader();

            replyHdr.deserialize(bbia, "header");
            if (replyHdr.getXid() == -2) {
                // -2 is the xid for pings
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got ping response for sessionid: 0x" + Long.toHexString(sessionId)
                            + " after " + ((System.nanoTime() - lastPingSentNs) / 1000000) + "ms");
                }
                return;
            }
            if (replyHdr.getXid() == -4) {
                // -4 is the xid for AuthPacket
                if (replyHdr.getErr() == KeeperException.Code.AUTHFAILED.intValue()) {
                    state = States.AUTH_FAILED;
                    eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.AuthFailed, null));
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got auth sessionid:0x" + Long.toHexString(sessionId));
                }
                return;
            }
            if (replyHdr.getXid() == -1) {
                // -1 means notification
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got notification sessionid:0x" + Long.toHexString(sessionId));
                }
                WatcherEvent event = new WatcherEvent();
                event.deserialize(bbia, "response");

                // convert from a server path to a client path
                if (chrootPath != null) {
                    String serverPath = event.getPath();
                    if (serverPath.compareTo(chrootPath) == 0)
                        event.setPath("/");
                    else if (serverPath.length() > chrootPath.length())
                        event.setPath(serverPath.substring(chrootPath.length()));
                    else {
                        LOG.warn("Got server path " + event.getPath() + " which is too short for chroot path " + chrootPath);
                    }
                }

                WatchedEvent we = new WatchedEvent(event);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got " + we + " for sessionid 0x" + Long.toHexString(sessionId));
                }

                eventThread.queueEvent(we);
                return;
            }

            // If SASL authentication is currently in progress, construct and
            // send a response packet immediately, rather than queuing a
            // response as with other packets.
            if (tunnelAuthInProgress()) {
                GetSASLRequest request = new GetSASLRequest();
                request.deserialize(bbia, "token");
                zooKeeperSaslClient.respondToServer(request.getToken(),
                        ClientCnxn.this);
                return;
            }

            Packet packet;
            synchronized (pendingQueue) {
                if (pendingQueue.size() == 0) {
                    throw new IOException("Nothing in the queue, but got " + replyHdr.getXid());
                }
                packet = pendingQueue.remove();
            }
            /*
             * Since requests are processed in order, we better get a response
             * to the first request!
             */
            try {
                if (packet.requestHeader.getXid() != replyHdr.getXid()) {
                    packet.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
                    throw new IOException("Xid out of order. Got Xid " + replyHdr.getXid() + " with err " + +replyHdr.getErr() + " expected Xid " + packet.requestHeader.getXid() + " for a packet with details: " + packet);
                }

                packet.replyHeader.setXid(replyHdr.getXid());
                packet.replyHeader.setErr(replyHdr.getErr());
                packet.replyHeader.setZxid(replyHdr.getZxid());
                if (replyHdr.getZxid() > 0) {
                    lastZxid = replyHdr.getZxid();
                }
                if (packet.response != null && replyHdr.getErr() == 0) {
                    packet.response.deserialize(bbia, "response");
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reading reply sessionid:0x" + Long.toHexString(sessionId) + ", packet:: " + packet);
                }
            } finally {
                finishPacket(packet);
            }
        }

        /**
         * Setup session, previous watches, authentication.
         */
        void primeConnection() throws IOException {
            LOG.info("Socket connection established, initiating session, client: {}, server: {}",
                    clientCnxnSocket.getLocalSocketAddress(),
                    clientCnxnSocket.getRemoteSocketAddress());
            isFirstConnect = false;
            long sessId = (seenRwServerBefore) ? sessionId : 0;
            ConnectRequest conReq = new ConnectRequest(0, lastZxid,
                    sessionTimeout, sessId, sessionPasswd);
            // We add backwards since we are pushing into the front
            // Only send if there's a pending watch
            // TODO: here we have the only remaining use of zooKeeper in
            // this class. It's to be eliminated!
            if (!clientConfig.getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET)) {
                List<String> dataWatches = zooKeeper.getDataWatches();
                List<String> existWatches = zooKeeper.getExistWatches();
                List<String> childWatches = zooKeeper.getChildWatches();
                if (!dataWatches.isEmpty()
                        || !existWatches.isEmpty() || !childWatches.isEmpty()) {
                    Iterator<String> dataWatchesIter = prependChroot(dataWatches).iterator();
                    Iterator<String> existWatchesIter = prependChroot(existWatches).iterator();
                    Iterator<String> childWatchesIter = prependChroot(childWatches).iterator();
                    long setWatchesLastZxid = lastZxid;

                    while (dataWatchesIter.hasNext()
                            || existWatchesIter.hasNext() || childWatchesIter.hasNext()) {
                        List<String> dataWatchesBatch = new ArrayList<String>();
                        List<String> existWatchesBatch = new ArrayList<String>();
                        List<String> childWatchesBatch = new ArrayList<String>();
                        int batchLength = 0;

                        // Note, we may exceed our max length by a bit when we add the last
                        // watch in the batch. This isn't ideal, but it makes the code simpler.
                        while (batchLength < SET_WATCHES_MAX_LENGTH) {
                            final String watch;
                            if (dataWatchesIter.hasNext()) {
                                watch = dataWatchesIter.next();
                                dataWatchesBatch.add(watch);
                            } else if (existWatchesIter.hasNext()) {
                                watch = existWatchesIter.next();
                                existWatchesBatch.add(watch);
                            } else if (childWatchesIter.hasNext()) {
                                watch = childWatchesIter.next();
                                childWatchesBatch.add(watch);
                            } else {
                                break;
                            }
                            batchLength += watch.length();
                        }

                        SetWatches sw = new SetWatches(setWatchesLastZxid,
                                dataWatchesBatch,
                                existWatchesBatch,
                                childWatchesBatch);
                        RequestHeader header = new RequestHeader(-8, OpCode.setWatches);
                        Packet packet = new Packet(header, new ReplyHeader(), sw, null, null);
                        outgoingQueue.addFirst(packet);
                    }
                }
            }

            for (AuthData id : authInfo) {
                outgoingQueue.addFirst(new Packet(new RequestHeader(-4,
                        OpCode.auth), null, new AuthPacket(0, id.scheme,
                        id.data), null, null));
            }
            outgoingQueue.addFirst(new Packet(null, null, conReq,
                    null, null, readOnly));
            clientCnxnSocket.connectionPrimed();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Session establishment request sent on "
                        + clientCnxnSocket.getRemoteSocketAddress());
            }
        }

        /**
         * 给路径前缀追加chrootPath
         *
         * @param paths
         * @return
         */
        private List<String> prependChroot(List<String> paths) {
            if (chrootPath != null && !paths.isEmpty()) {
                for (int i = 0; i < paths.size(); ++i) {
                    String clientPath = paths.get(i);
                    String serverPath;
                    // handle clientPath = "/"
                    if (clientPath.length() == 1) {
                        serverPath = chrootPath;
                    } else {
                        serverPath = chrootPath + clientPath;
                    }
                    paths.set(i, serverPath);
                }
            }
            return paths;
        }

        /**
         * 发送一个ping请求
         */
        private void sendPing() {
            lastPingSentNs = System.nanoTime();
            RequestHeader h = new RequestHeader(-2, OpCode.ping);
            queuePacket(h, null, null, null, null, null, null, null, null);
        }

        /**
         * 开始连接指定地址的zk服务器
         *
         * @param addr
         * @throws IOException
         */
        private void startConnect(InetSocketAddress addr) throws IOException {
            // initializing it for new connection
            saslLoginFailed = false;

            // 如果不是首次连接，则暂停1秒再尝试连接
            if (!isFirstConnect) {
                try {
                    Thread.sleep(r.nextInt(1000));
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected exception", e);
                }
            }

            // 设置连接状态，表示正在尝试连接zk服务
            state = States.CONNECTING;

            String hostPort = addr.getHostString() + ":" + addr.getPort();
            MDC.put("myid", hostPort);
            setName(getName().replaceAll("\\(.*\\)", "(" + hostPort + ")"));
            if (clientConfig.isSaslClientEnabled()) {
                try {
                    if (zooKeeperSaslClient != null) {
                        zooKeeperSaslClient.shutdown();
                    }
                    zooKeeperSaslClient = new ZooKeeperSaslClient(getServerPrincipal(addr), clientConfig);
                } catch (LoginException e) {
                    // An authentication error occurred when the SASL client tried to initialize:
                    // for Kerberos this means that the client failed to authenticate with the KDC.
                    // This is different from an authentication error that occurs during communication
                    // with the Zookeeper server, which is handled below.
                    LOG.warn("SASL configuration failed: " + e + " Will continue connection to Zookeeper server without " + "SASL authentication, if Zookeeper server allows it.");
                    // SASL认证失败，则发送一个时间
                    eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.AuthFailed, null));
                    saslLoginFailed = true;
                }
            }
            logStartConnect(addr);

            // 连接目标机器
            clientCnxnSocket.connect(addr);
        }

        private String getServerPrincipal(InetSocketAddress addr) {
            String principalUserName = clientConfig.getProperty(ZKClientConfig.ZK_SASL_CLIENT_USERNAME,
                    ZKClientConfig.ZK_SASL_CLIENT_USERNAME_DEFAULT);
            String serverPrincipal = principalUserName + "/" + addr.getHostString();
            return serverPrincipal;
        }

        private void logStartConnect(InetSocketAddress addr) {
            String msg = "Opening socket connection to server " + addr;
            if (zooKeeperSaslClient != null) {
                msg += ". " + zooKeeperSaslClient.getConfigStatus();
            }
            LOG.info(msg);
        }

        private void pingRwServer() throws RWServerFoundException {
            String result = null;
            InetSocketAddress addr = hostProvider.next(0);
            LOG.info("Checking server " + addr + " for being r/w." + " Timeout " + pingRwTimeout);

            Socket sock = null;
            BufferedReader br = null;
            try {
                sock = new Socket(addr.getHostString(), addr.getPort());
                sock.setSoLinger(false, -1);
                sock.setSoTimeout(1000);
                sock.setTcpNoDelay(true);
                sock.getOutputStream().write("isro".getBytes());
                sock.getOutputStream().flush();
                sock.shutdownOutput();
                br = new BufferedReader(
                        new InputStreamReader(sock.getInputStream()));
                result = br.readLine();
            } catch (ConnectException e) {
                // ignore, this just means server is not up
            } catch (IOException e) {
                // some unexpected error, warn about it
                LOG.warn("Exception while seeking for r/w server " +
                        e.getMessage(), e);
            } finally {
                if (sock != null) {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
            }

            if ("rw".equals(result)) {
                pingRwTimeout = minPingRwTimeout;
                // 保存找到的地址，以便在下一次连接尝试时使用
                rwServerAddress = addr;
                throw new RWServerFoundException("Majority server found at " + addr.getHostString() + ":" + addr.getPort());
            }
        }

        /**
         * 1、socket不再监听事件
         * 2、给剩余的相应的包的响应头设置对应的错误编码，并将包清掉
         */
        private void cleanup() {
            // socket不再监听事件
            clientCnxnSocket.cleanup();
            // 给剩余的相应的包的响应头设置对应的错误编码，并将包清掉
            synchronized (pendingQueue) {
                for (Packet p : pendingQueue) {
                    conLossPacket(p);
                }
                pendingQueue.clear();
            }

            // We can't call outgoingQueue.clear() here because between iterating and clear up there might be new packets added in queuePacket().
            Iterator<Packet> iter = outgoingQueue.iterator();
            while (iter.hasNext()) {
                Packet p = iter.next();
                conLossPacket(p);
                iter.remove();
            }
        }

        /**
         * 连接建立后，由ClientCnxnSocket调用的回调：
         * 一方面需要通知SendThread线程，进一步对客户端进行会话参数的设置，包括readTimeout和connectTimeout等，并更新客户端状态；
         * 另一方面，需要通知地址管理器HostProvider当前成功连接的服务器地址。
         *
         * @param _negotiatedSessionTimeout
         * @param _sessionId
         * @param _sessionPasswd
         * @param isRO
         * @throws IOException
         */
        void onConnected(int _negotiatedSessionTimeout, long _sessionId, byte[] _sessionPasswd, boolean isRO) throws IOException {
            negotiatedSessionTimeout = _negotiatedSessionTimeout;
            if (negotiatedSessionTimeout <= 0) {
                state = States.CLOSED;

                eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null));
                eventThread.queueEventOfDeath();

                String warnInfo;
                warnInfo = "Unable to reconnect to ZooKeeper service, session 0x" + Long.toHexString(sessionId) + " has expired";
                LOG.warn(warnInfo);
                throw new SessionExpiredException(warnInfo);
            }
            if (!readOnly && isRO) {
                LOG.error("Read/write client got connected to read-only server");
            }
            readTimeout = negotiatedSessionTimeout * 2 / 3;
            connectTimeout = negotiatedSessionTimeout / hostProvider.size();
            hostProvider.onConnected();
            // 建立连接以后，设置sessionId
            sessionId = _sessionId;
            sessionPasswd = _sessionPasswd;
            state = (isRO) ? States.CONNECTEDREADONLY : States.CONNECTED;
            seenRwServerBefore |= !isRO;
            LOG.info("Session establishment complete on server " + clientCnxnSocket.getRemoteSocketAddress()
                    + ", sessionid = 0x" + Long.toHexString(sessionId) + ", negotiated timeout = " + negotiatedSessionTimeout + (isRO ? " (READ-ONLY mode)" : ""));
            KeeperState eventState = (isRO) ? KeeperState.ConnectedReadOnly : KeeperState.SyncConnected;
            // SyncConnected-None，为了能够让上层应用感知到会话的成功创建，SendThread会生成一个事件SyncConnected-None，代表客户端与服务器会话创建成功，并将该事件传递给EventThread线程。
            eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, eventState, null));
        }

        void close() {
            state = States.CLOSED;
            clientCnxnSocket.onClosing();
        }

        void testableCloseSocket() throws IOException {
            clientCnxnSocket.testableCloseSocket();
        }

        public boolean tunnelAuthInProgress() {
            // 1. SASL client is disabled.
            if (!clientConfig.isSaslClientEnabled()) {
                return false;
            }

            // 2. SASL login failed.
            if (saslLoginFailed == true) {
                return false;
            }

            // 3. SendThread has not created the authenticating object yet,
            // therefore authentication is (at the earliest stage of being) in progress.
            if (zooKeeperSaslClient == null) {
                return true;
            }

            // 4. authenticating object exists, so ask it for its progress.
            return zooKeeperSaslClient.clientTunneledAuthenticationInProgress();
        }

        /**
         * 将Packet包发送到服务端
         *
         * @param p
         * @throws IOException
         */
        public void sendPacket(Packet p) throws IOException {
            clientCnxnSocket.sendPacket(p);
        }



        ZooKeeper.States getZkState() {
            return state;
        }
        ClientCnxnSocket getClientCnxnSocket() {
            return clientCnxnSocket;
        }
    }
    /**
     * EventThread是客户端ClientCnxn内部的一个核心线程，负责客户端的事件处理，并触发客户端注册的Watcher监听。
     *
     * EventThread中有一个waitingEvents队列，用于临时存放那些需要被触发的监听和回调（包括那些客户端注册的Watcher和异步接口中注册的回调器AsyncCallback）。
     *
     * 同时，EventThread会不断地从waitingEvents这个队列中取出Object，识别其具体类型（Watcher或者AsyncCallback），并分别调用process和processResult接口方法来实现对事件的触发和回调。
     */
    class EventThread extends ZooKeeperThread {

        /**
         * 保存客户端注册的Watcher和异步接口中注册的回调器AsyncCallback，当相应的事件发生时，会将对应的事件保存到该队列中，
         * 然后EventThread 线程会不断从该队列获取事件并进行响应的处理
         */
        private final LinkedBlockingQueue<Object> waitingEvents = new LinkedBlockingQueue<Object>();
        /** 表示客户端开始处理发生的事件时，客户端与服务端的连接状态 */
        private volatile KeeperState sessionState = KeeperState.Disconnected;
        /** 标识该线程是否被killed */
        private volatile boolean wasKilled = false;
        /** 标识该线程是否是running状态 */
        private volatile boolean isRunning = false;

        EventThread() {
            // 设置线程名，并设置为守护线程
            super(makeThreadName("-EventThread"));
            setDaemon(true);
        }

        /**
         * 添加一个监听事件到队列中
         *
         * @param event
         */
        public void queueEvent(WatchedEvent event) {
            queueEvent(event, null);
        }

        /**
         * 添加一个监听事件到队列中
         *
         * @param event
         * @param materializedWatchers
         */
        private void queueEvent(WatchedEvent event, Set<Watcher> materializedWatchers) {
            // 处理客户端连接上zk服务后，没有任何节点信息
            if (event.getType() == EventType.None && sessionState == event.getState()) {
                return;
            }

            sessionState = event.getState();
            final Set<Watcher> watchers;
            if (materializedWatchers == null) {
                // materialize the watchers based on the event 根据事件具体化观察者
                watchers = watcher.materialize(event.getState(), event.getType(), event.getPath());
            } else {
                watchers = new HashSet<Watcher>();
                watchers.addAll(materializedWatchers);
            }

            // 将时间及对应的监听器封装一下，然后放到队列中
            WatcherSetEventPair pair = new WatcherSetEventPair(watchers, event);
            waitingEvents.add(pair);
        }

        /**
         * 添加一个回调到队列中
         *
         * @param cb
         * @param rc
         * @param path
         * @param ctx
         */
        public void queueCallback(AsyncCallback cb, int rc, String path, Object ctx) {
            waitingEvents.add(new LocalCallback(cb, rc, path, ctx));
        }

        /**
         * 添加一个响应包到队列中
         *
         * @param packet
         */
        public void queuePacket(Packet packet) {
            if (wasKilled) {
                synchronized (waitingEvents) {
                    if (isRunning) waitingEvents.add(packet);
                    else processEvent(packet);
                }
            } else {
                waitingEvents.add(packet);
            }
        }

        /**
         * 添加一个死亡事件到队列中，客户端发送致命错误导致客户端被close时，会调用该方法，添加一个死亡事件
         */
        public void queueEventOfDeath() {
            waitingEvents.add(eventOfDeath);
        }

        @Override
        public void run() {
            try {
                isRunning = true;
                while (true) {
                    Object event = waitingEvents.take();

                    // 如果zk服务端发生致命错误，则会往 waitingEvents 队列中插入一个eventOfDeath对象，表示zk服务已经停止
                    if (event == eventOfDeath) {
                        wasKilled = true;
                    } else {
                        processEvent(event);
                    }

                    // 如果该线程已经被killed，等到waitingEvents中所有的时间都处理完成后将线程状态设置为停止运行的状态
                    if (wasKilled)
                        synchronized (waitingEvents) {
                            if (waitingEvents.isEmpty()) {
                                isRunning = false;
                                break;
                            }
                        }
                }
            } catch (InterruptedException e) {
                LOG.error("Event thread exiting due to interruption", e);
            }

            LOG.info("EventThread shut down for session: 0x{}", Long.toHexString(getSessionId()));
        }

        /**
         * 处理{@link #waitingEvents}中的事件
         *
         * @param event
         */
        private void processEvent(Object event) {
            try {
                // 事件发生后通知所有监听器
                if (event instanceof WatcherSetEventPair) {
                    // each watcher will process the event
                    WatcherSetEventPair pair = (WatcherSetEventPair) event;
                    for (Watcher watcher : pair.watchers) {
                        try {
                            watcher.process(pair.event);
                        } catch (Throwable t) {
                            LOG.error("Error while calling watcher ", t);
                        }
                    }
                }

                // 事件发生后，执行对应的回调逻辑
                else if (event instanceof LocalCallback) {
                    LocalCallback lcb = (LocalCallback) event;
                    if (lcb.cb instanceof StatCallback) {
                        ((StatCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof DataCallback) {
                        ((DataCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof ACLCallback) {
                        ((ACLCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof ChildrenCallback) {
                        ((ChildrenCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof Children2Callback) {
                        ((Children2Callback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof StringCallback) {
                        ((StringCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else {
                        ((VoidCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx);
                    }
                }

                else {
                    Packet p = (Packet) event;

                    // 表示相应头的错误编码
                    int rc = 0;
                    String clientPath = p.clientPath;
                    if (p.replyHeader.getErr() != 0) {
                        rc = p.replyHeader.getErr();
                    }

                    if (p.cb == null) {
                        LOG.warn("Somehow a null cb got to EventThread!");
                    } else if (p.response instanceof ExistsResponse
                            || p.response instanceof SetDataResponse
                            || p.response instanceof SetACLResponse) {
                        StatCallback cb = (StatCallback) p.cb;
                        if (rc == 0) {
                            if (p.response instanceof ExistsResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((ExistsResponse) p.response).getStat());
                            } else if (p.response instanceof SetDataResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((SetDataResponse) p.response).getStat());
                            } else if (p.response instanceof SetACLResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((SetACLResponse) p.response).getStat());
                            }
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetDataResponse) {
                        DataCallback cb = (DataCallback) p.cb;
                        GetDataResponse rsp = (GetDataResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getData(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof GetACLResponse) {
                        ACLCallback cb = (ACLCallback) p.cb;
                        GetACLResponse rsp = (GetACLResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getAcl(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof GetChildrenResponse) {
                        ChildrenCallback cb = (ChildrenCallback) p.cb;
                        GetChildrenResponse rsp = (GetChildrenResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getChildren());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetChildren2Response) {
                        Children2Callback cb = (Children2Callback) p.cb;
                        GetChildren2Response rsp = (GetChildren2Response) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getChildren(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof CreateResponse) {
                        StringCallback cb = (StringCallback) p.cb;
                        CreateResponse rsp = (CreateResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx,
                                    (chrootPath == null
                                            ? rsp.getPath()
                                            : rsp.getPath()
                                            .substring(chrootPath.length())));
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof Create2Response) {
                        Create2Callback cb = (Create2Callback) p.cb;
                        Create2Response rsp = (Create2Response) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx,
                                    (chrootPath == null
                                            ? rsp.getPath()
                                            : rsp.getPath()
                                            .substring(chrootPath.length())), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof MultiResponse) {
                        MultiCallback cb = (MultiCallback) p.cb;
                        MultiResponse rsp = (MultiResponse) p.response;
                        if (rc == 0) {
                            List<OpResult> results = rsp.getResultList();
                            int newRc = rc;
                            for (OpResult result : results) {
                                if (result instanceof ErrorResult
                                        && KeeperException.Code.OK.intValue() != (newRc = ((ErrorResult) result)
                                        .getErr())) {
                                    break;
                                }
                            }
                            cb.processResult(newRc, clientPath, p.ctx, results);
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.cb instanceof VoidCallback) {
                        VoidCallback cb = (VoidCallback) p.cb;
                        cb.processResult(rc, clientPath, p.ctx);
                    }
                }
            } catch (Throwable t) {
                LOG.error("Caught unexpected throwable", t);
            }
        }
    }


}

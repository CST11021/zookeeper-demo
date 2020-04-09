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

import org.apache.jute.Record;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoWatcherException;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.client.*;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.EphemeralType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

/**
 * This is the main class of ZooKeeper client library. To use a ZooKeeper
 * service, an application must first instantiate an object of ZooKeeper class.
 * All the iterations will be done by calling the methods of ZooKeeper class.
 * The methods of this class are thread-safe unless otherwise noted.
 * <p>
 * Once a connection to a server is established, a session ID is assigned to the
 * client. The client will send heart beats to the server periodically to keep
 * the session valid.
 * <p>
 * The application can call ZooKeeper APIs through a client as long as the
 * session ID of the client remains valid.
 * <p>
 * If for some reason, the client fails to send heart beats to the server for a
 * prolonged period of time (exceeding the sessionTimeout value, for instance),
 * the server will expire the session, and the session ID will become invalid.
 * The client object will no longer be usable. To make ZooKeeper API calls, the
 * application must create a new client object.
 * <p>
 * If the ZooKeeper server the client currently connects to fails or otherwise
 * does not respond, the client will automatically try to connect to another
 * server before its session ID expires. If successful, the application can
 * continue to use the client.
 * <p>
 * The ZooKeeper API methods are either synchronous or asynchronous. Synchronous
 * methods blocks until the server has responded. Asynchronous methods just queue
 * the request for sending and return immediately. They take a callback object that
 * will be executed either on successful execution of the request or on error with
 * an appropriate return code (rc) indicating the error.
 * <p>
 * Some successful ZooKeeper API calls can leave watches on the "data nodes" in
 * the ZooKeeper server. Other successful ZooKeeper API calls can trigger those
 * watches. Once a watch is triggered, an event will be delivered to the client
 * which left the watch at the first place. Each watch can be triggered only
 * once. Thus, up to one event will be delivered to a client for every watch it
 * leaves.
 * <p>
 * A client needs an object of a class implementing Watcher interface for
 * processing the events delivered to the client.
 *
 * When a client drops the current connection and re-connects to a server, all the
 * existing watches are considered as being triggered but the undelivered events
 * are lost. To emulate this, the client will generate a special event to tell
 * the event handler a connection has been dropped. This special event has
 * EventType None and KeeperState Disconnected.
 *
 */
@SuppressWarnings("try")
@InterfaceAudience.Public
public class ZooKeeper implements AutoCloseable {

    private static final Logger LOG;

    /**
     * @deprecated Use {@link ZKClientConfig#ZOOKEEPER_CLIENT_CNXN_SOCKET} instead.
     */
    @Deprecated
    public static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";

    /**
     * @deprecated Use {@link ZKClientConfig#SECURE_CLIENT} instead.
     */
    @Deprecated
    public static final String SECURE_CLIENT = "zookeeper.client.secure";

    /** 用于向zk服务端发起请求以及触发客户端注册的Watcher或回调等 */
    protected final ClientCnxn cnxn;

    /** 表示该客户端实例所连接的zk服务，可能是多个服务 */
    protected final HostProvider hostProvider;

    /** zk客户端的Watcher管理器，该管理器用于管理客户端注册或移除的Watcher */
    protected final ZKWatchManager watchManager;

    /** 表示zk客户端配置 */
    private final ZKClientConfig clientConfig;

    static {
        //Keep these two lines together to keep the initialization order explicit
        LOG = LoggerFactory.getLogger(ZooKeeper.class);
        Environment.logEnv("Client environment:", LOG);
    }


    // 两个核心构造器

    /**
     * 初始化阶段
     *
     * 1. 初始化ZooKeeper对象：通过调用ZooKeeper的构造方法来实例化一个ZooKeeper对象，在初始化过程中，会创建一个客户端Watcher管理器：ClientWatchManager。
     * 2. 设置默认的Watcher：如果在构造方法中传入一个Watcher对象，那么客户端会将这个对象作为默认Watcher保存在ClientWatchManager中。
     * 3. 构造ZooKeeper服务器地址列表管理器：HostProvider，对于构造方法中传入的服务器地址，客户端会将其存放在服务器地址列表管理器HostProvider中。
     * 4. 创建并初始化客户端网络连接器：ClientCnxn，ZooKeeper客户端首先会创建一个网络连接器ClientCnxn，用来管理客户端与服务器的网络交互。另外客户端在创建ClientCnxn的同时，还会初始化客户端两个核心队列outgoingQueue和pendingQueue，分别作为客户端的请求发送队列和服务端响应的等待队列。
     * 5. 初始化SendThread和EventThread：客户端会创建两个核心网络线程SendThread和EventThread，前者用于管理客户端和服务端之间的所有网络I/O，后者则用于进行客户端的事件处理。同时，客户端会将将ClientCnxnSocket分配给SendThread作为底层网络I/O处理器，并初始化EventThread的待处理事件队列waitingEvents，用于存放所有等待被客户端处理的事件。
     *
     * @param connectString
     * @param sessionTimeout
     * @param watcher
     * @param canBeReadOnly
     * @param aHostProvider
     * @param clientConfig
     * @throws IOException
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly, HostProvider aHostProvider, ZKClientConfig clientConfig) throws IOException {
        LOG.info("Initiating client connection, connectString=" + connectString + " sessionTimeout=" + sessionTimeout + " watcher=" + watcher);

        if (clientConfig == null) {
            clientConfig = new ZKClientConfig();
        }

        this.clientConfig = clientConfig;
        watchManager = defaultWatchManager();
        watchManager.defaultWatcher = watcher;
        ConnectStringParser connectStringParser = new ConnectStringParser(connectString);
        hostProvider = aHostProvider;


        // ClientCnxn类中有SendThread和EventThread两个线程，SendThread负责io（发送和接收），EventThread负责事件处理
        cnxn = new ClientCnxn(connectStringParser.getChrootPath(), hostProvider, sessionTimeout, this, watchManager, getClientCnxnSocket(), canBeReadOnly);
        // 启动SendThread线程和EventThread线程
        cnxn.start();
    }

    /**
     * 初始化阶段
     *
     * 1. 初始化ZooKeeper对象：通过调用ZooKeeper的构造方法来实例化一个ZooKeeper对象，在初始化过程中，会创建一个客户端Watcher管理器：ClientWatchManager。
     * 2. 设置默认的Watcher：如果在构造方法中传入一个Watcher对象，那么客户端会将这个对象作为默认Watcher保存在ClientWatchManager中。
     * 3. 构造ZooKeeper服务器地址列表管理器：HostProvider，对于构造方法中传入的服务器地址，客户端会将其存放在服务器地址列表管理器HostProvider中。
     * 4. 创建并初始化客户端网络连接器：ClientCnxn，ZooKeeper客户端首先会创建一个网络连接器ClientCnxn，用来管理客户端与服务器的网络交互。另外客户端在创建ClientCnxn的同时，还会初始化客户端两个核心队列outgoingQueue和pendingQueue，分别作为客户端的请求发送队列和服务端响应的等待队列。
     * 5. 初始化SendThread和EventThread：客户端会创建两个核心网络线程SendThread和EventThread，前者用于管理客户端和服务端之间的所有网络I/O，后者则用于进行客户端的事件处理。同时，客户端会将将ClientCnxnSocket分配给SendThread作为底层网络I/O处理器，并初始化EventThread的待处理事件队列waitingEvents，用于存放所有等待被客户端处理的事件。
     *
     * @param connectString
     * @param sessionTimeout
     * @param watcher
     * @param sessionId
     * @param sessionPasswd
     * @param canBeReadOnly
     * @param aHostProvider
     * @throws IOException
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, long sessionId, byte[] sessionPasswd, boolean canBeReadOnly, HostProvider aHostProvider) throws IOException {
        LOG.info("Initiating client connection, connectString=" + connectString
                + " sessionTimeout=" + sessionTimeout
                + " watcher=" + watcher
                + " sessionId=" + Long.toHexString(sessionId)
                + " sessionPasswd="
                + (sessionPasswd == null ? "<null>" : "<hidden>"));

        this.clientConfig = new ZKClientConfig();
        watchManager = defaultWatchManager();
        watchManager.defaultWatcher = watcher;

        ConnectStringParser connectStringParser = new ConnectStringParser(connectString);
        hostProvider = aHostProvider;

        cnxn = new ClientCnxn(connectStringParser.getChrootPath(),
                hostProvider, sessionTimeout, this, watchManager,
                getClientCnxnSocket(), sessionId, sessionPasswd, canBeReadOnly);
        // since user has provided sessionId
        cnxn.seenRwServerBefore = true;
        cnxn.start();
    }

    // 一下构造器都调用上面的构造器方法

    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
        this(connectString, sessionTimeout, watcher, false);
    }
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, ZKClientConfig conf) throws IOException {
        this(connectString, sessionTimeout, watcher, false, conf);
    }
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly, HostProvider aHostProvider) throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly, aHostProvider, null);
    }
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly, createDefaultHostProvider(connectString));
    }
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly, ZKClientConfig conf) throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly, createDefaultHostProvider(connectString), conf);
    }
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, long sessionId, byte[] sessionPasswd) throws IOException {
        this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, false);
    }
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, long sessionId, byte[] sessionPasswd, boolean canBeReadOnly) throws IOException {
        this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, canBeReadOnly, createDefaultHostProvider(connectString));
    }





    // 同步创建节点

    /**
     * Create a node with the given path. The node data will be the given data,
     * and node acl will be the given acl.
     * <p>
     * The flags argument specifies whether the created node will be ephemeral
     * or not.
     * <p>
     * An ephemeral node will be removed by the ZooKeeper automatically when the
     * session associated with the creation of the node expires.
     * <p>
     * The flags argument can also specify to create a sequential node. The
     * actual path name of a sequential node will be the given path plus a
     * suffix "i" where i is the current sequential number of the node. The sequence
     * number is always fixed length of 10 digits, 0 padded. Once
     * such a node is created, the sequential number will be incremented by one.
     * <p>
     * If a node with the same actual path already exists in the ZooKeeper, a
     * KeeperException with error code KeeperException.NodeExists will be
     * thrown. Note that since a different actual path is used for each
     * invocation of creating sequential node with the same path argument, the
     * call will never throw "file exists" KeeperException.
     * <p>
     * If the parent node does not exist in the ZooKeeper, a KeeperException
     * with error code KeeperException.NoNode will be thrown.
     * <p>
     * An ephemeral node cannot have children. If the parent node of the given
     * path is ephemeral, a KeeperException with error code
     * KeeperException.NoChildrenForEphemerals will be thrown.
     * <p>
     * This operation, if successful, will trigger all the watches left on the
     * node of the given path by exists and getData API calls, and the watches
     * left on the parent node by getChildren API calls.
     * <p>
     * If a node is created successfully, the ZooKeeper server will trigger the
     * watches on the path left by exists calls, and the watches on the parent
     * of the node by getChildren calls.
     * <p>
     * The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
     * Arrays larger than this will cause a KeeperExecption to be thrown.
     *
     * @param path
     *                the path for the node
     * @param data
     *                the initial data for the node
     * @param acl
     *                the acl for the node
     * @param createMode
     *                specifying whether the node to be created is ephemeral
     *                and/or sequential
     * @return the actual path of the created node
     * @throws KeeperException if the server returns a non-zero error code
     * @throws KeeperException.InvalidACLException if the ACL is invalid, null, or empty
     * @throws InterruptedException if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public String create(final String path, byte data[], List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        final String clientPath = path;
        // 检查节点路径是否合法
        PathUtils.validatePath(clientPath, createMode.isSequential());
        // 校验节点类型和ttl是否合法
        EphemeralType.validateTTL(createMode, -1);
        // 如果设置了chroot（隔离命名空间），则在路径添追加chroot
        final String serverPath = prependChroot(clientPath);

        // 设置：RequestHeader
        RequestHeader h = new RequestHeader();
        h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create);

        // 设置：CreateRequest和CreateResponse
        CreateRequest request = new CreateRequest();
        CreateResponse response = new CreateResponse();
        request.setData(data);
        request.setFlags(createMode.toFlag());
        request.setPath(serverPath);
        if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException();
        }
        request.setAcl(acl);

        // 提交请求
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), clientPath);
        }

        // 如果设置chroot，则返回的节点路径需要移除chroot
        if (cnxn.chrootPath == null) {
            return response.getPath();
        } else {
            return response.getPath().substring(cnxn.chrootPath.length());
        }
    }
    /**
     * Create a node with the given path and returns the Stat of that node. The
     * node data will be the given data and node acl will be the given acl.
     * <p>
     * The flags argument specifies whether the created node will be ephemeral
     * or not.
     * <p>
     * An ephemeral node will be removed by the ZooKeeper automatically when the
     * session associated with the creation of the node expires.
     * <p>
     * The flags argument can also specify to create a sequential node. The
     * actual path name of a sequential node will be the given path plus a
     * suffix "i" where i is the current sequential number of the node. The sequence
     * number is always fixed length of 10 digits, 0 padded. Once
     * such a node is created, the sequential number will be incremented by one.
     * <p>
     * If a node with the same actual path already exists in the ZooKeeper, a
     * KeeperException with error code KeeperException.NodeExists will be
     * thrown. Note that since a different actual path is used for each
     * invocation of creating sequential node with the same path argument, the
     * call will never throw "file exists" KeeperException.
     * <p>
     * If the parent node does not exist in the ZooKeeper, a KeeperException
     * with error code KeeperException.NoNode will be thrown.
     * <p>
     * An ephemeral node cannot have children. If the parent node of the given
     * path is ephemeral, a KeeperException with error code
     * KeeperException.NoChildrenForEphemerals will be thrown.
     * <p>
     * This operation, if successful, will trigger all the watches left on the
     * node of the given path by exists and getData API calls, and the watches
     * left on the parent node by getChildren API calls.
     * <p>
     * If a node is created successfully, the ZooKeeper server will trigger the
     * watches on the path left by exists calls, and the watches on the parent
     * of the node by getChildren calls.
     * <p>
     * The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
     * Arrays larger than this will cause a KeeperExecption to be thrown.
     *
     * @param path
     *                the path for the node
     * @param data
     *                the initial data for the node
     * @param acl
     *                the acl for the node
     * @param createMode
     *                specifying whether the node to be created is ephemeral
     *                and/or sequential
     * @param stat
     *                The output Stat object.
     * @return the actual path of the created node
     * @throws KeeperException if the server returns a non-zero error code
     * @throws KeeperException.InvalidACLException if the ACL is invalid, null, or empty
     * @throws InterruptedException if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public String create(final String path, byte data[], List<ACL> acl, CreateMode createMode, Stat stat) throws KeeperException, InterruptedException {
        return create(path, data, acl, createMode, stat, -1);
    }
    /**
     * same as {@link #create(String, byte[], List, CreateMode, Stat)} but
     * allows for specifying a TTL when mode is {@link CreateMode#PERSISTENT_WITH_TTL}
     * or {@link CreateMode#PERSISTENT_SEQUENTIAL_WITH_TTL}. If the znode has not been modified
     * within the given TTL, it will be deleted once it has no children. The TTL unit is
     * milliseconds and must be greater than 0 and less than or equal to
     * {@link EphemeralType#maxValue()} for {@link EphemeralType#TTL}.
     */
    public String create(final String path, byte data[], List<ACL> acl, CreateMode createMode, Stat stat, long ttl) throws KeeperException, InterruptedException {
        final String clientPath = path;
        // 检查节点路径是否合法
        PathUtils.validatePath(clientPath, createMode.isSequential());
        // 校验节点类型和ttl是否合法
        EphemeralType.validateTTL(createMode, ttl);
        // 如果设置了chroot（隔离命名空间），则在路径添追加chroot
        final String serverPath = prependChroot(clientPath);

        // 创建请求头
        RequestHeader h = new RequestHeader();
        setCreateHeader(createMode, h);
        // 创建响应体
        Create2Response response = new Create2Response();
        if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException();
        }
        // 创建请求体
        Record record = makeCreateRecord(createMode, serverPath, data, acl, ttl);

        // 提交请求
        ReplyHeader r = cnxn.submitRequest(h, record, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }

        // 如果设置chroot，则返回的节点路径需要移除chroot
        if (cnxn.chrootPath == null) {
            return response.getPath();
        } else {
            return response.getPath().substring(cnxn.chrootPath.length());
        }
    }

    // 异步创建节点
    /**
     * The asynchronous version of create.
     *
     * @see #create(String, byte[], List, CreateMode)
     */
    public void create(final String path, byte data[], List<ACL> acl, CreateMode createMode, StringCallback cb, Object ctx) {
        final String clientPath = path;
        // 检查节点路径是否合法
        PathUtils.validatePath(clientPath, createMode.isSequential());
        // 校验节点类型和ttl是否合法
        EphemeralType.validateTTL(createMode, -1);
        // 如果设置了chroot（隔离命名空间），则在路径添追加chroot
        final String serverPath = prependChroot(clientPath);

        // 创建请求头
        RequestHeader h = new RequestHeader();
        h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create);

        CreateRequest request = new CreateRequest();
        CreateResponse response = new CreateResponse();
        ReplyHeader r = new ReplyHeader();
        request.setData(data);
        request.setFlags(createMode.toFlag());
        request.setPath(serverPath);
        request.setAcl(acl);
        cnxn.queuePacket(h, r, request, response, cb, clientPath, serverPath, ctx, null);
    }
    /**
     * The asynchronous version of create.
     *
     * @see #create(String, byte[], List, CreateMode, Stat)
     */
    public void create(final String path, byte data[], List<ACL> acl, CreateMode createMode, Create2Callback cb, Object ctx) {
        create(path, data, acl, createMode, cb, ctx, -1);
    }
    /**
     * The asynchronous version of create with ttl.
     *
     * @see #create(String, byte[], List, CreateMode, Stat, long)
     */
    public void create(final String path, byte data[], List<ACL> acl, CreateMode createMode, Create2Callback cb, Object ctx, long ttl) {
        final String clientPath = path;
        // 检查节点路径是否合法
        PathUtils.validatePath(clientPath, createMode.isSequential());
        // 校验节点类型和ttl是否合法
        EphemeralType.validateTTL(createMode, ttl);
        // 如果设置了chroot（隔离命名空间），则在路径添追加chroot
        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        setCreateHeader(createMode, h);
        ReplyHeader r = new ReplyHeader();
        Create2Response response = new Create2Response();
        Record record = makeCreateRecord(createMode, serverPath, data, acl, ttl);
        cnxn.queuePacket(h, r, record, response, cb, clientPath, serverPath, ctx, null);
    }


    /**
     * Delete the node with the given path. The call will succeed if such a node
     * exists, and the given version matches the node's version (if the given
     * version is -1, it matches any node's versions).
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if the nodes does not exist.
     * <p>
     * A KeeperException with error code KeeperException.BadVersion will be
     * thrown if the given version does not match the node's version.
     * <p>
     * A KeeperException with error code KeeperException.NotEmpty will be thrown
     * if the node has children.
     * <p>
     * This operation, if successful, will trigger all the watches on the node
     * of the given path left by exists API calls, and the watches on the parent
     * node left by getChildren API calls.
     *
     * @param path
     *                the path of the node to be deleted.
     * @param version
     *                the expected node version.
     * @throws InterruptedException IF the server transaction is interrupted
     * @throws KeeperException If the server signals an error with a non-zero
     *   return code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public void delete(final String path, int version) throws InterruptedException, KeeperException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath;

        // maintain semantics even in chroot case specifically - root cannot be deleted
        // I think this makes sense even in chroot case.
        if (clientPath.equals("/")) {
            // a bit of a hack, but delete(/) will never succeed and ensures that the same semantics are maintained
            serverPath = clientPath;
        } else {
            serverPath = prependChroot(clientPath);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.delete);
        DeleteRequest request = new DeleteRequest();
        request.setPath(serverPath);
        request.setVersion(version);
        ReplyHeader r = cnxn.submitRequest(h, request, null, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), clientPath);
        }
    }
    /**
     * The asynchronous version of delete.
     *
     * @see #delete(String, int)
     */
    public void delete(final String path, int version, VoidCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath;

        // maintain semantics even in chroot case
        // specifically - root cannot be deleted
        // I think this makes sense even in chroot case.
        if (clientPath.equals("/")) {
            // a bit of a hack, but delete(/) will never succeed and ensures
            // that the same semantics are maintained
            serverPath = clientPath;
        } else {
            serverPath = prependChroot(clientPath);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.delete);
        DeleteRequest request = new DeleteRequest();
        request.setPath(serverPath);
        request.setVersion(version);
        cnxn.queuePacket(h, new ReplyHeader(), request, null, cb, clientPath,
                serverPath, ctx, null);
    }


    /**
     * 返回给定路径的节点的状态。如果不存在这样的节点，则返回null。
     *
     * If the watch is non-null and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch will be
     * triggered by a successful operation that creates/delete the node or sets
     * the data on the node.
     *
     * @param path the node path
     * @param watcher explicit watcher
     * @return the stat of the node of the given path; return null if no such a
     *         node exists.
     * @throws KeeperException If the server signals an error
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public Stat exists(final String path, Watcher watcher) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ExistsWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.exists);
        ExistsRequest request = new ExistsRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        SetDataResponse response = new SetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            if (r.getErr() == KeeperException.Code.NONODE.intValue()) {
                return null;
            }
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), clientPath);
        }

        return response.getStat().getCzxid() == -1 ? null : response.getStat();
    }
    /**
     * Return the stat of the node of the given path. Return null if no such a
     * node exists.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch will be
     * triggered by a successful operation that creates/delete the node or sets
     * the data on the node.
     *
     * @param path
     *                the node path
     * @param watch
     *                whether need to watch this node
     * @return the stat of the node of the given path; return null if no such a
     *         node exists.
     * @throws KeeperException If the server signals an error
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return exists(path, watch ? watchManager.defaultWatcher : null);
    }
    /**
     * The asynchronous version of exists.
     *
     * @see #exists(String, Watcher)
     */
    public void exists(final String path, Watcher watcher, StatCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ExistsWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.exists);
        ExistsRequest request = new ExistsRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        SetDataResponse response = new SetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb, clientPath, serverPath, ctx, wcb);
    }
    /**
     * The asynchronous version of exists.
     *
     * @see #exists(String, boolean)
     */
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        exists(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }



    /**
     * Return the list of the children of the node of the given path.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch willbe
     * triggered by a successful operation that deletes the node of the given
     * path or creates/delete a child under the node.
     * <p>
     * The list of children returned is not sorted and no guarantee is provided
     * as to its natural or lexical order.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path
     * @param watcher explicit watcher
     * @return an unordered array of children of the node with the given path
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public List<String> getChildren(final String path, Watcher watcher) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren);
        GetChildrenRequest request = new GetChildrenRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildrenResponse response = new GetChildrenResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), clientPath);
        }
        return response.getChildren();
    }
    /**
     * Return the list of the children of the node of the given path.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch willbe
     * triggered by a successful operation that deletes the node of the given
     * path or creates/delete a child under the node.
     * <p>
     * The list of children returned is not sorted and no guarantee is provided
     * as to its natural or lexical order.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path
     * @param watch
     * @return an unordered array of children of the node with the given path
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     */
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return getChildren(path, watch ? watchManager.defaultWatcher : null);
    }
    /**
     * The asynchronous version of getChildren.
     *
     * @see #getChildren(String, Watcher)
     */
    public void getChildren(final String path, Watcher watcher, ChildrenCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren);
        GetChildrenRequest request = new GetChildrenRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildrenResponse response = new GetChildrenResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb, clientPath, serverPath, ctx, wcb);
    }
    /**
     * The asynchronous version of getChildren.
     *
     * @see #getChildren(String, boolean)
     */
    public void getChildren(String path, boolean watch, ChildrenCallback cb, Object ctx) {
        getChildren(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }
    /**
     * For the given znode path return the stat and children list.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch willbe
     * triggered by a successful operation that deletes the node of the given
     * path or creates/delete a child under the node.
     * <p>
     * The list of children returned is not sorted and no guarantee is provided
     * as to its natural or lexical order.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @since 3.3.0
     *
     * @param path
     * @param watcher explicit watcher
     * @param stat stat of the znode designated by path
     * @return an unordered array of children of the node with the given path
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public List<String> getChildren(final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren2);
        GetChildren2Request request = new GetChildren2Request();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildren2Response response = new GetChildren2Response();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getChildren();
    }
    /**
     * For the given znode path return the stat and children list.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch willbe
     * triggered by a successful operation that deletes the node of the given
     * path or creates/delete a child under the node.
     * <p>
     * The list of children returned is not sorted and no guarantee is provided
     * as to its natural or lexical order.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @since 3.3.0
     *
     * @param path
     * @param watch
     * @param stat stat of the znode designated by path
     * @return an unordered array of children of the node with the given path
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero
     *  error code.
     */
    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return getChildren(path, watch ? watchManager.defaultWatcher : null, stat);
    }
    /**
     * The asynchronous version of getChildren.
     *
     * @since 3.3.0
     *
     * @see #getChildren(String, Watcher, Stat)
     */
    public void getChildren(final String path, Watcher watcher, Children2Callback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren2);
        GetChildren2Request request = new GetChildren2Request();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildren2Response response = new GetChildren2Response();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb, clientPath, serverPath, ctx, wcb);
    }
    /**
     * The asynchronous version of getChildren.
     *
     * @since 3.3.0
     *
     * @see #getChildren(String, boolean, Stat)
     */
    public void getChildren(String path, boolean watch, Children2Callback cb, Object ctx) {
        getChildren(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }



    /**
     * Return the data and the stat of the node of the given path.
     * <p>
     * If the watch is non-null and the call is successful (no exception is
     * thrown), a watch will be left on the node with the given path. The watch
     * will be triggered by a successful operation that sets data on the node, or
     * deletes the node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path the given path
     * @param watcher explicit watcher
     * @param stat the stat of the node
     * @return the data of the node
     * @throws KeeperException If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public byte[] getData(final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getData();
    }
    /**
     * Return the data and the stat of the node of the given path.
     * <p>
     * If the watch is true and the call is successful (no exception is
     * thrown), a watch will be left on the node with the given path. The watch
     * will be triggered by a successful operation that sets data on the node, or
     * deletes the node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path the given path
     * @param watch whether need to watch this node
     * @param stat the stat of the node
     * @return the data of the node
     * @throws KeeperException If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return getData(path, watch ? watchManager.defaultWatcher : null, stat);
    }
    /**
     * The asynchronous version of getData.
     *
     * @see #getData(String, Watcher, Stat)
     */
    public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);
    }
    /**
     * The asynchronous version of getData.
     *
     * @see #getData(String, boolean, Stat)
     */
    public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
        getData(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }


    // 同步设置节点数据
    /**
     * Set the data for the node of the given path if such a node exists and the
     * given version matches the version of the node (if the given version is
     * -1, it matches any node's versions). Return the stat of the node.
     * <p>
     * This operation, if successful, will trigger all the watches on the node
     * of the given path left by getData calls.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     * <p>
     * A KeeperException with error code KeeperException.BadVersion will be
     * thrown if the given version does not match the node's version.
     * <p>
     * The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
     * Arrays larger than this will cause a KeeperException to be thrown.
     *
     * @param path
     *                the path of the node
     * @param data
     *                the data to set
     * @param version
     *                the expected matching version
     * @return the state of the node
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public Stat setData(final String path, byte data[], int version) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);
        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setData);
        SetDataRequest request = new SetDataRequest();
        request.setPath(serverPath);
        request.setData(data);
        request.setVersion(version);
        SetDataResponse response = new SetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), clientPath);
        }
        return response.getStat();
    }
    // 异步设置节点数据
    /**
     * The asynchronous version of setData.
     *
     * @see #setData(String, byte[], int)
     */
    public void setData(final String path, byte data[], int version, StatCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setData);
        SetDataRequest request = new SetDataRequest();
        request.setPath(serverPath);
        request.setData(data);
        request.setVersion(version);
        SetDataResponse response = new SetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb, clientPath, serverPath, ctx, null);
    }


    // 获取zk的"/zookeeper/config"节点的配置

    /**
     * 给 /zookeeper/config 节点注册一个监听，并返回节点的数据
     *
     * 如果配置节点不存在，则抛出{@link KeeperException.NoNodeException}异常
     *
     * @param watcher explicit watcher
     * @param stat the stat of the configuration node ZooDefs.CONFIG_NODE
     * @return configuration data stored in ZooDefs.CONFIG_NODE
     * @throws KeeperException If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public byte[] getConfig(Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        // "/zookeeper/config"节点
        final String configZnode = ZooDefs.CONFIG_NODE;

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, configZnode);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(configZnode);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), configZnode);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getData();
    }
    /**
     * getConfig的异步版本
     *
     * @see #getConfig(Watcher, Stat)
     */
    public void getConfig(Watcher watcher, DataCallback cb, Object ctx) {
        final String configZnode = ZooDefs.CONFIG_NODE;

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, configZnode);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(configZnode);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb, configZnode, configZnode, ctx, wcb);
    }
    /**
     * Return the last committed configuration (as known to the server to which the client is connected)
     * and the stat of the configuration.
     * <p>
     * If the watch is true and the call is successful (no exception is
     * thrown), a watch will be left on the configuration node (ZooDefs.CONFIG_NODE). The watch
     * will be triggered by a successful reconfig operation
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param watch whether need to watch this node
     * @param stat the stat of the configuration node ZooDefs.CONFIG_NODE
     * @return configuration data stored in ZooDefs.CONFIG_NODE
     * @throws KeeperException If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public byte[] getConfig(boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return getConfig(watch ? watchManager.defaultWatcher : null, stat);
    }
    /**
     * getConfig的异步版本。
     *
     * @see #getData(String, boolean, Stat)
     */
    public void getConfig(boolean watch, DataCallback cb, Object ctx) {
        getConfig(watch ? watchManager.defaultWatcher : null, cb, ctx);
    }


    // 重置zk的服务集群节点

    /**
     * 重新配置zk集群，该方法目前已经废弃，并将方法迁移至{@link org.apache.zookeeper.admin.ZooKeeperAdmin}
     *
     * @deprecated instead use the reconfigure() methods instead in {@link org.apache.zookeeper.admin.ZooKeeperAdmin}
     */
    @Deprecated
    public byte[] reconfig(String joiningServers, String leavingServers, String newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }
    /**
     * @deprecated instead use the reconfigure() methods instead in {@link org.apache.zookeeper.admin.ZooKeeperAdmin}
     */
    @Deprecated
    public byte[] reconfig(List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }
    /**
     * @deprecated instead use the reconfigure() methods instead in {@link org.apache.zookeeper.admin.ZooKeeperAdmin}
     */
    @Deprecated
    public void reconfig(String joiningServers, String leavingServers, String newMembers, long fromConfig, DataCallback cb, Object ctx) {
        internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }
    /**
     * @deprecated instead use the reconfigure() methods instead in {@link org.apache.zookeeper.admin.ZooKeeperAdmin}
     */
    @Deprecated
    public void reconfig(List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, DataCallback cb, Object ctx) {
        internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }
    /**
     * 重新配置zk集群(同步的方式)
     *
     * @param joiningServers
     * @param leavingServers
     * @param newMembers
     * @param fromConfig
     * @param stat
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected byte[] internalReconfig(String joiningServers, String leavingServers, String newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        // 设置请求头
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.reconfig);

        // 设置请求体
        ReconfigRequest request = new ReconfigRequest(joiningServers, leavingServers, newMembers, fromConfig);

        // 设置响应体
        GetDataResponse response = new GetDataResponse();

        // 提交请求
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), "");
        }

        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getData();
    }
    /**
     * 重新配置zk集群(同步的方式)
     *
     * @param joiningServers
     * @param leavingServers
     * @param newMembers
     * @param fromConfig
     * @param stat
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected byte[] internalReconfig(List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(
                StringUtils.joinStrings(joiningServers, ","),
                StringUtils.joinStrings(leavingServers, ","),
                StringUtils.joinStrings(newMembers, ","),
                fromConfig,
                stat);
    }
    /**
     * 重新配置zk集群(异步的方式)
     *
     * @param joiningServers
     * @param leavingServers
     * @param newMembers
     * @param fromConfig
     * @param cb
     * @param ctx
     */
    protected void internalReconfig(String joiningServers, String leavingServers, String newMembers, long fromConfig, DataCallback cb, Object ctx) {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.reconfig);
        ReconfigRequest request = new ReconfigRequest(joiningServers, leavingServers, newMembers, fromConfig);
        GetDataResponse response = new GetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                ZooDefs.CONFIG_NODE, ZooDefs.CONFIG_NODE, ctx, null);
    }
    /**
     * 重新配置zk集群(异步的方式)
     *
     * @param joiningServers
     * @param leavingServers
     * @param newMembers
     * @param fromConfig
     * @param cb
     * @param ctx
     */
    protected void internalReconfig(List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, DataCallback cb, Object ctx) {
        internalReconfig(
                StringUtils.joinStrings(joiningServers, ","),
                StringUtils.joinStrings(leavingServers, ","),
                StringUtils.joinStrings(newMembers, ","),
                fromConfig,
                cb,
                ctx);
    }


    /**
     * Return the ACL and stat of the node of the given path.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path
     *                the given path for the node
     * @param stat
     *                the stat of the node will be copied to this parameter if
     *                not null.
     * @return the ACL array of the given node.
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public List<ACL> getACL(final String path, Stat stat) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getACL);
        GetACLRequest request = new GetACLRequest();
        request.setPath(serverPath);
        GetACLResponse response = new GetACLResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getAcl();
    }
    /**
     * The asynchronous version of getACL.
     *
     * @see #getACL(String, Stat)
     */
    public void getACL(final String path, Stat stat, ACLCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getACL);
        GetACLRequest request = new GetACLRequest();
        request.setPath(serverPath);
        GetACLResponse response = new GetACLResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }
    /**
     * Set the ACL for the node of the given path if such a node exists and the
     * given aclVersion matches the acl version of the node. Return the stat of the
     * node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     * <p>
     * A KeeperException with error code KeeperException.BadVersion will be
     * thrown if the given aclVersion does not match the node's aclVersion.
     *
     * @param path the given path for the node
     * @param acl the given acl for the node
     * @param aclVersion the given acl version of the node
     * @return the stat of the node.
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws org.apache.zookeeper.KeeperException.InvalidACLException If the acl is invalide.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public Stat setACL(final String path, List<ACL> acl, int aclVersion) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setACL);
        SetACLRequest request = new SetACLRequest();
        request.setPath(serverPath);
        if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException(clientPath);
        }
        request.setAcl(acl);
        request.setVersion(aclVersion);
        SetACLResponse response = new SetACLResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        return response.getStat();
    }
    /**
     * The asynchronous version of setACL.
     *
     * @see #setACL(String, List, int)
     */
    public void setACL(final String path, List<ACL> acl, int version, StatCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setACL);
        SetACLRequest request = new SetACLRequest();
        request.setPath(serverPath);
        request.setAcl(acl);
        request.setVersion(version);
        SetACLResponse response = new SetACLResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }








    public ZooKeeperSaslClient getSaslClient() {
        return cnxn.zooKeeperSaslClient;
    }

    /**
     * 获取zk客户端配置
     *
     * @return
     */
    public ZKClientConfig getClientConfig() {
        return clientConfig;
    }

    /**
     * 返回所有监听数据节点的监听器对应的节点
     *
     * @return
     */
    protected List<String> getDataWatches() {
        synchronized (watchManager.dataWatches) {
            List<String> rc = new ArrayList<String>(watchManager.dataWatches.keySet());
            return rc;
        }
    }

    /**
     * 返回所有监听节点是否存在的监听器
     *
     * @return
     */
    protected List<String> getExistWatches() {
        synchronized (watchManager.existWatches) {
            List<String> rc = new ArrayList<String>(watchManager.existWatches.keySet());
            return rc;
        }
    }

    /**
     * 返回所有子节点监听器监听的节点路径
     *
     * @return
     */
    protected List<String> getChildWatches() {
        synchronized (watchManager.childWatches) {
            List<String> rc = new ArrayList<String>(watchManager.childWatches.keySet());
            return rc;
        }
    }


    /**
     * 创建一个StaticHostProvider实例，用于管理客户端要连接的zk服务集群地址
     *
     * @param connectString
     * @return
     */
    private static HostProvider createDefaultHostProvider(String connectString) {
        return new StaticHostProvider(new ConnectStringParser(connectString).getServerAddresses());
    }

    /**
     * 这个函数允许客户端通过提供一个新的逗号分隔的host:port对列表来更新连接字符串，每个列表对应一个ZooKeeper服务器。
     *
     * @param connectString
     *
     * @throws IOException in cases of network failure
     */
    public void updateServerList(String connectString) throws IOException {
        ConnectStringParser connectStringParser = new ConnectStringParser(connectString);
        Collection<InetSocketAddress> serverAddresses = connectStringParser.getServerAddresses();

        ClientCnxnSocket clientCnxnSocket = cnxn.sendThread.getClientCnxnSocket();
        InetSocketAddress currentHost = (InetSocketAddress) clientCnxnSocket.getRemoteSocketAddress();

        //
        boolean reconfigMode = hostProvider.updateServerList(serverAddresses, currentHost);

        // 断开连接原因：这将导致调用next，而next将调用nextReconfigMode
        if (reconfigMode) clientCnxnSocket.testableCloseSocket();
    }

    /**
     * 获取当前客户端实例的Testable，Testable通常用于测试
     *
     * @return
     */
    public Testable getTestable() {
        return new ZooKeeperTestable(this, cnxn);
    }

    /**
     * 创建一个ZKWatchManager
     *
     * @return
     */
    protected ZKWatchManager defaultWatchManager() {
        return new ZKWatchManager(getClientConfig().getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET));
    }




    /**
     * 返回ZooKeeper客户端实例的会话id，这个方法不是线程安全的，在客户端连接到服务器之前返回的值是无效的，并且在重新连接后可能会更改。
     *
     * @return current session id
     */
    public long getSessionId() {
        return cnxn.getSessionId();
    }
    /**
     * 返回这个ZooKeeper客户端实例的会话密码，这个方法不是线程安全的，在客户端连接到服务器之前返回的值是无效的，并且在重新连接后可能会更改。
     *
     * @return current session password
     */
    public byte[] getSessionPasswd() {
        return cnxn.getSessionPasswd();
    }
    /**
     * 返回该ZooKeeper客户端实例的会话超时时间，这个方法不是线程安全的，在客户端连接到服务器之前返回的值是无效的，并且在重新连接后可能会更改。
     *
     * @return current session timeout
     */
    public int getSessionTimeout() {
        return cnxn.getSessionTimeout();
    }




    /**
     * 将指定的scheme:auth信息添加到此连接。
     *
     * 这个方法不是线程安全的
     *
     * @param scheme
     * @param auth
     */
    public void addAuthInfo(String scheme, byte auth[]) {
        cnxn.addAuthInfo(scheme, auth);
    }

    /**
     * 为该ZooKeeper客户端实例指定默认的Watcher(覆盖构造期间指定的Watcher)。
     *
     * @param watcher
     */
    public synchronized void register(Watcher watcher) {
        watchManager.defaultWatcher = watcher;
    }

    /**
     * 关闭此客户端对象。一旦客户端关闭，其会话将无效。
     * 与会话相关的ZooKeeper服务器中的所有临时节点将被删除。
     * 这些节点(以及它们的父节点)上的Watcher将被触发。
     *
     * 我们在这里禁止“尝试”警告，因为close()方法的签名允许抛出InterruptedException, AutoCloseable强烈建议不要抛出这个异常
     * (see: http://docs.oracle.com/javase/7/docs/api/java/lang/AutoCloseable.html#close()).
     * close()永远不会抛出InterruptedException，但是出于向后兼容的目的，这个异常保留在签名中。
     *
     * <p>
     * Added in 3.5.3: <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">try-with-resources</a>
     * may be used instead of calling close directly.
     * </p>
     * <p>
     * This method does not wait for all internal threads to exit.
     * Use the {@link #close(int) } method to wait for all resources to be released
     * </p>
     *
     * @throws InterruptedException
     */
    public synchronized void close() throws InterruptedException {
        if (!cnxn.getState().isAlive()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Close called on already closed client");
            }
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing session: 0x" + Long.toHexString(getSessionId()));
        }

        try {
            cnxn.close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring unexpected exception during close", e);
            }
        }

        LOG.info("Session: 0x" + Long.toHexString(getSessionId()) + " closed");
    }
    /**
     * 以{@link # Close()}方法关闭此客户端对象，此方法将等待内部资源被释放。
     *
     * @param waitForShutdownTimeoutMs 表示等待资源释放的时间，使用0或负数来跳过等待
     * @throws InterruptedException
     * @return true if waitForShutdownTimeout is greater than zero and all of the resources have been released
     *
     * @since 3.5.4
     */
    public boolean close(int waitForShutdownTimeoutMs) throws InterruptedException {
        close();
        return testableWaitForShutdown(waitForShutdownTimeoutMs);
    }

    /**
     * 如何设置了chroot（隔离命名空间），则在路径添加chroot
     *
     * @param clientPath path to the node
     * @return server view of the path (chroot prepended to client path)
     */
    private String prependChroot(String clientPath) {
        if (cnxn.chrootPath != null) {
            // handle clientPath = "/"
            if (clientPath.length() == 1) {
                return cnxn.chrootPath;
            }
            return cnxn.chrootPath + clientPath;
        } else {
            return clientPath;
        }
    }

    /**
     * 客户端创建节点时，会调用该方法，根据节点的类型设置请求头
     *
     * @param createMode
     * @param h
     */
    private void setCreateHeader(CreateMode createMode, RequestHeader h) {
        if (createMode.isTTL()) {
            h.setType(ZooDefs.OpCode.createTTL);
        } else {
            h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create2);
        }
    }

    /**
     * 根据参数创建请求体
     *
     * @param createMode
     * @param serverPath
     * @param data
     * @param acl
     * @param ttl
     * @return
     */
    private Record makeCreateRecord(CreateMode createMode, String serverPath, byte[] data, List<ACL> acl, long ttl) {
        Record record;
        if (createMode.isTTL()) {
            CreateTTLRequest request = new CreateTTLRequest();
            request.setData(data);
            request.setFlags(createMode.toFlag());
            request.setPath(serverPath);
            request.setAcl(acl);
            request.setTtl(ttl);
            record = request;
        } else {
            CreateRequest request = new CreateRequest();
            request.setData(data);
            request.setFlags(createMode.toFlag());
            request.setPath(serverPath);
            request.setAcl(acl);
            record = request;
        }
        return record;
    }




    /**
     * Executes multiple ZooKeeper operations or none of them.
     * <p>
     * On success, a list of results is returned.
     * On failure, an exception is raised which contains partial results and
     * error details, see {@link KeeperException#getResults}
     * <p>
     * Note: The maximum allowable size of all of the data arrays in all of
     * the setData operations in this single request is typically 1 MB
     * (1,048,576 bytes). This limit is specified on the server via
     * <a href="http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#Unsafe+Options">jute.maxbuffer</a>.
     * Requests larger than this will cause a KeeperException to be
     * thrown.
     *
     * @param ops An iterable that contains the operations to be done.
     * These should be created using the factory methods on {@link Op}.
     * @return A list of results, one for each input Op, the order of
     * which exactly matches the order of the <code>ops</code> input
     * operations.
     * @throws InterruptedException If the operation was interrupted.
     * The operation may or may not have succeeded, but will not have
     * partially succeeded if this exception is thrown.
     * @throws KeeperException If the operation could not be completed
     * due to some error in doing one of the specified ops.
     * @throws IllegalArgumentException if an invalid path is specified
     *
     * @since 3.4.0
     */
    public List<OpResult> multi(Iterable<Op> ops) throws InterruptedException, KeeperException {
        for (Op op : ops) {
            op.validate();
        }
        return multiInternal(generateMultiTransaction(ops));
    }
    /**
     * The asynchronous version of multi.
     *
     * @see #multi(Iterable)
     */
    public void multi(Iterable<Op> ops, MultiCallback cb, Object ctx) {
        List<OpResult> results = validatePath(ops);
        if (results.size() > 0) {
            cb.processResult(KeeperException.Code.BADARGUMENTS.intValue(),
                    null, ctx, results);
            return;
        }
        multiInternal(generateMultiTransaction(ops), cb, ctx);
    }

    private List<OpResult> validatePath(Iterable<Op> ops) {
        List<OpResult> results = new ArrayList<OpResult>();
        boolean error = false;
        for (Op op : ops) {
            try {
                op.validate();
            } catch (IllegalArgumentException iae) {
                LOG.error("IllegalArgumentException: " + iae.getMessage());
                ErrorResult err = new ErrorResult(
                        KeeperException.Code.BADARGUMENTS.intValue());
                results.add(err);
                error = true;
                continue;
            } catch (KeeperException ke) {
                LOG.error("KeeperException: " + ke.getMessage());
                ErrorResult err = new ErrorResult(ke.code().intValue());
                results.add(err);
                error = true;
                continue;
            }
            ErrorResult err = new ErrorResult(
                    KeeperException.Code.RUNTIMEINCONSISTENCY.intValue());
            results.add(err);
        }
        if (false == error) {
            results.clear();
        }
        return results;
    }

    private MultiTransactionRecord generateMultiTransaction(Iterable<Op> ops) {
        // reconstructing transaction with the chroot prefix
        List<Op> transaction = new ArrayList<Op>();
        for (Op op : ops) {
            transaction.add(withRootPrefix(op));
        }
        return new MultiTransactionRecord(transaction);
    }

    /**
     * 给该Op操作的节点添加chroot前缀
     *
     * @param op
     * @return
     */
    private Op withRootPrefix(Op op) {
        if (null != op.getPath()) {
            final String serverPath = prependChroot(op.getPath());
            if (!op.getPath().equals(serverPath)) {
                return op.withChroot(serverPath);
            }
        }
        return op;
    }

    protected void multiInternal(MultiTransactionRecord request, MultiCallback cb, Object ctx) {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.multi);
        MultiResponse response = new MultiResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb, null, null, ctx, null);
    }
    protected List<OpResult> multiInternal(MultiTransactionRecord request) throws InterruptedException, KeeperException {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.multi);
        MultiResponse response = new MultiResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()));
        }

        List<OpResult> results = response.getResultList();

        ErrorResult fatalError = null;
        for (OpResult result : results) {
            if (result instanceof ErrorResult && ((ErrorResult) result).getErr() != KeeperException.Code.OK.intValue()) {
                fatalError = (ErrorResult) result;
                break;
            }
        }

        if (fatalError != null) {
            KeeperException ex = KeeperException.create(KeeperException.Code.get(fatalError.getErr()));
            ex.setMultiResults(results);
            throw ex;
        }

        return results;
    }

    /**
     * 事务是{@link #multi}方法上的一个简单包装，它提供了一个构建器对象，可用于构造和提交一组原子操作。
     *
     * @since 3.4.0
     *
     * @return a Transaction builder object
     */
    public Transaction transaction() {
        return new Transaction(this);
    }














    /**
     * Asynchronous sync. Flushes channel between process and leader.
     * @param path
     * @param cb a handler for the callback
     * @param ctx context to be provided to the callback
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public void sync(final String path, VoidCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.sync);
        SyncRequest request = new SyncRequest();
        SyncResponse response = new SyncResponse();
        request.setPath(serverPath);
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    /**
     * 移除指定类型的一个watcher监听
     *
     * <p>
     * Watcher shouldn't be null. A successful call guarantees that, the
     * removed watcher won't be triggered.
     * </p>
     *
     * @param path
     *            - the path of the node
     * @param watcher
     *            - a concrete watcher
     * @param watcherType
     *            - the type of watcher to be removed
     * @param local
     *            - whether the watcher can be removed locally when there is no
     *            server connection
     * @throws InterruptedException
     *             if the server transaction is interrupted.
     * @throws KeeperException.NoWatcherException
     *             if no watcher exists that match the specified parameters
     * @throws KeeperException
     *             if the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException
     *             if any of the following is true:
     *             <ul>
     *             <li> {@code path} is invalid
     *             <li> {@code watcher} is null
     *             </ul>
     *
     * @since 3.5.0
     */
    public void removeWatches(String path, Watcher watcher, WatcherType watcherType, boolean local) throws InterruptedException, KeeperException {
        validateWatcher(watcher);
        removeWatches(ZooDefs.OpCode.checkWatches, path, watcher,
                watcherType, local);
    }
    /**
     * {@link #removeWatches(String path, Watcher watcher, WatcherType watcherType, boolean local)}的异步版本。
     *
     * @see #removeWatches
     */
    public void removeWatches(String path, Watcher watcher, WatcherType watcherType, boolean local, VoidCallback cb, Object ctx) {
        validateWatcher(watcher);
        removeWatches(ZooDefs.OpCode.checkWatches, path, watcher,
                watcherType, local, cb, ctx);
    }

    /**
     * 对于给定的znode路径，删除给定watcherType的所有Watcher
     *
     * <p>
     * A successful call guarantees that, the removed watchers won't be
     * triggered.
     * </p>
     *
     * @param path - the path of the node
     * @param watcherType - the type of watcher to be removed
     * @param local - whether watches can be removed locally when there is no server connection
     * @throws InterruptedException
     *             if the server transaction is interrupted.
     * @throws KeeperException.NoWatcherException
     *             if no watcher exists that match the specified parameters
     * @throws KeeperException
     *             if the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException
     *             if an invalid {@code path} is specified
     *
     * @since 3.5.0
     */
    public void removeAllWatches(String path, WatcherType watcherType, boolean local) throws InterruptedException, KeeperException {

        removeWatches(ZooDefs.OpCode.removeWatches, path, null, watcherType,
                local);
    }
    /**
     * {@link #removeAllWatches(String, WatcherType, boolean, VoidCallback, Object)}的异步版本。
     *
     * @see #removeAllWatches
     */
    public void removeAllWatches(String path, WatcherType watcherType, boolean local, VoidCallback cb, Object ctx) {

        removeWatches(ZooDefs.OpCode.removeWatches, path, null, watcherType, local, cb, ctx);
    }

    /**
     * 检查watcher监听是否不为空
     *
     * @param watcher
     */
    private void validateWatcher(Watcher watcher) {
        if (watcher == null) {
            throw new IllegalArgumentException("Invalid Watcher, shouldn't be null!");
        }
    }

    /**
     * 移除Watcher监听
     *
     * @param opCode
     * @param path
     * @param watcher
     * @param watcherType
     * @param local
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void removeWatches(int opCode, String path, Watcher watcher, WatcherType watcherType, boolean local) throws InterruptedException, KeeperException {
        PathUtils.validatePath(path);
        final String clientPath = path;
        final String serverPath = prependChroot(clientPath);
        WatchDeregistration wcb = new WatchDeregistration(clientPath, watcher,
                watcherType, local, watchManager);

        RequestHeader h = new RequestHeader();
        h.setType(opCode);
        Record request = getRemoveWatchesRequest(opCode, watcherType,
                serverPath);

        ReplyHeader r = cnxn.submitRequest(h, request, null, null, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
    }

    /**
     * 移除Watcher监听
     *
     * @param opCode
     * @param path
     * @param watcher
     * @param watcherType
     * @param local
     * @param cb
     * @param ctx
     */
    private void removeWatches(int opCode, String path, Watcher watcher, WatcherType watcherType, boolean local, VoidCallback cb, Object ctx) {
        PathUtils.validatePath(path);
        final String clientPath = path;
        final String serverPath = prependChroot(clientPath);
        WatchDeregistration wcb = new WatchDeregistration(clientPath, watcher,
                watcherType, local, watchManager);

        RequestHeader h = new RequestHeader();
        h.setType(opCode);
        Record request = getRemoveWatchesRequest(opCode, watcherType,
                serverPath);

        cnxn.queuePacket(h, new ReplyHeader(), request, null, cb, clientPath,
                serverPath, ctx, null, wcb);
    }

    /**
     * 根据参数创建一个发往服务端的请求对象，仅支持移除和检查监听两种请求
     *
     * @param opCode        对应{@link ZooDefs.OpCode}
     * @param watcherType   Watcher监听的类型
     * @param serverPath    节点
     * @return
     */
    private Record getRemoveWatchesRequest(int opCode, WatcherType watcherType, final String serverPath) {
        Record request = null;
        switch (opCode) {
            case ZooDefs.OpCode.checkWatches:
                CheckWatchesRequest chkReq = new CheckWatchesRequest();
                chkReq.setPath(serverPath);
                chkReq.setType(watcherType.getIntValue());
                request = chkReq;
                break;
            case ZooDefs.OpCode.removeWatches:
                RemoveWatchesRequest rmReq = new RemoveWatchesRequest();
                rmReq.setPath(serverPath);
                rmReq.setType(watcherType.getIntValue());
                request = rmReq;
                break;
            default:
                LOG.warn("unknown type " + opCode);
                break;
        }
        return request;
    }

    /**
     * 获取当前客户端与zk服务连接状态
     *
     * @return
     */
    public States getState() {
        return cnxn.getState();
    }

    /**
     * String representation of this ZooKeeper client. Suitable for things
     * like logging.
     *
     * Do NOT count on the format of this string, it may change without
     * warning.
     *
     * @since 3.3.0
     */
    @Override
    public String toString() {
        States state = getState();
        return ("State:" + state.toString()
                + (state.isConnected() ?
                " Timeout:" + getSessionTimeout() + " " :
                " ")
                + cnxn);
    }








    /**
     * 管理观察者，该观察这用于处理ClientCnxn对象生成的事件。
     *
     * 我们将其实现为ZooKeeper的嵌套类，这样就不会将公共方法作为ZooKeeper客户端API的一部分公开。
     */
    static class ZKWatchManager implements ClientWatchManager {

        /** 保存客户端监听操作数据的数据监听器, 一个节点可能有多个监听器，Map<节点路径，监听器> */
        private final Map<String, Set<Watcher>> dataWatches = new HashMap<String, Set<Watcher>>();
        /** 保存客户端判断节点是否存在时的监听器, 一个节点可能有多个监听器，Map<节点路径，监听器> */
        private final Map<String, Set<Watcher>> existWatches = new HashMap<String, Set<Watcher>>();
        /** 保存客户端操作子节点的监听器, 一个节点可能有多个监听器，Map<节点路径，监听器> */
        private final Map<String, Set<Watcher>> childWatches = new HashMap<String, Set<Watcher>>();

        /** 表示当客户端与服务器端创建连接成功时，是否允许自动clear监听器 */
        private boolean disableAutoWatchReset;

        ZKWatchManager(boolean disableAutoWatchReset) {
            this.disableAutoWatchReset = disableAutoWatchReset;
        }

        /** 表示一个默认的监听器，当客户端与服务器端创建连接成功后，会向该管理器添加一个默认的监听器 */
        protected volatile Watcher defaultWatcher;

        /**
         * 将from添加到to
         *
         * @param from
         * @param to
         */
        final private void addTo(Set<Watcher> from, Set<Watcher> to) {
            if (from != null) {
                to.addAll(from);
            }
        }

        /**
         *
         * @param clientPath
         * @param watcher
         * @param watcherType
         * @param local
         * @param rc
         * @return
         * @throws KeeperException
         */
        public Map<EventType, Set<Watcher>> removeWatcher(String clientPath, Watcher watcher, WatcherType watcherType, boolean local, int rc) throws KeeperException {
            // 验证节点路径是否包含watcherType的给定监视程序
            containsWatcher(clientPath, watcher, watcherType);

            Map<EventType, Set<Watcher>> removedWatchers = new HashMap<EventType, Set<Watcher>>();
            HashSet<Watcher> childWatchersToRem = new HashSet<Watcher>();
            removedWatchers.put(EventType.ChildWatchRemoved, childWatchersToRem);
            HashSet<Watcher> dataWatchersToRem = new HashSet<Watcher>();
            removedWatchers.put(EventType.DataWatchRemoved, dataWatchersToRem);
            boolean removedWatcher = false;

            switch (watcherType) {
                case Children: {
                    synchronized (childWatches) {
                        removedWatcher = removeWatches(childWatches, watcher, clientPath, local, rc, childWatchersToRem);
                    }
                    break;
                }
                case Data: {
                    synchronized (dataWatches) {
                        removedWatcher = removeWatches(dataWatches, watcher, clientPath, local, rc, dataWatchersToRem);
                    }

                    synchronized (existWatches) {
                        boolean removedDataWatcher = removeWatches(existWatches, watcher, clientPath, local, rc, dataWatchersToRem);
                        removedWatcher |= removedDataWatcher;
                    }
                    break;
                }
                case Any: {
                    synchronized (childWatches) {
                        removedWatcher = removeWatches(childWatches, watcher, clientPath, local, rc, childWatchersToRem);
                    }

                    synchronized (dataWatches) {
                        boolean removedDataWatcher = removeWatches(dataWatches, watcher, clientPath, local, rc, dataWatchersToRem);
                        removedWatcher |= removedDataWatcher;
                    }
                    synchronized (existWatches) {
                        boolean removedDataWatcher = removeWatches(existWatches, watcher, clientPath, local, rc, dataWatchersToRem);
                        removedWatcher |= removedDataWatcher;
                    }
                }
            }
            // Watcher function doesn't exists for the specified params
            if (!removedWatcher) {
                throw new KeeperException.NoWatcherException(clientPath);
            }
            return removedWatchers;
        }

        /**
         * 判断pathVsWatchers中是否包含watcherObj对象
         *
         * @param path
         * @param watcherObj
         * @param pathVsWatchers
         * @return
         */
        private boolean contains(String path, Watcher watcherObj, Map<String, Set<Watcher>> pathVsWatchers) {
            boolean watcherExists = true;
            if (pathVsWatchers == null || pathVsWatchers.size() == 0) {
                watcherExists = false;
            } else {
                Set<Watcher> watchers = pathVsWatchers.get(path);
                if (watchers == null) {
                    watcherExists = false;
                } else if (watcherObj == null) {
                    watcherExists = watchers.size() > 0;
                } else {
                    watcherExists = watchers.contains(watcherObj);
                }
            }
            return watcherExists;
        }

        /**
         * 验证节点路径是否包含watcherType的给定监视程序
         *
         * @param path          client path
         * @param watcher       watcher object reference
         * @param watcherType   type of the watcher
         * @throws NoWatcherException
         */
        void containsWatcher(String path, Watcher watcher, WatcherType watcherType) throws NoWatcherException {
            boolean containsWatcher = false;

            /**  */
            switch (watcherType) {

                // 判断监听子节点变更情况的监听列表是否包含该监听器
                case Children: {
                    synchronized (childWatches) {
                        containsWatcher = contains(path, watcher, childWatches);
                    }
                    break;
                }
                case Data: {
                    synchronized (dataWatches) {
                        containsWatcher = contains(path, watcher, dataWatches);
                    }

                    synchronized (existWatches) {
                        boolean contains_temp = contains(path, watcher, existWatches);
                        containsWatcher |= contains_temp;
                    }
                    break;
                }
                case Any: {
                    synchronized (childWatches) {
                        containsWatcher = contains(path, watcher, childWatches);
                    }

                    synchronized (dataWatches) {
                        boolean contains_temp = contains(path, watcher, dataWatches);
                        containsWatcher |= contains_temp;
                    }
                    synchronized (existWatches) {
                        boolean contains_temp = contains(path, watcher, existWatches);
                        containsWatcher |= contains_temp;
                    }
                }
            }
            // Watcher function doesn't exists for the specified params
            if (!containsWatcher) {
                throw new KeeperException.NoWatcherException(path);
            }
        }

        protected boolean removeWatches(Map<String, Set<Watcher>> pathVsWatcher, Watcher watcher, String path, boolean local, int rc, Set<Watcher> removedWatchers) throws KeeperException {
            if (!local && rc != Code.OK.intValue()) {
                throw KeeperException
                        .create(KeeperException.Code.get(rc), path);
            }
            boolean success = false;
            // When local flag is true, remove watchers for the given path
            // irrespective of rc. Otherwise shouldn't remove watchers locally
            // when sees failure from server.
            if (rc == Code.OK.intValue() || (local && rc != Code.OK.intValue())) {
                // Remove all the watchers for the given path
                if (watcher == null) {
                    Set<Watcher> pathWatchers = pathVsWatcher.remove(path);
                    if (pathWatchers != null) {
                        // found path watchers
                        removedWatchers.addAll(pathWatchers);
                        success = true;
                    }
                } else {
                    Set<Watcher> watchers = pathVsWatcher.get(path);
                    if (watchers != null) {
                        if (watchers.remove(watcher)) {
                            // found path watcher
                            removedWatchers.add(watcher);
                            // cleanup <path vs watchlist>
                            if (watchers.size() <= 0) {
                                pathVsWatcher.remove(path);
                            }
                            success = true;
                        }
                    }
                }
            }
            return success;
        }

        /**
         * 根据事件的状态、类型和节点路径，来管理客户端对应的监听器，比如：当节点被移除的事件发生后，客户端需要移除对应的监听器
         *
         * @param state         事件状态
         * @param type          事件类型
         * @param clientPath    节点路径
         * @return 可以返回empty，但不能返回null
         */
        @Override
        public Set<Watcher> materialize(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String clientPath) {
            Set<Watcher> result = new HashSet<Watcher>();

            switch (type) {
                // 节点没有发生任何变化的情况，例如：客户端与服务器端创建连接成功后没有任何节点信息
                case None:
                    result.add(defaultWatcher);

                    boolean clear = disableAutoWatchReset && state != Watcher.Event.KeeperState.SyncConnected;
                    synchronized (dataWatches) {
                        for (Set<Watcher> ws : dataWatches.values()) {
                            result.addAll(ws);
                        }
                        if (clear) {
                            dataWatches.clear();
                        }
                    }

                    synchronized (existWatches) {
                        for (Set<Watcher> ws : existWatches.values()) {
                            result.addAll(ws);
                        }
                        if (clear) {
                            existWatches.clear();
                        }
                    }

                    synchronized (childWatches) {
                        for (Set<Watcher> ws : childWatches.values()) {
                            result.addAll(ws);
                        }
                        if (clear) {
                            childWatches.clear();
                        }
                    }

                    return result;
                case NodeDataChanged:
                // 如果节点被创建的情况
                case NodeCreated:
                    synchronized (dataWatches) {
                        addTo(dataWatches.remove(clientPath), result);
                    }
                    synchronized (existWatches) {
                        addTo(existWatches.remove(clientPath), result);
                    }
                    break;
                // 如果子节点发生变更，则客户端移除监听该子节点的监听器，并返回被移除的监听器
                case NodeChildrenChanged:
                    synchronized (childWatches) {
                        addTo(childWatches.remove(clientPath), result);
                    }
                    break;
                // 如果数据节点被删除，则客户端移除对应的监听，并返回被移除的监听器
                case NodeDeleted:
                    synchronized (dataWatches) {
                        addTo(dataWatches.remove(clientPath), result);
                    }
                    synchronized (existWatches) {
                        Set<Watcher> list = existWatches.remove(clientPath);
                        if (list != null) {
                            addTo(list, result);
                            LOG.warn("We are triggering an exists watch for delete! Shouldn't happen!");
                        }
                    }
                    synchronized (childWatches) {
                        addTo(childWatches.remove(clientPath), result);
                    }
                    break;
                default:
                    String msg = "Unhandled watch event type " + type + " with state " + state + " on path " + clientPath;
                    LOG.error(msg);
                    throw new RuntimeException(msg);
            }

            return result;
        }
    }

    /**
     * 封装监听器和对应的节点路径
     */
    public abstract class WatchRegistration {

        /** 监听器 */
        private Watcher watcher;

        /** 被监听的节点路径 */
        private String clientPath;

        public WatchRegistration(Watcher watcher, String clientPath) {
            this.watcher = watcher;
            this.clientPath = clientPath;
        }

        abstract protected Map<String, Set<Watcher>> getWatches(int rc);

        /**
         * Register the watcher with the set of watches on path.
         *
         * @param rc the result code of the operation that attempted to
         * add the watch on the path.
         */
        public void register(int rc) {
            if (shouldAddWatch(rc)) {
                Map<String, Set<Watcher>> watches = getWatches(rc);
                synchronized (watches) {
                    Set<Watcher> watchers = watches.get(clientPath);
                    if (watchers == null) {
                        watchers = new HashSet<Watcher>();
                        watches.put(clientPath, watchers);
                    }
                    watchers.add(watcher);
                }
            }
        }

        /**
         * 根据返回代码判断是否应该添加Watcher
         *
         * @param rc 试图在节点上添加Watcher的操作的结果代码
         * @return true if the watch should be added, otw false
         */
        protected boolean shouldAddWatch(int rc) {
            return rc == 0;
        }
    }

    /**
     * Handle the special case of exists watches - they add a watcher even in the case where NONODE result code is returned.
     */
    class ExistsWatchRegistration extends WatchRegistration {
        public ExistsWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            return rc == 0 ? watchManager.dataWatches : watchManager.existWatches;
        }

        @Override
        protected boolean shouldAddWatch(int rc) {
            return rc == 0 || rc == KeeperException.Code.NONODE.intValue();
        }
    }

    class DataWatchRegistration extends WatchRegistration {
        public DataWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            return watchManager.dataWatches;
        }
    }

    class ChildWatchRegistration extends WatchRegistration {
        public ChildWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            return watchManager.childWatches;
        }
    }

    /**
     * 表示当前客户端与zk服务连接状态
     */
    @InterfaceAudience.Public
    public enum States {
        /** 表示zk客户端正在连接的状态 */
        CONNECTING,
        ASSOCIATING,
        /** 表示zk客户端已经连接是zk服务端 */
        CONNECTED,
        /** 表示zk客户端以只读方式连接上了zk服务端的状态 */
        CONNECTEDREADONLY,
        /** 表示zk客户端已经关闭的状态 */
        CLOSED,
        /** 认证失败 */
        AUTH_FAILED,
        /** 表示zk客户端还未连接的状态 */
        NOT_CONNECTED;

        /**
         * 该方法返回值表示当前zk客户端与zk服务的一个连接状态是否正常的
         *
         * @return
         */
        public boolean isAlive() {
            return this != CLOSED && this != AUTH_FAILED;
        }

        /**
         * 表示zk客户端是否已经连接上zk服务，即是否可以正常通信了
         */
        public boolean isConnected() {
            return this == CONNECTED || this == CONNECTEDREADONLY;
        }
    }





    /**
     * Wait up to wait milliseconds for the underlying threads to shutdown.
     * THIS METHOD IS EXPECTED TO BE USED FOR TESTING ONLY!!!
     *
     * @since 3.3.0
     *
     * @param wait max wait in milliseconds
     * @return true iff all threads are shutdown, otw false
     */
    protected boolean testableWaitForShutdown(int wait) throws InterruptedException {
        cnxn.sendThread.join(wait);
        if (cnxn.sendThread.isAlive()) return false;
        cnxn.eventThread.join(wait);
        if (cnxn.eventThread.isAlive()) return false;
        return true;
    }

    /**
     * Returns the address to which the socket is connected. Useful for testing
     * against an ensemble - test client may need to know which server
     * to shutdown if interested in verifying that the code handles
     * disconnection/reconnection correctly.
     * THIS METHOD IS EXPECTED TO BE USED FOR TESTING ONLY!!!
     *
     * @since 3.3.0
     *
     * @return ip address of the remote side of the connection or null if
     *         not connected
     */
    protected SocketAddress testableRemoteSocketAddress() {
        return cnxn.sendThread.getClientCnxnSocket().getRemoteSocketAddress();
    }

    /**
     * Returns the local address to which the socket is bound.
     * THIS METHOD IS EXPECTED TO BE USED FOR TESTING ONLY!!!
     *
     * @since 3.3.0
     *
     * @return ip address of the remote side of the connection or null if
     *         not connected
     */
    protected SocketAddress testableLocalSocketAddress() {
        return cnxn.sendThread.getClientCnxnSocket().getLocalSocketAddress();
    }

    /**
     * 通过反射创建一个ClientCnxnSocket实例，可以通过zookeeper.clientCnxnSocket配置，指定ClientCnxnSocket实现
     *
     * @return
     * @throws IOException
     */
    private ClientCnxnSocket getClientCnxnSocket() throws IOException {
        // 获取zookeeper.clientCnxnSocket配置
        String clientCnxnSocketName = getClientConfig().getProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        if (clientCnxnSocketName == null) {
            clientCnxnSocketName = ClientCnxnSocketNIO.class.getName();
        }

        try {
            Constructor<?> clientCxnConstructor = Class.forName(clientCnxnSocketName).getDeclaredConstructor(ZKClientConfig.class);
            ClientCnxnSocket clientCxnSocket = (ClientCnxnSocket) clientCxnConstructor.newInstance(getClientConfig());
            return clientCxnSocket;
        } catch (Exception e) {
            IOException ioe = new IOException("Couldn't instantiate " + clientCnxnSocketName);
            ioe.initCause(e);
            throw ioe;
        }
    }


}

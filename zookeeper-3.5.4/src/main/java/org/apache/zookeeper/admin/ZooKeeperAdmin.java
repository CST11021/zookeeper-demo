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

package org.apache.zookeeper.admin;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * 这是ZooKeeperAdmin客户端库的主类。
 * 这个库用于执行集群管理任务，例如重新配置集群成员。
 * ZooKeeperAdmin类继承了ZooKeeper类，其使用模式与ZooKeeper类类似。
 * 详情请查看{@link ZooKeeper}类文档。
 *
 * @since 3.5.3
 */
// See ZooKeeper.java for an explanation of why we need @SuppressWarnings("try")
@SuppressWarnings("try")
@InterfaceAudience.Public
public class ZooKeeperAdmin extends ZooKeeper {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperAdmin.class);

    /**
     * Create a ZooKeeperAdmin object which is used to perform dynamic reconfiguration
     * operations.
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     *
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     *
     * @see ZooKeeper#ZooKeeper(String, int, Watcher)
     *
     */
    public ZooKeeperAdmin(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
        super(connectString, sessionTimeout, watcher);
    }
    /**
     * Create a ZooKeeperAdmin object which is used to perform dynamic reconfiguration
     * operations.
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param conf
     *            passing this conf object gives each client the flexibility of
     *            configuring properties differently compared to other instances
     *
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     *
     * @see ZooKeeper#ZooKeeper(String, int, Watcher, ZKClientConfig)
     */
    public ZooKeeperAdmin(String connectString, int sessionTimeout, Watcher watcher, ZKClientConfig conf) throws IOException {
        super(connectString, sessionTimeout, watcher, conf);
    }
    /**
     * Create a ZooKeeperAdmin object which is used to perform dynamic reconfiguration
     * operations.
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param canBeReadOnly
     *            whether the created client is allowed to go to
     *            read-only mode in case of partitioning. Read-only mode
     *            basically means that if the client can't find any majority
     *            servers but there's partitioned server it could reach, it
     *            connects to one in read-only mode, i.e. read requests are
     *            allowed while write requests are not. It continues seeking for
     *            majority in the background.
     *
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     *
     * @see ZooKeeper#ZooKeeper(String, int, Watcher, boolean)
     */
    public ZooKeeperAdmin(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws IOException {
        super(connectString, sessionTimeout, watcher, canBeReadOnly);
    }



    /**
     * 添加/删除服务器，并返回新配置。
     * @param joiningServers    要添加的服务器列表(增量重新配置)
     * @param leavingServers    被删除的服务器的逗号分隔列表(增量重新配置)
     * @param newMembers        逗号分隔的新成员列表(非增量重新配置)
     * @param fromConfig        version of the current configuration (optional - causes reconfiguration to throw an exception if configuration is no longer current) parameter if not null.
     * @return new configuration
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     */
    public byte[] reconfigure(String joiningServers, String leavingServers, String newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }

    /**
     * Convenience wrapper around reconfig that takes Lists of strings instead of comma-separated servers.
     *
     * @see #reconfigure
     *
     */
    public byte[] reconfigure(List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }

    /**
     * The Asynchronous version of reconfig.
     *
     * @see #reconfigure
     *
     **/
    public void reconfigure(String joiningServers, String leavingServers, String newMembers, long fromConfig, DataCallback cb, Object ctx) {
        internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }

    /**
     * Convenience wrapper around asynchronous reconfig that takes Lists of strings instead of comma-separated servers.
     *
     * @see #reconfigure
     *
     */
    public void reconfigure(List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, DataCallback cb, Object ctx) {
        internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }

    /**
     * String representation of this ZooKeeperAdmin client. Suitable for things
     * like logging.
     *
     * Do NOT count on the format of this string, it may change without
     * warning.
     *
     * @since 3.5.3
     */
    @Override
    public String toString() {
        return super.toString();
    }
}

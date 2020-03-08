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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Server configuration storage.
 *
 * We use this instead of Properties as it's typed.
 *
 */
@InterfaceAudience.Public
public class ServerConfig {
    ////
    //// If you update the configuration parameters be sure
    //// to update the "conf" 4letter word
    ////

    /**
     * 对客户端暴露的端口，一般为2181
     */
    protected InetSocketAddress clientPortAddress;

    /**
     *
     */
    protected InetSocketAddress secureClientPortAddress;

    /**
     * 表示内存数据库快照存放地址
     *
     * 该参数无默认值，必须配置，不支持系统属性方式配置。
     * 该参数用于配置zk服务器的存储快照文件的目录。默认情况下，如果没有配置参数dataLogDir，那么事务日志也会存储在这个目录中。
     * 考虑到事务日志的写性能直接影响zk整体的服务能力，因此建议同时通过dataLogDir参数来配置zk事务日志的存储目录。
     * (内存数据库快照存放地址，如果没有指定事务日志存放地址(dataLogDir)，默认也是存放在这个路径下，建议两个地址分开存放到不同的设备上。)
     */
    protected File dataDir;

    /**
     * 表示事务日志文件的目录
     *
     * 该参数有默认值：dataDir，可以不配置，不支持系统属性方式配置。
     * 参数dataLogDir用于配置zk服务器存储事务日志文件的目录。默认情况下，zk会将事务日志文件和快照数据存储在同一个目录中，读者应尽量将这两者的目录区分开来。
     * 另外，如果条件允许，可以将事务日志的存储配置在一个单独的磁盘上。事务日志记录对于磁盘的性能要求非常高，为了保证数据的一致性，zk在返回客户端事务请求响应之前，
     * 必须将本次请求对应的事务日志写入到磁盘中。因此，事务日志写入的性能直接决定了zk在处理事务请求时的吞吐。针对同一块磁盘的其他并发读写操作
     * （例如zk运行时日志输出和操作系统自身的读写等），尤其是上文中提到的数据快照操作，会极大地影响事务日志的写性能。因此尽量给事务日志的输出
     * 配置一个单独的磁盘或是挂载点，将极大地提升zk的整体性能。
     */
    protected File dataLogDir;

    /**
     * 该参数有默认值：3000，单位是毫秒，可以不配置，不支持系统属性方式配置。
     * 参数tickTime用于配置zk中最小时间单元的长度，很多运行时的时间间隔都是使用tickTime的倍数来表示的。
     * 例如，zk中会话的最小超时时间默认是2*tickTime。(心跳基本时间单位，毫秒级，ZK基本上所有的时间都是这个时间的整数倍)。
     */
    protected int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;

    /**
     * 控制最大客户端连接数
     */
    protected int maxClientCnxns;

    /** 最小的客户端session超时时间，默认值为2个tickTime，单位是毫秒，如果没有显式设置，则默认为-1，表示使用默认值 */
    protected int minSessionTimeout = -1;
    /** 最大的客户端session超时时间，默认值为20个tickTime，单位是毫秒，如果没有显式设置，则默认为-1，表示使用默认值 */
    protected int maxSessionTimeout = -1;

    /**
     * Parse arguments for server configuration
     * @param args clientPort dataDir and optional tickTime and maxClientCnxns
     * @return ServerConfig configured wrt arguments
     * @throws IllegalArgumentException on invalid usage
     */
    public void parse(String[] args) {
        if (args.length < 2 || args.length > 4) {
            throw new IllegalArgumentException("Invalid number of arguments:" + Arrays.toString(args));
        }

        clientPortAddress = new InetSocketAddress(Integer.parseInt(args[0]));
        dataDir = new File(args[1]);
        dataLogDir = dataDir;
        if (args.length >= 3) {
            tickTime = Integer.parseInt(args[2]);
        }
        if (args.length == 4) {
            maxClientCnxns = Integer.parseInt(args[3]);
        }
    }

    /**
     * Parse a ZooKeeper configuration file
     * @param path the patch of the configuration file
     * @return ServerConfig configured wrt arguments
     * @throws ConfigException error processing configuration
     */
    public void parse(String path) throws ConfigException {
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(path);

        // let qpconfig parse the file and then pull the stuff we are
        // interested in
        readFrom(config);
    }

    /**
     * Read attributes from a QuorumPeerConfig.
     * @param config
     */
    public void readFrom(QuorumPeerConfig config) {
        clientPortAddress = config.getClientPortAddress();
        secureClientPortAddress = config.getSecureClientPortAddress();
        dataDir = config.getDataDir();
        dataLogDir = config.getDataLogDir();
        tickTime = config.getTickTime();
        maxClientCnxns = config.getMaxClientCnxns();
        minSessionTimeout = config.getMinSessionTimeout();
        maxSessionTimeout = config.getMaxSessionTimeout();
    }

    public InetSocketAddress getClientPortAddress() {
        return clientPortAddress;
    }
    public InetSocketAddress getSecureClientPortAddress() {
        return secureClientPortAddress;
    }
    public File getDataDir() { return dataDir; }
    public File getDataLogDir() { return dataLogDir; }
    public int getTickTime() { return tickTime; }
    public int getMaxClientCnxns() { return maxClientCnxns; }
    /** minimum session timeout in milliseconds, -1 if unset */
    public int getMinSessionTimeout() { return minSessionTimeout; }
    /** maximum session timeout in milliseconds, -1 if unset */
    public int getMaxSessionTimeout() { return maxSessionTimeout; }
}

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

package org.apache.zookeeper.server.admin;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.Environment.Entry;
import org.apache.zookeeper.Version;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.server.util.OSMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class containing static methods for registering and running Commands, as well as default Command definitions.
 *
 *
 *
 * zk中有一系列的命令可以查看服务器的运行状态，它们的长度通常都是4个英文字母，因此又被称之为“四字命令”。
 *
 * 四字命令的使用方式非常简单，通常有两种方式：
 *
 * 第一种是通过Telnet方式，使用Telnet客户端登录 ZooKeeper的对外服务端口，然后直接输入四字命令即可；
 *
 * 第二种则是使用nc方式，使用方式： `echo {command} | nc localhost 2181`
 *
 * * **conf**
 *
 * conf命令用于输出ZooKeeper服务器运行时使用的基本配置信息，包括clientPort、dataDir和tickTime等。
 * * **cons**
 *
 * cons命令用于输出当前这台服务器上所有客户端连接的详细信息，包括每个客户端的客户端IP、会话ID和最后一次与服务器交互的操作类型等。
 *
 * * **crst**
 *
 * crst命令是一个功能性命令，用于重置所有的客户端连接统计信息。
 *
 * * **dump**
 *
 * dump命令用于输出当前集群的所有会话信息，包括这些会话的会话ID，以及每个会话创建的临时节点等信息。
 *
 * * **envi**
 *
 * envi命令用于输出ZooKeeper所在服务器运行时的环境信息，包括os.version、java.version和user.home等。
 *
 * * **ruok**
 *
 * ruok命令用于输出当前ZooKeeper服务器是否正在运行。该命令的名字非常有趣，其谐音正好是“Are you ok”。执行该命令后，如果当前ZooKeeper服务器正在运行，那么返回“imok”，否则没有任何响应输出。
 * 请注意，ruok命令的输出仅仅只能表明当前服务器是否正在运行，准确地讲，只能说明2181端口打开着，同时四字命令执行流程正常，但是不能代表ZooKeeper服务器是否运行正常。在很多时候，如果当前服务器无法正常处理客户端的读写请求，甚至已经无法和集群中的其他机器进行通信，ruok命令依然返回“imok”。
 *
 * * **stat**
 *
 * stat命令用于获取ZooKeeper服务器的运行时状态信息，包括基本的ZooKeeper版本、打包信息、运行时角色、集群数据节点个数等信息。
 *
 * * **srvr**
 *
 * srvr命令和stat命令的功能一致，唯一的区别是srvr不会将客户端的连接情况输出，仅仅输出服务器的自身信息
 *
 * * **srst**
 *
 * srst命令是一个功能行命令，用于重置所有服务器的统计信息。
 *
 * * **wchs**
 *
 * wchs命令用于输出当前服务器上管理的Watcher的概要信息。
 *
 * * **wchc**
 *
 * wchc命令用于输出当前服务器上管理的Watcher的详细信息，以会话为单位进行归组，同时列出被该会话注册了Watcher的节点路径。
 *
 * * **wchp**
 *
 * wchp命令和wchc命令非常类似，也是用于输出当前服务器上管理的Watcher的详细信息，不同点在于wchp命令的输出信息以节点路径为单位进行归组。
 *
 * * **mntr**
 *
 * mntr命令用于输出比stat命令更为详尽的服务器统计信息，包括请求处理的延迟情况、服务器内存数据库大小和集群的数据同步情况。
 *
 * @see Command
 * @see JettyAdminServer
 */
public class Commands {

    static final Logger LOG = LoggerFactory.getLogger(Commands.class);

    /** Maps command names to Command instances */
    private static Map<String, Command> commands = new HashMap<String, Command>();
    private static Set<String> primaryNames = new HashSet<String>();

    private Commands() {}

    /**
     * 注册命令
     *
     * @param command
     */
    public static void registerCommand(Command command) {
        for (String name : command.getNames()) {
            Command prev = commands.put(name, command);
            if (prev != null) {
                LOG.warn("Re-registering command %s (primary name = %s)", name, command.getPrimaryName());
            }
        }
        primaryNames.add(command.getPrimaryName());
    }

    /**
     * Run the registered command with name cmdName. Commands should not produce
     * any exceptions; any (anticipated) errors should be reported in the
     * "error" entry of the returned map. Likewise, if no command with the given
     * name is registered, this will be noted in the "error" entry.
     *
     * @param cmdName
     * @param zkServer
     * @param kwargs String-valued keyword arguments to the command
     *        (may be null if command requires no additional arguments)
     * @return Map representing response to command containing at minimum:
     *    - "command" key containing the command's primary name
     *    - "error" key containing a String error message or null if no error
     */
    public static CommandResponse runCommand(String cmdName, ZooKeeperServer zkServer, Map<String, String> kwargs) {
        if (!commands.containsKey(cmdName)) {
            return new CommandResponse(cmdName, "Unknown command: " + cmdName);
        }
        if (zkServer == null || !zkServer.isRunning()) {
            return new CommandResponse(cmdName, "This ZooKeeper instance is not currently serving requests");
        }
        return commands.get(cmdName).run(zkServer, kwargs);
    }

    /**
     * Returns the primary names of all registered commands.
     */
    public static Set<String> getPrimaryNames() {
        return primaryNames;
    }

    /**
     * Returns the commands registered under cmdName with registerCommand, or
     * null if no command is registered with that name.
     */
    public static Command getCommand(String cmdName) {
        return commands.get(cmdName);
    }

    static {
        registerCommand(new CnxnStatResetCommand());
        registerCommand(new ConfCommand());
        registerCommand(new ConsCommand());
        registerCommand(new DirsCommand());
        registerCommand(new DumpCommand());
        registerCommand(new EnvCommand());
        registerCommand(new GetTraceMaskCommand());
        registerCommand(new IsroCommand());
        registerCommand(new MonitorCommand());
        registerCommand(new RuokCommand());
        registerCommand(new SetTraceMaskCommand());
        registerCommand(new SrvrCommand());
        registerCommand(new StatCommand());
        registerCommand(new StatResetCommand());
        registerCommand(new WatchCommand());
        registerCommand(new WatchesByPathCommand());
        registerCommand(new WatchSummaryCommand());
    }

    /**
     * 重置所有的连接的命令
     */
    public static class CnxnStatResetCommand extends CommandBase {
        public CnxnStatResetCommand() {
            super(Arrays.asList("connection_stat_reset", "crst"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            zkServer.getServerCnxnFactory().resetAllConnectionStats();
            return response;

        }
    }

    /**
     * 获取zk配置的命令
     *
     * @see ZooKeeperServer#getConf()
     */
    public static class ConfCommand extends CommandBase {
        public ConfCommand() {
            super(Arrays.asList("configuration", "conf", "config"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.putAll(zkServer.getConf().toMap());
            return response;
        }
    }

    /**
     * Information on client connections to server. Returned Map contains:
     *   - "connections": list of connection info objects
     * @see org.apache.zookeeper.server.ServerCnxn#getConnectionInfo(boolean)
     */
    public static class ConsCommand extends CommandBase {
        public ConsCommand() {
            super(Arrays.asList("connections", "cons"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("connections", zkServer.getServerCnxnFactory().getAllConnectionInfo(false));
            return response;
        }
    }

    /**
     * Information on ZK datadir and snapdir size in bytes
     */
    public static class DirsCommand extends CommandBase {
        public DirsCommand() {
            super(Arrays.asList("dirs"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("datadir_size", zkServer.getDataDirSize());
            response.put("logdir_size", zkServer.getLogDirSize());
            return response;
        }
    }

    /**
     * dump命令用于输出当前集群的所有会话信息，包括这些会话的会话ID，以及每个会话创建的临时节点等信息。
     *
     * Information on session expirations and ephemerals. Returned map contains:
     *   - "expiry_time_to_session_ids": Map<Long, Set<Long>>
     *                                   time -> sessions IDs of sessions that expire at time
     *   - "sesssion_id_to_ephemeral_paths": Map<Long, Set<String>>
     *                                       session ID -> ephemeral paths created by that session
     * @see ZooKeeperServer#getSessionExpiryMap()
     * @see ZooKeeperServer#getEphemerals()
     */
    public static class DumpCommand extends CommandBase {
        public DumpCommand() {
            super(Arrays.asList("dump"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("expiry_time_to_session_ids", zkServer.getSessionExpiryMap());
            response.put("session_id_to_ephemeral_paths", zkServer.getEphemerals());
            return response;
        }
    }

    /**
     * envi命令用于输出ZooKeeper所在服务器运行时的环境信息，包括os.version、java.version和user.home等。
     */
    public static class EnvCommand extends CommandBase {
        public EnvCommand() {
            super(Arrays.asList("environment", "env", "envi"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            for (Entry e : Environment.list()) {
                response.put(e.getKey(), e.getValue());
            }
            return response;
        }
    }

    /**
     * The current trace mask. Returned map contains:
     *   - "tracemask": Long
     */
    public static class GetTraceMaskCommand extends CommandBase {
        public GetTraceMaskCommand() {
            super(Arrays.asList("get_trace_mask", "gtmk"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("tracemask", ZooTrace.getTextTraceLevel());
            return response;
        }
    }

    /**
     * Is this server in read-only mode. Returned map contains:
     *   - "is_read_only": Boolean
     */
    public static class IsroCommand extends CommandBase {
        public IsroCommand() {
            super(Arrays.asList("is_read_only", "isro"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("read_only", zkServer instanceof ReadOnlyZooKeeperServer);
            return response;
        }
    }

    /**
     * Some useful info for monitoring. Returned map contains:
     *   - "version": String
     *                server version
     *   - "avg_latency": Long
     *   - "max_latency": Long
     *   - "min_latency": Long
     *   - "packets_received": Long
     *   - "packets_sents": Long
     *   - "num_alive_connections": Integer
     *   - "outstanding_requests": Long
     *                             number of unprocessed requests
     *   - "server_state": "leader", "follower", or "standalone"
     *   - "znode_count": Integer
     *   - "watch_count": Integer
     *   - "ephemerals_count": Integer
     *   - "approximate_data_size": Long
     *   - "open_file_descriptor_count": Long (unix only)
     *   - "max_file_descritpor_count": Long (unix only)
     *   - "followers": Integer (leader only)
     *   - "synced_followers": Integer (leader only)
     *   - "pending_syncs": Integer (leader only)
     */
    public static class MonitorCommand extends CommandBase {
        public MonitorCommand() {
            super(Arrays.asList("monitor", "mntr"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            ZKDatabase zkdb = zkServer.getZKDatabase();
            ServerStats stats = zkServer.serverStats();

            CommandResponse response = initializeResponse();

            response.put("version", Version.getFullVersion());

            response.put("avg_latency", stats.getAvgLatency());
            response.put("max_latency", stats.getMaxLatency());
            response.put("min_latency", stats.getMinLatency());

            response.put("packets_received", stats.getPacketsReceived());
            response.put("packets_sent", stats.getPacketsSent());
            response.put("num_alive_connections", stats.getNumAliveClientConnections());

            response.put("outstanding_requests", stats.getOutstandingRequests());

            response.put("server_state", stats.getServerState());
            response.put("znode_count", zkdb.getNodeCount());

            response.put("watch_count", zkdb.getDataTree().getWatchCount());
            response.put("ephemerals_count", zkdb.getDataTree().getEphemeralsCount());
            response.put("approximate_data_size", zkdb.getDataTree().approximateDataSize());

            OSMXBean osMbean = new OSMXBean();
            response.put("open_file_descriptor_count", osMbean.getOpenFileDescriptorCount());
            response.put("max_file_descriptor_count", osMbean.getMaxFileDescriptorCount());

            if (zkServer instanceof LeaderZooKeeperServer) {
                Leader leader = ((LeaderZooKeeperServer) zkServer).getLeader();

                response.put("followers", leader.getLearners().size());
                response.put("synced_followers", leader.getForwardingFollowers().size());
                response.put("pending_syncs", leader.getNumPendingSyncs());
            }

            return response;

        }}

    /**
     * ruok命令用于输出当前ZooKeeper服务器是否正在运行。该命令的名字非常有趣，其谐音正好是“Are you ok”。执行该命令后，如果当前ZooKeeper服务器正在运行，那么返回“imok”，否则没有任何响应输出。
     * 请注意，ruok命令的输出仅仅只能表明当前服务器是否正在运行，准确地讲，只能说明2181端口打开着，同时四字命令执行流程正常，但是不能代表ZooKeeper服务器是否运行正常。在很多时候，如果当前服务器无法正常处理客户端的读写请求，甚至已经无法和集群中的其他机器进行通信，ruok命令依然返回“imok”。
     */
    public static class RuokCommand extends CommandBase {
        public RuokCommand() {
            super(Arrays.asList("ruok"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            return initializeResponse();
        }
    }

    /**
     * Sets the trace mask. Required arguments:
     *   - "traceMask": Long
     *  Returned Map contains:
     *   - "tracemask": Long
     */
    public static class SetTraceMaskCommand extends CommandBase {
        public SetTraceMaskCommand() {
            super(Arrays.asList("set_trace_mask", "stmk"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            long traceMask;
            if (!kwargs.containsKey("traceMask")) {
                response.put("error", "setTraceMask requires long traceMask argument");
                return response;
            }
            try {
                traceMask = Long.parseLong(kwargs.get("traceMask"));
            } catch (NumberFormatException e) {
                response.put("error", "setTraceMask requires long traceMask argument, got "
                                      + kwargs.get("traceMask"));
                return response;
            }

            ZooTrace.setTextTraceLevel(traceMask);
            response.put("tracemask", traceMask);
            return response;
        }
    }

    /**
     * srvr命令和stat命令的功能一致，唯一的区别是srvr不会将客户端的连接情况输出，仅仅输出服务器的自身信息
     *
     * Server information. Returned map contains:
     *   - "version": String
     *                version of server
     *   - "read_only": Boolean
     *                  is server in read-only mode
     *   - "server_stats": ServerStats object
     *   - "node_count": Integer
     */
    public static class SrvrCommand extends CommandBase {
        public SrvrCommand() {
            super(Arrays.asList("server_stats", "srvr"));
        }

        // Allow subclasses (e.g. StatCommand) to specify their own names
        protected SrvrCommand(List<String> names) {
            super(names);
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            LOG.info("running stat");
            response.put("version", Version.getFullVersion());
            response.put("read_only", zkServer instanceof ReadOnlyZooKeeperServer);
            response.put("server_stats", zkServer.serverStats());
            response.put("node_count", zkServer.getZKDatabase().getNodeCount());
            return response;

        }
    }

    /**
     * stat命令用于获取ZooKeeper服务器的运行时状态信息，包括基本的ZooKeeper版本、打包信息、运行时角色、集群数据节点个数等信息。
     *
     * Same as SrvrCommand but has extra "connections" entry.
     */
    public static class StatCommand extends SrvrCommand {
        public StatCommand() {
            super(Arrays.asList("stats", "stat"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = super.run(zkServer, kwargs);
            response.put("connections", zkServer.getServerCnxnFactory().getAllConnectionInfo(true));
            return response;
        }
    }

    /**
     * Resets server statistics.
     */
    public static class StatResetCommand extends CommandBase {
        public StatResetCommand() {
            super(Arrays.asList("stat_reset", "srst"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            zkServer.serverStats().reset();
            return response;
        }
    }

    /**
     * Watch information aggregated by session. Returned Map contains:
     *   - "session_id_to_watched_paths": Map<Long, Set<String>> session ID -> watched paths
     * @see DataTree#getWatches()
     */
    public static class WatchCommand extends CommandBase {
        public WatchCommand() {
            super(Arrays.asList("watches", "wchc"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.put("session_id_to_watched_paths", dt.getWatches().toMap());
            return response;
        }
    }

    /**
     * Watch information aggregated by path. Returned Map contains:
     *   - "path_to_session_ids": Map<String, Set<Long>> path -> session IDs of sessions watching path
     * @see DataTree#getWatchesByPath()
     */
    public static class WatchesByPathCommand extends CommandBase {
        public WatchesByPathCommand() {
            super(Arrays.asList("watches_by_path", "wchp"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.put("path_to_session_ids", dt.getWatchesByPath().toMap());
            return response;
        }
    }

    /**
     * Summarized watch information.
     * @see DataTree#getWatchesSummary()
     */
    public static class WatchSummaryCommand extends CommandBase {
        public WatchSummaryCommand() {
            super(Arrays.asList("watch_summary", "wchs"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.putAll(dt.getWatchesSummary().toMap());
            return response;
        }
    }


}

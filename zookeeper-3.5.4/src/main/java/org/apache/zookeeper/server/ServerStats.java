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



import org.apache.zookeeper.common.Time;

/**
 * zk服务器运行时的统计器
 */
public class ServerStats {
    /** 从zk启动开始，或是最近一次重置服务端统计信息之后，服务端向客户端发送响应包次数 */
    private long packetsSent;
    /** 从zk启动开始，或是最近一次重置服务端统计信息之后，服务端接收到的来自客户端的请求包次数 */
    private long packetsReceived;
    /** 从zk启动开始，或是最近一次重置服务端统计信息之后，服务端请求处理的最大延时 */
    private long maxLatency;
    /** 从zk启动开始，或是最近一次重置服务端统计信息之后，服务端请求处理的最小延时 */
    private long minLatency = Long.MAX_VALUE;
     /** 从zk启动开始，或是最近一次重置服务端统计信息之后，服务端请求处理的总延时 */
    private long totalLatency = 0;
    /** 从zk启动开始，或是最近一次重置服务端统计信息之后，服务端处理的客户端请求总次数 */
    private long count = 0;

    private final Provider provider;

    /**
     * zk服务
     */
    public interface Provider {

        /**
         * 返回队列中尚未处理的请求，这些请求尚未被处理
         */
        public long getOutstandingRequests();

        /**
         * 获取最后操作DataTree的事务ID
         *
         * @return
         */
        public long getLastProcessedZxid();

        /**
         * 表示服务的类型，这里使用getState命名表示当前zk节点的状态是可变的，比如：当前可能follower类型角色，当重新进行选举后，可能变为leader状态
         *
         * @return
         */
        public String getState();

        /**
         * 返回此服务器上活动的客户端连接的总数
         */
        public int getNumAliveConnections();

        /**
         * 获取zk的快照文件数量
         *
         * @return
         */
        public long getDataDirSize();

        /**
         * 获取zk事务日志文件数量
         *
         * @return
         */
        public long getLogDirSize();
    }
    
    public ServerStats(Provider provider) {
        this.provider = provider;
    }
    
    // getters
    synchronized public long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }

    synchronized public long getAvgLatency() {
        if (count != 0) {
            return totalLatency / count;
        }
        return 0;
    }

    synchronized public long getMaxLatency() {
        return maxLatency;
    }

    public long getOutstandingRequests() {
        return provider.getOutstandingRequests();
    }
    
    public long getLastProcessedZxid(){
        return provider.getLastProcessedZxid();
    }

    public long getDataDirSize() {
        return provider.getDataDirSize();
    }

    public long getLogDirSize() {
        return provider.getLogDirSize();
    }
    
    synchronized public long getPacketsReceived() {
        return packetsReceived;
    }

    synchronized public long getPacketsSent() {
        return packetsSent;
    }

    public String getServerState() {
        return provider.getState();
    }
    
    /** The number of client connections alive to this server */
    public int getNumAliveClientConnections() {
    	return provider.getNumAliveConnections();
    }

    public boolean isProviderNull() {
        return provider == null;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Latency min/avg/max: " + getMinLatency() + "/"
                + getAvgLatency() + "/" + getMaxLatency() + "\n");
        sb.append("Received: " + getPacketsReceived() + "\n");
        sb.append("Sent: " + getPacketsSent() + "\n");
        sb.append("Connections: " + getNumAliveClientConnections() + "\n");

        if (provider != null) {
            sb.append("Outstanding: " + getOutstandingRequests() + "\n");
            sb.append("Zxid: 0x"+ Long.toHexString(getLastProcessedZxid())+ "\n");
        }
        sb.append("Mode: " + getServerState() + "\n");
        return sb.toString();
    }

    /**
     * 每次zk处理请求时，会调用该方法，记录延时相关的统计信息
     *
     * @param requestCreateTime 表示请求的创建时间
     */
    synchronized void updateLatency(long requestCreateTime) {
        // 计算请求创建的时间当前时间的时间差
        long latency = Time.currentElapsedTime() - requestCreateTime;
        totalLatency += latency;
        count++;

        // 记录最小延时
        if (latency < minLatency) {
            minLatency = latency;
        }

        // 记录最大延时
        if (latency > maxLatency) {
            maxLatency = latency;
        }
    }
    synchronized public void resetLatency(){
        totalLatency = 0;
        count = 0;
        maxLatency = 0;
        minLatency = Long.MAX_VALUE;
    }
    synchronized public void resetMaxLatency(){
        maxLatency = getMinLatency();
    }
    synchronized public void incrementPacketsReceived() {
        packetsReceived++;
    }
    synchronized public void incrementPacketsSent() {
        packetsSent++;
    }
    synchronized public void resetRequestCounters(){
        packetsReceived = 0;
        packetsSent = 0;
    }
    synchronized public void reset() {
        resetLatency();
        resetRequestCounters();
    }

}

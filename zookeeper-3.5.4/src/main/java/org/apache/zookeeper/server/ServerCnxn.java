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

import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface to a Server connection - represents a connection from a client to the server.
 */
public abstract class ServerCnxn implements Stats, Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ServerCnxn.class);

    /** 这只是一个表示请求的任意对象 */
    final public static Object me = new Object();

    /** 表示认证过的用户 */
    protected ArrayList<Id> authInfo = new ArrayList<Id>();

    /**
     * If the client is of old version, we don't send r-o mode info to it.
     * The reason is that if we would, old C client doesn't read it, which
     * results in TCP RST packet, i.e. "connection reset by peer".
     */
    boolean isOldClient = true;

    // ----------------------
    // 统计ServerCnxn相关的信息
    // ----------------------

    /** 建立连接的日期/时间  */
    protected final Date established = new Date();
    /** 表示接收到的packets个数 */
    protected final AtomicLong packetsReceived = new AtomicLong();
    /** 表示已经发送packet个数 */
    protected final AtomicLong packetsSent = new AtomicLong();
    /** 最低延迟，毫秒 */
    protected long minLatency;
    /** 最高延迟，毫秒 */
    protected long maxLatency;
    /** 连接最后一次操作 */
    protected String lastOp;
    /** 最后一次连接的cxid */
    protected long lastCxid;
    /** 最后一次连接的zxid */
    protected long lastZxid;
    /** 上次回复的时间 */
    protected long lastResponseTime;
    /** 上一次回复的延迟 */
    protected long lastLatency;
    /** response的次数 */
    protected long count;
    /** 总共累计的延时毫秒数 */
    protected long totalLatency;


    /**
     * session超时时间
     *
     * @return
     */
    abstract int getSessionTimeout();

    /**
     * 设置session超时时间
     *
     * @param sessionTimeout
     */
    abstract void setSessionTimeout(int sessionTimeout);

    /**
     * 关闭服务
     */
    abstract void close();

    /**
     * 发送响应体
     *
     * @param h
     * @param r
     * @param tag
     * @throws IOException
     */
    public abstract void sendResponse(ReplyHeader h, Record r, String tag) throws IOException;

    /**
     * 通知客户端会话正在关闭并关闭/清理套接字
     */
    abstract void sendCloseSession();

    /**
     * ServerCnxn也实现了Watcher接口，当相应的时间发生时，会调用该方法
     *
     * @param event
     */
    public abstract void process(WatchedEvent event);

    /**
     * 获取sessionId
     *
     * @return
     */
    public abstract long getSessionId();

    /**
     * 设置sessionId
     *
     * @param sessionId
     */
    abstract void setSessionId(long sessionId);

    /** auth info for the cnxn, returns an unmodifyable list */
    public List<Id> getAuthInfo() {
        return Collections.unmodifiableList(authInfo);
    }

    /**
     * 添加授权用户
     *
     * @param id
     */
    public void addAuthInfo(Id id) {
        if (authInfo.contains(id) == false) {
            authInfo.add(id);
        }
    }

    public boolean removeAuthInfo(Id id) {
        return authInfo.remove(id);
    }

    abstract void sendBuffer(ByteBuffer closeConn);

    abstract void enableRecv();

    abstract void disableRecv();

    protected ZooKeeperSaslServer zooKeeperSaslServer = null;

    protected static class CloseRequestException extends IOException {
        private static final long serialVersionUID = -7854505709816442681L;

        public CloseRequestException(String msg) {
            super(msg);
        }
    }

    protected static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -8255690282104294178L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }

    protected void packetReceived() {
        incrPacketsReceived();
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            serverStats().incrementPacketsReceived();
        }
    }

    protected void packetSent() {
        incrPacketsSent();
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            serverStats().incrementPacketsSent();
        }
    }

    protected abstract ServerStats serverStats();

    /**
     * 获取客户端的地址，即请求来自哪里
     *
     * @return
     */
    public abstract InetSocketAddress getRemoteSocketAddress();

    public abstract int getInterestOps();
    public abstract boolean isSecure();
    public abstract Certificate[] getClientCertificateChain();
    public abstract void setClientCertificateChain(Certificate[] chain);

    /**
     * Print information about the connection.
     * @param brief iff true prints brief details, otw full detail
     * @return information about this connection
     */
    public synchronized void dumpConnectionInfo(PrintWriter pwriter, boolean brief) {
        pwriter.print(" ");
        pwriter.print(getRemoteSocketAddress());
        pwriter.print("[");
        int interestOps = getInterestOps();
        pwriter.print(interestOps == 0 ? "0" : Integer.toHexString(interestOps));
        pwriter.print("](queued=");
        pwriter.print(getOutstandingRequests());
        pwriter.print(",recved=");
        pwriter.print(getPacketsReceived());
        pwriter.print(",sent=");
        pwriter.print(getPacketsSent());

        if (!brief) {
            long sessionId = getSessionId();
            if (sessionId != 0) {
                pwriter.print(",sid=0x");
                pwriter.print(Long.toHexString(sessionId));
                pwriter.print(",lop=");
                pwriter.print(getLastOperation());
                pwriter.print(",est=");
                pwriter.print(getEstablished().getTime());
                pwriter.print(",to=");
                pwriter.print(getSessionTimeout());
                long lastCxid = getLastCxid();
                if (lastCxid >= 0) {
                    pwriter.print(",lcxid=0x");
                    pwriter.print(Long.toHexString(lastCxid));
                }
                pwriter.print(",lzxid=0x");
                pwriter.print(Long.toHexString(getLastZxid()));
                pwriter.print(",lresp=");
                pwriter.print(getLastResponseTime());
                pwriter.print(",llat=");
                pwriter.print(getLastLatency());
                pwriter.print(",minlat=");
                pwriter.print(getMinLatency());
                pwriter.print(",avglat=");
                pwriter.print(getAvgLatency());
                pwriter.print(",maxlat=");
                pwriter.print(getMaxLatency());
            }
        }
        pwriter.print(")");
    }

    public synchronized Map<String, Object> getConnectionInfo(boolean brief) {
        Map<String, Object> info = new LinkedHashMap<String, Object>();
        info.put("remote_socket_address", getRemoteSocketAddress());
        info.put("interest_ops", getInterestOps());
        info.put("outstanding_requests", getOutstandingRequests());
        info.put("packets_received", getPacketsReceived());
        info.put("packets_sent", getPacketsSent());
        if (!brief) {
            info.put("session_id", getSessionId());
            info.put("last_operation", getLastOperation());
            info.put("established", getEstablished());
            info.put("session_timeout", getSessionTimeout());
            info.put("last_cxid", getLastCxid());
            info.put("last_zxid", getLastZxid());
            info.put("last_response_time", getLastResponseTime());
            info.put("last_latency", getLastLatency());
            info.put("min_latency", getMinLatency());
            info.put("avg_latency", getAvgLatency());
            info.put("max_latency", getMaxLatency());
        }
        return info;
    }

    /**
     * clean up the socket related to a command and also make sure we flush the
     * data before we do that
     *
     * @param pwriter
     *            the pwriter for a command socket
     */
    public void cleanupWriterSocket(PrintWriter pwriter) {
        try {
            if (pwriter != null) {
                pwriter.flush();
                pwriter.close();
            }
        } catch (Exception e) {
            LOG.info("Error closing PrintWriter ", e);
        } finally {
            try {
                close();
            } catch (Exception e) {
                LOG.error("Error closing a command socket ", e);
            }
        }
    }





    // ---------------------------------------
    // 关于 ServerCnxn 的统计信息，Stats接口的实现
    // ---------------------------------------

    /**
     * 建立连接的日期/时间
     */
    public Date getEstablished() {
        return (Date)established.clone();
    }
    /**
     * 获取已经提交但是尚未回复的请求个数
     *
     * @return
     */
    public abstract long getOutstandingRequests();
    /**
     * 获取接收到的packets个数
     * @return
     */
    public long getPacketsReceived() {
        return packetsReceived.longValue();
    }
    /**
     * 获取已经发送packet个数
     *
     * @return
     */
    public long getPacketsSent() {
        return packetsSent.longValue();
    }
    /**
     * 最低延迟，毫秒
     *
     * @return
     */
    public synchronized long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }
    /**
     * 平均延迟，毫秒
     * @return
     */
    public synchronized long getAvgLatency() {
        return count == 0 ? 0 : totalLatency / count;
    }
    /**
     * 最高延迟，毫秒
     *
     * @return
     */
    public synchronized long getMaxLatency() {
        return maxLatency;
    }
    /**
     * 连接最后一次操作
     *
     * @return
     */
    public synchronized String getLastOperation() {
        return lastOp;
    }
    /**
     * 最后一次连接的cxid
     *
     * @return
     */
    public synchronized long getLastCxid() {
        return lastCxid;
    }
    /**
     * 最后一次连接的zxid
     *
     * @return
     */
    public synchronized long getLastZxid() {
        return lastZxid;
    }
    /**
     * 上次回复的时间
     *
     * @return
     */
    public synchronized long getLastResponseTime() {
        return lastResponseTime;
    }
    /**
     * 上一次回复的延迟
     *
     * @return
     */
    public synchronized long getLastLatency() {
        return lastLatency;
    }
    /**
     * 还原各种计数器
     */
    public synchronized void resetStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        minLatency = Long.MAX_VALUE;
        maxLatency = 0;
        lastOp = "NA";
        lastCxid = -1;
        lastZxid = -1;
        lastResponseTime = 0;
        lastLatency = 0;

        count = 0;
        totalLatency = 0;
    }
    /**
     * 将接收到的packets个数+1
     *
     * @return
     */
    protected long incrPacketsReceived() {
        return packetsReceived.incrementAndGet();
    }
    /**
     * 将请求已提交到队列但尚未处理的请求的数目+1
     *
     * @param h
     */
    protected void incrOutstandingRequests(RequestHeader h) {
    }
    /**
     * 将已经发送packet个数
     *
     * @return
     */
    protected long incrPacketsSent() {
        return packetsSent.incrementAndGet();
    }
    /**
     * response后，调用该方法更新状态
     *
     * @param cxid
     * @param zxid
     * @param op
     * @param start
     * @param end
     */
    protected synchronized void updateStatsForResponse(long cxid, long zxid, String op, long start, long end) {
        // don't overwrite with "special" xids - we're interested in the clients last real operation
        if (cxid >= 0) {
            lastCxid = cxid;
        }
        lastZxid = zxid;
        lastOp = op;
        lastResponseTime = end;
        long elapsed = end - start;
        lastLatency = elapsed;
        if (elapsed < minLatency) {
            minLatency = elapsed;
        }
        if (elapsed > maxLatency) {
            maxLatency = elapsed;
        }
        count++;
        totalLatency += elapsed;
    }

    /**
     * Prints detailed stats information for the connection.
     *
     * @see #dumpConnectionInfo(PrintWriter, boolean) for brief stats
     */
    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpConnectionInfo(pwriter, false);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

}

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

package org.apache.zookeeper;

import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * zookeeper客户端和服务器的连接主要是通过ClientCnxnSocket来实现的，有两个具体的实现类ClientCnxnSocketNetty和ClientCnxnSocketNIO，其工作流程如下：
 *
 * 1、先判断是否连接，没有连接则调用connect方法进行连接，有连接则进入下一步；
 * 2、然后调用doTransport方法进行通信，若连接过程中出现异常，则调用cleanup()方法；
 * 3、最后关闭连接。
 *
 *
 * ClientCnxnSocket使用套接字实现进行低层通信
 */
abstract class ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocket.class);

    protected boolean initialized;

    /**
     * 此缓冲区仅用于读取传入消息的长度。
     */
    protected final ByteBuffer lenBuffer = ByteBuffer.allocateDirect(4);

    /**
     * After the length is read, a new incomingBuffer is allocated in
     * readLength() to receive the full message.
     */
    protected ByteBuffer incomingBuffer = lenBuffer;
    protected long sentCount = 0;
    protected long recvCount = 0;
    protected long lastHeard;
    protected long lastSend;

    /**
     * 表示给zk服务发送请求的时间
     */
    protected long now;

    /**
     * 表示给zk服务器发送请求的线程对象
     */
    protected ClientCnxn.SendThread sendThread;

    /**
     * 对应{@link ClientCnxn#outgoingQueue}，该队列是一个请求发送队列，专门用于存储那些需要发送到服务端的Packet集合
     */
    protected LinkedBlockingDeque<Packet> outgoingQueue;

    /**
     * zk客户端配置
     */
    protected ZKClientConfig clientConfig;

    private int packetLen = ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT;

    /**
     * 客户端给zk服务发送请求的携带的sessionId
     */
    protected long sessionId;

    /**
     * 设置发送请求的线程，sessionId和保存发送请求的队列
     *
     * @param sendThread
     * @param sessionId
     * @param outgoingQueue
     */
    void introduce(ClientCnxn.SendThread sendThread, long sessionId, LinkedBlockingDeque<Packet> outgoingQueue) {
        this.sendThread = sendThread;
        this.sessionId = sessionId;
        this.outgoingQueue = outgoingQueue;
    }

    void updateNow() {
        now = Time.currentElapsedTime();
    }

    int getIdleRecv() {
        return (int) (now - lastHeard);
    }

    int getIdleSend() {
        return (int) (now - lastSend);
    }

    long getSentCount() {
        return sentCount;
    }

    long getRecvCount() {
        return recvCount;
    }

    void updateLastHeard() {
        this.lastHeard = now;
    }

    void updateLastSend() {
        this.lastSend = now;
    }

    /**
     * 更新lastSend和lastHeard为当前时间
     */
    void updateLastSendAndHeard() {
        this.lastSend = now;
        this.lastHeard = now;
    }

    protected void readLength() throws IOException {
        int len = incomingBuffer.getInt();
        if (len < 0 || len >= packetLen) {
            throw new IOException("Packet len" + len + " is out of range!");
        }
        incomingBuffer = ByteBuffer.allocate(len);
    }

    void readConnectResult() throws IOException {
        if (LOG.isTraceEnabled()) {
            StringBuilder buf = new StringBuilder("0x[");
            for (byte b : incomingBuffer.array()) {
                buf.append(Integer.toHexString(b) + ",");
            }
            buf.append("]");
            LOG.trace("readConnectResult " + incomingBuffer.remaining() + " " + buf.toString());
        }
        ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
        ConnectResponse conRsp = new ConnectResponse();
        conRsp.deserialize(bbia, "connect");

        // read "is read-only" flag
        boolean isRO = false;
        try {
            isRO = bbia.readBool("readOnly");
        } catch (IOException e) {
            // this is ok -- just a packet from an old server which
            // doesn't contain readOnly field
            LOG.warn("Connected to an old server; r-o mode will be unavailable");
        }

        this.sessionId = conRsp.getSessionId();
        sendThread.onConnected(conRsp.getTimeOut(), this.sessionId, conRsp.getPasswd(), isRO);
    }

    abstract boolean isConnected();

    /**
     * 连接到目标机器
     *
     * @param addr  目标机器地址
     * @throws IOException
     */
    abstract void connect(InetSocketAddress addr) throws IOException;

    /**
     * Returns the address to which the socket is connected.
     */
    abstract SocketAddress getRemoteSocketAddress();

    /**
     * Returns the address to which the socket is bound.
     */
    abstract SocketAddress getLocalSocketAddress();

    /**
     * Clean up resources for a fresh new socket.
     * It's called before reconnect or close.
     */
    abstract void cleanup();

    /**
     * new packets are added to outgoingQueue.
     */
    abstract void packetAdded();

    /**
     * connState is marked CLOSED and notify ClientCnxnSocket to react.
     */
    abstract void onClosing();

    /**
     * Sasl completes. Allows non-priming packgets to be sent.
     * Note that this method will only be called if Sasl starts and completes.
     */
    abstract void saslCompleted();

    /**
     * being called after ClientCnxn finish PrimeConnection
     */
    abstract void connectionPrimed();

    /**
     * Do transportation work:
     * - read packets into incomingBuffer.
     * - write outgoing queue packets.
     * - update relevant timestamp.
     *
     * @param waitTimeOut timeout in blocking wait. Unit in MilliSecond.
     * @param pendingQueue These are the packets that have been sent and
     *                     are waiting for a response.
     * @param cnxn
     * @throws IOException
     * @throws InterruptedException
     */
    abstract void doTransport(int waitTimeOut, List<Packet> pendingQueue, ClientCnxn cnxn) throws IOException, InterruptedException;

    /**
     * Close the socket.
     */
    abstract void testableCloseSocket() throws IOException;

    /**
     * Close this client.
     */
    abstract void close();

    /**
     * Send Sasl packets directly.
     * The Sasl process will send the first (requestHeader == null) packet,
     * and then block the doTransport write,
     * finally unblock it when finished.
     */
    abstract void sendPacket(Packet p) throws IOException;

    protected void initProperties() throws IOException {
        try {
            packetLen = clientConfig.getInt(ZKConfig.JUTE_MAXBUFFER,
                    ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT);
            LOG.info("{} value is {} Bytes", ZKConfig.JUTE_MAXBUFFER,
                    packetLen);
        } catch (NumberFormatException e) {
            String msg = MessageFormat.format(
                    "Configured value {0} for property {1} can not be parsed to int",
                    clientConfig.getProperty(ZKConfig.JUTE_MAXBUFFER),
                    ZKConfig.JUTE_MAXBUFFER);
            LOG.error(msg);
            throw new IOException(msg);
        }
    }

}

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

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.NIOServerCnxnFactory.SelectorThread;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.NopCommand;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class handles communication with clients using NIO. There is one per
 * client, but only one thread doing the communication.
 */
public class NIOServerCnxn extends ServerCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxn.class);

    private final NIOServerCnxnFactory factory;

    /**
     * 监听客户端的SocketChannel，客户端发起的TCP连接,也即SocketChannel类
     */
    private final SocketChannel sock;

    private final SelectorThread selectorThread;

    private final SelectionKey sk;

    /**
     * 当第一次从channel中获取请求的数据，并保存到incomingBuffer后，该值设置为true
     */
    private boolean initialized;

    private final ByteBuffer lenBuffer = ByteBuffer.allocate(4);

    /** 从SocketChannel读取请求数据后，会将数据保存放在这里 */
    private ByteBuffer incomingBuffer = lenBuffer;

    private final Queue<ByteBuffer> outgoingBuffers = new LinkedBlockingQueue<ByteBuffer>();

    private int sessionTimeout;

    private final ZooKeeperServer zkServer;

    /**
     * 用于统计来自客户端的请求，这些请求已提交到队列但尚未处理的请求的数目
     */
    private final AtomicInteger outstandingRequests = new AtomicInteger(0);

    /**
     * 这是唯一标识客户端会话的id。一旦这个会话不再活动，临时节点就会消失。
     */
    private long sessionId;

    private final int outstandingLimit;

    /**
     *
     *
     * @param zk                表示与zk客户端建立连接的zk服务器
     * @param sock              表示与zk客户端建立连接的SocketChannel（即TCP长连接）
     * @param sk
     * @param factory
     * @param selectorThread    表示处理该连接的SelectorThread线程实例
     * @throws IOException
     */
    public NIOServerCnxn(ZooKeeperServer zk, SocketChannel sock, SelectionKey sk, NIOServerCnxnFactory factory, SelectorThread selectorThread) throws IOException {
        this.zkServer = zk;
        this.sock = sock;
        this.sk = sk;
        this.factory = factory;
        this.selectorThread = selectorThread;
        if (this.factory.login != null) {
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
        if (zk != null) {
            outstandingLimit = zk.getGlobalOutstandingLimit();
        } else {
            outstandingLimit = 1;
        }
        sock.socket().setTcpNoDelay(true);
        /* set socket linger to false, so that socket close does not block */
        sock.socket().setSoLinger(false, -1);
        InetAddress addr = ((InetSocketAddress) sock.socket()
                .getRemoteSocketAddress()).getAddress();
        authInfo.add(new Id("ip", addr.getHostAddress()));
        this.sessionTimeout = factory.sessionlessCnxnTimeout;
    }

    /* Send close connection packet to the client, doIO will eventually
     * close the underlying machinery (like socket, selectorkey, etc...)
     */
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    /**
     * send buffer without using the asynchronous
     * calls to selector and then close the socket
     * @param bb
     */
    void sendBufferSync(ByteBuffer bb) {
       try {
           /* configure socket to be blocking
            * so that we dont have to do write in
            * a tight while loop
            */
           if (bb != ServerCnxnFactory.closeConn) {
               if (sock.isOpen()) {
                   sock.configureBlocking(true);
                   sock.write(bb);
               }
               packetSent();
           }
       } catch (IOException ie) {
           LOG.error("Error sending data synchronously ", ie);
       }
    }

    /**
     * sendBuffer pushes a byte buffer onto the outgoing buffer queue for
     * asynchronous writes.
     */
    public void sendBuffer(ByteBuffer bb) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Add a buffer to outgoingBuffers, sk " + sk
                      + " is valid: " + sk.isValid());
        }
        outgoingBuffers.add(bb);
        requestInterestOpsUpdate();
    }

    /**
     * 有两种情况会调用此方法:
     *  1.根据lengthBuffer的值为incomingBuffer分配空间后,此时尚未将数据从socketChannel读取至incomingBuffer中
     *  2.已经将数据从socketChannel中读取至incomingBuffer,且读取完毕
     *
     * Read the request payload (everything following the length prefix) */
    private void readPayload() throws IOException, InterruptedException {
        // have we read length bytes?
        if (incomingBuffer.remaining() != 0) {
            // sock is non-blocking, so ok
            //对应情况1,此时刚为incomingBuffer分配空间,incomingBuffer为空,进行一次数据读取
            //(1)若将incomingBuffer读满,则直接进行处理;
            //(2)若未将incomingBuffer读满,则说明此次发送的数据不能构成一个完整的请求,则等待下一次数据到达后调用doIo()时再次将数据
            //从socketChannel读取至incomingBuffer
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new EndOfStreamException("Unable to read additional data from client sessionid 0x"
                        + Long.toHexString(sessionId) + ", likely client has closed socket");
            }
        }

        // have we read length bytes?
        if (incomingBuffer.remaining() == 0) {
            // 不管是情况1还是情况2,此时incomingBuffer已读满,其中内容必是一个request,处理该request
            // 更新统计值
            packetReceived();
            incomingBuffer.flip();
            // 当第一次从channel中获取请求的数据，并保存到incomingBuffer后，initialized设置为true
            if (!initialized) {
                // 处理连接请求，如果initialized=false说明之前为处理过请求，则该请求是连接请求
                readConnectRequest();
            } else {
                readRequest();
            }

            //请求处理结束,重置lenBuffer和incomingBuffer
            lenBuffer.clear();
            incomingBuffer = lenBuffer;
        }
    }

    /**
     * This boolean tracks whether the connection is ready for selection or
     * not. A connection is marked as not ready for selection while it is
     * processing an IO request. The flag is used to gatekeep pushing interest
     * op updates onto the selector.
     */
    private final AtomicBoolean selectable = new AtomicBoolean(true);

    public boolean isSelectable() {
        return sk.isValid() && selectable.get();
    }

    public void disableSelectable() {
        selectable.set(false);
    }

    public void enableSelectable() {
        selectable.set(true);
    }

    private void requestInterestOpsUpdate() {
        if (isSelectable()) {
            selectorThread.addInterestOpsUpdateRequest(sk);
        }
    }

    void handleWrite(SelectionKey k) throws IOException, CloseRequestException {
        if (outgoingBuffers.isEmpty()) {
            return;
        }

        /*
         * This is going to reset the buffer position to 0 and the
         * limit to the size of the buffer, so that we can fill it
         * with data from the non-direct buffers that we need to
         * send.
         */
        ByteBuffer directBuffer = NIOServerCnxnFactory.getDirectBuffer();
        if (directBuffer == null) {
            ByteBuffer[] bufferList = new ByteBuffer[outgoingBuffers.size()];
            // Use gathered write call. This updates the positions of the
            // byte buffers to reflect the bytes that were written out.
            sock.write(outgoingBuffers.toArray(bufferList));

            // Remove the buffers that we have sent
            ByteBuffer bb;
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (bb.remaining() > 0) {
                    break;
                }
                packetSent();
                outgoingBuffers.remove();
            }
         } else {
            directBuffer.clear();

            for (ByteBuffer b : outgoingBuffers) {
                if (directBuffer.remaining() < b.remaining()) {
                    /*
                     * When we call put later, if the directBuffer is to
                     * small to hold everything, nothing will be copied,
                     * so we've got to slice the buffer if it's too big.
                     */
                    b = (ByteBuffer) b.slice().limit(
                            directBuffer.remaining());
                }
                /*
                 * put() is going to modify the positions of both
                 * buffers, put we don't want to change the position of
                 * the source buffers (we'll do that after the send, if
                 * needed), so we save and reset the position after the
                 * copy
                 */
                int p = b.position();
                directBuffer.put(b);
                b.position(p);
                if (directBuffer.remaining() == 0) {
                    break;
                }
            }
            /*
             * Do the flip: limit becomes position, position gets set to
             * 0. This sets us up for the write.
             */
            directBuffer.flip();

            int sent = sock.write(directBuffer);

            ByteBuffer bb;

            // Remove the buffers that we have sent
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (sent < bb.remaining()) {
                    /*
                     * We only partially sent this buffer, so we update
                     * the position and exit the loop.
                     */
                    bb.position(bb.position() + sent);
                    break;
                }
                packetSent();
                /* We've sent the whole buffer, so drop the buffer */
                sent -= bb.remaining();
                outgoingBuffers.remove();
            }
        }
    }

    /**
     * Only used in order to allow testing
     */
    protected boolean isSocketOpen() {
        return sock.isOpen();
    }

    /**
     * 当SocketChannel上有数据可读时,worker thread调用NIOServerCnxn.doIO()进行读操作
     *
     * @param k
     * @throws InterruptedException
     */
    void doIO(SelectionKey k) throws InterruptedException {
        try {
            if (isSocketOpen() == false) {
                LOG.warn("trying to do i/o on a null socket for session:0x" + Long.toHexString(sessionId));
                return;
            }

            // 处理读操作的流程
            // 1.最开始incomingBuffer就是lenBuffer,容量为4.第一次读取4个字节,即此次请求报文的长度
            // 2.根据请求报文的长度分配incomingBuffer的大小
            // 3.将读到的字节存放在incomingBuffer中,直至读满(由于第2步中为incomingBuffer分配的长度刚好是报文的长度,此时incomingBuffer中刚好是一个报文)
            // 4.处理报文
            if (k.isReadable()) {
                // 若是客户端请求,此时触发读事件
                // 初始化时incomingBuffer即时lengthBuffer,只分配了4个字节,供用户读取一个int(此int值就是此次请求报文的总长度)
                int rc = sock.read(incomingBuffer);
                if (rc < 0) {
                    throw new EndOfStreamException("Unable to read additional data from client sessionid 0x"
                            + Long.toHexString(sessionId) + ", likely client has closed socket");
                }

                /*
                只有incomingBuffer.remaining() == 0,才会进行下一步的处理,否则一直读取数据直到incomingBuffer读满,此时有两种可能:
                一、incomingBuffer就是lenBuffer,此时incomingBuffer的内容是此次请求报文的长度.
                    根据lenBuffer为incomingBuffer分配空间后调用readPayload().
                    在readPayload()中会立马进行一次数据读取,
                    (1)若可以将incomingBuffer读满,则incomingBuffer中就是一个完整的请求,处理该请求;
                    (2)若不能将incomingBuffer读满,说明出现了拆包问题,此时不能构造一个完整的请求,只能等待客户端继续发送数据,等到下次socketChannel可读时,继续将数据读取到incomingBuffer中
                二、incomingBuffer不是lenBuffer,说明上次读取时出现了拆包问题,incomingBuffer中只有一个请求的部分数据.
                而这次读取的数据加上上次读取的数据凑成了一个完整的请求,调用readPayload()
                 */
                if (incomingBuffer.remaining() == 0) {
                    boolean isPayload;
                    // 开始下一个请求：解析上文中读取的报文总长度,同时为"incomingBuffer"分配len的空间供读取全部报文
                    if (incomingBuffer == lenBuffer) {
                        incomingBuffer.flip();
                        // 为incomeingBuffer分配空间时还包括了判断是否是"4字命令"的逻辑
                        isPayload = readLength(k);
                        incomingBuffer.clear();
                    } else {
                        //2.incomingBuffer不是lenBuffer,此时incomingBuffer的内容是payload
                        // continuation
                        isPayload = true;
                    }

                    if (isPayload) {
                        // 不是4字的情况
                        // 处理报文
                        readPayload();
                    }
                    else {
                        // four letter words take care
                        // need not do anything else
                        return;
                    }
                }
            }


            // 处理写操作
            if (k.isWritable()) {
                handleWrite(k);

                if (!initialized && !getReadInterest() && !getWriteInterest()) {
                    throw new CloseRequestException("responded to info probe");
                }
            }
        } catch (CancelledKeyException e) {
            LOG.warn("CancelledKeyException causing close of session 0x" + Long.toHexString(sessionId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CancelledKeyException stack trace", e);
            }
            close();
        } catch (CloseRequestException e) {
            // expecting close to log session closure
            close();
        } catch (EndOfStreamException e) {
            LOG.warn(e.getMessage());
            // expecting close to log session closure
            close();
        } catch (IOException e) {
            LOG.warn("Exception causing close of session 0x" + Long.toHexString(sessionId) + ": " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("IOException stack trace", e);
            }
            close();
        }
    }

    /**
     * 获取来自客户端的请求，并处理它
     *
     * @throws IOException
     */
    private void readRequest() throws IOException {
        zkServer.processPacket(this, incomingBuffer);
    }

    /**
     * 仅在从zkServer.processPacket()回调时调用。
     *
     * @param h
     */
    protected void incrOutstandingRequests(RequestHeader h) {
        if (h.getXid() >= 0) {
            outstandingRequests.incrementAndGet();
            int inProcess = zkServer.getInProcess();
            if (inProcess > outstandingLimit) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Throttling recv " + inProcess);
                }
                disableRecv();
            }
        }
    }

    // returns whether we are interested in writing, which is determined
    // by whether we have any pending buffers on the output queue or not
    private boolean getWriteInterest() {
        return !outgoingBuffers.isEmpty();
    }

    // 返回我们是否有兴趣接收新的请求，这取决于我们当前是否处于节流状态
    private boolean getReadInterest() {
        return !throttled.get();
    }

    private final AtomicBoolean throttled = new AtomicBoolean(false);

    // Throttle acceptance of new requests. If this entailed a state change,
    // register an interest op update request with the selector.
    public void disableRecv() {
        if (throttled.compareAndSet(false, true)) {
            requestInterestOpsUpdate();
        }
    }

    /**
     * 禁用节流并恢复对新请求的接受。如果这导致了状态更改，则使用选择器注册一个感兴趣的op更新请求。
     */
    public void enableRecv() {
        if (throttled.compareAndSet(true, false)) {
            requestInterestOpsUpdate();
        }
    }

    /**
     * 将来自客户端的请求建立连接的请求的数据报文读取到incomingBuffer
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void readConnectRequest() throws IOException, InterruptedException {
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        // 读取请求的数据到incomingBuffer
        zkServer.processConnectRequest(this, incomingBuffer);
        initialized = true;
    }

    /**
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     */
    private class SendBufferWriter extends Writer {
        private StringBuffer sb = new StringBuffer();

        /**
         * Check if we are ready to send another chunk.
         * @param force force sending, even if not a full chunk
         */
        private void checkFlush(boolean force) {
            if ((force && sb.length() > 0) || sb.length() > 2048) {
                sendBufferSync(ByteBuffer.wrap(sb.toString().getBytes()));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) return;
            checkFlush(true);
            sb = null; // clear out the ref to ensure no reuse
        }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);
            checkFlush(false);
        }
    }
    /** Return if four letter word found and responded to, otw false **/
    private boolean checkFourLetterWord(final SelectionKey k, final int len)
    throws IOException
    {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int
        if (!FourLetterCommands.isKnown(len)) {
            return false;
        }

        String cmd = FourLetterCommands.getCommandString(len);
        packetReceived();

        /** cancel the selection key to remove the socket handling
         * from selector. This is to prevent netcat problem wherein
         * netcat immediately closes the sending side after sending the
         * commands and still keeps the receiving channel open.
         * The idea is to remove the selectionkey from the selector
         * so that the selector does not notice the closed read on the
         * socket channel and keep the socket alive to write the data to
         * and makes sure to close the socket after its done writing the data
         */
        if (k != null) {
            try {
                k.cancel();
            } catch(Exception e) {
                LOG.error("Error cancelling command selection key ", e);
            }
        }

        final PrintWriter pwriter = new PrintWriter(
                new BufferedWriter(new SendBufferWriter()));

        // ZOOKEEPER-2693: don't execute 4lw if it's not enabled.
        if (!FourLetterCommands.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(pwriter, this, cmd +
                    " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing " + cmd + " command from "
                + sock.socket().getRemoteSocketAddress());

        if (len == FourLetterCommands.setTraceMaskCmd) {
            incomingBuffer = ByteBuffer.allocate(8);
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new IOException("Read error");
            }
            incomingBuffer.flip();
            long traceMask = incomingBuffer.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, this, traceMask);
            setMask.start();
            return true;
        } else {
            CommandExecutor commandExecutor = new CommandExecutor();
            return commandExecutor.execute(this, pwriter, len, zkServer, factory);
        }
    }

    /**
     * Reads the first 4 bytes of lenBuffer, which could be true length or four letter word.
     *
     * @param k selection key
     * @return true if length read, otw false (wasn't really the length)
     * @throws IOException if buffer size exceeds maxBuffer size
     */
    private boolean readLength(SelectionKey k) throws IOException {
        // Read the length, now get the buffer
        int len = lenBuffer.getInt();
        if (!initialized && checkFourLetterWord(sk, len)) {
            return false;
        }
        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
            throw new IOException("Len error " + len);
        }
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        incomingBuffer = ByteBuffer.allocate(len);
        return true;
    }

    /**
     * @return true if the server is running, false otherwise.
     */
    boolean isZKServerRunning() {
        return zkServer != null && zkServer.isRunning();
    }

    public long getOutstandingRequests() {
        return outstandingRequests.get();
    }


    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Used by "dump" 4-letter command to list all connection in
     * cnxnExpiryMap
     */
    @Override
    public String toString() {
        return "ip: " + sock.socket().getRemoteSocketAddress() +
               " sessionId: 0x" + Long.toHexString(sessionId);
    }

    /**
     * Close the cnxn and remove it from the factory cnxns list.
     */
    @Override
    public void close() {
        if (!factory.removeCnxn(this)) {
            return;
        }

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        if (sk != null) {
            try {
                // need to cancel this selection key from the selector
                sk.cancel();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ignoring exception during selectionkey cancel", e);
                }
            }
        }

        closeSock();
    }

    /**
     * Close resources associated with the sock of this cnxn.
     */
    private void closeSock() {
        if (sock.isOpen() == false) {
            return;
        }

        LOG.info("Closed socket connection for client "
                + sock.socket().getRemoteSocketAddress()
                + (sessionId != 0 ?
                        " which had sessionid 0x" + Long.toHexString(sessionId) :
                        " (no session established for client)"));
        closeSock(sock);
    }

    /**
     * Close resources associated with a sock.
     */
    public static void closeSock(SocketChannel sock) {
        if (sock.isOpen() == false) {
            return;
        }

        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            sock.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during output shutdown", e);
            }
        }
        try {
            sock.socket().shutdownInput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during input shutdown", e);
            }
        }
        try {
            sock.socket().close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socket close", e);
            }
        }
        try {
            sock.close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socketchannel close", e);
            }
        }
    }

    private final static byte fourBytes[] = new byte[4];


    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // Make space for length
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            try {
                baos.write(fourBytes);
                bos.writeRecord(h, "header");
                if (r != null) {
                    bos.writeRecord(r, tag);
                }
                baos.close();
            } catch (IOException e) {
                LOG.error("Error serializing response");
            }
            byte b[] = baos.toByteArray();
            ByteBuffer bb = ByteBuffer.wrap(b);
            bb.putInt(b.length - 4).rewind();
            sendBuffer(bb);
            if (h.getXid() > 0) {
                // check throttling
                if (outstandingRequests.decrementAndGet() < 1 ||
                    zkServer.getInProcess() < outstandingLimit) {
                    enableRecv();
                }
            }
         } catch(Exception e) {
            LOG.warn("Unexpected exception. Destruction averted.", e);
         }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#process(org.apache.zookeeper.proto.WatcherEvent)
     */
    @Override
    public void process(WatchedEvent event) {
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                                     "Deliver event " + event + " to 0x"
                                     + Long.toHexString(this.sessionId)
                                     + " through " + this);
        }

        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper();

        sendResponse(h, e, "notification");
    }

    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        factory.addSession(sessionId, this);
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        factory.touchCnxn(this);
    }

    @Override
    public int getInterestOps() {
        if (!isSelectable()) {
            return 0;
        }
        int interestOps = 0;
        if (getReadInterest()) {
            interestOps |= SelectionKey.OP_READ;
        }
        if (getWriteInterest()) {
            interestOps |= SelectionKey.OP_WRITE;
        }
        return interestOps;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return (InetSocketAddress) sock.socket().getRemoteSocketAddress();
    }

    public InetAddress getSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return sock.socket().getInetAddress();
    }

    @Override
    protected ServerStats serverStats() {
        if (zkServer == null) {
            return null;
        }
        return zkServer.serverStats();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        throw new UnsupportedOperationException(
                "SSL is unsupported in NIOServerCnxn");
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        throw new UnsupportedOperationException(
                "SSL is unsupported in NIOServerCnxn");
    }

}

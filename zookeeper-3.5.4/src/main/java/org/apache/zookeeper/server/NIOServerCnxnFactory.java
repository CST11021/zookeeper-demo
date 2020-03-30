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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * 摘自：https://www.jianshu.com/p/469259a72a42
 *
 * NIOServerCnxnFactory基于NIO实现了一个多线程的ServerCnxnFactory，线程间的通信都是通过queue来完成的。NIOServerCnxnFactory包含的线程如下：
 *
 * 1个accept线程NIOServerCnxnFactory.AcceptThread，用来监听端口并接收连接，然后把该连接分派给selector线程。
 * N个selecotr线程NIOServerCnxnFactory.SelectorThread，每个selctor线程平均负责1/N的连接。使用N个selector线程的原因在于，在大量连接的场景下，select()操作本身可能会成为性能瓶颈。
 * N个worker线程protected WorkerService workerPool;，用来负责socket的读写。如果N为0，那么selector线程自身会进行socket读写。
 * 1个管理连接的线程NIOServerCnxnFactory.ConnectionExpirerThread，用来关闭空闲而且没有建立session的连接。
 * 这几种线程相互配合的原理如下：
 *
 * AcceptThread和SelectorThread都继承自NIOServerCnxnFactory.AbstractSelectThread，AbstractSelectThread会在内部维护一个Selector。因此AcceptThread和SelectorThread各自会维护一个Selector。
 * 一般的流程大约是:
 * AcceptThread监听新连接，并根据轮询法，选择一个SelectorThread，将新连接置于后者的acceptedQueue(LinkedBlockingQueue)中。
 * 后者从acceptedQueue中取出连接，将读写事件注册在自己的Selector上，并监听读写事件，将触发的连接包装成IOWorkRequest，交给workerPool线程池服务运行。
 * IOWorkRequest的doWork会被线程池运行，在这之中，会调用cnxn.doIO调用NIOServerCnxn的方法。
 * 后者会读取4字节int数据，并交给CommandExecutor找到对应的命令AbstractFourLetterCommand，根据命令模式，去执行。这些命令可能会需要ZooKeeperServer和ServerCnxnFactory的协调。
 * 关于过期连接的关闭，一般由private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;维护，并由ConnectionExpirerThread周期性检查、关闭过久未被激活的连接。
 *
 *
 *
 *
 *
 *
 * NIOServerCnxnFactory implements a multi-threaded ServerCnxnFactory using
 * NIO non-blocking socket calls. Communication between threads is handled via
 * queues.
 *
 *   - 1   accept thread, which accepts new connections and assigns to a
 *         selector thread
 *   - 1-N selector threads, each of which selects on 1/N of the connections.
 *         The reason the factory supports more than one selector thread is that
 *         with large numbers of connections, select() itself can become a
 *         performance bottleneck.
 *   - 0-M socket I/O worker threads, which perform basic socket reads and
 *         writes. If configured with 0 worker threads, the selector threads
 *         do the socket I/O directly.
 *   - 1   connection expiration thread, which closes idle connections; this is
 *         necessary to expire connections on which no session is established.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 accept thread,
 * 1 connection expiration thread, 4 selector threads, and 64 worker threads.
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    /** Default sessionless connection timeout in ms: 10000 (10s) */
    public static final String ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT = "zookeeper.nio.sessionlessCnxnTimeout";
    /**
     * With 500 connections to an observer with watchers firing on each, is
     * unable to exceed 1GigE rates with only 1 selector.
     * Defaults to using 2 selector threads with 8 cores and 4 with 32 cores.
     * Expressed as sqrt(numCores/2). Must have at least 1 selector thread.
     */
    public static final String ZOOKEEPER_NIO_NUM_SELECTOR_THREADS = "zookeeper.nio.numSelectorThreads";
    /** Default: 2 * numCores */
    public static final String ZOOKEEPER_NIO_NUM_WORKER_THREADS = "zookeeper.nio.numWorkerThreads";
    /** Default: 64kB */
    public static final String ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES = "zookeeper.nio.directBufferBytes";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT = "zookeeper.nio.shutdownTimeout";

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Thread " + t + " died", e);
                }
            });
        /**
         * this is to avoid the jvm bug:
         * NullPointerException in Selector.open()
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        try {
            Selector.open().close();
        } catch(IOException ie) {
            LOG.error("Selector failed to open", ie);
        }

        /**
         * 值0禁止使用直接缓冲区，而是使用收集的写调用。
         *
         * 默认使用64k直接缓冲区。
         */
        directBufferBytes = Integer.getInteger(ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES, 64 * 1024);
    }

    /** 表示zk服务维持与客户端信息交互的通道，当SocketChannel上有数据可读时,worker thread调用NIOServerCnxn.doIO()进行读操作 */
    ServerSocketChannel ss;

    /**
     * 我们使用这个缓冲区来执行高效的套接字I/O。因为I/O是由工作线程处理的(如果没有创建工作线程池，则直接由选择器线程处理)，所以我们可以创建一组由连接共享的固定的I/O。
     */
    private static final ThreadLocal<ByteBuffer> directBuffer = new ThreadLocal<ByteBuffer>() {
        // 这里涉及到NIOServerCnxnFactory中的直接内存及JVM堆外内存的一些知识，参考链接：https://blog.csdn.net/jpf254/article/details/80792086

        //  DirectByteBuffer
        //     由于Zookeeper中使用了DirectByteBuffer进行IO操作,在此简单介绍下DirectByteBuffer和HeapByteBuffer的区别.
        //             HeapByteBuffer是在堆上分配的内存,而DirectByteBuffer是在堆外分配的内存,又称直接内存.使用HeapByteBuffer进行IO时,比如调用FileChannel.write(HeapByteBuffer)将数据写到File中时,有两个步骤:
        //
        //     将HeapByteBuffer的数据拷贝到DirectByteBuffer
        //     再从堆外内存将数据写入到文件中.
        //             问题1:为什么要将HeapByteBuffer的数据拷贝到DirectByteBuffer呢?不能将数据直接从HeapByteBuffer拷贝到文件中吗?
        //
        //     并不是说操作系统无法直接访问jvm中分配的内存区域，显然操作系统是可以访问所有的本机内存区域的，但是为什么对io的操作都需要将jvm内存区的数据拷贝到堆外内存呢？是因为jvm需要进行GC，如果io设备直接和jvm堆上的数据进行交互，这个时候jvm进行了GC，那么有可能会导致没有被回收的数据进行了压缩，位置被移动到了连续的存储区域，这样会导致正在进行的io操作相关的数据全部乱套，显然是不合理的，所以对io的操作会将jvm的数据拷贝至堆外内存，然后再进行处理，将不会被jvm上GC的操作影响。
        //
        //     问题2:DirectByteBuffer是相当于固定的内核buffer还是JVM进程内的堆外内存?
        //     不管是Java堆还是直接内存,都是JVM进程通过malloc申请的内存,其都是用户空间的内存,只不过是JVM进程将这两块用户空间的内存用作不同的用处罢了.Java内存模型如下:
        //
        //
        //     问题3:将HeapByteBuffer的数据拷贝到DirectByteBuffer这一过程是操作系统执行还是JVM执行?
        //     在问题2中已经回答,DirectByteBuffer是JVM进程申请的用户空间内存,其使用和分配都是由JVM进程管理,因此这一过程是JVM执行的.也正是因为JVM知道堆内存会经常GC,数据地址经常移动,而底层通过write,read,pwrite,pread等函数进行系统调用时,需要传入buffer的起始地址和buffer count作为参数,因此JVM在执行读写时会做判断,若是HeapByteBuffer,就将其拷贝到直接内存后再调用系统调用执行步骤2.
        //             代码在sun.nio.ch.IOUtil.write()和sun.nio.ch.IOUtil.read()中,我们看下write()的代码:
        //     知乎不能复制,代码地址如下:
        //     Java NIO direct buffer的优势在哪儿？,第一个答案中有代码.
        //
        //             问题4:在将数据写到文件的过程中需要将数据拷贝到内核空间吗?
        //     需要.在步骤3中,是不能直接将数据从直接内存拷贝到文件中的,需要将数据从直接内存->内核空间->文件,因此使用DirectByteBuffer代替HeapByteBuffer也只是减少了数据拷贝的一个步骤,但对性能已经有提升了.
        //
        //             问题5:还有其他减少数据拷贝的方法吗?
        //     有,我目前知道的有两种,分别是sendFile系统调用和内存映射.
        //             比如想要将数据从磁盘文件发送到socket,使用read/write系统调用需要将数据从磁盘文件->read buffer(内核空间中)->用户空间->socket buffer(也在内核空间中)->NIC buffer(网卡),而使用sendFile(即FileChannel.transferTo())系统调用就可减少复制到用户空间的过程,变为数据从磁盘文件->read buffer(内核空间中)->socket buffer(也在内核空间中)->NIC buffer(网卡),当然,还会有其他的优化手段,详见什么是Zero-Copy？
        //     内存映射我也不是很清楚,详见JAVA NIO之浅谈内存映射文件原理与DirectMemory
        //
        //     问题6:netty中使用了哪几种方式实现高效IO?
        //     netty中使用了3种方式实现其zero-copy机制,如下:
        //
        //     使用DirectByteBuffer
        // 使用FileChannel.transferTo()
        //     ntty提供了组合Buffer对象,可以聚合多个ByteBuffer对象,用户可以像操作一个Buffer那样方便的对组合Buffer进行操作,避免了传统通过内存拷贝的方式将几个小Buffer合并成一个大的Buffer

        @Override
            protected ByteBuffer initialValue() {
                return ByteBuffer.allocateDirect(directBufferBytes);
            }
        };

    /**
     * Map<sessionId, NIOServerCnxn>, sessionMap is used by closeSession()
     */
    private final ConcurrentHashMap<Long, NIOServerCnxn> sessionMap = new ConcurrentHashMap<Long, NIOServerCnxn>();
    /** 记录一个ip对应的ServerCnxn列表，用于管理一个ip最大允许的连接数，Map<客户端IP地址，该客户端与zk服务器的连接实例> */
    private final ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>> ipMap = new ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>>( );

    /** 控制客户端的最大连接数 */
    protected int maxClientCnxns = 60;

    int sessionlessCnxnTimeout;
    private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;
    private static int directBufferBytes;
    private int numSelectorThreads;
    private int numWorkerThreads;
    private long workerShutdownTimeoutMS;

    /** 标记工厂是否已经停止 */
    private volatile boolean stopped = true;

    /**
     * 该线程启动后会一直监听来自client的连接，当AcceptThread接收到新的连接并将其分配给一个SelectorThread
     *
     * 1个accept线程NIOServerCnxnFactory.AcceptThread，用来监听端口并接收连接，然后把该连接分派给selector线程。
     */
    private AcceptThread acceptThread;
    /**
     * N个selecotr线程NIOServerCnxnFactory.SelectorThread，每个selctor线程平均负责1/N的连接。
     * 使用N个selector线程的原因在于，在大量连接的场景下，select()操作本身可能会成为性能瓶颈。
     */
    private final Set<SelectorThread> selectorThreads = new HashSet<SelectorThread>();
    /**
     * N个worker线程protected WorkerService workerPool;，用来负责socket的读写。
     * 如果N为0，那么selector线程自身会进行socket读写。
     */
    protected WorkerService workerPool;
    /** 1个管理连接的线程NIOServerCnxnFactory.ConnectionExpirerThread，用来关闭空闲而且没有建立session的连接。 */
    private ConnectionExpirerThread expirerThread;


    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     */
    public NIOServerCnxnFactory() {
    }

    @Override
    public void start() {
        stopped = false;
        if (workerPool == null) {
            workerPool = new WorkerService("NIOWorker", numWorkerThreads, false);
        }

        for(SelectorThread thread : selectorThreads) {
            if (thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }

        // 确保线程只启动一次
        if (acceptThread.getState() == Thread.State.NEW) {
            acceptThread.start();
        }

        if (expirerThread.getState() == Thread.State.NEW) {
            expirerThread.start();
        }
    }

    @Override
    public void startup(ZooKeeperServer zks, boolean startServer) throws IOException, InterruptedException {
        start();
        setZooKeeperServer(zks);
        if (startServer) {
            // 从zk快照文件中加载节点数据
            zks.startdata();
            // 启动zk服务
            zks.startup();
        }
    }


    /**
     * zk设置
     *
     * @param addr      zk服务IP及暴露给客户端的端口
     * @param maxcc     控制最大客户端连接数
     * @param secure    是否启用了SSL
     * @throws IOException
     */
    @Override
    public void configure(InetSocketAddress addr, int maxcc, boolean secure) throws IOException {
        if (secure) {
            throw new UnsupportedOperationException("SSL isn't supported in NIOServerCnxn");
        }
        configureSaslLogin();

        maxClientCnxns = maxcc;
        sessionlessCnxnTimeout = Integer.getInteger(
            ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT, 10000);
        // We also use the sessionlessCnxnTimeout as expiring interval for
        // cnxnExpiryQueue. These don't need to be the same, but the expiring
        // interval passed into the ExpiryQueue() constructor below should be
        // less than or equal to the timeout.
        cnxnExpiryQueue =
            new ExpiryQueue<NIOServerCnxn>(sessionlessCnxnTimeout);
        expirerThread = new ConnectionExpirerThread();

        int numCores = Runtime.getRuntime().availableProcessors();
        // 32 cores sweet spot seems to be 4 selector threads
        numSelectorThreads = Integer.getInteger(
            ZOOKEEPER_NIO_NUM_SELECTOR_THREADS,
            Math.max((int) Math.sqrt((float) numCores/2), 1));
        if (numSelectorThreads < 1) {
            throw new IOException("numSelectorThreads must be at least 1");
        }

        numWorkerThreads = Integer.getInteger(
            ZOOKEEPER_NIO_NUM_WORKER_THREADS, 2 * numCores);
        workerShutdownTimeoutMS = Long.getLong(
            ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring NIO connection handler with "
                 + (sessionlessCnxnTimeout/1000) + "s sessionless connection"
                 + " timeout, " + numSelectorThreads + " selector thread(s), "
                 + (numWorkerThreads > 0 ? numWorkerThreads : "no")
                 + " worker threads, and "
                 + (directBufferBytes == 0 ? "gathered writes." :
                    ("" + (directBufferBytes/1024) + " kB direct buffers.")));
        for(int i=0; i<numSelectorThreads; ++i) {
            selectorThreads.add(new SelectorThread(i));
        }

        this.ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);
        LOG.info("binding to port " + addr);
        ss.socket().bind(addr);
        ss.configureBlocking(false);
        acceptThread = new AcceptThread(ss, addr, selectorThreads);
    }

    private void tryClose(ServerSocketChannel s) {
        try {
            s.close();
        } catch (IOException sse) {
            LOG.error("Error while closing server socket.", sse);
        }
    }

    /**
     * 设置通道连接的客户端的IP及端口
     *
     * @param addr
     */
    @Override
    public void reconfigure(InetSocketAddress addr) {
        ServerSocketChannel oldSS = ss;        
        try {
            this.ss = ServerSocketChannel.open();
            ss.socket().setReuseAddress(true);
            LOG.info("binding to port " + addr);
            ss.socket().bind(addr);
            ss.configureBlocking(false);
            acceptThread.setReconfiguring();
            tryClose(oldSS);
            acceptThread.wakeupSelector();
            try {
                acceptThread.join();
            } catch (InterruptedException e) {
                LOG.error("Error joining old acceptThread when reconfiguring client port {}",
                            e.getMessage());
                Thread.currentThread().interrupt();
            }
            acceptThread = new AcceptThread(ss, addr, selectorThreads);
            acceptThread.start();
        } catch(IOException e) {
            LOG.error("Error reconfiguring client port to {} {}", addr, e.getMessage());
            tryClose(oldSS);
        }
    }

    /** {@inheritDoc} */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /** {@inheritDoc} */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }


    /**
     * 获取缓冲区
     *
     * @return
     */
    public static ByteBuffer getDirectBuffer() {
        return directBufferBytes > 0 ? directBuffer.get() : null;
    }


    @Override
    public InetSocketAddress getLocalAddress(){
        return (InetSocketAddress)ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort(){
        return ss.socket().getLocalPort();
    }

    /**
     * De-registers the connection from the various mappings maintained
     * by the factory.
     */
    public boolean removeCnxn(NIOServerCnxn cnxn) {
        // If the connection is not in the master list it's already been closed
        if (!cnxns.remove(cnxn)) {
            return false;
        }
        cnxnExpiryQueue.remove(cnxn);

        long sessionId = cnxn.getSessionId();
        if (sessionId != 0) {
            sessionMap.remove(sessionId);
        }

        InetAddress addr = cnxn.getSocketAddress();
        if (addr != null) {
            Set<NIOServerCnxn> set = ipMap.get(addr);
            if (set != null) {
                set.remove(cnxn);
                // Note that we make no effort here to remove empty mappings
                // from ipMap.
            }
        }

        // unregister from JMX
        unregisterConnection(cnxn);
        return true;
    }

    /**
     * 添加或更新一个NIOServerCnxn实例到cnxnExpiryQueue队列中
     *
     * @param cnxn
     */
    public void touchCnxn(NIOServerCnxn cnxn) {
        cnxnExpiryQueue.update(cnxn, cnxn.getSessionTimeout());
    }

    /**
     * 当zk客户端与zk服务端创建连接时，会创建一个NIOServerCnxn实例，然后调用该方法保存该实例
     *
     * @param cnxn
     */
    private void addCnxn(NIOServerCnxn cnxn) {
        // 客户端IP
        InetAddress addr = cnxn.getSocketAddress();
        Set<NIOServerCnxn> set = ipMap.get(addr);
        if (set == null) {
            // in general we will see 1 connection from each host, setting the initial cap to 2 allows us to minimize mem usage in the common case of 1 entry
            // --  we need to set the initial cap to 2 to avoid rehash when the first entry is added Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<NIOServerCnxn, Boolean>(2));
            // Put the new set in the map, but only if another thread hasn't beaten us to it
            Set<NIOServerCnxn> existingSet = ipMap.putIfAbsent(addr, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(cnxn);

        cnxns.add(cnxn);
        touchCnxn(cnxn);
    }

    /**
     * 创建连接：客户端发起连接zk服务的请求后，服务端通过该方法创建一个NIOServerCnxn对象，表示一个TCP长连接
     *
     * @param sock
     * @param sk
     * @param selectorThread
     * @return
     * @throws IOException
     */
    protected NIOServerCnxn createConnection(SocketChannel sock, SelectionKey sk, SelectorThread selectorThread) throws IOException {
        return new NIOServerCnxn(zkServer, sock, sk, this, selectorThread);
    }

    private int getClientCnxnCount(InetAddress cl) {
        Set<NIOServerCnxn> s = ipMap.get(cl);
        if (s == null) return 0;
        return s.size();
    }

    /**
     * clear all the connections in the selector
     *
     */
    @Override
    @SuppressWarnings("unchecked")
    public void closeAll() {
        // clear all the connections on which we are selecting
        for (ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close();
            } catch (Exception e) {
                LOG.warn("Ignoring exception closing cnxn sessionid 0x"
                         + Long.toHexString(cnxn.getSessionId()), e);
            }
        }
    }

    public void stop() {
        stopped = true;

        // Stop queuing connection attempts
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Error closing listen socket", e);
        }

        if (acceptThread != null) {
            acceptThread.wakeupSelector();
        }
        if (expirerThread != null) {
            expirerThread.interrupt();
        }
        for (SelectorThread thread : selectorThreads) {
            thread.wakeupSelector();
        }
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        try {
            // close listen socket and signal selector threads to stop
            stop();

            // wait for selector and worker threads to shutdown
            join();

            // close all open connections
            closeAll();

            if (login != null) {
                login.shutdown();
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    public void addSession(long sessionId, NIOServerCnxn cnxn) {
        sessionMap.put(sessionId, cnxn);
    }

    @Override
    public boolean closeSession(long sessionId) {
        NIOServerCnxn cnxn = sessionMap.remove(sessionId);
        if (cnxn != null) {
            cnxn.close();
            return true;
        }
        return false;
    }

    @Override
    public void join() throws InterruptedException {
        if (acceptThread != null) {
            acceptThread.join();
        }

        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    public void dumpConnections(PrintWriter pwriter) {
        pwriter.print("Connections ");
        cnxnExpiryQueue.dump(pwriter);
    }

    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for(ServerCnxn c : cnxns){
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String,Object>> info = new HashSet<Map<String,Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }




    /**
     * AbstractSelectThread is an abstract base class containing a few bits of code shared by the AcceptThread (which selects on the listen socket) and SelectorThread (which selects on client connections) classes.
     */
    private abstract class AbstractSelectThread extends ZooKeeperThread {

        /**
         * NIO的Selector用于管理多个channels：
         * Selector一般称为选择器，当然你也可以翻译为多路复用器。
         * 它是Java NIO核心组件中的一个，用于检查一个或多个NIO Channel（通道）的状态是否处于可读、可写。
         * 如此可以实现单线程管理多个channels,也就是可以管理多个网络链接
         */
        protected final Selector selector;

        /**
         * 调用该构造方法时，完成Selector的创建
         *
         * @param name
         * @throws IOException
         */
        public AbstractSelectThread(String name) throws IOException {
            super(name);
            // 允许JVM关闭，即使这个线程仍然在运行。
            setDaemon(true);
            this.selector = Selector.open();
        }

        /**
         * 唤醒阻塞在selector.select上的线程
         */
        public void wakeupSelector() {
            // selector.wakeup主要是为了唤醒阻塞在selector.select上的线程，让该线程及时去处理其他事情，例如注册channel，改变interestOps、判断超时等等。
            selector.wakeup();
        }

        /**
         *关闭选择器。当线程即将退出，并且不会对选择器或SelectionKey执行任何操作时，应该调用这个函数
         */
        protected void closeSelector() {
            try {
                selector.close();
            } catch (IOException e) {
                LOG.warn("ignored exception during selector close " + e.getMessage());
            }
        }

        /**
         * 当处理SelectionKey事件时，如果连接被关闭，则该事件将失效，则会调用该方法清除该事件
         *
         * @param key
         */
        protected void cleanupSelectionKey(SelectionKey key) {
            if (key != null) {
                try {
                    key.cancel();
                } catch (Exception ex) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ignoring exception during selectionkey cancel", ex);
                    }
                }
            }
        }

        protected void fastCloseSock(SocketChannel sc) {
            if (sc != null) {
                try {
                    // Hard close immediately, discarding buffers
                    sc.socket().setSoLinger(true, 0);
                } catch (SocketException e) {
                    LOG.warn("Unable to set socket linger to 0, socket close" + " may stall in CLOSE_WAIT", e);
                }
                NIOServerCnxn.closeSock(sc);
            }
        }
    }

    /**
     * 主要负责处理client的连接，AcceptThread接受新的连接并将其分配给一个SelectorThread，使用简单的循环机制将它们分散到多个SelectorThreads中。
     */
    private class AcceptThread extends AbstractSelectThread {

        private final RateLogger acceptErrorLogger = new RateLogger(LOG);

        /** zk服务端连接客户端进行通信的channel */
        private final ServerSocketChannel acceptSocket;
        /**
         * 通道触发了一个事件意思是该事件已经就绪：
         * 1、比如某个Channel成功连接到另一个服务器称为“ 连接就绪 ”。
         * 2、一个Server Socket Channel准备好接收新进入的连接称为“ 接收就绪 ”。
         * 3、一个有数据可读的通道可以说是“ 读就绪 ”。
         * 4、等待写数据的通道可以说是“ 写就绪 ”。
         *
         * 这四种事件用SelectionKey的四个常量来表示：
         * SelectionKey.OP_CONNECT
         * SelectionKey.OP_ACCEPT
         * SelectionKey.OP_READ
         * SelectionKey.OP_WRITE
         */
        private final SelectionKey acceptKey;

        /**
         * AcceptThread监听新连接，并根据轮询法，选择一个SelectorThread，将新连接置于后者的acceptedQueue(LinkedBlockingQueue)中。
         * 后者从acceptedQueue中取出连接，将读写事件注册在自己的Selector上，并监听读写事件，将触发的连接包装成IOWorkRequest，交给workerPool线程池服务运行。
         */
        private final Collection<SelectorThread> selectorThreads;
        /** 对应{@link #selectorThreads}方便于遍历 */
        private Iterator<SelectorThread> selectorIterator;
        /** 当zk服务与客户端建立socket连接时，改值置为true */
        private volatile boolean reconfiguring = false;

        /**
         *
         * @param ss                    服务端连接客户端进行通信的channel
         * @param addr                  zk服务IP及暴露给客户端的端口
         * @param selectorThreads       接收处理请求的一组SelectorThread线程
         * @throws IOException
         */
        public AcceptThread(ServerSocketChannel ss, InetSocketAddress addr, Set<SelectorThread> selectorThreads) throws IOException {
            super("NIOServerCxnFactory.AcceptThread:" + addr);
            this.acceptSocket = ss;
            // acceptSocket注册监听OP_ACCEPT事件
            this.acceptKey = acceptSocket.register(selector, SelectionKey.OP_ACCEPT);
            this.selectorThreads = Collections.unmodifiableList(new ArrayList<SelectorThread>(selectorThreads));
            selectorIterator = this.selectorThreads.iterator();
        }

        public void run() {
            try {
                // 服务运行期间不断执行select()方法
                while (!stopped && !acceptSocket.socket().isClosed()) {
                    try {
                        select();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
            } finally {
                closeSelector();
                // This will wake up the selector threads, and tell the worker thread pool to begin shutdown.
                if (!reconfiguring) {
                    NIOServerCnxnFactory.this.stop();
                }
                LOG.info("accept thread exitted run method");
            }
        }

        /**
         * 通过Selector的select()方法可以选择已经准备就绪的通道（这些通道包含你感兴趣的的事件）。
         * 比如你对读就绪的通道感兴趣，那么select()方法就会返回读事件已经就绪的那些通道。下面是Selector几个重载的select()方法：
         *
         * int select()：阻塞到至少有一个通道在你注册的事件上就绪了。
         * int select(long timeout)：和select()一样，但最长阻塞时间为timeout毫秒。
         * int selectNow()：非阻塞，只要有通道就绪就立刻返回。
         *
         */
        private void select() {
            try {
                selector.select();

                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    // 检测此key是否有效.当key被取消,或者通道被关闭,或者selector被关闭,都将导致此key无效.在AbstractSelector.removeKey(key)中,会导致selectionKey被置为无效
                    if (!key.isValid()) {
                        continue;
                    }

                    // 如果通道已经准备好接收新进入的连接，则开始建立连接
                    if (key.isAcceptable()) {
                        if (!doAccept()) {
                            // If unable to pull a new connection off the accept queue, pause accepting to give us time to free up file descriptors and so the accept thread doesn't spin in a tight loop.
                            pauseAccept(10);
                        }
                    } else {
                        LOG.warn("Unexpected ops in accept select " + key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }
        /**
         * Mask off the listen socket interest ops and use select() to sleep
         * so that other threads can wake us up by calling wakeup() on the
         * selector.
         */
        private void pauseAccept(long millisecs) {
            acceptKey.interestOps(0);
            try {
                selector.select(millisecs);
            } catch (IOException e) {
                // ignore
            } finally {
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        }
        /**
         * 接收新的socket连接,每个IP地址有其连接个数上限, 使用轮询为新连接分配selector thread
         *
         * @return whether was able to accept a connection or not
         */
        private boolean doAccept() {
            boolean accepted = false;
            SocketChannel sc = null;
            try {
                // 创建一个socket连接通道
                sc = acceptSocket.accept();
                accepted = true;
                // 获取客户端IP
                InetAddress ia = sc.socket().getInetAddress();

                // 检查同一IP是否超过了最大连接数
                int cnxncount = getClientCnxnCount(ia);
                if (maxClientCnxns > 0 && cnxncount >= maxClientCnxns){
                    throw new IOException("Too many connections from " + ia + " - max is " + maxClientCnxns );
                }

                LOG.info("Accepted socket connection from " + sc.socket().getRemoteSocketAddress());
                // 设置在该socket上的读写都不阻塞
                sc.configureBlocking(false);

                // 采用轮询的方式为新收到的连接分配一个selectorThread处理
                if (!selectorIterator.hasNext()) {
                    selectorIterator = selectorThreads.iterator();
                }
                SelectorThread selectorThread = selectorIterator.next();

                // 把该连接放到selectorThread线程的acceptedQueue队列中
                if (!selectorThread.addAcceptedConnection(sc)) {
                    throw new IOException("Unable to add connection to selector queue" + (stopped ? " (shutdown in progress)" : ""));
                }
                acceptErrorLogger.flush();
            } catch (IOException e) {
                // accept, maxClientCnxns, configureBlocking
                acceptErrorLogger.rateLimitLog("Error accepting new connection: " + e.getMessage());
                fastCloseSock(sc);
            }
            return accepted;
        }
        public void setReconfiguring() {
            reconfiguring = true;
        }
    }

    /**
     *
     * AcceptThread监听新连接，并根据轮询法，选择一个 SelectorThread，将新连接放到SelectorThread的acceptedQueue中。
     * SelectorThread从acceptedQueue中取出连接，将读写事件注册在自己的Selector上，并监听读写事件，将触发的连接包装成IOWorkRequest，交给workerPool线程池服务运行。
     *
     * 在selector thread的run()中,主要执行3件事情
     *
     * 调用select()读取就绪的IO事件,交由worker thread处理(在交由worker thread 处理之前会调用key.interestOps(0))
     * 处理accept线程新分派的连接,
     * (1)将新连接注册到selector上;
     * (2)包装为NIOServerCnxn后注册到NIOServerCnxnFactory中
     * (3)更新updateQueue中连接的监听事件
     */
    class SelectorThread extends AbstractSelectThread {
        /** 表示SelectorThread线程的ID */
        private final int id;
        /** zk服务器监听到来自客户端的连接请求后，会将建立的socket通道放到该队列中，SelectorThread会从该队列进行读取并处理 */
        private final Queue<SocketChannel> acceptedQueue;

        private final Queue<SelectionKey> updateQueue;

        public SelectorThread(int id) throws IOException {
            super("NIOServerCxnFactory.SelectorThread-" + id);
            this.id = id;
            acceptedQueue = new LinkedBlockingQueue<SocketChannel>();
            updateQueue = new LinkedBlockingQueue<SelectionKey>();
        }

        /**
         * The main loop for the thread selects() on the connections and
         * dispatches ready I/O work requests, then registers all pending
         * newly accepted connections and updates any interest ops on the
         * queue.
         */
        public void run() {
            try {
                while (!stopped) {
                    try {
                        // 1.调用select()读取就绪的IO事件,交由worker thread处理
                        select();
                        // 2.处理 acceptThread 新分派的连接：
                        // (1)将新连接注册到selector上;
                        // (2)包装为NIOServerCnxn后注册到NIOServerCnxnFactory中
                        processAcceptedConnections();
                        // 3.更新updateQueue中连接的监听事件
                        processInterestOpsUpdateRequests();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }

                // Close connections still pending on the selector. Any others with in-flight work, let drain out of the work queue.
                for (SelectionKey key : selector.keys()) {
                    // 从SelectionKey中获取事件的上下文，即NIOServerCnxn
                    NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                    if (cnxn.isSelectable()) {
                        cnxn.close();
                    }
                    cleanupSelectionKey(key);
                }

                SocketChannel accepted;
                while ((accepted = acceptedQueue.poll()) != null) {
                    fastCloseSock(accepted);
                }

                updateQueue.clear();
            } finally {
                closeSelector();
                // This will wake up the accept thread and the other selector threads, and tell the worker thread pool to begin shutdown.
                NIOServerCnxnFactory.this.stop();
                LOG.info("selector thread exitted run method");
            }
        }

        private void select() {
            try {
                // 通过Selector的select()方法可以选择已经准备就绪的通道（这些通道包含你感兴趣的的事件）。
                // 比如你对读就绪的通道感兴趣，那么select()方法就会返回读事件已经就绪的那些通道。下面是Selector几个重载的select()方法：
                //
                // int select()：阻塞到至少有一个通道在你注册的事件上就绪了。
                // int select(long timeout)：和select()一样，但最长阻塞时间为timeout毫秒。
                // int selectNow()：非阻塞，只要有通道就绪就立刻返回。
                selector.select();

                Set<SelectionKey> selected = selector.selectedKeys();
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(selected);
                Collections.shuffle(selectedList);
                Iterator<SelectionKey> selectedKeys = selectedList.iterator();
                while(!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selected.remove(key);

                    // 检测此key是否有效.当key被取消,或者通道被关闭,或者selector被关闭,都将导致此key无效.在AbstractSelector.removeKey(key)中,会导致selectionKey被置为无效
                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }

                    // 如果该通道处于可读或可写状态时，则调用handleIO()方法处理该通道事件
                    if (key.isReadable() || key.isWritable()) {
                        handleIO(key);
                    } else {
                        LOG.warn("Unexpected ops in select " + key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * 当SocketChannel处于可读或可写状态时，则调用handleIO()方法处理该通道事件
         *
         * @param key
         */
        private void handleIO(SelectionKey key) {
            // 创建一个IOWorkRequest实例，并委托给workerPool执行，执行逻辑IOWorkRequest#doWork()
            IOWorkRequest workRequest = new IOWorkRequest(this, key);
            NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();

            // 在处理其连接时停止SelectionKey
            cnxn.disableSelectable();
            key.interestOps(0);

            // 添加或更新一个NIOServerCnxn实例到cnxnExpiryQueue队列中
            touchCnxn(cnxn);
            // 将workRequest提交到workerPool
            workerPool.schedule(workRequest);
        }

        /**
         * 遍历已分配给此线程但尚未放置在选择器上的已接受连接队列。
         */
        private void processAcceptedConnections() {
            SocketChannel accepted;
            while (!stopped && (accepted = acceptedQueue.poll()) != null) {
                SelectionKey key = null;
                try {
                    key = accepted.register(selector, SelectionKey.OP_READ);

                    // 创建一个NIOServerCnxn实例，并到到SelectionKey事件上下文中
                    NIOServerCnxn cnxn = createConnection(accepted, key, this);
                    key.attach(cnxn);

                    // 保存该NIOServerCnxn实例
                    addCnxn(cnxn);
                } catch (IOException e) {
                    // register, createConnection
                    cleanupSelectionKey(key);
                    fastCloseSock(accepted);
                }
            }
        }

        /**
         * Iterate over the queue of connections ready to resume selection, and restore their interest ops selection mask.
         */
        private void processInterestOpsUpdateRequests() {
            SelectionKey key;
            while (!stopped && (key = updateQueue.poll()) != null) {

                // 检测此key是否有效.当key被取消,或者通道被关闭,或者selector被关闭,都将导致此key无效.在AbstractSelector.removeKey(key)中,会导致selectionKey被置为无效
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                }

                // 从SelectionKey事件上下文中获取NIOServerCnxn实例
                NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                if (cnxn.isSelectable()) {
                    key.interestOps(cnxn.getInterestOps());
                }
            }
        }

        /**
         * AcceptThread线程接收到连接请求后，会与客户端创建一个SocketChannel连接实例，并调用该方法，将SocketChannel放到acceptedQueue队列中
         *
         * @param accepted
         * @return
         */
        public boolean addAcceptedConnection(SocketChannel accepted) {
            if (stopped || !acceptedQueue.offer(accepted)) {
                return false;
            }
            // 唤醒阻塞在selector.select上的线程，让该线程及时去处理其他事情，例如注册channel，改变interestOps、判断超时等等。
            wakeupSelector();
            return true;
        }

        /**
         * Place interest op update requests onto a queue so that only the
         * selector thread modifies interest ops, because interest ops
         * reads/sets are potentially blocking operations if other select
         * operations are happening.
         */
        public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
            if (stopped || !updateQueue.offer(sk)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

    }

    /**
     * IOWorkRequest is a small wrapper class to allow doIO() calls to be
     * run on a connection using a WorkerService.
     */
    private class IOWorkRequest extends WorkerService.WorkRequest {

        private final SelectorThread selectorThread;
        private final SelectionKey key;
        private final NIOServerCnxn cnxn;

        IOWorkRequest(SelectorThread selectorThread, SelectionKey key) {
            this.selectorThread = selectorThread;
            this.key = key;
            this.cnxn = (NIOServerCnxn) key.attachment();
        }

        /**
         * 处理WorkRequest
         *
         * @throws InterruptedException
         */
        public void doWork() throws InterruptedException {
            // 检测此key是否有效.当key被取消,或者通道被关闭,或者selector被关闭,都将导致此key无效.在AbstractSelector.removeKey(key)中,会导致selectionKey被置为无效
            if (!key.isValid()) {
                selectorThread.cleanupSelectionKey(key);
                return;
            }

            // 当SocketChannel上有数据可读时,worker thread调用 NIOServerCnxn.doIO() 进行读操作
            if (key.isReadable() || key.isWritable()) {
                cnxn.doIO(key);

                // Check if we shutdown or doIO() closed this connection
                if (stopped) {
                    cnxn.close();
                    return;
                }

                if (!key.isValid()) {
                    selectorThread.cleanupSelectionKey(key);
                    return;
                }

                // 添加或更新一个NIOServerCnxn实例到cnxnExpiryQueue队列中
                touchCnxn(cnxn);
            }

            // 再次将此连接标记为已准备好进行选择
            // Mark this connection as once again ready for selection
            cnxn.enableSelectable();
            // Push an update request on the queue to resume selecting on the current set of interest ops, which may have changed as a result of the I/O operations we just performed.
            if (!selectorThread.addInterestOpsUpdateRequest(key)) {
                cnxn.close();
            }
        }

        @Override
        public void cleanup() {
            cnxn.close();
        }
    }

    /**
     * This thread is responsible for closing stale connections so that
     * connections on which no session is established are properly expired.
     */
    private class ConnectionExpirerThread extends ZooKeeperThread {
        ConnectionExpirerThread() {
            super("ConnnectionExpirer");
        }

        public void run() {
            try {
                while (!stopped) {
                    long waitTime = cnxnExpiryQueue.getWaitTime();
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                        continue;
                    }
                    for (NIOServerCnxn conn : cnxnExpiryQueue.poll()) {
                        conn.close();
                    }
                }

            } catch (InterruptedException e) {
                LOG.info("ConnnectionExpirerThread interrupted");
            }
        }
    }
}

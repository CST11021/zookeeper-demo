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

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 事务提交处理器。对于非事务请求，该处理器会直接将其交付给下一级处理器处理；
 * 对于事务请求，其会等待集群内针对Proposal的投票直到该Proposal可被提交，利用CommitProcessor，每个服务器都可以很好地控制对事务请求的顺序处理。
 * <p>
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 * <p>
 * The CommitProcessor is multi-threaded. Communication between threads is
 * handled via queues, atomics, and wait/notifyAll synchronized on the
 * processor. The CommitProcessor acts as a gateway for allowing requests to
 * continue with the remainder of the processing pipeline. It will allow many
 * read requests but only a single write request to be in flight simultaneously,
 * thus ensuring that write requests are processed in transaction id order.
 * <p>
 * - 1   commit processor main thread, which watches the request queues and
 * assigns requests to worker threads based on their sessionId so that
 * read and write requests for a particular session are always assigned
 * to the same thread (and hence are guaranteed to run in order).
 * - 0-N worker threads, which run the rest of the request processor pipeline
 * on the requests. If configured with 0 worker threads, the primary
 * commit processor thread runs the pipeline directly.
 * <p>
 * Typical (default) thread counts are: on a 32 core machine, 1 commit
 * processor thread and 32 worker threads.
 * <p>
 * Multi-threading constraints:
 * - Each session's requests must be processed in order.
 * - Write requests must be processed in zxid order
 * - Must ensure no race condition between writes in one session that would
 * trigger a watch being set by a read request in another session
 * <p>
 * The current implementation solves the third constraint by simply allowing no
 * read requests to be processed in parallel with write requests.
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /** 用于标记处理器是否停止 */
    protected volatile boolean stopped = true;

    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS = "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT = "zookeeper.commitProcessor.shutdownTimeout";

    /** 保存在提交之前我们一直持有的请求，提交请求后，将请求从该队列移除 */
    protected final LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    /** 保存已经提交的请求，该队列通过{@link #commit(Request)}方法添加请求，通过{@link #processCommitted()}移除 */
    protected final LinkedBlockingQueue<Request> committedRequests = new LinkedBlockingQueue<Request>();
    /** 保存正在等待提交的请求：在{@link #run()}方法中，从{@link #queuedRequests}获取一个请求，并判断该请求是否需要提交，如果需要则将nextPending指向该请求，如果不需要则直接给下一个处理器处理 */
    protected final AtomicReference<Request> nextPending = new AtomicReference<Request>();
    /** 当CommitProcessor调用下一个处理器处理该请求时，会保存该请求的引用 */
    private final AtomicReference<Request> currentlyCommitting = new AtomicReference<Request>();
    /** 用于统计当前正在处理的请求的数量，当请求交给下一个处理器处理时，该计数器会+1，当下一个处理器处理完成后该计数器-1 */
    protected AtomicInteger numRequestsProcessing = new AtomicInteger(0);

    /** workerPool延迟shutdown的时间 */
    private long workerShutdownTimeoutMS;
    /** 表示下一个处理器 */
    RequestProcessor nextProcessor;
    /** 用于处理事务请求 */
    protected WorkerService workerPool;

    /**
     * This flag indicates whether we need to wait for a response to come back from the leader or we just let the sync operation flow through like a read.
     * The flag will be false if the CommitProcessor is in a Leader pipeline.
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id, boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    @Override
    public void start() {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numWorkerThreads = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring CommitProcessor with " + (numWorkerThreads > 0 ? numWorkerThreads : "no") + " worker threads.");
        if (workerPool == null) {
            workerPool = new WorkerService("CommitProcWork", numWorkerThreads, true);
        }
        stopped = false;
        super.start();
    }

    @Override
    public void run() {
        Request request;
        try {
            while (!stopped) {

                // CommitProcessor处理会将 queuedRequests 队列中的请求取出来交给下一个处理器处理，如果委托的处理器正在处理请求，则该线程会阻塞

                synchronized (this) {
                    // （待处理的请求队列是空的 || 存在等待提交的请求 || 正在提交的请求）&& （已经提交的请求队列是空的 || 存在正在处理的请求）
                    while (!stopped
                            && ((queuedRequests.isEmpty() || isWaitingForCommit() || isProcessingCommit())
                            && (committedRequests.isEmpty() || isProcessingRequest()))) {
                        wait();
                    }
                }

                /*
                 * 处理queuedRequests:
                 * 处理下一个请求，直到找到一个需要等待提交的请求。
                 * 我们在处理写请求时不能处理读请求。
                 */
                while (!stopped && !isWaitingForCommit() && !isProcessingCommit() && (request = queuedRequests.poll()) != null) {

                    // 如果请求需要被提交，则将请求赋值给nextPending
                    if (needCommit(request)) {
                        nextPending.set(request);
                    } else {
                        sendToNextProcessor(request);
                    }
                }

                /*
                 * 处理committedRequests:
                 * 检查并查看是否提交了挂起的请求。
                 * 我们只能在没有其他请求被处理的情况下提交请求。
                 */
                processCommitted();
            }
        } catch (Throwable e) {
            handleException(this.getName(), e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    /**
     * 处理来自客户端的请求
     *
     * @param request
     */
    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }

        // 将请求添加到请求队列
        queuedRequests.add(request);

        // 如果不存在等待提交的请求，则唤醒线程
        if (!isWaitingForCommit()) {
            wakeup();
        }
    }

    /**
     * 是否存在正在被处理的请求
     *
     * @return
     */
    private boolean isProcessingRequest() {
        return numRequestsProcessing.get() != 0;
    }

    /**
     * 当前是否正在处理请求
     *
     * @return
     */
    private boolean isProcessingCommit() {
        return currentlyCommitting.get() != null;
    }

    /**
     * 判断这个请求是需要commit
     *
     * @param request
     * @return
     */
    protected boolean needCommit(Request request) {
        switch (request.type) {
            // 以下写相关的请求都需要事务
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
            case OpCode.delete:
            case OpCode.deleteContainer:
            case OpCode.setData:
            case OpCode.reconfig:
            case OpCode.multi:
            case OpCode.setACL:
                return true;

            // 以下请求类型判断是否为本地事务
            case OpCode.sync:
                return matchSyncs;
            case OpCode.createSession:
            case OpCode.closeSession:
                return !request.isLocalSession();
            default:
                return false;
        }
    }

    /**
     * 处理提交的请求，从{@link #committedRequests}队列取出一个请求，调用下一个处理器处理该请求
     */
    protected void processCommitted() {
        Request request;
        // 如果当前不存在正在被处理的请求的同时也不存在已经提交的请求，peek()方法用于获取队列的头元素
        if (!stopped && !isProcessingRequest() && (committedRequests.peek() != null)) {

            // zookeper -1863:只有在队列请求中没有新请求等待或正在等待提交时才继续。
            if (!isWaitingForCommit() && !queuedRequests.isEmpty()) {
                return;
            }

            // poll()：将头元素取出来
            request = committedRequests.poll();

            // 检查nextPending和这个request是否一样
            Request pending = nextPending.get();
            if (pending != null && pending.sessionId == request.sessionId && pending.cxid == request.cxid) {
                // we want to send our version of the request. the pointer to the connection in the request
                pending.setHdr(request.getHdr());
                pending.setTxn(request.getTxn());
                pending.zxid = request.zxid;

                // Set currentlyCommitting so we will block until this completes. Cleared by CommitWorkRequest after nextProcessor returns.
                currentlyCommitting.set(pending);
                nextPending.set(null);

                // 将请求交给下一个处理器处理
                sendToNextProcessor(pending);
            } else {
                // 这个请求来自别人，所以只要发送提交包就可以了
                currentlyCommitting.set(request);

                // 将请求交给下一个处理器处理
                sendToNextProcessor(request);
            }
        }
    }

    /**
     * 调用下一个处理器处理该请求，将计数器+1，当处理完成后，再将计数器-1，并换线阻塞线程，继续处理下一个请求
     *
     * @param request
     */
    private void sendToNextProcessor(Request request) {
        numRequestsProcessing.incrementAndGet();
        workerPool.schedule(new CommitWorkRequest(request), request.sessionId);
    }

    /**
     * 将请求添加到请求队列中，并且如果当前处理器没有正在处理请求，则先唤醒阻塞的线程
     *
     * @param request
     */
    public void commit(Request request) {
        if (stopped || request == null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing request:: " + request);
        }

        // 保存提交的请求，并且如果当前没有正在处理请求，则唤醒线程处理请求
        committedRequests.add(request);
        if (!isProcessingCommit()) {
            wakeup();
        }
    }

    /**
     * 唤醒当前阻塞的线程
     */
    synchronized private void wakeup() {
        notifyAll();
    }

    /**
     * 判断是否存在等待提交的请求
     *
     * @return
     */
    private boolean isWaitingForCommit() {
        return nextPending.get() != null;
    }

    private void halt() {
        stopped = true;
        wakeup();
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    /**
     * 调用下一个处理器处理该请求，处理完成后，将计数器-1，并换线阻塞线程，继续处理下一个请求
     */
    private class CommitWorkRequest extends WorkerService.WorkRequest {

        private final Request request;

        CommitWorkRequest(Request request) {
            this.request = request;
        }

        /**
         * 调用下一个处理器处理该请求
         *
         * @throws RequestProcessorException
         */
        public void doWork() throws RequestProcessorException {
            try {
                nextProcessor.processRequest(request);
            } finally {

                // 下一个处理器处理完成后
                currentlyCommitting.compareAndSet(request, null);

                // 请求的处理完成后，将计数器-1，并换线阻塞线程，继续处理下一个请求
                if (numRequestsProcessing.decrementAndGet() == 0) {
                    // 待处理的请求队列不是空的 或者 已经提交的请求队列不是空的，则唤醒阻塞线程
                    if (!queuedRequests.isEmpty() || !committedRequests.isEmpty()) {
                        wakeup();
                    }
                }
            }
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor,"
                        + " unable to continue.");
                CommitProcessor.this.halt();
            }
        }

    }


}

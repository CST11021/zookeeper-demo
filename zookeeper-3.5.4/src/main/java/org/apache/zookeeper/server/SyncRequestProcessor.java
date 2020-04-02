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

package org.apache.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 用于向磁盘中写入事务日志跟快照信息，在日志被同步到磁盘之前，请求不会被传递给下一个RequestProcessor。
 *
 * SyncRequestProcessor用于3种不同的情况：
 * 1、Leader     将请求同步到磁盘并将其转发给AckRequestProcessor，后者将ack发送回自身。
 * 2、Follower   将请求同步到磁盘，并将请求转发到SendAckRequestProcessor，后者将数据包发送给leader。
                 SendAckRequestProcessor是可刷新的，它允许我们将数据包推送给leader。
 * 3、Observer   同步提交到磁盘的请求(作为通知包接收)。
 *               它从不发送ack回leader，所以下一个处理器将是空的。
 *               这改变了observer上txnlog的语义，因为它只包含提交的txns。
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    private final LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();
    private final RequestProcessor nextProcessor;

    /** 该线程用于将数据树和会话保存到快照文件中 */
    private Thread snapInProcess = null;
    /** 用于标记{@link #run()}方法是否正在执行 */
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to disk.
     * Basically this is the list of SyncItems whose callbacks will be invoked after flush returns successfully.
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random(System.nanoTime());
    /** 表示事务日志文件的日志条数 */
    private static int snapCount = ZooKeeperServer.getSnapCount();
    /** 标记服务器停止的请求 */
    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    @Override
    public void run() {
        try {
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers in the ensemble take a snapshot at the same time
            int randRoll = r.nextInt(snapCount / 2);
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) {
                    // take如果没有则线程一直wait
                    si = queuedRequests.take();
                } else {
                    // poll如果没有则世界返回null
                    si = queuedRequests.poll();
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }

                if (si == requestOfDeath) {
                    break;
                }

                if (si != null) {
                    // 追加请求到事务日志中
                    if (zks.getZKDatabase().append(si)) {
                        logCount++;

                        // snapCount(系统属性：zookeeper.snapCount) //默认为100000，在新增log（txn log）条数达到snapCount/2 + Random.nextInt(snapCount/2)时，
                        // 将会对zkDatabase(内存数据库)进行snapshot，将内存中DataTree反序为snapshot文件数据，同时log计数置为0，以此循环。
                        // snapshot过程中，同时也伴随txn log的新文件创建（这也是snapCount与preAllocSize参数的互相协调原因）。
                        // snapshot时使用随机数的原因：让每个server snapshot的时机具有随即且可控，避免所有的server同时snapshot（snapshot过程中将阻塞请求）。

                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount / 2);
                            // roll the log
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                // 将数据树和会话保存到快照文件中
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                    public void run() {
                                        try {
                                            zks.takeSnapshot();
                                        } catch (Exception e) {
                                            LOG.warn("Unexpected exception", e);
                                        }
                                    }
                                };
                                snapInProcess.start();
                            }
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads iff this is a read, and there are no pending flushes (writes), then just pass this to the next processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable) nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    toFlush.add(si);
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally {
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    private void flush(LinkedList<Request> toFlush) throws IOException, RequestProcessorException {
        if (toFlush.isEmpty())
            return;

        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable) nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if (running) {
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }



}

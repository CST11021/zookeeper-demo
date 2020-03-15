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

import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WorkerService is a worker thread pool for running tasks and is implemented
 * using one or more ExecutorServices. A WorkerService can support assignable
 * threads, which it does by creating N separate single thread ExecutorServices,
 * or non-assignable threads, which it does by creating a single N-thread
 * ExecutorService.
 *   - NIOServerCnxnFactory uses a non-assignable WorkerService because the
 *     socket IO requests are order independent and allowing the
 *     ExecutorService to handle thread assignment gives optimal performance.
 *   - CommitProcessor uses an assignable WorkerService because requests for
 *     a given session must be processed in order.
 * ExecutorService provides queue management and thread restarting, so it's
 * useful even with a single thread.
 */
public class WorkerService {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerService.class);

    private final ArrayList<ExecutorService> workers = new ArrayList<ExecutorService>();

    /** 线程名称前缀 */
    private final String threadNamePrefix;

    /** 线程池数量 */
    private int numWorkerThreads;

    /** 该值如果为true，则创建对应{@link #numWorkerThreads}数量的线程池，否则创建一个线程池，并添加到{@link #workers}， */
    private boolean threadsAreAssignable;

    private long shutdownTimeoutMS = 5000;

    /** 标记服务是否开始 */
    private volatile boolean stopped = true;

    /**
     * @param name                  工作线程名为：${} + "Thread"
     * @param numThreads            工作线程的数量(0 - N)如果为0，则调度的工作将由调用线程立即运行。
     * @param useAssignableThreads  工作线程是否应该单独分配
     */
    public WorkerService(String name, int numThreads, boolean useAssignableThreads) {
        this.threadNamePrefix = (name == null ? "" : name) + "Thread";
        this.numWorkerThreads = numThreads;
        this.threadsAreAssignable = useAssignableThreads;
        start();
    }

    /**
     * 如果没有配置使用线程池，则直接调用{@link WorkRequest#doWork()}方法同步执行
     *
     * @param workRequest
     */
    public void schedule(WorkRequest workRequest) {
        schedule(workRequest, 0);
    }

    /**
     * 如果没有配置使用线程池，则直接调用{@link WorkRequest#doWork()}方法同步执行
     *
     * @param workRequest
     * @param id
     */
    public void schedule(WorkRequest workRequest, long id) {
        if (stopped) {
            workRequest.cleanup();
            return;
        }

        ScheduledWorkRequest scheduledWorkRequest = new ScheduledWorkRequest(workRequest);

        // 如果我们有一个工作线程池，使用它;否则，直接做工作。
        int size = workers.size();
        if (size > 0) {
            try {
                // make sure to map negative ids as well to [0, size-1]
                int workerNum = ((int) (id % size) + size) % size;
                ExecutorService worker = workers.get(workerNum);
                worker.execute(scheduledWorkRequest);
            } catch (RejectedExecutionException e) {
                LOG.warn("ExecutorService rejected execution", e);
                workRequest.cleanup();
            }
        } else {
            // 当没有工作线程池时，直接执行工作并等待其完成
            scheduledWorkRequest.start();
            try {
                scheduledWorkRequest.join();
            } catch (InterruptedException e) {
                LOG.warn("Unexpected exception", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 创建线程池，并添加到{@link #workers}中
     */
    public void start() {
        if (numWorkerThreads > 0) {
            if (threadsAreAssignable) {
                for (int i = 1; i <= numWorkerThreads; ++i) {
                    workers.add(Executors.newFixedThreadPool(1, new DaemonThreadFactory(threadNamePrefix, i)));
                }
            } else {
                workers.add(Executors.newFixedThreadPool(numWorkerThreads, new DaemonThreadFactory(threadNamePrefix)));
            }
        }
        stopped = false;
    }

    /**
     * shutdown线程池
     */
    public void stop() {
        stopped = true;

        // Signal for graceful shutdown
        for (ExecutorService worker : workers) {
            worker.shutdown();
        }
    }

    /**
     * 延迟shutdown
     *
     * @param shutdownTimeoutMS
     */
    public void join(long shutdownTimeoutMS) {
        // Give the worker threads time to finish executing
        long now = Time.currentElapsedTime();
        long endTime = now + shutdownTimeoutMS;
        for (ExecutorService worker : workers) {
            boolean terminated = false;
            while ((now = Time.currentElapsedTime()) <= endTime) {
                try {
                    terminated = worker.awaitTermination(endTime - now, TimeUnit.MILLISECONDS);
                    break;
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            if (!terminated) {
                // If we've timed out, do a hard shutdown
                worker.shutdownNow();
            }
        }
    }


    /**
     * Callers should implement a class extending WorkRequest in order to
     * schedule work with the service.
     */
    public static abstract class WorkRequest {

        /**
         * Must be implemented. Is called when the work request is run.
         */
        public abstract void doWork() throws Exception;

        /**
         * (可选)如果实现该方法，则在服务停止或无法调度请求时调用。
         */
        public void cleanup() {
        }

    }

    private class ScheduledWorkRequest extends ZooKeeperThread {

        private final WorkRequest workRequest;

        ScheduledWorkRequest(WorkRequest workRequest) {
            super("ScheduledWorkRequest");
            this.workRequest = workRequest;
        }

        @Override
        public void run() {
            try {
                // 检查请求在队列中是否停止
                if (stopped) {
                    workRequest.cleanup();
                    return;
                }
                workRequest.doWork();
            } catch (Exception e) {
                LOG.warn("Unexpected exception", e);
                workRequest.cleanup();
            }
        }
    }

    /**
     * ThreadFactory for the worker thread pool. We don't use the default
     * thread factory because (1) we want to give the worker threads easier
     * to identify names; and (2) we want to make the worker threads daemon
     * threads so they don't block the server from shutting down.
     */
    private static class DaemonThreadFactory implements ThreadFactory {
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        DaemonThreadFactory(String name) {
            this(name, 1);
        }

        DaemonThreadFactory(String name, int firstThreadNum) {
            threadNumber.set(firstThreadNum);
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = name + "-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (!t.isDaemon())
                t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}

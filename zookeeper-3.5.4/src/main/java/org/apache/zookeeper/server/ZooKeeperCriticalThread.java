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

/**
 * 代表重要的线程，当线程抛出未捕获异常时，将退出系统。
 */
public class ZooKeeperCriticalThread extends ZooKeeperThread {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCriticalThread.class);

    private final ZooKeeperServerListener listener;

    public ZooKeeperCriticalThread(String threadName, ZooKeeperServerListener listener) {
        super(threadName);
        this.listener = listener;
    }

    /**
     * 处理线程异常：该处理方式，会发布一个线程停止的事件通知
     *
     * @param threadName    异常的线程名
     * @param e             异常堆栈
     */
    @Override
    protected void handleException(String threadName, Throwable e) {
        LOG.error("Severe unrecoverable error, from thread : {}", threadName, e);
        listener.notifyStopping(threadName, ExitCode.UNEXPECTED_ERROR);
    }
}

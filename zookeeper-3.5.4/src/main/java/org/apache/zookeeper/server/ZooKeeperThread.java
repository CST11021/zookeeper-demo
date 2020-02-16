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
 * 这是捕获线程抛出的所有未捕获异常的主类。
 */
public class ZooKeeperThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperThread.class);

    /** 线程类如果内部没有对相应的异常进行处理，则当异常发生时，会调用该处理器，进行处理 */
    private UncaughtExceptionHandler uncaughtExceptionalHandler = new UncaughtExceptionHandler() {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            handleException(t.getName(), e);
        }

    };

    public ZooKeeperThread(String threadName) {
        super(threadName);
        setUncaughtExceptionHandler(uncaughtExceptionalHandler);
    }

    /**
     * 处理异常
     * 
     * @param thName    异常的线程名
     * @param e         异常堆栈
     */
    protected void handleException(String thName, Throwable e) {
        LOG.warn("Exception occurred from thread {}", thName, e);
    }
}

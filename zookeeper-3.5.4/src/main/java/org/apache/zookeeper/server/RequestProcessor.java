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

/**
 * RequestProcessors are chained together to process transactions.
 * （requestprocessor被链接在一起来处理事务。）
 * Requests are always processed in order. The standalone server, follower, and leader all have slightly different RequestProcessors chained together.
 * （请求总是按顺序处理。独立的服务器、Follower和leader都有略微不同的请求处理器链接在一起。）
 *
 * Requests always move forward through the chain of RequestProcessors.
 * Requests are passed to a RequestProcessor through processRequest().
 * Generally method will always be invoked by a single thread.
 * （请求总是通过请求处理器链向前移动。）
 * （请求通过processRequest()传递给RequestProcessor。）
 * （通常方法总是由单个线程调用。）
 *
 * When shutdown is called, the request RequestProcessor should also shutdown any RequestProcessors that it is connected to.
 * （当调用shutdown时，请求处理器也关闭它所连接的任何请求处理器。）
 */
public interface RequestProcessor {

    @SuppressWarnings("serial")
    public static class RequestProcessorException extends Exception {
        public RequestProcessorException(String msg, Throwable t) {
            super(msg, t);
        }
    }

    /**
     * 处理来自客户端的请求
     *
     * @param request
     * @throws RequestProcessorException
     */
    void processRequest(Request request) throws RequestProcessorException;

    /**
     * 关闭处理器，有些处理器的实现是通过异步、线程池等方式处理的，该方法用于处理例如这些线程池的关闭，当调用shutdown时，请求处理器也关闭它所连接的任何请求处理器。
     */
    void shutdown();
}

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
 * 服务端的请求处理器都继承该接口，用于处理来自客户端的请求
 *
 * PrepRequestProcessor处理器：用于构造请求对象，校验session合法性等。
 * SyncRequestProcessor处理器：用于向磁盘中写入事务日志跟快照信息。
 * FinalRequestProcessor处理器：用于修改ZK内存中的数据结构并触发watcher。
 *
 *
 * 这里要补充下，ZK中所有的数据都放在内存中，封装在类ZKDatabase里面，包括所有的节点信息，会话信息以及集群模式下要使用的committed log。也就是说处理链路上只有最后一步FinalRequestProcessor才会让数据真正生效。
 *
 *
 * Leader和Follower有各自的RequestProcessor处理链；
 *
 * LeaderZooKeeperServer：	LeaderRequestProcessor -> PrepRequestProcessor -> ProposalRequestProcessor（SyncRequestProcessor->AckRequestProcessor）-> CommitProcessor -> Leader.ToBeAppliedRequestProcessor -> FinalRequestProcessor
 * FollowerZooKeeperServer：	FollowerRequestProcessor-> CommitProcessor->FinalRequestProcessor；SyncRequestProcessor->SendAckRequestProcessor
 * ObserverZooKeeperServer：	ObserverRequestProcessor->CommitProcessor->FinalRequestProcessor；SyncRequestProcessor
 * ReadOnlyZooKeeperServer：	ReadOnlyRequestProcessor->PrepRequestProcessor->FinalRequestProcessor
 *
 */
public interface RequestProcessor {

    /**
     * 请求处理异常时，会抛出该异常
     */
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

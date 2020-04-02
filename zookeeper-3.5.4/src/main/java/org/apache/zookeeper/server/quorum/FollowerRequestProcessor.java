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

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 此RequestProcessor将修改系统状态的任何请求转发给Leader。
 */
public class FollowerRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerRequestProcessor.class);

    /** 对应Follower类型的zk节点 */
    FollowerZooKeeperServer zks;

    /** 表示下一个请求处理器 */
    RequestProcessor nextProcessor;

    /** 用于保存哪些等待被处理的请求 */
    LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    /** 用于标记该处理器是否可用，当zk shutdown时，会将该值置为true，则此时处理不可用 */
    boolean finished = false;

    public FollowerRequestProcessor(FollowerZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("FollowerRequestProcessor:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    /**
     * 从请求队列中获取一个请求，并将请求交给下一个处理器处理
     */
    @Override
    public void run() {
        try {
            while (!finished) {
                Request request = queuedRequests.take();
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, ZooTrace.CLIENT_REQUEST_TRACE_MASK, 'F', request, "");
                }

                if (request == Request.requestOfDeath) {
                    break;
                }

                // 我们希望在将请求提交给leader之前对将要处理的请求进行排队，以便准备接收响应
                nextProcessor.processRequest(request);

                // 我们现在把请求发送给leader
                // 与所有其他仲裁操作一样，sync也遵循此代码路径，但与其他操作不同的是，我们需要跟踪这个关注者的挂起的同步操作，因此我们将其添加到pendingSyncs中。
                switch (request.type) {
                    case OpCode.sync:
                        zks.pendingSyncs.add(request);
                        zks.getFollower().request(request);
                        break;
                    case OpCode.create:
                    case OpCode.create2:
                    case OpCode.createTTL:
                    case OpCode.createContainer:
                    case OpCode.delete:
                    case OpCode.deleteContainer:
                    case OpCode.setData:
                    case OpCode.reconfig:
                    case OpCode.setACL:
                    case OpCode.multi:
                    case OpCode.check:
                        zks.getFollower().request(request);
                        break;
                    case OpCode.createSession:
                    case OpCode.closeSession:
                        // Don't forward local sessions to the leader.
                        if (!request.isLocalSession()) {
                            zks.getFollower().request(request);
                        }
                        break;
                }
            }
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("FollowerRequestProcessor exited loop!");
    }

    /**
     * 处理来自客户端的请求，判断该请求是否需要升级为全局会话，如果需要则，添加一个创建session的请求到队列中
     *
     * @param request
     */
    public void processRequest(Request request) {
        if (!finished) {
            // 在发送请求之前，检查请求是否需要全局会话，而我们拥有的是本地会话。如果是，就升级。
            // 如果当前请求是事务请求，但是会话又是本地会话，此时需要升级会话为全局会话
            Request upgradeRequest = null;
            try {
                // 返回一个创建session的请求
                upgradeRequest = zks.checkUpgradeSession(request);
            } catch (KeeperException ke) {
                if (request.getHdr() != null) {
                    request.getHdr().setType(OpCode.error);
                    request.setTxn(new ErrorTxn(ke.code().intValue()));
                }
                request.setException(ke);
                LOG.info("Error creating upgrade request",  ke);
            } catch (IOException ie) {
                LOG.error("Unexpected error in upgrade", ie);
            }

            if (upgradeRequest != null) {
                queuedRequests.add(upgradeRequest);
            }
            queuedRequests.add(request);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        finished = true;
        queuedRequests.clear();
        queuedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

}

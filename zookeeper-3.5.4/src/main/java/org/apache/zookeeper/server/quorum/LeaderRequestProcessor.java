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
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 负责执行本地会话升级，只有直接提交给leader服务器的请求使用这个处理器
 */
public class LeaderRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderRequestProcessor.class);

    private final LeaderZooKeeperServer lzks;

    private final RequestProcessor nextProcessor;

    public LeaderRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
        this.lzks = zks;
        this.nextProcessor = nextProcessor;
    }

    /**
     * 处理来自客户端的请求，判断该请求是否需要升级为全局会话，如果需要则，添加一个创建session的请求到队列中
     *
     * @param request
     * @throws RequestProcessorException
     */
    @Override
    public void processRequest(Request request) throws RequestProcessorException {
        // 检查这是否是一个本地会话，并且我们试图创建一个临时节点，在这种情况下，我们升级会话
        Request upgradeRequest = null;
        try {
            // 如果需要升级会话，则返回一个创建session的请求
            upgradeRequest = lzks.checkUpgradeSession(request);
        } catch (KeeperException ke) {
            if (request.getHdr() != null) {
                LOG.debug("Updating header");
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(ke.code().intValue()));
            }
            request.setException(ke);
            LOG.info("Error creating upgrade request " + ke.getMessage());
        } catch (IOException ie) {
            LOG.error("Unexpected error in upgrade", ie);
        }

        if (upgradeRequest != null) {
            nextProcessor.processRequest(upgradeRequest);
        }

        nextProcessor.processRequest(request);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
    }

}

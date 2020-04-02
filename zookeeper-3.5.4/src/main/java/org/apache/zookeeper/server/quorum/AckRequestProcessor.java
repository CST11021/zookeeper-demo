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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;


/**
 * AckRequestProcessor是leader端的处理器：负责在SyncRequestProcessor完成事务日志记录后，向Proposal的投票收集器发送ACK反馈，以通知投票收集器当前服务器已经完成了对该Proposal的事务日志记录。
 *
 *
 *
 *
 * 在SyncRequestProcessor完成日志记录之后，不同角色服务器需要告知ACK代表是否日志记录完成
 * 在leader端，该处理器就是AckRequestProcessor
 * 在Follower端，该处理器就是SendAckRequestProcessor
 * 在observer端，由于observer并没有投票权，不需要对应的处理器
 */
class AckRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AckRequestProcessor.class);

    Leader leader;

    AckRequestProcessor(Leader leader) {
        this.leader = leader;
    }

    /**
     * 将请求作为ACK转发给leader
     */
    public void processRequest(Request request) {
        QuorumPeer self = leader.self;
        if(self != null)
            leader.processAck(self.getId(), request.zxid, null);
        else
            LOG.error("Null QuorumPeer");
    }

    public void shutdown() {
        // XXX No need to do anything
    }
}

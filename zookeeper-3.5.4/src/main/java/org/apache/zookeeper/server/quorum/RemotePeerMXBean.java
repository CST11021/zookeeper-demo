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

/**
 * A proxy for a remote quorum peer.
 */
public interface RemotePeerMXBean {

    /**
     * 返回zk节点的名称，一般为机器的serverId，即myid
     *
     * @return name of the peer
     */
    public String getName();

    /**
     * 获取zk节点的IP地址
     *
     * @return IP address of the quorum peer 
     */
    public String getQuorumAddress();

    /**
     * 返回选举的地址和端口{@link org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer#electionAddr}
     *
     * @return the election address
     */
    public String getElectionAddress();

    /**
     * {@link org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer#clientAddr}
     *
     * @return the client address
     */
    public String getClientAddress();

    /**
     * zk节点的类型，{@link org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType}
     *
     * @return the learner type
     */
    public String getLearnerType();
}

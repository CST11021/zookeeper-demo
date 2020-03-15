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

import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;

/**
 * Vote类是zk选举的实体类，Zookeeper的快速选举算法就是利用id, zxid和epoch来选举出新的leader。
 */
public class Vote {

    final private int version;
    /** 选票推举的leader的SID(配置文件中配置的myId，即唯一的服务ID) */
    final private long id;
    /** 被推举的leader的事务ID */
    final private long zxid;
    /** 逻辑时钟。是一个递增的数字，通过对比electionEpoch来判断server自己的Vote和其他Vote是否在同一个选举轮次中。每次进入一个新的选举轮次，electionEpoch都会+1 */
    final private long electionEpoch;
    /** 被推举的leader的epoch。 */
    final private long peerEpoch;
    /** 当前zk节点所处的状态 */
    final private ServerState state;

    public Vote(long id, long zxid) {
        this.version = 0x0;
        this.id = id;
        this.zxid = zxid;
        this.electionEpoch = -1;
        this.peerEpoch = -1;
        this.state = ServerState.LOOKING;
    }
    public Vote(long id, long zxid, long peerEpoch) {
        this.version = 0x0;
        this.id = id;
        this.zxid = zxid;
        this.electionEpoch = -1;
        this.peerEpoch = peerEpoch;
        this.state = ServerState.LOOKING;
    }
    public Vote(long id, long zxid, long electionEpoch, long peerEpoch) {
        this.version = 0x0;
        this.id = id;
        this.zxid = zxid;
        this.electionEpoch = electionEpoch;
        this.peerEpoch = peerEpoch;
        this.state = ServerState.LOOKING;
    }
    public Vote(int version, long id, long zxid, long electionEpoch, long peerEpoch, ServerState state) {
        this.version = version;
        this.id = id;
        this.zxid = zxid;
        this.electionEpoch = electionEpoch;
        this.state = state;
        this.peerEpoch = peerEpoch;
    }
    public Vote(long id, long zxid, long electionEpoch, long peerEpoch, ServerState state) {
        this.id = id;
        this.zxid = zxid;
        this.electionEpoch = electionEpoch;
        this.state = state;
        this.peerEpoch = peerEpoch;
        this.version = 0x0;
    }


    QuorumVerifier
    public int getVersion() {
        return version;
    }
    public long getId() {
        return id;
    }
    public long getZxid() {
        return zxid;
    }
    public long getElectionEpoch() {
        return electionEpoch;
    }
    public long getPeerEpoch() {
        return peerEpoch;
    }
    public ServerState getState() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Vote)) {
            return false;
        }
        Vote other = (Vote) o;
        return (id == other.id
                && zxid == other.zxid
                && electionEpoch == other.electionEpoch
                && peerEpoch == other.peerEpoch);
    }

    @Override
    public int hashCode() {
        return (int) (id & zxid);
    }

    public String toString() {
        return "(" + id + ", " + Long.toHexString(zxid) + ", " + Long.toHexString(peerEpoch) + ")";
    }
}

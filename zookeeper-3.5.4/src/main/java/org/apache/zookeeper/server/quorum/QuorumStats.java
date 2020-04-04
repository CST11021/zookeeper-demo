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
 * 表示选举成员的状态
 */
public class QuorumStats {

    private final Provider provider;

    protected QuorumStats(Provider provider) {
        this.provider = provider;
    }

    /**
     * {@link QuorumStats.Provider}
     *
     * @return
     */
    public String getServerState() {
        return provider.getServerState();
    }

    public String[] getQuorumPeers() {
        return provider.getQuorumPeers();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        String state = getServerState();
        if (state.equals(Provider.LEADING_STATE)) {
            sb.append("Followers:");
            for (String f : getQuorumPeers()) {
                sb.append(" ").append(f);
            }
            sb.append("\n");
        } else if (state.equals(Provider.FOLLOWING_STATE)
                || state.equals(Provider.OBSERVING_STATE)) {
            sb.append("Leader: ");
            String[] ldr = getQuorumPeers();
            if (ldr.length > 0)
                sb.append(ldr[0]);
            else
                sb.append("not connected");
            sb.append("\n");
        }
        return sb.toString();
    }

    public interface Provider {
        /**
         * 表示位置的服务状态
         */
        static public final String UNKNOWN_STATE = "unknown";
        /**
         * 对应{@link org.apache.zookeeper.server.quorum.QuorumPeer.ServerState#LOOKING}：
         * 表示寻找Leader状态，当服务器处于该状态时，它会认为当前集群中没有Leader，因此需要进入Leader选举状态
         */
        static public final String LOOKING_STATE = "leaderelection";
        /**
         * 对应{@link org.apache.zookeeper.server.quorum.QuorumPeer.ServerState#LEADING}：
         * 领导者状态，表明当前服务器角色是Leader。
         */
        static public final String LEADING_STATE = "leading";
        /**
         * 对应{@link org.apache.zookeeper.server.quorum.QuorumPeer.ServerState#FOLLOWING}：
         * 跟随者状态，表明当前服务器角色是Follower。
         */
        static public final String FOLLOWING_STATE = "following";
        /**
         * 对应{@link org.apache.zookeeper.server.quorum.QuorumPeer.ServerState#OBSERVING}：
         * OBSERVING：观察者状态，表明当前服务器角色是Observer。
         */
        static public final String OBSERVING_STATE = "observing";

        /**
         *
         * @return
         */
        public String[] getQuorumPeers();

        /**
         * 返回服务器状态，服务状态包括：leaderelection、leading、following和observing，
         * 对应{@link org.apache.zookeeper.server.quorum.QuorumPeer.ServerState}
         *
         * @return
         */
        public String getServerState();
    }
}

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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.SessionTracker;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 可将本地session升级为全局session的session管理器
 */
public abstract class UpgradeableSessionTracker implements SessionTracker {

    private static final Logger LOG = LoggerFactory.getLogger(UpgradeableSessionTracker.class);

    /** 保存会话及对应的超时时间 */
    private ConcurrentMap<Long, Integer> localSessionsWithTimeouts;

    protected LocalSessionTracker localSessionTracker;

    public void start() {
    }

    public void createLocalSessionTracker(SessionExpirer expirer, int tickTime, long id, ZooKeeperServerListener listener) {
        this.localSessionsWithTimeouts = new ConcurrentHashMap<Long, Integer>();
        this.localSessionTracker = new LocalSessionTracker(expirer, this.localSessionsWithTimeouts, tickTime, id, listener);
    }

    /**
     * 返回SessionTracker是否知道这个会话
     *
     * @param sessionId
     * @return
     */
    public boolean isTrackingSession(long sessionId) {
        return isLocalSession(sessionId) || isGlobalSession(sessionId);
    }

    /**
     * 判断sessionId是否为本地会话
     *
     * @param sessionId
     * @return
     */
    public boolean isLocalSession(long sessionId) {
        return localSessionTracker != null && localSessionTracker.isTrackingSession(sessionId);
    }

    abstract public boolean isGlobalSession(long sessionId);

    /**
     * 将会话升级为全局会话，该方法只是从本地跟踪器中删除会话并将其标记为全局的，这是由调用者实际排队的会话事务。
     *
     * @param sessionId
     * @return session timeout (-1 if not a local session)
     */
    public int upgradeSession(long sessionId) {
        if (localSessionsWithTimeouts == null) {
            return -1;
        }

        // 移除本地会话，添加一个全局会话
        Integer timeout = localSessionsWithTimeouts.remove(sessionId);
        if (timeout != null) {
            LOG.info("Upgrading session 0x" + Long.toHexString(sessionId));
            // Add as global before removing as local
            addGlobalSession(sessionId, timeout);
            localSessionTracker.removeSession(sessionId);
            return timeout;
        }
        return -1;
    }

    public void checkGlobalSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        throw new UnsupportedOperationException();
    }
}

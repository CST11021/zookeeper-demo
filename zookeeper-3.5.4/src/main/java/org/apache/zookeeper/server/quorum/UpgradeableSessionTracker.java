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

    /** 本地是session管理器 */
    protected LocalSessionTracker localSessionTracker;
    /** 保存本地会话及对应的过期时间 */
    private ConcurrentMap<Long, Integer> localSessionsWithTimeouts;

    public void start() {
    }

    /**
     * 创建本地会话管理器
     *
     * @param expirer
     * @param tickTime  对应session过期队列的区间大小{@link org.apache.zookeeper.server.ExpiryQueue#expirationInterval}
     * @param id
     * @param listener
     */
    public void createLocalSessionTracker(SessionExpirer expirer, int tickTime, long id, ZooKeeperServerListener listener) {
        this.localSessionsWithTimeouts = new ConcurrentHashMap<Long, Integer>();
        this.localSessionTracker = new LocalSessionTracker(expirer, this.localSessionsWithTimeouts, tickTime, id, listener);
    }

    /**
     * 返回SessionTracker存在这个sessionId
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

    /**
     * 判断该session是否是一个全局会话
     *
     * @param sessionId
     * @return
     */
    abstract public boolean isGlobalSession(long sessionId);

    /**
     * 将会话升级为全局会话，该方法只是从本地跟踪器中删除会话并将其标记为全局的，这是由调用者实际排队的会话事务。
     *
     * @param sessionId
     * @return 返回该会话的的过期时间
     */
    public int upgradeSession(long sessionId) {
        if (localSessionsWithTimeouts == null) {
            return -1;
        }

        // 移除本地会话，添加一个全局会话
        Integer timeout = localSessionsWithTimeouts.remove(sessionId);
        if (timeout != null) {
            LOG.info("Upgrading session 0x" + Long.toHexString(sessionId));
            // 添加一个全局会话
            addGlobalSession(sessionId, timeout);
            // 移除本地会话
            localSessionTracker.removeSession(sessionId);
            return timeout;
        }
        return -1;
    }

    /**
     * 不支持校验全局session
     *
     * @param sessionId     sessionId
     * @param owner         session归属，通常为{@link org.apache.zookeeper.server.Request#owner}
     * @throws KeeperException.SessionExpiredException
     * @throws KeeperException.SessionMovedException
     */
    public void checkGlobalSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        throw new UnsupportedOperationException();
    }
}

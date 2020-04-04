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

package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

/**
 * 这是zk用来跟踪会话的基本接口，由SessionTracker维护对session的操作，包括检测过期的session，调用SessionExpirer关闭session；
 *
 * Leader服务器的实现为：LeaderSessionTracker，Follower和Observer服务器的实现为LearnerSessionTracker
 */
public interface SessionTracker {


    /**
     * 用于查看session信息的接口
     */
    public static interface Session {
        /**
         * 获取sessionId
         *
         * @return
         */
        long getSessionId();

        /**
         * 获取session超时时间
         *
         * @return
         */
        int getTimeout();

        /**
         * session是否已经关闭
         * @return
         */
        boolean isClosing();
    }
    /**
     * 使session过期的接口
     */
    public static interface SessionExpirer {

        /**
         * 使session过期
         *
         * @param session
         */
        void expire(Session session);

        /**
         * 表示zk的serverId，即MyId
         * @return
         */
        long getServerId();
    }



    /**
     * 根据超时时间创建一个session
     *
     * @param sessionTimeout
     * @return
     */
    long createSession(int sessionTimeout);

    /**
     * 添加一个全局会话
     *
     * @param id sessionId
     * @param to session超时时间
     * @return 是否添加成功
     */
    boolean addGlobalSession(long id, int to);

    /**
     * 向被跟踪的会话添加一个会话。如果启用了会话，则将其作为本地会话添加，否则将作为全局会话添加。
     *
     * @param id sessionId
     * @param to session超时时间
     * @return 是否添加成功
     */
    boolean addSession(long id, int to);

    /**
     * 判断sessionId是否合法，并且是否有过期
     *
     * @param sessionId
     * @param sessionTimeout
     * @return false if session is no longer active
     */
    boolean touchSession(long sessionId, int sessionTimeout);

    /**
     * 标记session正在关闭
     *
     * @param sessionId
     */
    void setSessionClosing(long sessionId);

    /**
     * 根据sessionId移除session
     *
     * @param sessionId
     */
    void removeSession(long sessionId);

    /**
     * 判断会话是否存在
     *
     * @param sessionId
     * @return SessionTracker是否知道这个会话
     */
    boolean isTrackingSession(long sessionId);

    /**
     * 检查SessionTracker是否知道这个会话，会话是否仍然是活动的，所有者是否匹配，如果之前没有设置所有者，则设置会话的所有者。
     *
     * UnknownSessionException不应该被抛出给客户端。它仅在内部用于处理来自其他机器的可能的本地会话
     *
     * @param sessionId
     * @param owner
     */
    public void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException, KeeperException.UnknownSessionException;

    /**
     * 严格检查给定的会话是否是全局会话
     *
     * @param sessionId     sessionId
     * @param owner         session归属，通常为{@link Request#owner}
     * @throws KeeperException.SessionExpiredException
     * @throws KeeperException.SessionMovedException
     */
    public void checkGlobalSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException;

    /**
     * 给sessionId设置owner
     *
     * @param id
     * @param owner
     * @throws SessionExpiredException
     */
    void setOwner(long id, Object owner) throws SessionExpiredException;

    /**
     * 会话信息的文本转储，适合调试。
     *
     * @param pwriter the output writer
     */
    void dumpSessions(PrintWriter pwriter);

    /**
     * 返回时间到届时到期的会话id的映射。
     */
    Map<Long, Set<Long>> getSessionExpiryMap();

    /**
     * 关闭SessionTracker服务
     */
    void shutdown();
}

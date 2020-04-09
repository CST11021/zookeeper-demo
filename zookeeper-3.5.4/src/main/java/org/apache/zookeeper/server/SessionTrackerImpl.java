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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这是一个功能齐全的SessionTracker。
 * 它按时间间隔分组跟踪会话。
 * 它总是将时间间隔四舍五入以提供一种宽限期。
 * 因此，会话是由在给定间隔内到期的会话组成的批过期的。
 *
 * SessionTrackerImpl作为一个单独的线程专门处理过期session，
 */
public class SessionTrackerImpl extends ZooKeeperCriticalThread implements SessionTracker {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTrackerImpl.class);

    /** 用于控制run方法的执行状态 */
    volatile boolean running = true;
    /** 表示下一个sessionId的值，一旦有新的连接（session）产生，就会nextSessionId++ */
    private final AtomicLong nextSessionId = new AtomicLong();
    /** 保存会话id及回应的会话过期时间，Map<sessionId, timeout>, 该数据结构和ZooKeeper内存数据库相连通，会被定期持久化到快照文件中去。 */
    private final ConcurrentMap<Long, Integer> sessionsWithTimeout;
    /** 保存sessionId及对应的session实例，{@link #addSession(long, int)} */
    protected final ConcurrentHashMap<Long, SessionImpl> sessionsById = new ConcurrentHashMap<Long, SessionImpl>();

    /** 会话超时清理器 */
    private final SessionExpirer expirer;
    /** 将会话按照各自的过期时间（优化为心跳时间的整数倍）分桶存放，可快速用于会话的超时校验 */
    private final ExpiryQueue<SessionImpl> sessionExpiryQueue;


    /**
     *
     * @param expirer
     * @param sessionsWithTimeout
     * @param tickTime              对应{@link ExpiryQueue#expirationInterval}
     * @param serverId
     * @param listener
     */
    public SessionTrackerImpl(SessionExpirer expirer, ConcurrentMap<Long, Integer> sessionsWithTimeout, int tickTime, long serverId, ZooKeeperServerListener listener) {
        super("SessionTracker", listener);
        this.expirer = expirer;
        this.sessionExpiryQueue = new ExpiryQueue<SessionImpl>(tickTime);
        this.sessionsWithTimeout = sessionsWithTimeout;
        this.nextSessionId.set(initializeNextSession(serverId));
        for (Entry<Long, Integer> e : sessionsWithTimeout.entrySet()) {
            addSession(e.getKey(), e.getValue());
        }

        EphemeralType.validateServerId(serverId);
    }


    /**
     * 生成初始sessionId，高阶字节是serverId，接下来的5个字节来自时间戳，低阶的2个字节是0。
     * 产生的值会存入nextSessionId属性，以后一旦有新的连接（session）产生，就会nextSessionId++
     *
     * @param id
     * @return
     */
    public static long initializeNextSession(long id) {
        long nextSid;
        nextSid = (Time.currentElapsedTime() << 24) >>> 8;
        nextSid = nextSid | (id << 56);
        if (nextSid == EphemeralType.CONTAINER_EPHEMERAL_OWNER) {
            ++nextSid;  // this is an unlikely edge case, but check it just in case
        }
        return nextSid;
    }

    /**
     * 循环从session过期队列中获取过期的session，并清除：
     * 当SessionTracker的会话超时检查线程整理出一些已经过期的会话后，那么就要开始进行会话清理了。会话清理的步骤大致可以分为以下七步。
     *
     * 1、标记会话状态为“已关闭”
     *  为了保证在清理期间不再处理来自该客户端的新请求，SessionTracker会首先将该会话的isClosing属性标记为true。
     *
     * 2、发起“会话关闭”请求
     *  为了使该会话的关闭操作在整个服务端集群中都生效，ZooKeeper使用了提交“会话关闭”请求的方式，并立即交付给PrepRequestProcessor处理器进行处理。
     *
     * 3、收集需要清理的临时节点
     *  在ZooKeeper的内存数据库中，为每个会话都单独保存了一份由该会话维护的所有临时节点集合，因此在会话清理阶段，只需要根据当前即将关闭的会话的sessionID从内存数据库中获取到这份临时节点列表即可。
     *
     * 实际上，有如下细节需要处理：在ZooKeeper处理会话关闭请求之前，正好有以下请求到达了服务端并正在处理中：
     *
     * 节点删除请求，删除的目标节点正好是上述临时节点中的一个。
     * 临时节点创建请求，创建的目标节点正好是上述临时节点中的一个。
     * 假定我们当前获取的临时节点列表是ephemerals，那么针对第一类请求，我们需要将所有这些请求对应的数据节点路径从ephemerals中移除，以避免重复删除。针对第二类，我们需要将所有这些请求对应的数据节点路径添加到ephemerals中去，以删除这些即将会被创建但是尚未保存到内存数据库中去的临时节点。
     *
     * 4、添加“节点删除”事务变更
     * 完成该会话相关的临时节点收集后，ZooKeeper会逐个将这些临时节点转换成“节点删除”请求，并放入事务变更队列outstandingChanges中去。
     *
     * 5、删除临时节点
     * FinalRequestProcessor处理器会触发内存数据库，删除该会话对应的所有临时节点。
     *
     * 6、移除会话
     * 完成节点删除后，需要将会话从SessionTracker中移除。主要就是从上面提到的三个数据结构(sessionById、sessionsWithTimeout和sessionSets)中将该会话移除掉。
     *
     * 7、关闭NIOServerCnxn
     * 最后，从NIOServerCnxnFactory找到该会话对应的NIOServerCnxn，将其关闭。
     */
    @Override
    public void run() {
        try {
            while (running) {
                long waitTime = sessionExpiryQueue.getWaitTime();
                if (waitTime > 0) {
                    Thread.sleep(waitTime);
                    continue;
                }

                for (SessionImpl s : sessionExpiryQueue.poll()) {
                    setSessionClosing(s.sessionId);
                    expirer.expire(s);
                }
            }
        } catch (InterruptedException e) {
            handleException(this.getName(), e);
        }
        LOG.info("SessionTrackerImpl exited loop!");
    }

    /**
     * dump session信息
     *
     * @param pwriter the output writer
     */
    public void dumpSessions(PrintWriter pwriter) {
        pwriter.print("Session ");
        sessionExpiryQueue.dump(pwriter);
    }

    /**
     * 返回从时间到当时到期的会话的会话id的映射。
     */
    synchronized public Map<Long, Set<Long>> getSessionExpiryMap() {
        // Convert time -> sessions map to time -> session IDs map
        Map<Long, Set<SessionImpl>> expiryMap = sessionExpiryQueue.getExpiryMap();

        Map<Long, Set<Long>> sessionExpiryMap = new TreeMap<Long, Set<Long>>();
        for (Entry<Long, Set<SessionImpl>> e : expiryMap.entrySet()) {
            Set<Long> ids = new HashSet<Long>();
            sessionExpiryMap.put(e.getKey(), ids);
            for (SessionImpl s : e.getValue()) {
                ids.add(s.sessionId);
            }
        }
        return sessionExpiryMap;
    }



    /**
     * 给session设置过期时间
     *
     * @param s
     * @param timeout
     */
    private void updateSessionExpiry(SessionImpl s, int timeout) {
        logTraceTouchSession(s.sessionId, timeout, "");
        sessionExpiryQueue.update(s, timeout);
    }

    private void logTraceTouchInvalidSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "invalid ");
    }
    private void logTraceTouchClosingSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "closing ");
    }
    private void logTraceTouchSession(long sessionId, int timeout, String sessionStatus) {
        if (!LOG.isTraceEnabled())
            return;

        String msg = MessageFormat.format("SessionTrackerImpl --- Touch {0}session: 0x{1} with timeout {2}",
                sessionStatus, Long.toHexString(sessionId), Integer.toString(timeout));

        ZooTrace.logTraceMessage(LOG, ZooTrace.CLIENT_PING_TRACE_MASK, msg);
    }


    /**
     * 根据sessoinId获取session超时时间
     *
     * @param sessionId
     * @return
     */
    public int getSessionTimeout(long sessionId) {
        return sessionsWithTimeout.get(sessionId);
    }

    /**
     * 将session设置为关闭状态
     *
     * @param sessionId
     */
    synchronized public void setSessionClosing(long sessionId) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Session closing: 0x" + Long.toHexString(sessionId));
        }
        SessionImpl s = sessionsById.get(sessionId);
        if (s == null) {
            return;
        }
        s.isClosing = true;
    }

    /**
     * 移除session
     *
     * @param sessionId
     */
    synchronized public void removeSession(long sessionId) {
        LOG.debug("Removing session 0x" + Long.toHexString(sessionId));
        SessionImpl s = sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "SessionTrackerImpl --- Removing session 0x" + Long.toHexString(sessionId));
        }
        if (s != null) {
            sessionExpiryQueue.remove(s);
        }
    }

    /**
     * 根据超时时间创建一个会话
     *
     * @param sessionTimeout
     * @return
     */
    public long createSession(int sessionTimeout) {
        long sessionId = nextSessionId.getAndIncrement();
        addSession(sessionId, sessionTimeout);
        return sessionId;
    }
    /**
     * 根据sessionId和timeout添加一个会话
     *
     * @param id sessionId      会话ID
     * @param sessionTimeout    会话超时时间
     * @return
     */
    public boolean addGlobalSession(long id, int sessionTimeout) {
        return addSession(id, sessionTimeout);
    }
    /**
     * 根据sessionId和timeout添加一个会话
     *
     * @param id sessionId      会话ID
     * @param sessionTimeout    会话超时时间
     * @return
     */
    public synchronized boolean addSession(long id, int sessionTimeout) {
        // 保存会话及对应的过期时间
        sessionsWithTimeout.put(id, sessionTimeout);

        boolean added = false;

        SessionImpl session = sessionsById.get(id);
        if (session == null) {
            session = new SessionImpl(id, sessionTimeout);
        }

        // findbugs2.0.3 complains about get after put. long term strategy would be use computeIfAbsent after JDK 1.8
        SessionImpl existedSession = sessionsById.putIfAbsent(id, session);

        if (existedSession != null) {
            session = existedSession;
        } else {
            added = true;
            LOG.debug("Adding session 0x" + Long.toHexString(id));
        }

        if (LOG.isTraceEnabled()) {
            String actionStr = added ? "Adding" : "Existing";
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "SessionTrackerImpl --- " + actionStr + " session 0x" + Long.toHexString(id) + " " + sessionTimeout);
        }

        updateSessionExpiry(session, sessionTimeout);
        return added;
    }



    /**
     * 判断sessionId是否合法，并且是否有过期
     *
     * @param sessionId
     * @param timeout
     * @return
     */
    synchronized public boolean touchSession(long sessionId, int timeout) {
        SessionImpl s = sessionsById.get(sessionId);

        if (s == null) {
            logTraceTouchInvalidSession(sessionId, timeout);
            return false;
        }

        if (s.isClosing()) {
            logTraceTouchClosingSession(sessionId, timeout);
            return false;
        }

        // 更新session设置过期时间
        updateSessionExpiry(s, timeout);
        return true;
    }
    /**
     * 判断会话是否存在
     *
     * @param sessionId
     * @return
     */
    public boolean isTrackingSession(long sessionId) {
        return sessionsById.containsKey(sessionId);
    }
    /**
     * 强校验会话是否存在，并且如果对应owner不存在，则设置owner
     *
     * @param sessionId
     * @param owner
     * @throws KeeperException.SessionExpiredException
     * @throws KeeperException.SessionMovedException
     * @throws KeeperException.UnknownSessionException
     */
    public synchronized void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException, KeeperException.UnknownSessionException {
        LOG.debug("Checking session 0x" + Long.toHexString(sessionId));
        SessionImpl session = sessionsById.get(sessionId);

        if (session == null) {
            throw new KeeperException.UnknownSessionException();
        }

        if (session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }

        if (session.owner == null) {
            session.owner = owner;
        } else if (session.owner != owner) {
            throw new KeeperException.SessionMovedException();
        }
    }
    /**
     * 严格检查给定的会话是否是全局会话
     *
     * @param sessionId     sessionId
     * @param owner         session归属，通常为{@link Request#owner}
     * @throws KeeperException.SessionExpiredException
     * @throws KeeperException.SessionMovedException
     */
    public void checkGlobalSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        try {
            checkSession(sessionId, owner);
        } catch (KeeperException.UnknownSessionException e) {
            throw new KeeperException.SessionExpiredException();
        }
    }



    /**
     * 设置session对应的owner
     *
     * @param id
     * @param owner
     * @throws SessionExpiredException
     */
    synchronized public void setOwner(long id, Object owner) throws SessionExpiredException {
        SessionImpl session = sessionsById.get(id);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        session.owner = owner;
    }


    public void shutdown() {
        LOG.info("Shutting down");

        running = false;
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                    "Shutdown SessionTrackerImpl!");
        }
    }


    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    public static class SessionImpl implements Session {
        SessionImpl(long sessionId, int timeout) {
            this.sessionId = sessionId;
            this.timeout = timeout;
            isClosing = false;
        }

        final long sessionId;
        final int timeout;
        boolean isClosing;

        Object owner;

        public long getSessionId() {
            return sessionId;
        }

        public int getTimeout() {
            return timeout;
        }

        public boolean isClosing() {
            return isClosing;
        }

        public String toString() {
            return "0x" + Long.toHexString(sessionId);
        }
    }

}

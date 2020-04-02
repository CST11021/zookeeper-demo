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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

/**
 * 参与仲裁的所有zookeeperserver的抽象基类。
 */
public abstract class QuorumZooKeeperServer extends ZooKeeperServer {

    /** 表示参与选举的zk实例 */
    public final QuorumPeer self;
    /** 可将本地session升级为全局session的session管理器 */
    protected UpgradeableSessionTracker upgradeableSessionTracker;

    protected QuorumZooKeeperServer(FileTxnSnapLog logFactory, int tickTime, int minSessionTimeout, int maxSessionTimeout, ZKDatabase zkDb, QuorumPeer self) {
        super(logFactory, tickTime, minSessionTimeout, maxSessionTimeout, zkDb);
        this.self = self;
    }

    @Override
    protected void startSessionTracker() {
        upgradeableSessionTracker = (UpgradeableSessionTracker) sessionTracker;
        upgradeableSessionTracker.start();
    }

    /**
     * 如果当前请求是事务请求，但是会话又是本地会话，此时需要升级会话为全局会话
     *
     * 如果这是一个本地会话的请求，并且它要创建一个临时节点，那么升级这个会话，并为leader返回一个新的会话请求。
     * 这是由请求处理器线程(要么是follower，要么是observer请求处理器)调用的，这对于learner来说是唯一的, 所以不会被两个线程并发调用。
     *
     * @param request
     * @return 如果需要升级会话，则返回一个创建session的请求
     * @throws IOException
     * @throws KeeperException
     */
    public Request checkUpgradeSession(Request request) throws IOException, KeeperException {
        // 如果不是create、create2、multi 或者 已经是全局会话了则不需要升级
        if ((request.type != OpCode.create && request.type != OpCode.create2 && request.type != OpCode.multi)
                || !upgradeableSessionTracker.isLocalSession(request.sessionId)) {
            return null;
        }

        if (OpCode.multi == request.type) {
            MultiTransactionRecord multiTransactionRecord = new MultiTransactionRecord();
            request.request.rewind();
            ByteBufferInputStream.byteBuffer2Record(request.request, multiTransactionRecord);
            request.request.rewind();
            boolean containsEphemeralCreate = false;
            for (Op op : multiTransactionRecord) {
                if (op.getType() == OpCode.create || op.getType() == OpCode.create2) {
                    CreateRequest createRequest = (CreateRequest) op.toRequestRecord();
                    CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
                    if (createMode.isEphemeral()) {
                        containsEphemeralCreate = true;
                        break;
                    }
                }
            }
            if (!containsEphemeralCreate) {
                return null;
            }
        } else {
            CreateRequest createRequest = new CreateRequest();
            request.request.rewind();
            ByteBufferInputStream.byteBuffer2Record(request.request, createRequest);
            request.request.rewind();
            CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());

            // 如果这个请求不是要创建一个临时节点，则返回null
            if (!createMode.isEphemeral()) {
                return null;
            }
        }

        // Uh oh.  We need to upgrade before we can proceed.
        if (!self.isLocalSessionsUpgradingEnabled()) {
            throw new KeeperException.EphemeralOnLocalSessionException();
        }

        // 返回一个创建session的请求
        return makeUpgradeRequest(request.sessionId);
    }

    /**
     * 将session升级为全局的会话
     *
     * @param sessionId
     */
    public void upgrade(long sessionId) {
        Request request = makeUpgradeRequest(sessionId);
        if (request != null) {
            LOG.info("Upgrading session 0x" + Long.toHexString(sessionId));
            // This must be a global request
            submitRequest(request);
        }
    }

    /**
     * 根据sessionId升级会话，返回一个创建session的请求
     *
     * @param sessionId
     * @return
     */
    private Request makeUpgradeRequest(long sessionId) {
        // Make sure to atomically check local session status, upgrade session, and make the session creation request.
        // This is to avoid another thread upgrading the session in parallel.
        synchronized (upgradeableSessionTracker) {
            // 判断是否为本地会话
            if (upgradeableSessionTracker.isLocalSession(sessionId)) {
                int timeout = upgradeableSessionTracker.upgradeSession(sessionId);
                ByteBuffer to = ByteBuffer.allocate(4);
                to.putInt(timeout);
                return new Request(null, sessionId, 0, OpCode.createSession, to, null);
            }
        }
        return null;
    }

    /**
     * 如果底层Zookeeper服务器支持本地会话，如果请求与本地会话相关联，则该方法将isLocalSession设置为true。
     *
     * @param si
     */
    @Override
    protected void setLocalSessionFlag(Request si) {
        // We need to set isLocalSession to tree for these type of request so that the request processor can process them correctly.
        switch (si.type) {
            case OpCode.createSession:
                if (self.areLocalSessionsEnabled()) {
                    // All new sessions local by default.
                    si.setLocalSession(true);
                }
                break;
            case OpCode.closeSession:
                String reqType = "global";
                if (upgradeableSessionTracker.isLocalSession(si.sessionId)) {
                    si.setLocalSession(true);
                    reqType = "local";
                }
                LOG.info("Submitting " + reqType + " closeSession request" + " for session 0x" + Long.toHexString(si.sessionId));
                break;
            default:
                break;
        }
    }

    /**
     * 根据不同的服务实例，会打印不同的zk配置信息
     *
     * @param pwriter
     */
    @Override
    public void dumpConf(PrintWriter pwriter) {
        super.dumpConf(pwriter);

        pwriter.print("initLimit=");
        pwriter.println(self.getInitLimit());
        pwriter.print("syncLimit=");
        pwriter.println(self.getSyncLimit());
        pwriter.print("electionAlg=");
        pwriter.println(self.getElectionType());
        pwriter.print("electionPort=");
        pwriter.println(self.getElectionAddress().getPort());
        pwriter.print("quorumPort=");
        pwriter.println(self.getQuorumAddress().getPort());
        pwriter.print("peerType=");
        pwriter.println(self.getLearnerType().ordinal());
        pwriter.println("membership: ");
        pwriter.print(new String(self.getQuorumVerifier().toString().getBytes()));
    }

    /**
     * 设置服务器的运行状态
     *
     * @param state new server state.
     */
    @Override
    protected void setState(State state) {
        this.state = state;
    }
}

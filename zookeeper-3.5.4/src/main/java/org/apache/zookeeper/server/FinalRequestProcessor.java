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

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.*;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.quorum.QuorumZooKeeperServer;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

/**
 * 用于修改ZK内存中的数据结构并触发watcher。
 *
 * 这个请求处理程序实际应用于与请求相关的任何事务，并为任何查询提供服务，它总是位于RequestProcessor链的末尾(因此得名)，所以它没有nextProcessor成员，
 * 这个RequestProcessor依赖于ZooKeeperServer来填充ZooKeeperServer的outstandingRequests成员。
 */
public class FinalRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FinalRequestProcessor.class);

    ZooKeeperServer zks;

    public FinalRequestProcessor(ZooKeeperServer zks) {
        this.zks = zks;
    }

    public void processRequest(Request request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        // request.addRQRec(">final");
        long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
        if (request.type == OpCode.ping) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logRequest(LOG, traceMask, 'E', request, "");
        }
        ProcessTxnResult rc = null;
        synchronized (zks.outstandingChanges) {
            // Need to process local session requests
            rc = zks.processTxn(request);

            // request.hdr is set for write requests, which are the only ones
            // that add to outstandingChanges.
            if (request.getHdr() != null) {
                TxnHeader hdr = request.getHdr();
                Record txn = request.getTxn();
                long zxid = hdr.getZxid();
                while (!zks.outstandingChanges.isEmpty() && zks.outstandingChanges.get(0).zxid <= zxid) {
                    ChangeRecord cr = zks.outstandingChanges.remove(0);
                    if (cr.zxid < zxid) {
                        LOG.warn("Zxid outstanding " + cr.zxid + " is less than current " + zxid);
                    }
                    if (zks.outstandingChangesForPath.get(cr.path) == cr) {
                        zks.outstandingChangesForPath.remove(cr.path);
                    }
                }
            }

            // do not add non quorum packets to the queue.
            if (request.isQuorum()) {
                zks.getZKDatabase().addCommittedProposal(request);
            }
        }

        // ZOOKEEPER-558:
        // In some cases the server does not close the connection (e.g., closeconn buffer
        // was not being queued — ZOOKEEPER-558) properly. This happens, for example,
        // when the client closes the connection. The server should still close the session, though.
        // Calling closeSession() after losing the cnxn, results in the client close session response being dropped.
        if (request.type == OpCode.closeSession && connClosedByClient(request)) {
            // We need to check if we can close the session id.
            // Sometimes the corresponding ServerCnxnFactory could be null because
            // we are just playing diffs from the leader.
            if (closeSession(zks.serverCnxnFactory, request.sessionId) || closeSession(zks.secureServerCnxnFactory, request.sessionId)) {
                return;
            }
        }

        if (request.cnxn == null) {
            return;
        }
        ServerCnxn cnxn = request.cnxn;

        String lastOp = "NA";
        zks.decInProcess();
        Code err = Code.OK;
        Record rsp = null;
        try {
            if (request.getHdr() != null && request.getHdr().getType() == OpCode.error) {
                /*
                 * When local session upgrading is disabled, leader will
                 * reject the ephemeral node creation due to session expire.
                 * However, if this is the follower that issue the request,
                 * it will have the correct error code, so we should use that
                 * and report to user
                 */
                if (request.getException() != null) {
                    throw request.getException();
                } else {
                    throw KeeperException.create(KeeperException.Code.get(((ErrorTxn) request.getTxn()).getErr()));
                }
            }

            KeeperException ke = request.getException();
            if (ke != null && request.type != OpCode.multi) {
                throw ke;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("{}", request);
            }
            switch (request.type) {
                case OpCode.ping: {
                    zks.serverStats().updateLatency(request.createTime);

                    lastOp = "PING";
                    cnxn.updateStatsForResponse(request.cxid, request.zxid, lastOp, request.createTime, Time.currentElapsedTime());
                    cnxn.sendResponse(new ReplyHeader(-2, zks.getZKDatabase().getDataTreeLastProcessedZxid(), 0), null, "response");
                    return;
                }
                case OpCode.createSession: {
                    zks.serverStats().updateLatency(request.createTime);
                    lastOp = "SESS";
                    cnxn.updateStatsForResponse(request.cxid, request.zxid, lastOp, request.createTime, Time.currentElapsedTime());
                    zks.finishSessionInit(request.cnxn, true);
                    return;
                }
                case OpCode.multi: {
                    lastOp = "MULT";
                    rsp = new MultiResponse();

                    for (ProcessTxnResult subTxnResult : rc.multiResult) {
                        OpResult subResult;
                        switch (subTxnResult.type) {
                            case OpCode.check:
                                subResult = new CheckResult();
                                break;
                            case OpCode.create:
                                subResult = new CreateResult(subTxnResult.path);
                                break;
                            case OpCode.create2:
                            case OpCode.createTTL:
                            case OpCode.createContainer:
                                subResult = new CreateResult(subTxnResult.path, subTxnResult.stat);
                                break;
                            case OpCode.delete:
                            case OpCode.deleteContainer:
                                subResult = new DeleteResult();
                                break;
                            case OpCode.setData:
                                subResult = new SetDataResult(subTxnResult.stat);
                                break;
                            case OpCode.error:
                                subResult = new ErrorResult(subTxnResult.err);
                                break;
                            default:
                                throw new IOException("Invalid type of op");
                        }

                        ((MultiResponse) rsp).add(subResult);
                    }

                    break;
                }
                case OpCode.create: {
                    lastOp = "CREA";
                    rsp = new CreateResponse(rc.path);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.create2:
                case OpCode.createTTL:
                case OpCode.createContainer: {
                    lastOp = "CREA";
                    rsp = new Create2Response(rc.path, rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.delete:
                case OpCode.deleteContainer: {
                    lastOp = "DELE";
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.setData: {
                    lastOp = "SETD";
                    rsp = new SetDataResponse(rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.reconfig: {
                    lastOp = "RECO";
                    rsp = new GetDataResponse(((QuorumZooKeeperServer) zks).self.getQuorumVerifier().toString().getBytes(), rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.setACL: {
                    lastOp = "SETA";
                    rsp = new SetACLResponse(rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.closeSession: {
                    lastOp = "CLOS";
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.sync: {
                    lastOp = "SYNC";
                    SyncRequest syncRequest = new SyncRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            syncRequest);
                    rsp = new SyncResponse(syncRequest.getPath());
                    break;
                }
                case OpCode.check: {
                    lastOp = "CHEC";
                    rsp = new SetDataResponse(rc.stat);
                    err = Code.get(rc.err);
                    break;
                }
                case OpCode.exists: {
                    lastOp = "EXIS";
                    // TODO we need to figure out the security requirement for this!
                    ExistsRequest existsRequest = new ExistsRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            existsRequest);
                    String path = existsRequest.getPath();
                    if (path.indexOf('\0') != -1) {
                        throw new KeeperException.BadArgumentsException();
                    }
                    Stat stat = zks.getZKDatabase().statNode(path, existsRequest
                            .getWatch() ? cnxn : null);
                    rsp = new ExistsResponse(stat);
                    break;
                }
                case OpCode.getData: {
                    lastOp = "GETD";
                    GetDataRequest getDataRequest = new GetDataRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            getDataRequest);
                    DataNode n = zks.getZKDatabase().getNode(getDataRequest.getPath());
                    if (n == null) {
                        throw new KeeperException.NoNodeException();
                    }
                    PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                            ZooDefs.Perms.READ,
                            request.authInfo);
                    Stat stat = new Stat();
                    byte b[] = zks.getZKDatabase().getData(getDataRequest.getPath(), stat,
                            getDataRequest.getWatch() ? cnxn : null);
                    rsp = new GetDataResponse(b, stat);
                    break;
                }
                case OpCode.setWatches: {
                    lastOp = "SETW";
                    SetWatches setWatches = new SetWatches();
                    // XXX We really should NOT need this!!!!
                    request.request.rewind();
                    ByteBufferInputStream.byteBuffer2Record(request.request, setWatches);
                    long relativeZxid = setWatches.getRelativeZxid();
                    zks.getZKDatabase().setWatches(relativeZxid,
                            setWatches.getDataWatches(),
                            setWatches.getExistWatches(),
                            setWatches.getChildWatches(), cnxn);
                    break;
                }
                case OpCode.getACL: {
                    lastOp = "GETA";
                    GetACLRequest getACLRequest = new GetACLRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            getACLRequest);
                    Stat stat = new Stat();
                    List<ACL> acl =
                            zks.getZKDatabase().getACL(getACLRequest.getPath(), stat);
                    rsp = new GetACLResponse(acl, stat);
                    break;
                }
                case OpCode.getChildren: {
                    lastOp = "GETC";
                    GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            getChildrenRequest);
                    DataNode n = zks.getZKDatabase().getNode(getChildrenRequest.getPath());
                    if (n == null) {
                        throw new KeeperException.NoNodeException();
                    }
                    PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                            ZooDefs.Perms.READ,
                            request.authInfo);
                    List<String> children = zks.getZKDatabase().getChildren(
                            getChildrenRequest.getPath(), null, getChildrenRequest
                                    .getWatch() ? cnxn : null);
                    rsp = new GetChildrenResponse(children);
                    break;
                }
                case OpCode.getChildren2: {
                    lastOp = "GETC";
                    GetChildren2Request getChildren2Request = new GetChildren2Request();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            getChildren2Request);
                    Stat stat = new Stat();
                    DataNode n = zks.getZKDatabase().getNode(getChildren2Request.getPath());
                    if (n == null) {
                        throw new KeeperException.NoNodeException();
                    }
                    PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().aclForNode(n),
                            ZooDefs.Perms.READ,
                            request.authInfo);
                    List<String> children = zks.getZKDatabase().getChildren(
                            getChildren2Request.getPath(), stat, getChildren2Request
                                    .getWatch() ? cnxn : null);
                    rsp = new GetChildren2Response(children, stat);
                    break;
                }
                case OpCode.checkWatches: {
                    lastOp = "CHKW";
                    CheckWatchesRequest checkWatches = new CheckWatchesRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            checkWatches);
                    WatcherType type = WatcherType.fromInt(checkWatches.getType());
                    boolean containsWatcher = zks.getZKDatabase().containsWatcher(
                            checkWatches.getPath(), type, cnxn);
                    if (!containsWatcher) {
                        String msg = String.format(Locale.ENGLISH, "%s (type: %s)",
                                checkWatches.getPath(), type);
                        throw new KeeperException.NoWatcherException(msg);
                    }
                    break;
                }
                case OpCode.removeWatches: {
                    lastOp = "REMW";
                    RemoveWatchesRequest removeWatches = new RemoveWatchesRequest();
                    ByteBufferInputStream.byteBuffer2Record(request.request,
                            removeWatches);
                    WatcherType type = WatcherType.fromInt(removeWatches.getType());
                    boolean removed = zks.getZKDatabase().removeWatch(
                            removeWatches.getPath(), type, cnxn);
                    if (!removed) {
                        String msg = String.format(Locale.ENGLISH, "%s (type: %s)",
                                removeWatches.getPath(), type);
                        throw new KeeperException.NoWatcherException(msg);
                    }
                    break;
                }
            }
        } catch (SessionMovedException e) {
            // session moved is a connection level error, we need to tear
            // down the connection otw ZOOKEEPER-710 might happen
            // ie client on slow follower starts to renew session, fails
            // before this completes, then tries the fast follower (leader)
            // and is successful, however the initial renew is then
            // successfully fwd/processed by the leader and as a result
            // the client and leader disagree on where the client is most
            // recently attached (and therefore invalid SESSION MOVED generated)
            cnxn.sendCloseSession();
            return;
        } catch (KeeperException e) {
            err = e.code();
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            err = Code.MARSHALLINGERROR;
        }

        long lastZxid = zks.getZKDatabase().getDataTreeLastProcessedZxid();
        ReplyHeader hdr = new ReplyHeader(request.cxid, lastZxid, err.intValue());

        zks.serverStats().updateLatency(request.createTime);
        cnxn.updateStatsForResponse(request.cxid, lastZxid, lastOp, request.createTime, Time.currentElapsedTime());

        try {
            cnxn.sendResponse(hdr, rsp, "response");
            if (request.type == OpCode.closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG", e);
        }
    }

    private boolean closeSession(ServerCnxnFactory serverCnxnFactory, long sessionId) {
        if (serverCnxnFactory == null) {
            return false;
        }
        return serverCnxnFactory.closeSession(sessionId);
    }

    private boolean connClosedByClient(Request request) {
        return request.cnxn == null;
    }

    public void shutdown() {
        // we are the final link in the chain
        LOG.info("shutdown of request processor complete");
    }

}

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
 * uuuuu
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "/RequuuAS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.txn.TxnHeader;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This is the structure that represents a request moving through a chain of
 * RequestProcessors. There are various pieces of information that is tacked
 * onto the request as it is processed.
 */
public class Request {

    /**
     * 请求时携带的sessionId，客户端连接zk的session
     */
    public final long sessionId;

    // xid用于记录客户端请求发起的先后序号，用来确保单个客户端请求的响应顺序
    public final int cxid;

    /** 表示请求的类型，对应{@link org.apache.zookeeper.ZooDefs.OpCode}，除了“会话创建”请求，其他所有客户端请求中都会带上请求头 */
    public final int type;

    public final ByteBuffer request;

    public final ServerCnxn cnxn;

    private TxnHeader hdr;

    private Record txn;

    public long zxid = -1;

    public final List<Id> authInfo;

    /** 表示请求的创建时间 */
    public final long createTime = Time.currentElapsedTime();

    private Object owner;

    private KeeperException e;

    public QuorumVerifier qv = null;

    /**
     * If this is a create or close request for a local-only session.
     */
    private boolean isLocalSession = false;

    /** 当关闭处理器之后，会将requestOfDeath的引用指向对应的请求对象 */
    public final static Request requestOfDeath = new Request(null, 0, 0, 0, null, null);



    public Request(ServerCnxn cnxn, long sessionId, int xid, int type, ByteBuffer bb, List<Id> authInfo) {
        this.cnxn = cnxn;
        this.sessionId = sessionId;
        this.cxid = xid;
        this.type = type;
        this.request = bb;
        this.authInfo = authInfo;
    }
    public Request(long sessionId, int xid, int type, TxnHeader hdr, Record txn, long zxid) {
        this.sessionId = sessionId;
        this.cxid = xid;
        this.type = type;
        this.hdr = hdr;
        this.txn = txn;
        this.zxid = zxid;
        this.request = null;
        this.cnxn = null;
        this.authInfo = null;
    }



    /**
     * is the packet type a valid packet in zookeeper
     *
     * @param type
     *                the type of the packet
     * @return true if a valid packet, false if not
     */
    static boolean isValid(int type) {
        // make sure this is always synchronized with Zoodefs!!
        switch (type) {
            case OpCode.notification:
                return false;
            case OpCode.check:
            case OpCode.closeSession:
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
            case OpCode.createSession:
            case OpCode.delete:
            case OpCode.deleteContainer:
            case OpCode.exists:
            case OpCode.getACL:
            case OpCode.getChildren:
            case OpCode.getChildren2:
            case OpCode.getData:
            case OpCode.multi:
            case OpCode.ping:
            case OpCode.reconfig:
            case OpCode.setACL:
            case OpCode.setData:
            case OpCode.setWatches:
            case OpCode.sync:
            case OpCode.checkWatches:
            case OpCode.removeWatches:
                return true;
            default:
                return false;
        }
    }

    public boolean isQuorum() {
        switch (this.type) {
            case OpCode.exists:
            case OpCode.getACL:
            case OpCode.getChildren:
            case OpCode.getChildren2:
            case OpCode.getData:
                return false;
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
            case OpCode.error:
            case OpCode.delete:
            case OpCode.deleteContainer:
            case OpCode.setACL:
            case OpCode.setData:
            case OpCode.check:
            case OpCode.multi:
            case OpCode.reconfig:
                return true;
            case OpCode.closeSession:
            case OpCode.createSession:
                return !this.isLocalSession;
            default:
                return false;
        }
    }

    static String op2String(int op) {
        switch (op) {
            case OpCode.notification:
                return "notification";
            case OpCode.create:
                return "create";
            case OpCode.create2:
                return "create2";
            case OpCode.createTTL:
                return "createTtl";
            case OpCode.createContainer:
                return "createContainer";
            case OpCode.setWatches:
                return "setWatches";
            case OpCode.delete:
                return "delete";
            case OpCode.deleteContainer:
                return "deleteContainer";
            case OpCode.exists:
                return "exists";
            case OpCode.getData:
                return "getData";
            case OpCode.check:
                return "check";
            case OpCode.multi:
                return "multi";
            case OpCode.setData:
                return "setData";
            case OpCode.sync:
                return "sync:";
            case OpCode.getACL:
                return "getACL";
            case OpCode.setACL:
                return "setACL";
            case OpCode.getChildren:
                return "getChildren";
            case OpCode.getChildren2:
                return "getChildren2";
            case OpCode.ping:
                return "ping";
            case OpCode.createSession:
                return "createSession";
            case OpCode.closeSession:
                return "closeSession";
            case OpCode.error:
                return "error";
            case OpCode.reconfig:
                return "reconfig";
            case OpCode.checkWatches:
                return "checkWatches";
            case OpCode.removeWatches:
                return "removeWatches";
            default:
                return "unknown " + op;
        }
    }



    // getter and setter ...

    public boolean isLocalSession() {
        return isLocalSession;
    }
    public void setLocalSession(boolean isLocalSession) {
        this.isLocalSession = isLocalSession;
    }
    public Object getOwner() {
        return owner;
    }
    public void setOwner(Object owner) {
        this.owner = owner;
    }
    public TxnHeader getHdr() {
        return hdr;
    }
    public void setHdr(TxnHeader hdr) {
        this.hdr = hdr;
    }
    public Record getTxn() {
        return txn;
    }
    public void setTxn(Record txn) {
        this.txn = txn;
    }
    public void setException(KeeperException e) {
        this.e = e;
    }
    public KeeperException getException() {
        return e;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("sessionid:0x").append(Long.toHexString(sessionId))
                .append(" type:").append(op2String(type))
                .append(" cxid:0x").append(Long.toHexString(cxid))
                .append(" zxid:0x").append(Long.toHexString(hdr == null ?
                -2 : hdr.getZxid()))
                .append(" txntype:").append(hdr == null ?
                "unknown" : "" + hdr.getType());

        // best effort to print the path assoc with this request
        String path = "n/a";
        if (type != OpCode.createSession
                && type != OpCode.setWatches
                && type != OpCode.closeSession
                && request != null
                && request.remaining() >= 4) {
            try {
                // make sure we don't mess with request itself
                ByteBuffer rbuf = request.asReadOnlyBuffer();
                rbuf.clear();
                int pathLen = rbuf.getInt();
                // sanity check
                if (pathLen >= 0
                        && pathLen < 4096
                        && rbuf.remaining() >= pathLen) {
                    byte b[] = new byte[pathLen];
                    rbuf.get(b);
                    path = new String(b);
                }
            } catch (Exception e) {
                // ignore - can't find the path, will output "n/a" instead
            }
        }
        sb.append(" reqpath:").append(path);

        return sb.toString();
    }
}

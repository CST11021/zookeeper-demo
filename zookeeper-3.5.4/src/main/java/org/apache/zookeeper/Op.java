/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper;

import org.apache.jute.Record;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.EphemeralType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 表示多操作事务中的单个操作。
 * 每个操作可以是创建、更新或删除，也可以只是版本检查。
 * 每个Op的子类代表每个详细的类型，但是通常不应该被引用，除非通过提供的工厂方法。
 *
 * @see ZooKeeper#create(String, byte[], java.util.List, CreateMode)
 * @see ZooKeeper#create(String, byte[], java.util.List, CreateMode, org.apache.zookeeper.AsyncCallback.StringCallback, Object)
 * @see ZooKeeper#delete(String, int)
 * @see ZooKeeper#setData(String, byte[], int)
 */
public abstract class Op {

    /** 对应{@link ZooDefs.OpCode} */
    private int type;

    /** 表示本次操作的节点 */
    private String path;

    // prevent untyped construction
    private Op(int type, String path) {
        this.type = type;
        this.path = path;
    }

    /**
     * Constructs a create operation.  Arguments are as for the ZooKeeper method of the same name.
     * @see ZooKeeper#create(String, byte[], java.util.List, CreateMode)
     * @see CreateMode#fromFlag(int)
     *
     * @param path
     *                the path for the node
     * @param data
     *                the initial data for the node
     * @param acl
     *                the acl for the node
     * @param flags
     *                specifying whether the node to be created is ephemeral
     *                and/or sequential but using the integer encoding.
     */
    public static Op create(String path, byte[] data, List<ACL> acl, int flags) {
        return new Create(path, data, acl, flags);
    }
    /**
     * Constructs a create operation.  Arguments are as for the ZooKeeper method of the same name
     * but adding an optional ttl
     * @see ZooKeeper#create(String, byte[], java.util.List, CreateMode)
     * @see CreateMode#fromFlag(int)
     *
     * @param path
     *                the path for the node
     * @param data
     *                the initial data for the node
     * @param acl
     *                the acl for the node
     * @param flags
     *                specifying whether the node to be created is ephemeral
     *                and/or sequential but using the integer encoding.
     * @param ttl
     *                optional ttl or 0 (flags must imply a TTL creation mode)
     */
    public static Op create(String path, byte[] data, List<ACL> acl, int flags, long ttl) {
        CreateMode createMode = CreateMode.fromFlag(flags, CreateMode.PERSISTENT);
        if (createMode.isTTL()) {
            return new CreateTTL(path, data, acl, createMode, ttl);
        }
        return new Create(path, data, acl, flags);
    }
    /**
     * Constructs a create operation.  Arguments are as for the ZooKeeper method of the same name.
     * @see ZooKeeper#create(String, byte[], java.util.List, CreateMode)
     *
     * @param path
     *                the path for the node
     * @param data
     *                the initial data for the node
     * @param acl
     *                the acl for the node
     * @param createMode
     *                specifying whether the node to be created is ephemeral
     *                and/or sequential
     */
    public static Op create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        return new Create(path, data, acl, createMode);
    }
    /**
     * Constructs a create operation.  Arguments are as for the ZooKeeper method of the same name
     * but adding an optional ttl
     * @see ZooKeeper#create(String, byte[], java.util.List, CreateMode)
     *
     * @param path
     *                the path for the node
     * @param data
     *                the initial data for the node
     * @param acl
     *                the acl for the node
     * @param createMode
     *                specifying whether the node to be created is ephemeral
     *                and/or sequential
     * @param ttl
     *                optional ttl or 0 (createMode must imply a TTL)
     */
    public static Op create(String path, byte[] data, List<ACL> acl, CreateMode createMode, long ttl) {
        if (createMode.isTTL()) {
            return new CreateTTL(path, data, acl, createMode, ttl);
        }
        return new Create(path, data, acl, createMode);
    }

    /**
     * Constructs a delete operation.  Arguments are as for the ZooKeeper method of the same name.
     * @see ZooKeeper#delete(String, int)
     *
     * @param path
     *                the path of the node to be deleted.
     * @param version
     *                the expected node version.
     */
    public static Op delete(String path, int version) {
        return new Delete(path, version);
    }

    /**
     * Constructs an update operation.  Arguments are as for the ZooKeeper method of the same name.
     * @see ZooKeeper#setData(String, byte[], int)
     *
     * @param path
     *                the path of the node
     * @param data
     *                the data to set
     * @param version
     *                the expected matching version
     */
    public static Op setData(String path, byte[] data, int version) {
        return new SetData(path, data, version);
    }

    /**
     * Constructs an version check operation.  Arguments are as for the ZooKeeper.setData method except that
     * no data is provided since no update is intended.  The purpose for this is to allow read-modify-write
     * operations that apply to multiple znodes, but where some of the znodes are involved only in the read,
     * not the write.  A similar effect could be achieved by writing the same data back, but that leads to
     * way more version updates than are necessary and more writing in general.
     *
     * @param path
     *                the path of the node
     * @param version
     *                the expected matching version
     */
    public static Op check(String path, int version) {
        return new Check(path, version);
    }

    /**
     * Gets the integer type code for an Op.  This code should be as from ZooDefs.OpCode
     * @see ZooDefs.OpCode
     * @return  The type code.
     */
    public int getType() {
        return type;
    }

    /**
     * Gets the path for an Op.
     * @return  The path.
     */
    public String getPath() {
        return path;
    }

    /**
     * Encodes an op for wire transmission.
     * @return An appropriate Record structure.
     */
    public abstract Record toRequestRecord() ;
    
    /**
     * Reconstructs the transaction with the chroot prefix.
     * @return transaction with chroot.
     */
    abstract Op withChroot(String addRootPrefix);

    /**
     * Performs client path validations.
     * 
     * @throws IllegalArgumentException
     *             if an invalid path is specified
     * @throws KeeperException.BadArgumentsException
     *             if an invalid create mode flag is specified
     */
    void validate() throws KeeperException {
        PathUtils.validatePath(path);
    }


    // 以下这些内部类是公共的，但是通常不应该被引用。

    public static class Create extends Op {
        /** 表示节点数据 */
        protected byte[] data;
        /** 表示节点的权限 */
        protected List<ACL> acl;
        /** 对应{@link ZooDefs.OpCode} */
        protected int flags;

        private Create(String path, byte[] data, List<ACL> acl, int flags) {
            super(getOpcode(CreateMode.fromFlag(flags, CreateMode.PERSISTENT)), path);
            this.data = data;
            this.acl = acl;
            this.flags = flags;
        }

        private static int getOpcode(CreateMode createMode) {
            if (createMode.isTTL()) {
                return ZooDefs.OpCode.createTTL;
            }
            return createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create;
        }

        private Create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
            super(getOpcode(createMode), path);
            this.data = data;
            this.acl = acl;
            this.flags = createMode.toFlag();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Create)) return false;

            Create op = (Create) o;

            boolean aclEquals = true;
            Iterator<ACL> i = op.acl.iterator();
            for (ACL acl : op.acl) {
                boolean hasMoreData = i.hasNext();
                if (!hasMoreData) {
                    aclEquals = false;
                    break;
                }
                ACL otherAcl = i.next();
                if (!acl.equals(otherAcl)) {
                    aclEquals = false;
                    break;
                }
            }
            return !i.hasNext() && getType() == op.getType() && Arrays.equals(data, op.data) && flags == op.flags && aclEquals;
        }

        @Override
        public int hashCode() {
            return getType() + getPath().hashCode() + Arrays.hashCode(data);
        }

        @Override
        public Record toRequestRecord() {
            return new CreateRequest(getPath(), data, acl, flags);
        }

        /**
         * 使用带有隔离命名空间的节点路径创建节点
         *
         * @param path
         * @return
         */
        @Override
        Op withChroot(String path) {
            return new Create(path, data, acl, flags);
        }

        @Override
        void validate() throws KeeperException {
            CreateMode createMode = CreateMode.fromFlag(flags);
            PathUtils.validatePath(getPath(), createMode.isSequential());
            EphemeralType.validateTTL(createMode, -1);
        }
    }

    public static class CreateTTL extends Create {
        private final long ttl;

        private CreateTTL(String path, byte[] data, List<ACL> acl, int flags, long ttl) {
            super(path, data, acl, flags);
            this.ttl = ttl;
        }

        private CreateTTL(String path, byte[] data, List<ACL> acl, CreateMode createMode, long ttl) {
            super(path, data, acl, createMode);
            this.ttl = ttl;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && (o instanceof CreateTTL) && (ttl == ((CreateTTL)o).ttl);
        }

        @Override
        public int hashCode() {
            return super.hashCode() + (int)(ttl ^ (ttl >>> 32));
        }

        @Override
        public Record toRequestRecord() {
            return new CreateTTLRequest(getPath(), data, acl, flags, ttl);
        }

        @Override
        Op withChroot(String path) {
            return new CreateTTL(path, data, acl, flags, ttl);
        }

        @Override
        void validate() throws KeeperException {
            CreateMode createMode = CreateMode.fromFlag(flags);
            PathUtils.validatePath(getPath(), createMode.isSequential());
            EphemeralType.validateTTL(createMode, ttl);
        }
    }

    public static class Delete extends Op {
        private int version;

        private Delete(String path, int version) {
            super(ZooDefs.OpCode.delete, path);
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Delete)) return false;

            Delete op = (Delete) o;

            return getType() == op.getType() && version == op.version 
                   && getPath().equals(op.getPath());
        }

        @Override
        public int hashCode() {
            return getType() + getPath().hashCode() + version;
        }

        @Override
        public Record toRequestRecord() {
            return new DeleteRequest(getPath(), version);
        }

        @Override
        Op withChroot(String path) {
            return new Delete(path, version);
        }
    }

    public static class SetData extends Op {
        private byte[] data;
        private int version;

        private SetData(String path, byte[] data, int version) {
            super(ZooDefs.OpCode.setData, path);
            this.data = data;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SetData)) return false;

            SetData op = (SetData) o;

            return getType() == op.getType() && version == op.version 
                   && getPath().equals(op.getPath()) && Arrays.equals(data, op.data);
        }

        @Override
        public int hashCode() {
            return getType() + getPath().hashCode() + Arrays.hashCode(data) + version;
        }

        @Override
        public Record toRequestRecord() {
            return new SetDataRequest(getPath(), data, version);
        }

        @Override
        Op withChroot(String path) {
            return new SetData(path, data, version);
        }
    }

    public static class Check extends Op {
        private int version;

        private Check(String path, int version) {
            super(ZooDefs.OpCode.check, path);
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Check)) return false;

            Check op = (Check) o;

            return getType() == op.getType() && getPath().equals(op.getPath()) && version == op.version;
        }

        @Override
        public int hashCode() {
            return getType() + getPath().hashCode() + version;
        }

        @Override
        public Record toRequestRecord() {
            return new CheckVersionRequest(getPath(), version);
        }

        @Override
        Op withChroot(String path) {
            return new Check(path, version);
        }
    }

}

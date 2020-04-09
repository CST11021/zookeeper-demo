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

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * DataNode类是zookeeper中数据存储的最小单元。
 * 在{@link DataTree}中，所有的datanode存在一个concurrentHashMap中，对zk中所有的znode进行操作，其实底层就是对这个map进行操作。其中path是key，datanode是value。
 *
 * DataNode中存储的信息共有三类：
 * 数据内容data[]
 * acl列表
 * 节点状态stat
 *
 * 其中数据内容和节点状态就是在客户端上getData获取到的那些数据。同时，DataNode中还记录了节点的父节点和子节点列表，并提供了对子节点列表的操作。
 */
public class DataNode implements Record {

    /** 表示一个空集合 */
    private static final Set<String> EMPTY_SET = Collections.emptySet();
    /** 节点数据 */
    byte data[];
    /** 表示节点的权限值，通过{@link ReferenceCountedACLCache#convertLong(Long)}方法返回一个节点的权利列表 */
    Long acl;
    /** 表示节点保存到磁盘的状态 */
    public StatPersisted stat;
    /** 此节点的子节点列表，注意，该字符串列表不包含父路径——只包含路径的最后一部分，除反序列化(用于加速问题)外，此操作应同步 */
    private Set<String> children = null;



    DataNode() {
        // default constructor
    }
    public DataNode(byte data[], Long acl, StatPersisted stat) {
        this.data = data;
        this.acl = acl;
        this.stat = stat;
    }


    // 子节点相关函数

    /**
     * 添加一个子节点
     * 
     * @param child to be inserted
     * @return 如果该集合尚未包含指定的元素，则返回true
     */
    public synchronized boolean addChild(String child) {
        if (children == null) {
            // let's be conservative on the typical number of children
            children = new HashSet<String>(8);
        }
        return children.add(child);
    }
    /**
     * 移除子节点
     * 
     * @param child
     * @return true if this set contained the specified element
     */
    public synchronized boolean removeChild(String child) {
        if (children == null) {
            return false;
        }
        return children.remove(child);
    }
    /**
     * 设置子节点集合
     * 
     * @param children
     */
    public synchronized void setChildren(HashSet<String> children) {
        this.children = children;
    }
    /**
     * 获取子节点集合
     * 
     * @return the children of this datanode. If the datanode has no children, empty set is returned
     */
    public synchronized Set<String> getChildren() {
        if (children == null) {
            return EMPTY_SET;
        }

        return Collections.unmodifiableSet(children);
    }




    /**
     * 返回节点数据的字节大小
     *
     * @return
     */
    public synchronized long getApproximateDataSize() {
        if(null==data) return 0;
        return data.length;
    }

    /**
     * 拷贝当前节点的状态到入参to
     *
     * @param to
     */
    synchronized public void copyStat(Stat to) {
        to.setAversion(stat.getAversion());
        to.setCtime(stat.getCtime());
        to.setCzxid(stat.getCzxid());
        to.setMtime(stat.getMtime());
        to.setMzxid(stat.getMzxid());
        to.setPzxid(stat.getPzxid());
        to.setVersion(stat.getVersion());
        to.setEphemeralOwner(getClientEphemeralOwner(stat));
        to.setDataLength(data == null ? 0 : data.length);
        int numChildren = 0;
        if (this.children != null) {
            numChildren = children.size();
        }
        // when we do the Cversion we need to translate from the count of the creates
        // to the count of the changes (v3 semantics)
        // for every create there is a delete except for the children still present
        to.setCversion(stat.getCversion()*2 - numChildren);
        to.setNumChildren(numChildren);
    }

    /**
     * 如果znode是ephemeral类型节点，则这是znode所有者的sessionId，如果znode不是ephemeral节点，则该字段设置为零
     *
     * @param stat
     * @return
     */
    private static long getClientEphemeralOwner(StatPersisted stat) {
        EphemeralType ephemeralType = EphemeralType.get(stat.getEphemeralOwner());
        if (ephemeralType != EphemeralType.NORMAL) {
            return 0;
        }

        return stat.getEphemeralOwner();
    }

    // 序列化和反序列化

    synchronized public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord("node");
        data = archive.readBuffer("data");
        acl = archive.readLong("acl");
        stat = new StatPersisted();
        stat.deserialize(archive, "statpersisted");
        archive.endRecord("node");
    }
    synchronized public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, "node");
        archive.writeBuffer(data, "data");
        archive.writeLong(acl, "acl");
        stat.serialize(archive, "statpersisted");
        archive.endRecord(this, "node");
    }
}

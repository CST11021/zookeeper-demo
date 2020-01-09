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
package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 *  CreateMode value determines how the znode is created on ZooKeeper.
 */
@InterfaceAudience.Public
public enum CreateMode {
    
    /**
     * 持久节点：当客户端断开连接时，znode将不会被自动删除
     */
    PERSISTENT (0, false, false, false, false),
    /**
     * 持久有序：当客户端断开连接时，znode将不会被自动删除，并且节点的名称将在后面追加一个递增的数字。
     */
    PERSISTENT_SEQUENTIAL (2, false, true, false, false),
    /**
     * 临时节点：当客户端断开连接时，znode将被删除。
     */
    EPHEMERAL (1, true, false, false, false),
    /**
     * 临时有序的节点：当客户端断开连接时，znode将被删除，并且节点的名称将在后面追加一个递增的数字。
     */
    EPHEMERAL_SEQUENTIAL (3, true, true, false, false),
    /**
     * 容器节点：如果节点中最后一个子Znode被删除，将会触发删除该Znode。
     * 容器节点是一种特殊用途的节点，对诸如leader、lock等非常有用。
     *
     */
    CONTAINER (4, false, false, true, false),
    /**
     * 客户端断开连接后不会自动删除 Znode，如果该 Znode 没有子 Znode 且在给定 TTL 时间内无修改，该 Znode 将会被删除；TTL 单位是毫秒，必须大于0 且小于或等于 EphemeralType.MAX_TTL
     */
    PERSISTENT_WITH_TTL(5, false, false, false, true),
    /**
     * 客户端断开连接后不会自动删除 Znode，如果该 Znode 没有子 Znode 且在给定 TTL 时间内无修改，该 Znode 将会被删除；TTL 单位是毫秒，必须大于0 且小于或等于 EphemeralType.MAX_TTL
     * 与{@link #PERSISTENT_WITH_TTL} 的区别是，该类型是有序的
     */
    PERSISTENT_SEQUENTIAL_WITH_TTL(6, false, true, false, true);

    private static final Logger LOG = LoggerFactory.getLogger(CreateMode.class);

    /** 是否为临时节点 */
    private boolean ephemeral;
    /** 是否有序 */
    private boolean sequential;
    /** 是否为容器节点 */
    private final boolean isContainer;
    /** 表示枚举值 */
    private int flag;
    /** 键值系统有一个对应用配置很有帮助的特性，可以给每一个键或目录指定一个存活时限（TTL，即Time To Life）。TTL的单位是秒，当指定的秒数过去以后，如果相应的键或目录没有得到更新，就会被自动从Etcd记录中移除。 */
    private boolean isTTL;

    CreateMode(int flag, boolean ephemeral, boolean sequential, boolean isContainer, boolean isTTL) {
        this.flag = flag;
        this.ephemeral = ephemeral;
        this.sequential = sequential;
        this.isContainer = isContainer;
        this.isTTL = isTTL;
    }

    public boolean isEphemeral() { 
        return ephemeral;
    }

    public boolean isSequential() { 
        return sequential;
    }

    public boolean isContainer() {
        return isContainer;
    }

    public boolean isTTL() {
        return isTTL;
    }

    public int toFlag() {
        return flag;
    }

    /**
     * Map an integer value to a CreateMode value
     */
    static public CreateMode fromFlag(int flag) throws KeeperException {
        switch(flag) {
        case 0: return CreateMode.PERSISTENT;

        case 1: return CreateMode.EPHEMERAL;

        case 2: return CreateMode.PERSISTENT_SEQUENTIAL;

        case 3: return CreateMode.EPHEMERAL_SEQUENTIAL ;

        case 4: return CreateMode.CONTAINER;

        case 5: return CreateMode.PERSISTENT_WITH_TTL;

        case 6: return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

        default:
            String errMsg = "Received an invalid flag value: " + flag
                    + " to convert to a CreateMode";
            LOG.error(errMsg);
            throw new KeeperException.BadArgumentsException(errMsg);
        }
    }

    /**
     * Map an integer value to a CreateMode value
     */
    static public CreateMode fromFlag(int flag, CreateMode defaultMode) {
        switch(flag) {
            case 0:
                return CreateMode.PERSISTENT;

            case 1:
                return CreateMode.EPHEMERAL;

            case 2:
                return CreateMode.PERSISTENT_SEQUENTIAL;

            case 3:
                return CreateMode.EPHEMERAL_SEQUENTIAL;

            case 4:
                return CreateMode.CONTAINER;

            case 5:
                return CreateMode.PERSISTENT_WITH_TTL;

            case 6:
                return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

            default:
                return defaultMode;
        }
    }
}

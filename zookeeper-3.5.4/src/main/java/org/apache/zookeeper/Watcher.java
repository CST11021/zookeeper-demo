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

/**
 * This interface specifies the public interface an event handler class must
 * implement. A ZooKeeper client will get various events from the ZooKeeper
 * server it connects to. An application using such a client handles these
 * events by registering a callback object with the client. The callback object
 * is expected to be an instance of a class that implements Watcher interface.
 * 
 */
@InterfaceAudience.Public
public interface Watcher {

    /**
     * This interface defines the possible states an Event may represent
     */
    @InterfaceAudience.Public
    public interface Event {

        /**
         * Zookeeper可能在事件中的状态枚举
         */
        @InterfaceAudience.Public
        public enum KeeperState {
            /** 没有用，服务器将永远不会生成此状态 */
            @Deprecated
            Unknown (-1),

            /** 客户端处于断开连接状态——它没有连接到集成中的任何服务器。 */
            Disconnected (0),

            /** 没有用，服务器将永远不会生成此状态 */
            @Deprecated
            NoSyncConnected (1),

            /**
             * 客户端处于连接状态：它连接到集群中的一台服务器(在创建ZooKeeper客户端期间在主机连接参数中指定的服务器之一)。
             */
            SyncConnected (3),

            /**
             * 身份认证失败的状态
             */
            AuthFailed (4),

            /**
             * 客户端连接到只读服务器，接收到此状态后允许的唯一操作是读操作。此状态仅为只读客户端生成，因为读/写客户端不允许连接到r/o服务器。
             */
            ConnectedReadOnly (5),

            /**
              * SaslAuthenticated:用于通知客户端他们是SASL-authenticated，这样它们就可以使用sasl授权的权限执行Zookeeper操作。
              */
            SaslAuthenticated(6),

            /**
             * 服务集群已在此会话中过期。
             * ZooKeeper客户端连接(会话)不再有效。
             * 如果你要访问集成，你必须创建一个新的客户端连接(实例化一个新的ZooKeeper实例)。
             */
            Expired (-112);

            /** 枚举值 */
            private final int intValue;

            KeeperState(int intValue) {
                this.intValue = intValue;
            }

            public int getIntValue() {
                return intValue;
            }

            public static KeeperState fromInt(int intValue) {
                switch(intValue) {
                    case   -1: return KeeperState.Unknown;
                    case    0: return KeeperState.Disconnected;
                    case    1: return KeeperState.NoSyncConnected;
                    case    3: return KeeperState.SyncConnected;
                    case    4: return KeeperState.AuthFailed;
                    case    5: return KeeperState.ConnectedReadOnly;
                    case    6: return KeeperState.SaslAuthenticated;
                    case -112: return KeeperState.Expired;

                    default:
                        throw new RuntimeException("Invalid integer value for conversion to KeeperState");
                }
            }
        }

        /**
         * 枚举可能在zookeeper上发生的事件类型
         */
        @InterfaceAudience.Public
        public enum EventType {
            /** 没有任何节点，表示创建连接成功(客户端与服务器端创建连接成功后没有任何节点信息) */
            None (-1),
            /** 创建节点的时候触发 */
            NodeCreated (1),
            /** 删除节点的时候触发 */
            NodeDeleted (2),
            /** 节点数据变更的时候触发 */
            NodeDataChanged (3),
            /** 子节点变更的时候触发 */
            NodeChildrenChanged (4),
            /** 节点数据监听移除的时候触发 */
            DataWatchRemoved (5),
            /** 子节点监听移除的时候触发 */
            ChildWatchRemoved (6);

            /**
             * 通过网络发送的值的整数表示
             */
            private final int intValue;

            EventType(int intValue) {
                this.intValue = intValue;
            }

            public int getIntValue() {
                return intValue;
            }

            public static EventType fromInt(int intValue) {
                switch(intValue) {
                    case -1: return EventType.None;
                    case  1: return EventType.NodeCreated;
                    case  2: return EventType.NodeDeleted;
                    case  3: return EventType.NodeDataChanged;
                    case  4: return EventType.NodeChildrenChanged;
                    case  5: return EventType.DataWatchRemoved;
                    case  6: return EventType.ChildWatchRemoved;

                    default:
                        throw new RuntimeException("Invalid integer value for conversion to EventType");
                }
            }           
        }
    }

    /**
     * 表示Watcher监听的类型
     */
    @InterfaceAudience.Public
    public enum WatcherType {
        /** 表示监听的是子节点 */
        Children(1),
        /** 表示监听的数据 */
        Data(2),
        /** 表示监听所有事件类型 */
        Any(3);

        private final int intValue;

        private WatcherType(int intValue) {
            this.intValue = intValue;
        }

        public int getIntValue() {
            return intValue;
        }

        public static WatcherType fromInt(int intValue) {
            switch (intValue) {
            case 1:
                return WatcherType.Children;
            case 2:
                return WatcherType.Data;
            case 3:
                return WatcherType.Any;

            default:
                throw new RuntimeException(
                        "Invalid integer value for conversion to WatcherType");
            }
        }
    }

    abstract public void process(WatchedEvent event);
}

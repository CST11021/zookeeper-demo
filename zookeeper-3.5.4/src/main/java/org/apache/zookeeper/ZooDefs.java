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

package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.ArrayList;
import java.util.Collections;

@InterfaceAudience.Public
public class ZooDefs {

    final public static String CONFIG_NODE = "/zookeeper/config";

    /**
     * 表示客户端命令的操作类型，例如：创建、删除、获取等命令的类型
     */
    @InterfaceAudience.Public
    public interface OpCode {
        public final int notification = 0;

        /** 创建节点 */
        public final int create = 1;

        /** 删除节点 */
        public final int delete = 2;

        /** 退出 */
        public final int exists = 3;

        /** 获取节点数据 */
        public final int getData = 4;

        /** 设置节点数据 */
        public final int setData = 5;

        /** 获取节点权限 */
        public final int getACL = 6;

        /** 设置节点权限 */
        public final int setACL = 7;

        public final int getChildren = 8;

        public final int sync = 9;

        public final int ping = 11;

        public final int getChildren2 = 12;

        public final int check = 13;

        public final int multi = 14;

        public final int create2 = 15;

        public final int reconfig = 16;

        public final int checkWatches = 17;

        public final int removeWatches = 18;

        public final int createContainer = 19;

        public final int deleteContainer = 20;

        public final int createTTL = 21;

        /**
         * 表示给用户做认证（或者说授权）的操作
         */
        public final int auth = 100;

        public final int setWatches = 101;

        public final int sasl = 102;

        public final int createSession = -10;

        public final int closeSession = -11;

        public final int error = -1;
    }

    @InterfaceAudience.Public
    public interface Perms {
        /** 获取节点的数据和它的子节点 */
        int READ = 1 << 0;
        /** 设置节点的数据 */
        int WRITE = 1 << 1;
        /** 创建子节点 */
        int CREATE = 1 << 2;
        /** 删除子节点 （仅下一级节点） */
        int DELETE = 1 << 3;
        /** 设置 ACL 权限 */
        int ADMIN = 1 << 4;
        /** 拥有所有权限 */
        int ALL = READ | WRITE | CREATE | DELETE | ADMIN;
    }

    @InterfaceAudience.Public
    public interface Ids {
        /**
         * This Id represents anyone.
         */
        public final Id ANYONE_ID_UNSAFE = new Id("world", "anyone");

        /**
         * This Id is only usable to set ACLs. It will get substituted with the Id's the client authenticated with.
         */
        public final Id AUTH_IDS = new Id("auth", "");

        /**
         * 这是一个完全开放的ACL，没有权限限制
         */
        public final ArrayList<ACL> OPEN_ACL_UNSAFE = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID_UNSAFE)));

        /**
         * 为通过身份认证的id提供所有权限
         */
        public final ArrayList<ACL> CREATOR_ALL_ACL = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.ALL, AUTH_IDS)));

        /**
         * 所有的用户都有只读权限
         */
        public final ArrayList<ACL> READ_ACL_UNSAFE = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.READ, ANYONE_ID_UNSAFE)));
    }

    final public static String[] opNames = {"notification", "create",
            "delete", "exists", "getData", "setData", "getACL", "setACL",
            "getChildren", "getChildren2", "getMaxChildren", "setMaxChildren", "ping", "reconfig", "getConfig"};
}

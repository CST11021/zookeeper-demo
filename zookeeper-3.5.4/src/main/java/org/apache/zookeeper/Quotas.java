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

/**
 * Quotas其实就是为ZNode设置的节点个数和数据量大小的限制（只是在日志中会提醒，并不是真正限制）。
 *
 * limitnode和statnode的区别：一个是在设置quota时的限制，一个是真实的情况。
 *
 * 这里说明一点，所有成功设立了quota的节点都会在/zookeeper/quota下建立一个树形的数据结构，并且每个节点都会有两个孩子接点，即"zookeeper_limits"和"zookeeper_stats"。
 * 特别的是，前面这句话中成功设立是有条件的，如果发现有父节点或者兄弟孩子节点有quota，那么设置quota会失败。
 */
public class Quotas {

    /** the zookeeper nodes that acts as the management and status node **/
    public static final String procZookeeper = "/zookeeper";

    /** the zookeeper quota node that acts as the quota management node for zookeeper */
    public static final String quotaZookeeper = "/zookeeper/quota";

    /**
     * the limit node that has the limit of a subtree
     */
    public static final String limitNode = "zookeeper_limits";

    /**
     * the stat node that monitors the limit of a subtree.
     */
    public static final String statNode = "zookeeper_stats";

    /**
     * 创建limitNode节点的路径，返回：/zookeeper/quota + ${path} + zookeeper_limits
     *
     * @param path the actual path in zookeeper.
     * @return the limit quota path
     */
    public static String quotaPath(String path) {
        return quotaZookeeper + path + "/" + limitNode;
    }

    /**
     * 创建statNode节点的路径，返回：/zookeeper/quota + ${path} + zookeeper_stats
     *
     * @param path the actual path in zookeeper
     * @return the stat quota path
     */
    public static String statPath(String path) {
        return quotaZookeeper + path + "/" + statNode;
    }
}

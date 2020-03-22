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

package org.apache.zookeeper.server.persistence;

import org.apache.zookeeper.server.DataTree;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * 持久层的快照接口，实现这个接口来实现快照。
 */
public interface SnapShot {

    /**
     * 反序列化最后一个有效快照的数据树，并返回最后一个反序列化的zxid
     *
     * @param dt        要反序列化的数据树
     * @param sessions  要反序列化的会话
     * @return the last zxid that was deserialized from the snapshot
     * @throws IOException
     */
    long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException;

    /**
     * 将数据树和会话持久化到持久性存储中
     *
     * @param dt the datatree to be serialized
     * @param sessions
     * @throws IOException
     */
    void serialize(DataTree dt, Map<Long, Integer> sessions, File name) throws IOException;

    /**
     * 查找最近的快照文件
     *
     * @return the most recent snapshot file
     * @throws IOException
     */
    File findMostRecentSnapshot() throws IOException;

    /**
     * 立即从该快照释放资源
     *
     * @throws IOException
     */
    void close() throws IOException;
} 

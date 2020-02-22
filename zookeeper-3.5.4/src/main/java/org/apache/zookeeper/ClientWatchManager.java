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

import java.util.Set;

/**
 * 客户端Watcher管理器：用于管理注册在节点上的监听器
 */
public interface ClientWatchManager {

    /**
     * 根据事件的状态、类型和节点路径，来管理客户端对应的监听器，比如：当节点被移除的事件发生后，客户端需要移除对应的监听器
     * 
     * @param state     事件状态
     * @param type      事件类型
     * @param path      节点路径
     * @return 可以返回empty，但不能返回null
     */
    public Set<Watcher> materialize(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String path);

}

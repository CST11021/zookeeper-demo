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
 * 抽象各种对测试ZooKeeper有用的方法
 */
public interface Testable {

    /**
     * 通过调用该方法，使zk客户端（ZooKeeper实例）的行为就像会话过期一样，但是实际会话可能没有过期，该接口仅仅实现会话过期时，zk客户端需要执行的一些行为
     */
    void injectSessionExpiration();

}

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

package org.apache.zookeeper.client;

import org.apache.yetus.audience.InterfaceAudience;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * 客户端地址列表管理器
 * A set of hosts a ZooKeeper client should connect to.
 * 
 * Classes implementing this interface must guarantee the following:
 * 
 * * Every call to next() returns an InetSocketAddress. So the iterator never
 * ends.
 * 
 * * The size() of a HostProvider may never be zero.
 * 
 * A HostProvider must return resolved InetSocketAddress instances on next(),
 * but it's up to the HostProvider, when it wants to do the resolving.
 * 
 * Different HostProvider could be imagined:
 * 
 * * A HostProvider that loads the list of Hosts from an URL or from DNS 
 * * A HostProvider that re-resolves the InetSocketAddress after a timeout. 
 * * A HostProvider that prefers nearby hosts.
 */
@InterfaceAudience.Public
public interface HostProvider {

    /**
     * 表示连接zk服务器地址的个数，该返回值不可以为0，也就说说，至少有个zk服务
     *
     * @return
     */
    public int size();

    /**
     * 下一个要连接的主机，该方法必须返回一个合法的InetSocketAddress对象，也就是说，不能返回null或其他不合法的Inet SocketAddress
     * 
     * @param spinDelay 该时间表示所有主机都尝试一次的等待时间，单位毫秒
     *
     *                  假如客户端传入这样一个地址列表：”host1,host2,host3,host4,host5“。经过一轮随机打散后，可能的一种顺序变为了
     *                  ”host2,host4,host5,host3“，并且形成了下上图所示的循环队里。此外，HostProvider还会为该循环队列创建两个游标：
     *                  currentIndex和lastIndex。currentIndex表示循环队列中当前遍历到的那个元素位置，lastIndex表示当前正在使用的
     *                  服务器地址位置。初始化的时候，currentIndex和lastIndex的值都为-1。
     *
     *                  在每次尝试获取一个服务器地址的时候，都会首先将currentIndex游标向前移动1位，如果发现游标移动超过了整个地址列表
     *                  的长度，那么就重置为0，回到开始的位置重新开始，这样一来，就实现了循环队列。当然，对于那些服务器地址列表提供得比
     *                  较少的场景，StaticHostProvider中做了一个小技巧，就是如果发现当前游标提供比较少的场景，StaticHostProvider中
     *                  做了一个小技巧，就是如果发现当前游标的位置和上次已经使用过的地址位置一样，即当currentIndex和lastIndex游标值相
     *                  同时，就进行spinDelay毫秒时间的等待。
     *
     *
     */
    public InetSocketAddress next(long spinDelay);

    /**
     * 客户端成功连接zk时，会调用该方法通知HostProvider，HostProvider通过该通知重置其内部状态
     */
    public void onConnected();

    /**
     * 更新服务器列表。如果需要更改连接以实现负载平衡，则返回true，否则返回false。
     *
     * @param serverAddresses   新的zk服务器列表
     * @param currentHost       表示此客户端当前连接到的主机
     * @return
     */
    boolean updateServerList(Collection<InetSocketAddress> serverAddresses, InetSocketAddress currentHost);
}

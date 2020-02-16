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

package org.apache.zookeeper.client;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Most simple HostProvider, resolves only on instantiation.
 * 举个例子来说，假如客户端传入这样一个地址列表：”host1,host2,host3,host4,host5“。经过一轮随机打散后，可能的一种顺序变为了”host2,host4,host5,host3“，
 * 并且形成了下上图所示的循环队里。此外，HostProvider还会为该循环队列创建两个游标：currentIndex和lastIndex。currentIndex表示循环队列
 * 中当前遍历到的那个元素位置，lastIndex表示当前正在使用的服务器地址位置。初始化的时候，currentIndex和lastIndex的值都为-1。
 *
 * 在每次尝试获取一个服务器地址的时候，都会首先将currentIndex游标向前移动1位，如果发现游标移动超过了整个地址列表的长度，那么就重置为0，
 * 回到开始的位置重新开始，这样一来，就实现了循环队列。当然，对于那些服务器地址列表提供得比较少的场景，StaticHostProvider中做了一个小技巧，
 * 就是如果发现当前游标提供比较少的场景，StaticHostProvider中做了一个小技巧，就是如果发现当前游标的位置和上次已经使用过的地址位置一样，
 * 即当currentIndex和lastIndex游标值相同时，就进行spinDelay毫秒时间的等待。
 *
 */
@InterfaceAudience.Public
public final class StaticHostProvider implements HostProvider {

    private static final Logger LOG = LoggerFactory.getLogger(StaticHostProvider.class);

    /**
     * 用于对zk服务地址进行随机排序的随机生成器
     */
    private Random sourceOfRandomness;

    /**
     * 表示zk服务器地址，客户端传入的地址顺序会被进行随机排序
     */
    private List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>(5);


    private int lastIndex = -1;

    /** 表示当前客户端连接的zk服务索引，对应{@link #serverAddresses}，比如，当前客户连接是2台zk服务器，则该值为1 */
    private int currentIndex = -1;

    /**
     * 用于在重新配置期间迁移客户端
     */
    private boolean reconfigMode = false;

    private final List<InetSocketAddress> oldServers = new ArrayList<InetSocketAddress>(5);

    private final List<InetSocketAddress> newServers = new ArrayList<InetSocketAddress>(5);

    private int currentIndexOld = -1;
    private int currentIndexNew = -1;

    private float pOld, pNew;


    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses) {
        sourceOfRandomness = new Random(System.currentTimeMillis() ^ this.hashCode());

        // 检查zk服务器地址，并进行随机排序后返回
        this.serverAddresses = resolveAndShuffle(serverAddresses);
        if (this.serverAddresses.isEmpty()) {
            throw new IllegalArgumentException("A HostProvider may not be empty!");
        }
        currentIndex = -1;
        lastIndex = -1;
    }
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses, long randomnessSeed) {
        sourceOfRandomness = new Random(randomnessSeed);

        this.serverAddresses = resolveAndShuffle(serverAddresses);
        if (this.serverAddresses.isEmpty()) {
            throw new IllegalArgumentException("A HostProvider may not be empty!");
        }
        currentIndex = -1;
        lastIndex = -1;
    }



    /**
     * 更新客户端连接的zk服务列表，如果需要更改连接以实现负载平衡，则为返回true
     *
     * @param serverAddresses   新的zk服务列表
     * @param currentHost       客户端当前连接的zk服务机器
     * @return
     */
    @Override
    public synchronized boolean updateServerList(Collection<InetSocketAddress> serverAddresses, InetSocketAddress currentHost) {

        // Resolve server addresses and shuffle them
        List<InetSocketAddress> resolvedList = resolveAndShuffle(serverAddresses);
        if (resolvedList.isEmpty()) {
            throw new IllegalArgumentException("A HostProvider may not be empty!");
        }

        // 表示客户端当前连接的服务器是否在新的服务器列表中
        boolean myServerInNewConfig = false;

        InetSocketAddress myServer = currentHost;
        // 重新选取一台当前的服务
        if (reconfigMode) {
            myServer = next(0);
        }

        // if the client is not currently connected to any server
        if (myServer == null) {
            // reconfigMode = false (next shouldn't return null).
            if (lastIndex >= 0) {
                // take the last server to which we were connected
                myServer = this.serverAddresses.get(lastIndex);
            } else {
                // take the first server on the list
                myServer = this.serverAddresses.get(0);
            }
        }

        for (InetSocketAddress addr : resolvedList) {
            if (addr.getPort() == myServer.getPort()
                    && ((addr.getAddress() != null
                    && myServer.getAddress() != null && addr
                    .getAddress().equals(myServer.getAddress())) || addr
                    .getHostString().equals(myServer.getHostString()))) {
                myServerInNewConfig = true;
                break;
            }
        }

        reconfigMode = true;

        newServers.clear();
        oldServers.clear();
        // Divide the new servers into oldServers that were in the previous list
        // and newServers that were not in the previous list
        for (InetSocketAddress resolvedAddress : resolvedList) {
            if (this.serverAddresses.contains(resolvedAddress)) {
                oldServers.add(resolvedAddress);
            } else {
                newServers.add(resolvedAddress);
            }
        }

        int numOld = oldServers.size();
        int numNew = newServers.size();

        // number of servers increased
        if (numOld + numNew > this.serverAddresses.size()) {
            if (myServerInNewConfig) {
                // my server is in new config, but load should be decreased.
                // Need to decide if this client is moving to one of the new servers
                if (sourceOfRandomness.nextFloat() <= (1 - ((float) this.serverAddresses.size()) / (numOld + numNew))) {
                    pNew = 1;
                    pOld = 0;
                } else {
                    // do nothing special - stay with the current server
                    reconfigMode = false;
                }
            } else {
                // my server is not in new config, and load on old servers must be decreased, so connect to one of the new servers
                pNew = 1;
                pOld = 0;
            }
        } else { // number of servers stayed the same or decreased
            if (myServerInNewConfig) {
                // my server is in new config, and load should be increased, so stay with this server and do nothing special
                reconfigMode = false;
            } else {
                pOld = ((float) (numOld * (this.serverAddresses.size() - (numOld + numNew))))
                        / ((numOld + numNew) * (this.serverAddresses.size() - numOld));
                pNew = 1 - pOld;
            }
        }

        if (!reconfigMode) {
            currentIndex = resolvedList.indexOf(getServerAtCurrentIndex());
        } else {
            currentIndex = -1;
        }
        this.serverAddresses = resolvedList;
        currentIndexOld = -1;
        currentIndexNew = -1;
        lastIndex = currentIndex;
        return reconfigMode;
    }

    /**
     * 返回zk服务端的集群格式
     *
     * @return
     */
    public synchronized int size() {
        return serverAddresses.size();
    }

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
    public InetSocketAddress next(long spinDelay) {
        boolean needToSleep = false;
        InetSocketAddress addr;

        synchronized (this) {
            if (reconfigMode) {
                addr = nextHostInReconfigMode();
                if (addr != null) {
                    currentIndex = serverAddresses.indexOf(addr);
                    return addr;
                }
                //tried all servers and couldn't connect
                reconfigMode = false;
                needToSleep = (spinDelay > 0);
            }
            ++currentIndex;
            if (currentIndex == serverAddresses.size()) {
                currentIndex = 0;
            }
            addr = serverAddresses.get(currentIndex);
            needToSleep = needToSleep || (currentIndex == lastIndex && spinDelay > 0);
            if (lastIndex == -1) {
                // We don't want to sleep on the first ever connect attempt.
                lastIndex = 0;
            }
        }
        if (needToSleep) {
            try {
                Thread.sleep(spinDelay);
            } catch (InterruptedException e) {
                LOG.warn("Unexpected exception", e);
            }
        }

        return addr;
    }

    public synchronized void onConnected() {
        lastIndex = currentIndex;
        reconfigMode = false;
    }

    /**
     * 检查zk服务器地址，并进行随机排序后返回
     *
     * @param serverAddresses
     * @return
     */
    private List<InetSocketAddress> resolveAndShuffle(Collection<InetSocketAddress> serverAddresses) {
        List<InetSocketAddress> tmpList = new ArrayList<InetSocketAddress>(serverAddresses.size());
        for (InetSocketAddress address : serverAddresses) {
            try {
                InetAddress ia = address.getAddress();
                String addr = (ia != null) ? ia.getHostAddress() : address.getHostString();
                InetAddress resolvedAddresses[] = InetAddress.getAllByName(addr);
                for (InetAddress resolvedAddress : resolvedAddresses) {
                    InetAddress taddr = InetAddress.getByAddress(address.getHostString(), resolvedAddress.getAddress());
                    tmpList.add(new InetSocketAddress(taddr, address.getPort()));
                }
            } catch (UnknownHostException ex) {
                LOG.warn("No IP address found for server: {}", address, ex);
            }
        }

        // 将tmpList进行洗牌，打乱顺序
        Collections.shuffle(tmpList, sourceOfRandomness);
        return tmpList;
    }

    /**
     * 当处于“reconfigMode”时，获取要连接的下一个服务器，这意味着您刚刚更新了服务器列表，现在正在尝试寻找要连接的服务器。
     * 一旦调用onConnected()， reconfigMode就被设置为false。类似地，如果我们尝试在new config中连接所有服务器，并且失败，则reconfigMode被设置为false。
     * While in reconfigMode, we should connect to a server in newServers with probability pNew and to servers in oldServers with probability pOld (which is just 1-pNew).
     * If we tried out all servers in either oldServers or newServers we continue to try servers from the other set, regardless of pNew or pOld.
     * If we tried all servers we give up and go back to the normal round robin mode
     *
     * When called, this should be protected by synchronized(this)
     */
    private InetSocketAddress nextHostInReconfigMode() {
        boolean takeNew = (sourceOfRandomness.nextFloat() <= pNew);

        // take one of the new servers if it is possible (there are still such servers we didn't try),
        // 如果可能的话，使用一个新的服务器(仍然有一些服务器我们没有尝试过)，
        // and either the probability tells us to connect to one of the new servers or if we already tried all the old servers
        // 要么是概率告诉我们连接到一个新服务器，要么是我们已经尝试过所有旧服务器
        if (((currentIndexNew + 1) < newServers.size()) && (takeNew || (currentIndexOld + 1) >= oldServers.size())) {
            ++currentIndexNew;
            return newServers.get(currentIndexNew);
        }

        // start taking old servers
        if ((currentIndexOld + 1) < oldServers.size()) {
            ++currentIndexOld;
            return oldServers.get(currentIndexOld);
        }

        return null;
    }

    /**
     * 获取{@link #serverAddresses}对应索引位置的zk服务地址
     *
     * @param i zk服务索引
     * @return
     */
    public synchronized InetSocketAddress getServerAtIndex(int i) {
        if (i < 0 || i >= serverAddresses.size()) return null;
        return serverAddresses.get(i);
    }

    /**
     * 获取当前的处理的zk服务地址
     *
     * @return
     */
    public synchronized InetSocketAddress getServerAtCurrentIndex() {
        return getServerAtIndex(currentIndex);
    }


}

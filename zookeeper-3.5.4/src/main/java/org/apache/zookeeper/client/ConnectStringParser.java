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

import org.apache.zookeeper.common.PathUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.apache.zookeeper.common.StringUtils.split;

/**
 * A parser for ZooKeeper Client connect strings.
 * 
 * This class is not meant to be seen or used outside of ZooKeeper itself.
 * 
 * The chrootPath member should be replaced by a Path object in issue
 * ZOOKEEPER-849.
 * 
 * @see org.apache.zookeeper.ZooKeeper
 */
public final class ConnectStringParser {

    /** 默认端口 */
    private static final int DEFAULT_PORT = 2181;

    /**
     * zk根目录：例如，192.168.1.1:2181,192.168.1.2:2181,192.168.1.3:2181/zk-book，这样就指定了该客户端连接上ZooKeeper服务器之后，
     * 所有对ZooKeeper的操作，都会基于这个根目录。例如，客户端对/foo/bar的操作，都会指向节点的操作，都会基于这个根目录，例如，客户端对/foo/bar的操作，
     * 都会指向节点/zk-book/foo/bar——这个目录也叫Chroot，即客户端隔离命名空间。
     */
    private final String chrootPath;

    /** 该集合保存了zk的服务机器，每个InetSocketAddress表示一个zk服务 */
    private final ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();

    /**
     * 解析zk集群
     *
     * @param connectString 例如：例如，192.168.1.1:2181,192.168.1.2:2181,192.168.1.3:2181/zk-book
     *
     * @throws IllegalArgumentException for an invalid chroot path.
     */
    public ConnectStringParser(String connectString) {

        // 1、解析zk的根目录
        int off = connectString.indexOf('/');
        if (off >= 0) {
            String chrootPath = connectString.substring(off);
            // ignore "/" chroot spec, same as null
            if (chrootPath.length() == 1) {
                this.chrootPath = null;
            } else {
                PathUtils.validatePath(chrootPath);
                this.chrootPath = chrootPath;
            }
            connectString = connectString.substring(0, off);
        } else {
            this.chrootPath = null;
        }




        // 2、多个zk用逗号隔开，遍历每个zk服务，将zk服务主机及端口保存到serverAddresses
        List<String> hostsList = split(connectString,",");
        for (String host : hostsList) {
            int port = DEFAULT_PORT;
            int pidx = host.lastIndexOf(':');
            if (pidx >= 0) {
                // otherwise : is at the end of the string, ignore
                if (pidx < host.length() - 1) {
                    port = Integer.parseInt(host.substring(pidx + 1));
                }
                host = host.substring(0, pidx);
            }
            serverAddresses.add(InetSocketAddress.createUnresolved(host, port));
        }
    }

    public String getChrootPath() {
        return chrootPath;
    }

    public ArrayList<InetSocketAddress> getServerAddresses() {
        return serverAddresses;
    }
}

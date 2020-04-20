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
package org.apache.zookeeper.cli;

import org.apache.commons.cli.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.EphemeralType;

import java.util.List;

/**
 * 创建节点命令
 */
public class CreateCommand extends CliCommand {

    private static Options options = new Options();

    /** 表示命令行参数 */
    private String[] args;

    /** 表示命令行信息 */
    private CommandLine cl;

    static {
        /** 表示是临时节点 */
        options.addOption(new Option("e", false, "ephemeral"));
        /** 表示是顺序节点 */
        options.addOption(new Option("s", false, "sequential"));
        /** 表示是容器节点 */
        options.addOption(new Option("c", false, "container"));
        /** 表示是ttl节点 */
        options.addOption(new Option("t", true, "ttl"));
    }

    public CreateCommand() {
        super("create", "[-s] [-e] [-c] [-t ttl] path [data] [acl]");
    }

    @Override
    public CliCommand parse(String[] cmdArgs) throws CliParseException {
        Parser parser = new PosixParser();
        try {
            // 将解析的命令行封装为一个CommandLine实例
            cl = parser.parse(options, cmdArgs);
        } catch (ParseException ex) {
            throw new CliParseException(ex);
        }

        args = cl.getArgs();
        if(args.length < 2) {
            throw new CliParseException(getUsageStr());
        }
        return this;
    }

    /**
     * 执行创建节点的命令
     *
     * @return
     * @throws CliException
     */
    @Override
    public boolean exec() throws CliException {
        boolean hasE = cl.hasOption("e");
        boolean hasS = cl.hasOption("s");
        boolean hasC = cl.hasOption("c");
        boolean hasT = cl.hasOption("t");

        // 容器节点不能是临时节点或顺序节点
        if (hasC && (hasE || hasS)) {
            throw new MalformedCommandException("-c cannot be combined with -s or -e. Containers cannot be ephemeral or sequential.");
        }

        long ttl;
        try {
            ttl = hasT ? Long.parseLong(cl.getOptionValue("t")) : 0;
        } catch (NumberFormatException e) {
            throw new MalformedCommandException("-t argument must be a long value");
        }

        // 如果是ttl节点不能是临时节点
        if ( hasT && hasE ) {
            throw new MalformedCommandException("TTLs cannot be used with Ephemeral znodes");
        }

        // 如果是ttl节点不能是容器节点
        if ( hasT && hasC ) {
            throw new MalformedCommandException("TTLs cannot be used with Container znodes");
        }

        CreateMode flags;
        if(hasE && hasS) {
            flags = CreateMode.EPHEMERAL_SEQUENTIAL;
        } else if (hasE) {
            flags = CreateMode.EPHEMERAL;
        } else if (hasS) {
            flags = hasT ? CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL : CreateMode.PERSISTENT_SEQUENTIAL;
        } else if (hasC) {
            flags = CreateMode.CONTAINER;
        } else {
            flags = hasT ? CreateMode.PERSISTENT_WITH_TTL : CreateMode.PERSISTENT;
        }
        if ( hasT ) {
            try {
                EphemeralType.TTL.toEphemeralOwner(ttl);
            } catch (IllegalArgumentException e) {
                throw new MalformedCommandException(e.getMessage());
            }
        }

        String path = args[1];
        byte[] data = null;
        if (args.length > 2) {
            data = args[2].getBytes();
        }
        List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
        if (args.length > 3) {
            acl = AclParser.parse(args[3]);
        }


        try {
            // 将创建节点的实现委托给ZooKeeper
            String newPath = hasT ? zk.create(path, data, acl, flags, new Stat(), ttl) : zk.create(path, data, acl, flags);
            err.println("Created " + newPath);
        } catch(IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch(KeeperException.EphemeralOnLocalSessionException e) {
            err.println("Unable to create ephemeral node on a local session");
            throw new CliWrapperException(e);
        } catch (KeeperException.InvalidACLException ex) {
            err.println(ex.getMessage());
            throw new CliWrapperException(ex);
        } catch (KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        return true;
    }
}

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

/**
 * addauth命令用于节点认证，使用方式：addauth digest ${username}:${password}，例如：
 * setAcl /t1 digest:whz:6DY5WhzOfGsWQ1XFuIyzxkpwdPo=:r             ##表示：whz这个用户对/t1这个节点拥有读权限，访问时需要输入账号密码认证，这里的"6DY5WhzOfGsWQ1XFuIyzxkpwdPo"是"123456"进行hash后的base64编码
 * addauth digest whz:123456                                        ##执行成功后，表示当前连接已经对whz该用户进行了认证，之后可以对/t1节点进行访问了
 */
public class AddAuthCommand extends CliCommand {

    private static Options options = new Options();

    /**
     * 例如，addauth digest whz:123456 命令解析为：
     * String[0] = addauth
     * String[1] = digest
     * String[2] = whz:123456
     */
    private String[] args;

    public AddAuthCommand() {
        super("addauth", "scheme auth");
    }

    @Override
    public CliCommand parse(String[] cmdArgs) throws CliParseException {
        Parser parser = new PosixParser();
        CommandLine cl;
        try {
            cl = parser.parse(options, cmdArgs);
        } catch (ParseException ex) {
            throw new CliParseException(ex);
        }

        args = cl.getArgs();
        if (args.length < 2) {
            throw new CliParseException(getUsageStr());
        }

        return this;
    }

    @Override
    public boolean exec() throws CliException {
        byte[] b = null;
        if (args.length >= 3) {
            b = args[2].getBytes();
        }

        zk.addAuthInfo(args[1], b);

        return false;
    }
}

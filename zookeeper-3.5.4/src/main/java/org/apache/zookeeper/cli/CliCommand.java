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
package org.apache.zookeeper.cli;

import org.apache.zookeeper.ZooKeeper;

import java.io.PrintStream;
import java.util.Map;

/**
 * 所有的zn客户端命令都继承该类
 */
abstract public class CliCommand {

    /** zk客户端，命令行通过该客户端操作zk服务 */
    protected ZooKeeper zk;

    /** 表示命令行执行的输出信息 */
    protected PrintStream out;
    /** 用于输出执行命令的错误信息 */
    protected PrintStream err;

    /** 表示执行的命令 */
    private String cmdStr;

    /** 表示命令行的参数 */
    private String optionStr;

    /**
     * 一个带有命令字符串和选项的CLI命令。使用System.out和System.err进行打印
     * @param cmdStr    用于调用此命令的字符串
     * @param optionStr 用于调用此命令的字符串
     */
    public CliCommand(String cmdStr, String optionStr) {
        this.out = System.out;
        this.err = System.err;
        this.cmdStr = cmdStr;
        this.optionStr = optionStr;
    }

    /**
     * 返回：命令 + " " + 选项
     *
     * @return
     */
    public String getUsageStr() {
        return cmdStr + " " + optionStr;
    }

    /**
     * 将此命令添加到map。使用命令字符串作为键。
     *
     * @param cmdMap
     */
    public void addToMap(Map<String, CliCommand> cmdMap) {
        cmdMap.put(cmdStr, this);
    }

    /**
     * 解析命令行参数
     *
     * @param cmdArgs
     * @return this CliCommand
     * @throws CliParseException
     */
    abstract public CliCommand parse(String cmdArgs[]) throws CliParseException;

    /**
     * 执行命令
     *
     * @return
     * @throws CliException
     */
    abstract public boolean exec() throws CliException;







    // getter and setter ...

    /**
     * Set out printStream (useable for testing)
     * @param out
     */
    public void setOut(PrintStream out) {
        this.out = out;
    }
    /**
     * Set err printStream (useable for testing)
     * @param err
     */
    public void setErr(PrintStream err) {
        this.err = err;
    }
    /**
     * set the zookeper instance
     * @param zk the ZooKeeper instance.
     */
    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }
    /**
     * get the string used to call this command
     * @return
     */
    public String getCmdStr() {
        return cmdStr;
    }
    /**
     * get the option string
     * @return
     */
    public String getOptionStr() {
        return optionStr;
    }
}

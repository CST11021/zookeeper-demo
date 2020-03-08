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

package org.apache.zookeeper.server.persistence;

import org.apache.jute.Record;
import org.apache.zookeeper.txn.TxnHeader;

import java.io.IOException;

/**
 * 读取zk事务日志文件的接口，该接口只有一个实现即FileTxnLog
 *
 */
public interface TxnLog {
    
    /**
     * 追加到的当前日志
     *
     * @throws IOException 
     */
    void rollLog() throws IOException;

    /**
     * 将请求追加到事务日志
     *
     * @param hdr   事务头
     * @param r     事务本身
     * returns 添加成功返回true，否则false
     * @throws IOException
     */
    boolean append(TxnHeader hdr, Record r) throws IOException;

    /**
     * 从给定的zxid开始读取事务日志
     *
     * @param zxid
     * @return 返回一个迭代器来读取日志中的下一个事务。
     * @throws IOException
     */
    TxnIterator read(long zxid) throws IOException;
    
    /**
     * 返回记录的事务的最后一个zxid。
     *
     * @return
     * @throws IOException
     */
    long getLastLoggedZxid() throws IOException;
    
    /**
     * 截断日志以与leader保持同步。
     *
     * @param zxid 要截断的zxid
     * @throws IOException 
     */
    boolean truncate(long zxid) throws IOException;
    
    /**
     * 返回此事务日志的dbid
     *
     * @return
     * @throws IOException
     */
    long getDbId() throws IOException;
    
    /**
     * 提交事务并确保它们被持久化
     *
     * @throws IOException
     */
    void commit() throws IOException;

    /**
     *
     * @return transaction log's elapsed sync time in milliseconds
     */
    long getTxnLogSyncElapsedTime();
   
    /** 
     * 关闭事务日志
     */
    void close() throws IOException;

    /**
     * 用于读取事务日志的迭代接口。
     */
    public interface TxnIterator {
        /**
         * 获取事务头
         *
         * @return return the transaction header.
         */
        TxnHeader getHeader();
        
        /**
         * 返回事务记录
         *
         * @return return the transaction record.
         */
        Record getTxn();
     
        /**
         * 转到下一个事务记录
         *
         * @throws IOException
         */
        boolean next() throws IOException;
        
        /**
         * 关闭文件并释放资源
         *
         * @throws IOException
         */
        void close() throws IOException;
        
        /**
         * 获取用于存储将由该迭代器返回的事务记录的估计存储空间
         *
         * @throws IOException
         */
        long getStorageSize() throws IOException;
    }
}


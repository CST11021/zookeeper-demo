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

package org.apache.zookeeper.server;

import java.util.Date;

/**
 * 关于 ServerCnxn 的统计信息
 */
interface Stats {

    /**
     * 建立连接的日期/时间
     */
    Date getEstablished();

    /**
     * 获取已经提交但是尚未回复的请求个数
     */
    long getOutstandingRequests();

    /** 获取接收到的packets个数 */
    long getPacketsReceived();

    /** 获取已经发送packet个数 */
    long getPacketsSent();

    /**
     * 最低延迟，毫秒
     * @since 3.3.0
     */
    long getMinLatency();

    /**
     * 平均延迟，毫秒
     *
     * @since 3.3.0
     */
    long getAvgLatency();

    /**
     * 最高延迟，毫秒
     * @since 3.3.0
     */
    long getMaxLatency();

    /**
     * 连接最后一次操作
     * @since 3.3.0
     */
    String getLastOperation();

    /**
     * 最后一次连接的cxid
     *
     * @since 3.3.0
     */
    long getLastCxid();

    /**
     * 最后一次连接的zxid
     * @since 3.3.0
     */
    long getLastZxid();

    /**
     * 上次回复的时间
     * @since 3.3.0
     */
    long getLastResponseTime();

    /**
     * 上一次回复的延迟
     * @since 3.3.0
     */
    long getLastLatency();

    /**
     * 还原各种计数器
     * @since 3.3.0
     */
    void resetStats();
}

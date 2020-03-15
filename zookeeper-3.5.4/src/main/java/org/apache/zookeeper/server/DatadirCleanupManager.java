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

package org.apache.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * 这个类通过使用指定的'autopurge.purgeInterval'来调度自动清除任务，从而管理快照和相应事务日志的清除。
 * 它会保存最近 autopurge.snapRetainCount 的快照数量和相应的事务日志。
 */
public class DatadirCleanupManager {

    private static final Logger LOG = LoggerFactory.getLogger(DatadirCleanupManager.class);

    /**
     * 表示清洗dataDir任务的状态
     */
    public enum PurgeTaskStatus {
        /** 分别表示：未开始、已经开始和完成 */
        NOT_STARTED, STARTED, COMPLETED;
    }

    /** 表示当前清楚日志任务的状态 */
    private PurgeTaskStatus purgeTaskStatus = PurgeTaskStatus.NOT_STARTED;

    /** 表示快照文件目录 */
    private final File snapDir;
    /** 表示事务日志文件的目录 */
    private final File dataLogDir;
    /** 清除后要保留的快照数 */
    private final int snapRetainCount;
    /** 清楚日志的时间间隔，单位小时 */
    private final int purgeInterval;
    /** 表示清除日志的定时任务 */
    private Timer timer;

    /**
     * Constructor of DatadirCleanupManager. It takes the parameters to schedule the purge task.
     * 
     * @param snapDir           snapshot directory
     * @param dataLogDir        transaction log directory
     * @param snapRetainCount   清除后要保留的快照数
     * @param purgeInterval     清洗间隔(小时)
     */
    public DatadirCleanupManager(File snapDir, File dataLogDir, int snapRetainCount, int purgeInterval) {
        this.snapDir = snapDir;
        this.dataLogDir = dataLogDir;
        this.snapRetainCount = snapRetainCount;
        this.purgeInterval = purgeInterval;
        LOG.info("autopurge.snapRetainCount set to " + snapRetainCount);
        LOG.info("autopurge.purgeInterval set to " + purgeInterval);
    }

    /**
     * Validates the purge configuration and schedules the purge task. Purge
     * task keeps the most recent <code>snapRetainCount</code> number of
     * snapshots and deletes the remaining for every <code>purgeInterval</code>
     * hour(s).
     * <p>
     * <code>purgeInterval</code> of <code>0</code> or
     * <code>negative integer</code> will not schedule the purge task.
     * </p>
     * 
     * @see PurgeTxnLog#purge(File, File, int)
     */
    public void start() {
        if (PurgeTaskStatus.STARTED == purgeTaskStatus) {
            LOG.warn("Purge task is already running.");
            return;
        }

        // 不要将清除任务安排为0或负清除间隔。
        if (purgeInterval <= 0) {
            LOG.info("Purge task is not scheduled.");
            return;
        }

        timer = new Timer("PurgeTask", true);
        TimerTask task = new PurgeTask(dataLogDir, snapDir, snapRetainCount);
        timer.scheduleAtFixedRate(task, 0, TimeUnit.HOURS.toMillis(purgeInterval));

        purgeTaskStatus = PurgeTaskStatus.STARTED;
    }

    /**
     * Shutdown the purge task.
     */
    public void shutdown() {
        if (PurgeTaskStatus.STARTED == purgeTaskStatus) {
            LOG.info("Shutting down purge task.");
            timer.cancel();
            purgeTaskStatus = PurgeTaskStatus.COMPLETED;
        } else {
            LOG.warn("Purge task not started. Ignoring shutdown!");
        }
    }

    // getter ...

    public PurgeTaskStatus getPurgeTaskStatus() {
        return purgeTaskStatus;
    }
    public File getSnapDir() {
        return snapDir;
    }
    public File getDataLogDir() {
        return dataLogDir;
    }
    public int getPurgeInterval() {
        return purgeInterval;
    }
    public int getSnapRetainCount() {
        return snapRetainCount;
    }

    /**
     * 清洗日志的任务
     */
    static class PurgeTask extends TimerTask {
        private File logsDir;
        private File snapsDir;
        private int snapRetainCount;

        public PurgeTask(File dataDir, File snapDir, int count) {
            logsDir = dataDir;
            snapsDir = snapDir;
            snapRetainCount = count;
        }

        @Override
        public void run() {
            LOG.info("Purge task started.");
            try {
                // 将清除逻辑委托给PurgeTxnLog
                PurgeTxnLog.purge(logsDir, snapsDir, snapRetainCount);
            } catch (Exception e) {
                LOG.error("Error occurred while purging.", e);
            }
            LOG.info("Purge task completed.");
        }
    }
}

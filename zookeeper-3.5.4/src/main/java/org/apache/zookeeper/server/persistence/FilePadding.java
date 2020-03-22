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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FilePadding {

    private static final Logger LOG;
    /** 表示日志文件的预分配大小以填充文件。 */
    private static long preAllocSize = 65536 * 1024;
    private static final ByteBuffer fill = ByteBuffer.allocateDirect(1);

    static {
        LOG = LoggerFactory.getLogger(FileTxnLog.class);

        String size = System.getProperty("zookeeper.preAllocSize");
        if (size != null) {
            try {
                preAllocSize = Long.parseLong(size) * 1024;
            } catch (NumberFormatException e) {
                LOG.warn(size + " is not a valid value for preAllocSize");
            }
        }
    }

    /** 我们将事务日志写到同一个文件，每次写完后，将在内存中记录文件的大小，表示为currentSize */
    private long currentSize;




    /**
     * 用0填充当前文件，将其大小增加到比当前大小和位置大的下一个preAllocSize的倍数
     *
     * @param fileChannel 要填充的日志文件
     * @throws IOException
     */
    long padFile(FileChannel fileChannel) throws IOException {
        // 通过查看实际的文件大小和当前zk与分配的文件大小，计算重新计算zk文件大小
        long newFileSize = calculateFileSizeWithPadding(fileChannel.position(), currentSize, preAllocSize);
        if (currentSize != newFileSize) {
            fileChannel.write((ByteBuffer) fill.position(0), newFileSize - fill.remaining());
            currentSize = newFileSize;
        }
        return currentSize;
    }

    /**
     * 计算一个新的文件大小填充。如果当前文件位置与文件结束位置非常接近(小于4K)，且preAllocSize为>0，则只返回新大小。
     *
     * @param position     当前磁盘日志文件大小
     * @param fileSize     当前zk记录的文件大小
     * @param preAllocSize 预分配的大小
     * @return 返回zk记录的文件大小, 如果没有填充，它可以与fileSize相同。
     * @throws IOException
     */
    public static long calculateFileSizeWithPadding(long position, long fileSize, long preAllocSize) {
        // 确保实际的文件大小和zk通过预分配的大小要在4k以内，否则需要扩容
        if (preAllocSize > 0 && position + 4096 >= fileSize) {
            // 当前磁盘文件的大小>zk记录大小，则重新计算zk记录大小，否则预分配一块大小
            if (position > fileSize) {
                fileSize = position + preAllocSize;
                fileSize -= fileSize % preAllocSize;
            } else {
                fileSize += preAllocSize;
            }
        }

        return fileSize;
    }



    public static long getPreAllocSize() {
        return preAllocSize;
    }
    public static void setPreallocSize(long size) {
        preAllocSize = size;
    }
    public void setCurrentSize(long currentSize) {
        this.currentSize = currentSize;
    }
}

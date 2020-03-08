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

package org.apache.zookeeper.server.persistence;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * A collection of utility methods for dealing with file name parsing, 
 * low level I/O file operations and marshalling/unmarshalling.
 */
public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);
    private static final String SNAP_DIR = "snapDir";
    private static final String LOG_DIR = "logDir";
    private static final String DB_FORMAT_CONV = "dbFormatConversion";

    public static String makeURIString(String dataDir, String dataLogDir,
                                       String convPolicy) {
        String uri = "file:" + SNAP_DIR + "=" + dataDir + ";" + LOG_DIR + "=" + dataLogDir;
        if (convPolicy != null)
            uri += ";" + DB_FORMAT_CONV + "=" + convPolicy;
        return uri.replace('\\', '/');
    }

    /**
     * Given two directory files the method returns a well-formed 
     * logfile provider URI. This method is for backward compatibility with the
     * existing code that only supports logfile persistence and expects these two
     * parameters passed either on the command-line or in the configuration file.
     *
     * @param dataDir snapshot directory
     * @param dataLogDir transaction log directory
     * @return logfile provider URI
     */
    public static URI makeFileLoggerURL(File dataDir, File dataLogDir) {
        return URI.create(makeURIString(dataDir.getPath(), dataLogDir.getPath(), null));
    }

    public static URI makeFileLoggerURL(File dataDir, File dataLogDir, String convPolicy) {
        return URI.create(makeURIString(dataDir.getPath(), dataLogDir.getPath(), convPolicy));
    }

    /**
     * Creates a valid transaction log file name. 
     *
     * @param zxid used as a file name suffix (extension)
     * @return file name
     */
    public static String makeLogName(long zxid) {
        return FileTxnLog.LOG_FILE_PREFIX + "." + Long.toHexString(zxid);
    }

    /**
     * 创建zk快照文件名
     *
     * @param zxid 用作后缀
     * @return file name
     */
    public static String makeSnapshotName(long zxid) {
        return FileSnap.SNAPSHOT_FILE_PREFIX + "." + Long.toHexString(zxid);
    }

    /**
     * Extracts snapshot directory property value from the container.
     *
     * @param props properties container
     * @return file representing the snapshot directory
     */
    public static File getSnapDir(Properties props) {
        return new File(props.getProperty(SNAP_DIR));
    }

    /**
     * Extracts transaction log directory property value from the container.
     *
     * @param props properties container
     * @return file representing the txn log directory
     */
    public static File getLogDir(Properties props) {
        return new File(props.getProperty(LOG_DIR));
    }

    /**
     * Extracts the value of the dbFormatConversion attribute.
     *
     * @param props properties container
     * @return value of the dbFormatConversion attribute
     */
    public static String getFormatConversionPolicy(Properties props) {
        return props.getProperty(DB_FORMAT_CONV);
    }

    /**
     * 从文件名中提取zxid。
     * 文件名应该是使用{@link #makeLogName(long)}或{@link #makeSnapshotName(long)}中的一个创建的。
     *
     * @param name the file name to parse
     * @param prefix the file name prefix (snapshot or log)
     * @return zxid
     */
    public static long getZxidFromName(String name, String prefix) {
        long zxid = -1;
        String nameParts[] = name.split("\\.");
        if (nameParts.length == 2 && nameParts[0].equals(prefix)) {
            try {
                zxid = Long.parseLong(nameParts[1], 16);
            } catch (NumberFormatException e) {
            }
        }
        return zxid;
    }

    /**
     * 验证文件是否是有效的快照。
     * 快照可能是无效的，如果它是不完整的情况下，当服务器死亡，而在存储快照的过程中。
     * 任何不是快照的文件也是无效的快照。
     *
     * @param f     要校验的快照文件
     * @return true if the snapshot is valid
     * @throws IOException
     */
    public static boolean isValidSnapshot(File f) throws IOException {
        if (f == null || Util.getZxidFromName(f.getName(), FileSnap.SNAPSHOT_FILE_PREFIX) == -1)
            return false;

        // Check for a valid snapshot
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            // including the header and the last / bytes
            // the snapshot should be at least 10 bytes
            if (raf.length() < 10) {
                return false;
            }
            raf.seek(raf.length() - 5);
            byte bytes[] = new byte[5];
            int readlen = 0;
            int l;
            while (readlen < 5 &&
                    (l = raf.read(bytes, readlen, bytes.length - readlen)) >= 0) {
                readlen += l;
            }
            if (readlen != bytes.length) {
                LOG.info("Invalid snapshot " + f + " too short, len = " + readlen);
                return false;
            }
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            int len = bb.getInt();
            byte b = bb.get();
            if (len != 1 || b != '/') {
                LOG.info("Invalid snapshot " + f + " len = " + len + " byte = " + (b & 0xff));
                return false;
            }
        }

        return true;
    }

    /**
     * Reads a transaction entry from the input archive.
     * @param ia archive to read from
     * @return null if the entry is corrupted or EOF has been reached; a buffer
     * (possible empty) containing serialized transaction record.
     * @throws IOException
     */
    public static byte[] readTxnBytes(InputArchive ia) throws IOException {
        try {
            byte[] bytes = ia.readBuffer("txtEntry");
            // Since we preallocate, we define EOF to be an
            // empty transaction
            if (bytes.length == 0)
                return bytes;
            if (ia.readByte("EOF") != 'B') {
                LOG.error("Last transaction was partial.");
                return null;
            }
            return bytes;
        } catch (EOFException e) {
        }
        return null;
    }


    /**
     * Serializes transaction header and transaction data into a byte buffer.
     *
     * @param hdr transaction header
     * @param txn transaction data
     * @return serialized transaction record
     * @throws IOException
     */
    public static byte[] marshallTxnEntry(TxnHeader hdr, Record txn)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputArchive boa = BinaryOutputArchive.getArchive(baos);

        hdr.serialize(boa, "hdr");
        if (txn != null) {
            txn.serialize(boa, "txn");
        }
        return baos.toByteArray();
    }

    /**
     * Write the serialized transaction record to the output archive.
     *
     * @param oa output archive
     * @param bytes serialized transaction record
     * @throws IOException
     */
    public static void writeTxnBytes(OutputArchive oa, byte[] bytes)
            throws IOException {
        oa.writeBuffer(bytes, "txnEntry");
        oa.writeByte((byte) 0x42, "EOR"); // 'B'
    }


    /**
     * Compare file file names of form "prefix.version". Sort order result
     * returned in order of version.
     */
    private static class DataDirFileComparator
            implements Comparator<File>, Serializable {
        private static final long serialVersionUID = -2648639884525140318L;

        private String prefix;
        private boolean ascending;

        public DataDirFileComparator(String prefix, boolean ascending) {
            this.prefix = prefix;
            this.ascending = ascending;
        }

        public int compare(File o1, File o2) {
            long z1 = Util.getZxidFromName(o1.getName(), prefix);
            long z2 = Util.getZxidFromName(o2.getName(), prefix);
            int result = z1 < z2 ? -1 : (z1 > z2 ? 1 : 0);
            return ascending ? result : -result;
        }
    }

    /**
     * 对文件列表进行排序。最近时间由文件名的版本组件决定。
     * 例如返回如下这样一组排序过的文件
     * /tmp/zookeeper/version-2/snapshot.8
     * /tmp/zookeeper/version-2/log.9
     * /tmp/zookeeper/version-2/log.1
     *
     *
     * @param files     要排序的文件
     * @param prefix    不匹配此前缀的文件被假定为version = -1)
     * @param ascending 真值按升序排序，假值按降序排序
     * @return 返回排序后文件
     */
    public static List<File> sortDataDir(File[] files, String prefix, boolean ascending) {
        if (files == null)
            return new ArrayList<File>(0);

        List<File> filelist = Arrays.asList(files);
        Collections.sort(filelist, new DataDirFileComparator(prefix, ascending));
        return filelist;
    }

    /**
     * 如果文件是以"log."开头的，则返回true
     *
     * @param fileName
     * @return
     */
    public static boolean isLogFileName(String fileName) {
        return fileName.startsWith(FileTxnLog.LOG_FILE_PREFIX + ".");
    }

    /**
     * 如果文件是以"snapshot."开头的，则返回true
     *
     * @param fileName
     * @return
     */
    public static boolean isSnapshotFileName(String fileName) {
        return fileName.startsWith(FileSnap.SNAPSHOT_FILE_PREFIX + ".");
    }

}

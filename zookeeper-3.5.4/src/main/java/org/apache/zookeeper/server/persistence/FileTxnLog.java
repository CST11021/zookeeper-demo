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

import org.apache.jute.*;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * 该类用于操作zk事务日志文件：
 *
 * 1、zk事务日志文件用来标记事务操作，每一个事务操作如添加，删除节点等等 都会在事务日志中记录一条记录，用来在zookeeper异常情况下恢复数据
 * 2、事务日志指zookeeper系统在正常运行过程中，针对所有的更新操作，在返回客户端“更新成功”的响应前，zookeeper会保证已经将本次更新操作的事务日志已经写到磁盘上，只有这样，整个更新操作才会生效。
 *
 *
 *
 *
 * This class implements the TxnLog interface. It provides api's
 * to access the txnlogs and add entries to it.
 * <p>
 * The format of a Transactional log is as follows:
 * <blockquote><pre>
 * LogFile:
 *     FileHeader TxnList ZeroPad
 *
 * FileHeader: {
 *     magic 4bytes (ZKLG)
 *     version 4bytes
 *     dbid 8bytes
 *   }
 *
 * TxnList:
 *     Txn || Txn TxnList
 *
 * Txn:
 *     checksum Txnlen TxnHeader Record 0x42
 *
 * checksum: 8bytes Adler32 is currently used
 *   calculated across payload -- Txnlen, TxnHeader, Record and 0x42
 *
 * Txnlen:
 *     len 4bytes
 *
 * TxnHeader: {
 *     sessionid 8bytes
 *     cxid 4bytes
 *     zxid 8bytes
 *     time 8bytes
 *     type 4bytes
 *   }
 *
 * Record:
 *     See Jute definition file for details on the various record types
 *
 * ZeroPad:
 *     0 padded to EOF (filled during preallocation stage)
 * </pre></blockquote>
 */
public class FileTxnLog implements TxnLog {

    private static final Logger LOG;

    public final static int TXNLOG_MAGIC = ByteBuffer.wrap("ZKLG".getBytes()).getInt();

    public final static int VERSION = 2;

    public static final String LOG_FILE_PREFIX = "log";

    /** 表示将{@link #streamsToFlush}中的单个FileOutputStream写到磁盘的文件，当syncElapsedMS > fsyncWarningThresholdMS时，输出一条warn日志 */
    private volatile long syncElapsedMS = -1L;

    /** 表示将内存中事务日志的内容写道磁盘上的时间最大时间，默认是1秒，如果超过，则会输出一条warn日志 */
    private final static long fsyncWarningThresholdMS;

    /** 初始化{@link fsyncWarningThresholdMS} */
    static {
        LOG = LoggerFactory.getLogger(FileTxnLog.class);

        /** Local variable to read fsync.warningthresholdms into */
        Long fsyncWarningThreshold;
        if ((fsyncWarningThreshold = Long.getLong("zookeeper.fsync.warningthresholdms")) == null)
            fsyncWarningThreshold = Long.getLong("fsync.warningthresholdms", 1000);
        fsyncWarningThresholdMS = fsyncWarningThreshold;
    }

    /** 表示最后写入事务日志文件的事务ID */
    long lastZxidSeen;





    /** 表示zk事务日志文件 */
    File logDir;

    /**  */
    private final boolean forceSync = !System.getProperty("zookeeper.forceSync", "yes").equals("no");
    long dbId;
    /** 每次要写入的事务数据都会缓存到streamsToFlush，在commit的时候，才写入磁盘 */
    private LinkedList<FileOutputStream> streamsToFlush = new LinkedList<FileOutputStream>();

    /** 对应事务日志文件，用于将事务数据写到日志文件 */
    File logFileWrite = null;
    /** 对应事务日志文件，用于将事务数据写到日志文件 */
    volatile FileOutputStream fos = null;
    /** 用于将事务日志输出到文件的流 */
    volatile BufferedOutputStream logStream = null;
    /** 用于将事务头和事务体数据序列化到日志文件的序列化器 */
    volatile OutputArchive oa;
    /** 来记录事务日志文件的大小，方便对齐填充 */
    private FilePadding filePadding = new FilePadding();



    /**
     * constructor for FileTxnLog. Take the directory where the txnlogs are stored
     *
     * @param logDir 存储txnlogs的文件
     */
    public FileTxnLog(File logDir) {
        this.logDir = logDir;
    }

    /**
     * 方法允许设置日志文件的预分配大小以填充文件。
     *
     * @param size 要设置为字节数的大小
     */
    public static void setPreallocSize(long size) {
        FilePadding.setPreallocSize(size);
    }

    /**
     * 创建要使用的校验和算法
     *
     * @return the checksum used for this txnlog
     */
    protected Checksum makeChecksumAlgorithm() {
        return new Adler32();
    }

    /**
     * 将缓存在内存中的事务日志保存到磁盘（将当前日志文件滚动到新的日志文件）
     *
     * @throws IOException
     */
    public synchronized void rollLog() throws IOException {
        if (logStream != null) {
            this.logStream.flush();
            this.logStream = null;
            oa = null;
        }
    }

    /**
     * 关闭所有打开的文件句柄
     *
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        if (logStream != null) {
            logStream.close();
        }

        for (FileOutputStream log : streamsToFlush) {
            log.close();
        }
    }

    /**
     * 将事务头和事务数据写到事务日志文件中
     *
     * @param hdr   事务头
     * @param txn   事务数据
     * returns true iff something appended, otw false
     */
    public synchronized boolean append(TxnHeader hdr, Record txn) throws IOException {
        if (hdr == null) {
            return false;
        }

        if (hdr.getZxid() <= lastZxidSeen) {
            LOG.warn("Current zxid " + hdr.getZxid() + " is <= " + lastZxidSeen + " for " + hdr.getType());
        } else {
            lastZxidSeen = hdr.getZxid();
        }

        if (logStream == null) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Creating new log file: " + Util.makeLogName(hdr.getZxid()));
            }

            logFileWrite = new File(logDir, Util.makeLogName(hdr.getZxid()));
            fos = new FileOutputStream(logFileWrite);
            logStream = new BufferedOutputStream(fos);
            oa = BinaryOutputArchive.getArchive(logStream);

            FileHeader fhdr = new FileHeader(TXNLOG_MAGIC, VERSION, dbId);
            fhdr.serialize(oa, "fileheader");
            // Make sure that the magic number is written before padding.
            logStream.flush();
            filePadding.setCurrentSize(fos.getChannel().position());
            streamsToFlush.add(fos);
        }

        // 文件对齐填充
        filePadding.padFile(fos.getChannel());

        // 将事务头和事务数据序列化为字节
        byte[] buf = Util.marshallTxnEntry(hdr, txn);
        if (buf == null || buf.length == 0) {
            throw new IOException("Faulty serialization for header " + "and txn");
        }
        Checksum crc = makeChecksumAlgorithm();
        crc.update(buf, 0, buf.length);
        oa.writeLong(crc.getValue(), "txnEntryCRC");
        Util.writeTxnBytes(oa, buf);

        return true;
    }

    /**
     * Find the log file that starts at, or just before, the snapshot.
     * Return this and all subsequent logs. Results are ordered by zxid of file, ascending order.
     * @param logDirList    需要要排序的文件
     * @param snapshotZxid return files at, or before this zxid
     * @return
     */
    public static File[] getLogFiles(File[] logDirList, long snapshotZxid) {
        // 给日志文件排序
        List<File> files = Util.sortDataDir(logDirList, LOG_FILE_PREFIX, true);

        long logZxid = 0;
        // Find the log file that starts before or at the same time as the zxid of the snapshot
        for (File f : files) {
            long fzxid = Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX);
            if (fzxid > snapshotZxid) {
                continue;
            }

            // the files are sorted with zxid's
            if (fzxid > logZxid) {
                logZxid = fzxid;
            }
        }

        List<File> v = new ArrayList<File>(5);
        for (File f : files) {
            long fzxid = Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX);
            if (fzxid < logZxid) {
                continue;
            }
            v.add(f);
        }
        return v.toArray(new File[0]);

    }

    /**
     * get the last zxid that was logged in the transaction logs
     * @return the last zxid logged in the transaction logs
     */
    public long getLastLoggedZxid() {
        File[] files = getLogFiles(logDir.listFiles(), 0);
        // 获取最后的事务日志
        long maxLog = files.length > 0 ?
                Util.getZxidFromName(files[files.length - 1].getName(), LOG_FILE_PREFIX) : -1;

        // 如果日志文件是最近的，我们必须扫描它以找到最高的zxid
        long zxid = maxLog;
        TxnIterator itr = null;
        try {
            FileTxnLog txn = new FileTxnLog(logDir);
            itr = txn.read(maxLog);
            while (true) {
                if (!itr.next())
                    break;
                TxnHeader hdr = itr.getHeader();
                zxid = hdr.getZxid();
            }
        } catch (IOException e) {
            LOG.warn("Unexpected exception", e);
        } finally {
            close(itr);
        }
        return zxid;
    }

    /**
     * 关闭事务日志迭代器，已释放资源
     *
     * @param itr
     */
    private void close(TxnIterator itr) {
        if (itr != null) {
            try {
                itr.close();
            } catch (IOException ioe) {
                LOG.warn("Error closing file iterator", ioe);
            }
        }
    }

    /**
     * 提交日志，并确保所有内容都保存到访问磁盘
     */
    public synchronized void commit() throws IOException {
        if (logStream != null) {
            logStream.flush();
        }

        for (FileOutputStream log : streamsToFlush) {
            log.flush();

            if (forceSync) {
                long startSyncNS = System.nanoTime();

                FileChannel channel = log.getChannel();
                // force()方法有一个boolean类型的参数, 指明是否同时将文件元数据(权限信息等)写到磁盘上
                channel.force(false);

                // 将内存中事务日志的内容写道磁盘上的时间如果超过10毫秒，则添加一个warn日志
                syncElapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startSyncNS);
                if (syncElapsedMS > fsyncWarningThresholdMS) {
                    LOG.warn("fsync-ing the write ahead log in "
                            + Thread.currentThread().getName()
                            + " took " + syncElapsedMS
                            + "ms which will adversely effect operation latency. "
                            + "File size is " + channel.size() + " bytes. "
                            + "See the ZooKeeper troubleshooting guide");
                }
            }
        }

        // 同步完成后，清除缓存数据
        while (streamsToFlush.size() > 1) {
            streamsToFlush.removeFirst().close();
        }
    }

    /**
     *
     * @return elapsed sync time of transaction log in milliseconds
     */
    public long getTxnLogSyncElapsedTime() {
        return syncElapsedMS;
    }

    /**
     * 开始从给定的zxid读取所有事务, zxid是指最小的事务id，这里返回的迭代器是指id > zxid 的事务日志文件
     *
     * @param zxid the zxid to start reading transactions from
     * @return returns an iterator to iterate through the transaction
     * logs
     */
    public TxnIterator read(long zxid) throws IOException {
        return read(zxid, true);
    }

    /**
     * start reading all the transactions from the given zxid.
     *
     * @param zxid the zxid to start reading transactions from
     * @param fastForward true if the iterator should be fast forwarded to point
     *        to the txn of a given zxid, else the iterator will point to the
     *        starting txn of a txnlog that may contain txn of a given zxid
     * @return returns an iterator to iterate through the transaction logs
     */
    public TxnIterator read(long zxid, boolean fastForward) throws IOException {
        return new FileTxnIterator(logDir, zxid, fastForward);
    }

    /**
     * truncate the current transaction logs
     *
     * @param zxid the zxid to truncate the logs to
     * @return true if successful false if not
     */
    public boolean truncate(long zxid) throws IOException {
        FileTxnIterator itr = null;
        try {
            itr = new FileTxnIterator(this.logDir, zxid);
            PositionInputStream input = itr.inputStream;
            if (input == null) {
                throw new IOException("No log files found to truncate! This could " +
                        "happen if you still have snapshots from an old setup or " +
                        "log files were deleted accidentally or dataLogDir was changed in zoo.cfg.");
            }
            long pos = input.getPosition();
            // now, truncate at the current position
            RandomAccessFile raf = new RandomAccessFile(itr.logFile, "rw");
            raf.setLength(pos);
            raf.close();
            while (itr.goToNextLog()) {
                if (!itr.logFile.delete()) {
                    LOG.warn("Unable to truncate {}", itr.logFile);
                }
            }
        } finally {
            close(itr);
        }
        return true;
    }

    /**
     * 将文件反序列化为FileHeader
     *
     * @param file the transaction file to read
     * @return header that was read from the file
     * @throws IOException
     */
    private static FileHeader readHeader(File file) throws IOException {
        InputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            InputArchive ia = BinaryInputArchive.getArchive(is);
            FileHeader hdr = new FileHeader();
            hdr.deserialize(ia, "fileheader");
            return hdr;
        } finally {
            try {
                if (is != null) is.close();
            } catch (IOException e) {
                LOG.warn("Ignoring exception during close", e);
            }
        }
    }

    /**
     * the dbid of this transaction database
     * @return the dbid of this database
     */
    public long getDbId() throws IOException {
        FileTxnIterator itr = new FileTxnIterator(logDir, 0);
        FileHeader fh = readHeader(itr.logFile);
        itr.close();
        if (fh == null)
            throw new IOException("Unsupported Format.");

        return fh.getDbid();
    }

    /**
     * the forceSync value. true if forceSync is enabled, false otherwise.
     * @return the forceSync value
     */
    public boolean isForceSync() {
        return forceSync;
    }





    /**
     * a class that keeps track of the position
     * in the input stream. The position points to offset
     * that has been consumed by the applications. It can
     * wrap buffered input streams to provide the right offset
     * for the application.
     */
    static class PositionInputStream extends FilterInputStream {
        long position;

        protected PositionInputStream(InputStream in) {
            super(in);
            position = 0;
        }

        @Override
        public int read() throws IOException {
            int rc = super.read();
            if (rc > -1) {
                position++;
            }
            return rc;
        }

        public int read(byte[] b) throws IOException {
            int rc = super.read(b);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int rc = super.read(b, off, len);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }

        @Override
        public long skip(long n) throws IOException {
            long rc = super.skip(n);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void mark(int readLimit) {
            throw new UnsupportedOperationException("mark");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("reset");
        }
    }

    /**
     * this class implements the txnlog iterator interface
     * which is used for reading the transaction logs
     */
    public static class FileTxnIterator implements TxnLog.TxnIterator {
        /** 校验异常的提示信息 */
        static final String CRC_ERROR = "CRC check failed";

        /** 表示事务日志文件 */
        File logDir;
        /** 表示开始读取的zxid */
        long zxid;
        /** 表示事务日志文件头 */
        TxnHeader hdr;
        Record record;
        File logFile;
        /** 表示事务日志文件的反序列器 */
        InputArchive ia;


        PositionInputStream inputStream = null;
        /** 用于保存事务日志文件列表，该列表中的事务日志文件的事务id都大于指定的zxid */
        private ArrayList<File> storedFiles;

        /**
         * 在事务数据库目录上创建一个迭代器
         *
         * @param logDir 事务数据库目录
         * @param zxid 开始读取的zxid
         * @param fastForward   true if the iterator should be fast forwarded to point to the txn of a given zxid, else the iterator will point to the starting txn of a txnlog that may contain txn of a given zxid
         * @throws IOException
         */
        public FileTxnIterator(File logDir, long zxid, boolean fastForward) throws IOException {
            this.logDir = logDir;
            this.zxid = zxid;
            init();

            if (fastForward && hdr != null) {
                while (hdr.getZxid() < zxid) {
                    if (!next())
                        break;
                }
            }
        }
        public FileTxnIterator(File logDir, long zxid) throws IOException {
            this(logDir, zxid, true);
        }

        /**
         * initialize to the zxid specified
         * this is inclusive of the zxid
         * @throws IOException
         */
        void init() throws IOException {
            storedFiles = new ArrayList<File>();
            // 根据事务日志目录，获取该目录下所有的事务日志文件
            List<File> files = Util.sortDataDir(FileTxnLog.getLogFiles(logDir.listFiles(), 0), LOG_FILE_PREFIX, false);
            for (File f : files) {
                if (Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX) >= zxid) {
                    storedFiles.add(f);
                }
                // 添加最后一个小于zxid的日志文件
                else if (Util.getZxidFromName(f.getName(), LOG_FILE_PREFIX) < zxid) {
                    storedFiles.add(f);
                    break;
                }
            }
            goToNextLog();
            next();
        }

        /**
         * the iterator that moves to the next transaction
         * @return true if there is more transactions to be read
         * false if not.
         */
        public boolean next() throws IOException {
            if (ia == null) {
                return false;
            }
            try {
                long crcValue = ia.readLong("crcvalue");
                byte[] bytes = Util.readTxnBytes(ia);
                // Since we preallocate, we define EOF to be an
                if (bytes == null || bytes.length == 0) {
                    throw new EOFException("Failed to read " + logFile);
                }
                // EOF or corrupted record
                // validate CRC
                Checksum crc = makeChecksumAlgorithm();
                crc.update(bytes, 0, bytes.length);
                if (crcValue != crc.getValue())
                    throw new IOException(CRC_ERROR);
                hdr = new TxnHeader();
                record = SerializeUtils.deserializeTxn(bytes, hdr);
            } catch (EOFException e) {
                LOG.debug("EOF exception " + e);
                inputStream.close();
                inputStream = null;
                ia = null;
                hdr = null;
                // this means that the file has ended
                // we should go to the next file
                if (!goToNextLog()) {
                    return false;
                }
                // if we went to the next log file, we should call next() again
                return next();
            } catch (IOException e) {
                inputStream.close();
                throw e;
            }
            return true;
        }

        /**
         * return the current header
         * @return the current header that
         * is read
         */
        public TxnHeader getHeader() {
            return hdr;
        }

        /**
         * return the current transaction
         * @return the current transaction
         * that is read
         */
        public Record getTxn() {
            return record;
        }

        /**
         * close the iterator
         * and release the resources.
         */
        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        /**
         * Return total storage size of txnlog that will return by this iterator.
         */
        public long getStorageSize() {
            long sum = 0;
            for (File f : storedFiles) {
                sum += f.length();
            }
            return sum;
        }

        /**
         * 转到下一个日志文件
         *
         * @return true if there is one and false if there is no
         * new file to be read
         * @throws IOException
         */
        private boolean goToNextLog() throws IOException {
            if (storedFiles.size() > 0) {
                this.logFile = storedFiles.remove(storedFiles.size() - 1);
                ia = createInputArchive(this.logFile);
                return true;
            }
            return false;
        }

        /**
         * read the header from the inputarchive
         * @param ia the inputarchive to be read from
         * @param is the inputstream
         * @throws IOException
         */
        protected void inStreamCreated(InputArchive ia, InputStream is) throws IOException {
            FileHeader header = new FileHeader();
            header.deserialize(ia, "fileheader");
            if (header.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
                throw new IOException("Transaction log: " + this.logFile + " has invalid magic number "
                        + header.getMagic()
                        + " != " + FileTxnLog.TXNLOG_MAGIC);
            }
        }

        /**
         * Invoked to indicate that the input stream has been created.
         * @throws IOException
         **/
        protected InputArchive createInputArchive(File logFile) throws IOException {
            if (inputStream == null) {
                inputStream = new PositionInputStream(new BufferedInputStream(new FileInputStream(logFile)));
                LOG.debug("Created new input stream " + logFile);
                ia = BinaryInputArchive.getArchive(inputStream);
                inStreamCreated(ia, inputStream);
                LOG.debug("Created new input archive " + logFile);
            }
            return ia;
        }

        /**
         * create a checksum algorithm
         * @return the checksum algorithm
         */
        protected Checksum makeChecksumAlgorithm() {
            return new Adler32();
        }


    }

}

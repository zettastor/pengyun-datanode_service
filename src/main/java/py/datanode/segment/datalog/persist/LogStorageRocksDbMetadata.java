/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.datanode.segment.datalog.persist;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang.Validate;
import org.rocksdb.ColumnFamilyHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.configuration.LogPersistRocksDbConfiguration;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogReaderWriterFactory;
import py.third.rocksdb.KvStoreException;
import py.third.rocksdb.RocksDbColumnFamilyHandle;

public class LogStorageRocksDbMetadata extends LogStorageMetadata {
  private static final Logger logger = LoggerFactory.getLogger(LogStorageRocksDbMetadata.class);
  private final LogStorageRocksDbIndexHandle handle;
  private final ReadWriteLock maxLogIdLock = new ReentrantReadWriteLock();
  private final long archiveId;

  public LogStorageRocksDbMetadata(LogPersistRocksDbConfiguration cfg,
      MutationLogReaderWriterFactory factory,
      SegId segId, long archiveId) {
    super(factory, cfg.getMaxNumLogsOfOneSegmentunit(), 0, LogImage.IMPOSSIBLE_LOG_ID, segId);
    super.setLogReader(factory.generateReader());
    super.setLogWriter(factory.generateWriter());
    assert (factory.generateReader() != null);
    assert (factory.generateWriter() != null);

    handle = new LogStorageRocksDbIndexHandle(cfg, segId, archiveId, factory);
    this.archiveId = archiveId;
  }

  public boolean init(boolean createIfMissing) throws IOException, KvStoreException {
    if (handle.open(createIfMissing)) {
      super.addNumLogs(handle.recordNums() - super.getNumLogs());
      super.setMaxLogId(handle.getCurrentMaxLogId());
      logger.warn("open rocks db column family success, seg is {}, archive is {}", getSegId(),
          archiveId);
      logger.warn("open rocks db column family success, seg is {}, archive is {}", getSegId(),
          archiveId);
      return true;
    }

    logger.error("open column family failed for segment unit(segment id {})", getSegId());
    return false;
  }

  public boolean put(MutationLogEntry log) throws IOException, KvStoreException {
    if (this.needCleanOldLogs()) {
      logger.debug("begin clear log file for write: {}", this.handle.getColumnFamilyName());
      this.clearOldLogs(this.getClearNumsDefault());
      logger.info("clear old logs once times, current log number is {}", handle.recordNums());
    }

    if (handle.put(log.getLogId(), log)) {
     

      setMaxLogId(log.getLogId());
     

      return true;
    }

    logger.error("put MutationLogEntry failed, segment id {}, column handle is {}",
        getSegId(), handle.getColumnFamilyHandle());
    return false;
  }

  public MutationLogEntry get(long logId) throws IOException, KvStoreException {
    return handle.get(logId);
  }

  public void delete(long logId) throws IOException, KvStoreException {
   
   
    handle.delete(logId);
  }

  public boolean exist(long logId) throws IOException, KvStoreException {
    return handle.exist(logId);
  }

  public void flush() throws KvStoreException {
    handle.sync();
  }

  public List<MutationLogEntry> getLatestLogs(int maxLogs) throws IOException {
    return handle.getLatestLogs(maxLogs);
  }

  public List<MutationLogEntry> getLogsAfter(long logId, int maxNums) throws IOException {
    return handle.getLogsAfter(logId, maxNums);
  }

  public int clearOldLogs(int numLogs) throws IOException, KvStoreException {
    return handle.clearOldLogs(numLogs);
  }

  public void clearAllLogs() throws IOException, KvStoreException {
    handle.clearAllLogs();
    return;
  }

  public void close() {
    try {
      if (super.getLogWriter() != null) {
        super.getLogWriter().close();
      }
      if (super.getLogReader() != null) {
        super.getLogReader().close();
      }
    } catch (IOException e) {
      logger.error("close resources failed", e);
    }
  }

  public void closeAll() {
    close();

    handle.closeColumnFamily();
  }

  public boolean needCleanOldLogs() throws IOException {
    int numLogs = handle.recordNums();
    if (numLogs >= super.getCapacity() * 4 / 5) {
      logger.info("the logs number reached {} , segId {}, we need clear old logs", numLogs,
          getSegId());
      return true;
    }
    return false;
  }

  public int getClearNumsDefault() {
    return super.getCapacity() * 1 / 5;
  }

  public boolean moreSpace() {
    try {
      return handle.recordNums() <= super.getCapacity();
    } catch (IOException e) {
      logger.error("more space get some exception", e);
      return false;
    }
  }

  public int leftSpace() {
    try {
      return super.getCapacity() - handle.recordNums();
    } catch (IOException e) {
      logger.error("left space get some exception", e);
      return 0;
    }
  }

  public int getNumLogs() {
    try {
      return handle.recordNums();
    } catch (IOException e) {
      logger.error("left space get some exception", e);
      return 0;
    }
  }

  public long getMaxLogId() {
    maxLogIdLock.readLock().lock();
    long id = super.getMaxLogId();
    maxLogIdLock.readLock().unlock();
    return id;
  }

  public void setMaxLogId(long id) {
    maxLogIdLock.writeLock().lock();
    if (id > super.getMaxLogId()) {
      super.setMaxLogId(id);
    }
    maxLogIdLock.writeLock().unlock();
  }

  public long getMinLogId() throws IOException {
    return handle.getMinLogId();
  }

  public boolean checkColumnFamilyBySegId(SegId segId) {
    if (segId.getIndex() != getSegId().getIndex()
        || segId.getVolumeId() != getSegId().getVolumeId()) {
      logger.warn("is not our segment id. super segment is {}, param segment id {}",
          getSegId(), segId);
      return false;
    }

    return handle.getColumnFamilyName()
        .equals(LogStorageRocksDbIndexHandle.packColumnFamilyNameBySegId(segId));
  }

  public boolean checkColumnFamilyByArchiveId(long archiveId) {
    if (archiveId != this.archiveId) {
      logger.warn("is not our segment id. super archive is {}, param archive id {}",
          this.archiveId, archiveId);
      return false;
    }

    return handle.getColumnFamilyName()
        .equals(LogStorageRocksDbIndexHandle.packColumnFamilyNameByArchiveId(archiveId));
  }

  public void deleteColumnFamily(byte[] name) throws KvStoreException {
    Validate.isTrue(Arrays.equals(name, handle.getColumnFamilyName().getBytes()));
    handle.deleteColumnFamily();
  }

  public void deleteColumnFamily() throws KvStoreException {
    handle.deleteColumnFamily();
  }

  public RocksDbColumnFamilyHandle getRocksDbColumnFamilyHandle() {
    return handle;
  }

  public ColumnFamilyHandle getColumnFamilyHandle() {
    return handle.getColumnFamilyHandle();
  }

}

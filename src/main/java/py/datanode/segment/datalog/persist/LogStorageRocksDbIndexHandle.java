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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.configuration.LogPersistRocksDbConfiguration;
import py.datanode.rocksdb.RocksDbOptionConfigurationFactory;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryReader;
import py.datanode.segment.datalog.MutationLogEntryWriter;
import py.datanode.segment.datalog.MutationLogReaderWriterFactory;
import py.exception.InvalidFormatException;
import py.third.rocksdb.KvRocksDbExceptionFactory;
import py.third.rocksdb.KvStoreException;
import py.third.rocksdb.TemplateRocksDbColumnFamilyHandle;

public class LogStorageRocksDbIndexHandle
    extends
    TemplateRocksDbColumnFamilyHandle<LogStorageRocksDbIndexKey, LogStorageRocksDbIndexValue> {
  private static final Logger logger = LoggerFactory.getLogger(LogStorageRocksDbIndexHandle.class);
  private final SegId segId;
  private final long archiveId;
  private final MutationLogReaderWriterFactory factory;
  private final LogPersistRocksDbConfiguration configuration;
  private final MutationLogEntryWriter writer;
  private final MutationLogEntryReader reader;

  public LogStorageRocksDbIndexHandle(LogPersistRocksDbConfiguration configuration, SegId segId,
      long archiveId,
      MutationLogReaderWriterFactory factory) {
    super(RocksDbOptionConfigurationFactory.generate(configuration));

    this.segId = segId;
    this.configuration = configuration;
    this.factory = factory;
    this.writer = factory.generateWriter();
    this.reader = factory.generateReader();

    this.archiveId = archiveId;
  }

  public static String packColumnFamilyNameBySegId(SegId segId) {
    String columnFamilyName = "seg_" + segId.getVolumeId().getId() + "." + segId.getIndex();
    return columnFamilyName;
  }

  public static SegId parseSegmentIdFromColumnFamily(String family) {
    String[] strs = family.split("[._]");
    if (strs.length != 3) {
      throw new InvalidFormatException(family + " has a wrong format of column family name");
    }

    try {
      return new SegId(Long.parseLong(strs[1]), Integer.parseInt(strs[2]));
    } catch (NumberFormatException e) {
      throw new InvalidFormatException(family + " has a wrong format", e);
    }
  }

  public static String packColumnFamilyNameByArchiveId(long archiveId) {
    String columnFamliyName = "COLUMN_WITH_ARCHIVE_" + archiveId;
    return columnFamliyName;
  }

  public static long parseArchiveIdFromColumnFamilyName(String family) {
    String[] strs = family.split("[_]");
    if (strs.length != 4) {
      throw new InvalidFormatException(family + " has a wrong format of column family name");
    }

    try {
      return Long.parseLong(strs[3]);
    } catch (NumberFormatException e) {
      throw new InvalidFormatException(family + " has a wrong format", e);
    }
  }

  public String packColumnFamilyName() {
   
    return LogStorageRocksDbIndexHandle.packColumnFamilyNameByArchiveId(archiveId);
  }

  @Override
  protected boolean needCacheRecordsNumInMemory() {
    return true;
  }

  public boolean put(long logId, MutationLogEntry log) throws IOException, KvStoreException {
   
    boolean result = super.put(new LogStorageRocksDbIndexKey(logId, segId),
        new LogStorageRocksDbIndexValue(log, factory, writer, reader));
    logger.debug("rocks(segId {}) put record key {}", segId, logId);
    return result;
  }

  public MutationLogEntry get(long logId) throws IOException, KvStoreException {
    LogStorageRocksDbIndexKey key = new LogStorageRocksDbIndexKey(logId, segId);
    byte[] keyBytes = new byte[key.size()];
    try {
      key.serialize(keyBytes);
    } catch (IOException ex) {
      logger.error("serialize logId failed for get MutationLogEntry from rocks db", ex);
      throw ex;
    }
    byte[] valueBytes = super.get(keyBytes);

    return deserializerValue(valueBytes);
  }

  public void delete(long logId) throws IOException, KvStoreException {
    super.delete(new LogStorageRocksDbIndexKey(logId, segId));
  }

  public boolean exist(long logId) throws IOException, KvStoreException {
    return super.exist(new LogStorageRocksDbIndexKey(logId, segId));
  }

  protected MutationLogEntry deserializerValue(byte[] valueBytes) throws IOException {
    if (null == valueBytes) {
      return null;
    }

    MutationLogEntry log = null;
    ByteArrayInputStream inputStream = new ByteArrayInputStream(valueBytes);
    synchronized (reader) {
      try {
        reader.open(inputStream);
        log = reader.read();
      } catch (IOException ex) {
        logger.error("log writer entry write log failed", ex);
        throw ex;
      } finally {
        try {
          reader.close();
        } catch (Exception ex) {
          logger.error("close log reader failed", ex);
        }
      }
    }

    return log;
  }

  public List<MutationLogEntry> getLogsAfter(long logId, int maxNums) throws IOException {
    List<MutationLogEntry> listLogs = new ArrayList<>();
    RocksIterator iterator = super.getRocksDb().newIterator(super.getColumnFamilyHandle());
    try {
      LogStorageRocksDbIndexKey key = new LogStorageRocksDbIndexKey(logId, segId);
      byte[] serializeKey = new byte[key.size()];
      key.serialize(serializeKey);
      iterator.seek(serializeKey);

      if (iterator.isValid() && Arrays.equals(iterator.key(), serializeKey)) {
        iterator.next();
      }

      while (iterator.isValid() && maxNums-- > 0) {
        key.deserialize(iterator.key());
        if (key.getVolumeId() != segId.getVolumeId().getId() || key.getIndexId() != segId
            .getIndex()) {
          break;
        }
        listLogs.add(deserializerValue(iterator.value()));
        iterator.next();
      }
      if (0 == listLogs.size() && this.recordNums() != 0) {
        logger.warn("has not got any logs, but record number is {}, segId {}, logId {}",
            this.recordNums(), segId,
            logId);
        if (iterator.isValid()) {
          key.deserialize(iterator.key());
          logger.warn("rocks db seek first log is {} {} {}", key.getVolumeId(), key.getIndexId(),
              key.getLogId());
        } else {
          logger.warn("rocks db has not seek any records");
        }
      }
    } finally {
      iterator.close();
    }

    return listLogs;
  }

  public List<MutationLogEntry> getLatestLogs(int maxNums) throws IOException {
    List<MutationLogEntry> listLogs = new ArrayList<>();
    RocksIterator iterator = super.getRocksDb().newIterator(super.getColumnFamilyHandle());
    try {
      LogStorageRocksDbIndexKey key = new LogStorageRocksDbIndexKey(Long.MAX_VALUE, segId);

      byte[] bytes = new byte[key.size()];
      key.serialize(bytes);
      iterator.seekForPrev(bytes);

      while (iterator.isValid() && maxNums-- > 0) {
        key.deserialize(iterator.key());
        if (key.getVolumeId() != segId.getVolumeId().getId() || key.getIndexId() != segId
            .getIndex()) {
          break;
        }

        listLogs.add(deserializerValue(iterator.value()));
        iterator.prev();
      }

      Collections.reverse(listLogs);
    } finally {
      iterator.close();
    }
    return listLogs;
  }

  public int clearOldLogs(int numRecords) throws IOException, KvStoreException {
    int countLogs = numRecords;
    RocksIterator iterator = getRocksDb().newIterator(getColumnFamilyHandle());
    RocksIterator beginItr = getRocksDb().newIterator(getColumnFamilyHandle());
    try {
      LogStorageRocksDbIndexKey key = new LogStorageRocksDbIndexKey(0, segId);
      byte[] serializeKey = new byte[key.size()];
      key.serialize(serializeKey);
      iterator.seek(serializeKey);

      beginItr.seek(serializeKey);

      while (iterator.isValid() && countLogs > 0) {
        key.deserialize(iterator.key());
        if (key.getVolumeId() != segId.getVolumeId().getId() || key.getIndexId() != segId
            .getIndex()) {
          break;
        }

        countLogs--;
        iterator.next();
      }

      if (!iterator.isValid()) {
        key.setLogId(Long.MAX_VALUE);
        key.serialize(serializeKey);

        iterator.seekForPrev(serializeKey);
        getRocksDb().deleteRange(getColumnFamilyHandle(), beginItr.key(), iterator.key());

        key.deserialize(beginItr.key());
        logger.debug("clear old log, begin key is {} {} {}", key.getVolumeId(), key.getIndexId(),
            key.getLogId());
        key.deserialize(iterator.key());
        logger.debug("clear old log, end   key is {} {} {}", key.getVolumeId(), key.getIndexId(),
            key.getLogId());

        key.deserialize(iterator.key());
        if (key.getVolumeId() == segId.getVolumeId().getId() || key.getIndexId() == segId
            .getIndex()) {
          getRocksDb().delete(getColumnFamilyHandle(), iterator.key());
        }
      } else {
        getRocksDb().deleteRange(getColumnFamilyHandle(), beginItr.key(), iterator.key());

        key.deserialize(beginItr.key());
        logger.debug("clear old log, begin key is {} {} {}", key.getVolumeId(), key.getIndexId(),
            key.getLogId());
        key.deserialize(iterator.key());
        logger.debug("clear old log, end   key is {} {} {}", key.getVolumeId(), key.getIndexId(),
            key.getLogId());
      }

      if (needCacheRecordsNumInMemory()) {
        incAndGet(countLogs - numRecords);
      }
     
      logger.info("{} logs has been clear, segId {}", countLogs - numRecords, segId);
    } catch (RocksDBException ex) {
      logger.error("put rocks db failed, rocks db exception code is {}, subcode is {}",
          ex.getStatus().getCode(),
          ex.getStatus().getSubCode(), ex);
      throw KvRocksDbExceptionFactory.build(ex);
    } finally {
      iterator.close();
      beginItr.close();
    }
    return numRecords - countLogs;
  }

  public long getCurrentMaxLogId() throws IOException {
    RocksIterator iterator = super.getRocksDb().newIterator(super.getColumnFamilyHandle());
    try {
      LogStorageRocksDbIndexKey key = new LogStorageRocksDbIndexKey(Long.MAX_VALUE, segId);
      byte[] bytes = new byte[key.size()];

      key.serialize(bytes);
      iterator.seekForPrev(bytes);

      if (iterator.isValid()) {
        key.deserialize(iterator.key());

        if (key.getVolumeId() == segId.getVolumeId().getId() && key.getIndexId() == segId
            .getIndex()) {
          return key.getLogId();
        }
      }
    } finally {
      iterator.close();
    }

    return LogImage.IMPOSSIBLE_LOG_ID;
  }

  public void clearAllLogs() throws IOException, KvStoreException {
    RocksIterator iteratorBegin = getRocksDb().newIterator(this.getColumnFamilyHandle());
    RocksIterator iteratorEnd = getRocksDb().newIterator(this.getColumnFamilyHandle());

    try {
      LogStorageRocksDbIndexKey key = new LogStorageRocksDbIndexKey(0, segId);
      byte[] bytes = new byte[key.size()];
      key.serialize(bytes);
      iteratorBegin.seek(bytes);

      key.setLogId(Long.MAX_VALUE);
      key.serialize(bytes);
      iteratorEnd.seekForPrev(bytes);

      key.deserialize(iteratorEnd.key());

      if (key.getVolumeId() != segId.getVolumeId().getId() || key.getIndexId() != segId
          .getIndex()) {
        return;
      }
      getRocksDb().deleteRange(getColumnFamilyHandle(), iteratorBegin.key(), iteratorEnd.key());
      getRocksDb().delete(getColumnFamilyHandle(), iteratorEnd.key());
    } catch (RocksDBException ex) {
      logger.error("delete range from rocks db failed", ex);
      throw KvRocksDbExceptionFactory.build(ex);
    } finally {
      iteratorBegin.close();
      iteratorEnd.close();
    }
  }

  public long getMinLogId() throws IOException {
    RocksIterator iterator = super.getRocksDb().newIterator(super.getColumnFamilyHandle());
    try {
      LogStorageRocksDbIndexKey key = new LogStorageRocksDbIndexKey(0, segId);
      byte[] bytes = new byte[key.size()];

      key.serialize(bytes);
      iterator.seek(bytes);

      if (iterator.isValid()) {
        key.deserialize(iterator.key());

        if (key.getVolumeId() == segId.getVolumeId().getId()
            && key.getIndexId() == segId.getIndex()) {
          return key.getLogId();
        }
      }
    } finally {
      iterator.close();
    }

    return LogImage.IMPOSSIBLE_LOG_ID;
  }

  public int getRowNum() throws IOException {
    int num = 0;
    RocksIterator iterator = getRocksDb().newIterator(this.getColumnFamilyHandle());

    try {
      LogStorageRocksDbIndexKey key = new LogStorageRocksDbIndexKey(0, segId);
      byte[] beginBytes = new byte[key.size()];
      key.serialize(beginBytes);
      iterator.seek(beginBytes);

      while (iterator.isValid()) {
        key.deserialize(iterator.key());
        if (key.getVolumeId() != segId.getVolumeId().getId() || key.getIndexId() != segId
            .getIndex()) {
          break;
        }

        num++;
        iterator.next();
      }
    } finally {
      iterator.close();
    }
    return num;
  }

}

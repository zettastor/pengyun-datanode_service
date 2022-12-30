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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.struct.Pair;
import py.datanode.archive.RawArchive;
import py.datanode.configuration.LogPersistRocksDbConfiguration;
import py.datanode.configuration.RocksDbPathConfig;
import py.datanode.exception.LogIdNotFoundException;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.rocksdb.RocksDbOptionConfigurationFactory;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogReaderWriterFactory;
import py.third.rocksdb.KvStoreException;
import py.third.rocksdb.RocksDbBaseHelper;
import py.third.rocksdb.RocksDbColumnFamilyHandle;
import py.third.rocksdb.RocksDbOptionConfiguration;

public class LogRocksDbSystem implements LogStorageSystem {
  private static final Logger logger = LoggerFactory.getLogger(LogRocksDbSystem.class);
  private final MutationLogReaderWriterFactory factory;
  private LogPersistRocksDbConfiguration cfg;
 
 
  private Map<SegId, LogStorageRocksDbMetadata> logRocksDbImpl = 
      new ConcurrentHashMap<SegId, LogStorageRocksDbMetadata>();
  private RocksDbColumnFamilyHandle rocksDbColumnFamilyHandleDefault;
  private SegmentUnitManager segmentUnitManager;

  public LogRocksDbSystem(LogPersistRocksDbConfiguration cfg,
      MutationLogReaderWriterFactory factory,
      SegmentUnitManager segmentUnitManager) throws IOException, KvStoreException {
    logger.warn("configuration: {} for log file system", cfg);
    this.cfg = cfg;
    this.factory = factory;
    this.segmentUnitManager = segmentUnitManager;
    init();
  }

  public Map<SegId, LogStorageMetadata> init() throws IOException, KvStoreException {
    try {
      Path dbPath = FileSystems.getDefault()
          .getPath(cfg.getRocksDbPathConfig().getLogPersistRocksDbPath());
      Files.createDirectories(dbPath);

      RocksDbOptionConfiguration rocksDbOptionConfiguration = RocksDbOptionConfigurationFactory
          .generate(cfg);
      if (!RocksDbBaseHelper
          .openDatabaseAndCreateIfMissing(rocksDbOptionConfiguration.generateDbOptions(),
              rocksDbOptionConfiguration.getDbRootPath())) {
        logger.error("create rocks db failed, cfg : {}", rocksDbOptionConfiguration);
      }

      rocksDbColumnFamilyHandleDefault = new LogStorageSegmentAndColumnMapHandle(
          rocksDbOptionConfiguration.getDbRootPath());
      rocksDbColumnFamilyHandleDefault.open(true);
      LogStorageSegmentAndColumnMapKey key = new LogStorageSegmentAndColumnMapKey(new SegId(0, 0));
      LogStorageSegmentAndColumnMapValue value = new LogStorageSegmentAndColumnMapValue(0);
      List<Pair<byte[], byte[]>> list = rocksDbColumnFamilyHandleDefault
          .getLatestRecords(Integer.MAX_VALUE);
      for (Pair<byte[], byte[]> item : list) {
        logger.info("begin initialize segment unit rocks db column family, name is {}",
            new String(item.getSecond()));
        SegId segId;
        long archiveId;
        try {
          key.deserialize(item.getFirst());
          segId = new SegId(key.getVolumeId(), key.getIndexId());
          value.deserialize(item.getSecond());
          archiveId = value.getArchiveId();
        } catch (IOException e) {
          logger.error(
              "caught an exception when parsing seg or archive. Delete the current family {}", item,
              e);
          rocksDbColumnFamilyHandleDefault.delete(item.getFirst());
          continue;
        }

        if (!initLogSystemForSeg(segId, archiveId)) {
          logger.error("init log meta data for segId {} failed", segId);
          throw new IOException("initLogSystemForSeg failed");
        }

        if (null == segmentUnitManager.get(segId)
            || segmentUnitManager.get(segId).getArchive().getArchiveId() != archiveId) {
          removeLogStorage(segId);
        }
      }

      List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
      List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor("dummy".getBytes(),
              rocksDbOptionConfiguration.generateColumnFamilyOptions())
      );
      RocksDbBaseHelper.open(
          rocksDbOptionConfiguration.generateDbOptions().setCreateMissingColumnFamilies(true),
          cfg.getRocksDbPathConfig().getLogPersistRocksDbPath(),
          columnFamilyDescriptors,
          columnFamilyHandleList);
      RocksDbBaseHelper.dropColumnFamily("dummy".getBytes(),
          cfg.getRocksDbPathConfig().getLogPersistRocksDbPath());
    } catch (KvStoreException e) {
      logger.error("init log storage system failed, rocks db throw some exception", e);

      if (cfg.getRocksDbPathConfig().getLogPersisterPathType() 
          == RocksDbPathConfig.LogPersisterPathType.LOG_PERSISTER_PATH_USING_DEFAULT) {
        logger.error(
            "current path has using default path, no need to try to reopen log storage system");
        throw e;
      }

      cfg.getRocksDbPathConfig().setLogPersisterPathType(
          RocksDbPathConfig.LogPersisterPathType.LOG_PERSISTER_PATH_USING_DEFAULT);
      logger.error("convert to default log persist path {} and try to reopen log storage system",
          cfg.getRocksDbPathConfig().getLogPersistRocksDbPath());

      return init();
    }

    return null;
  }

  public synchronized boolean initLogSystemForSeg(SegId segId, long archiveId)
      throws IOException, KvStoreException {
    try {
      LogStorageRocksDbMetadata rocksDb = this.logRocksDbImpl.get(segId);
      if (rocksDb != null) {
        rocksDb.close();
      }

      rocksDb = new LogStorageRocksDbMetadata(cfg, factory, segId, archiveId);
      if (!rocksDb.init(true)) {
        logger.error("open rocks db failed");
        return false;
      }
      this.logRocksDbImpl.put(segId, rocksDb);
    } catch (KvStoreException e) {
      logger.error("can not create column family in rocks db");
      throw e;
    }

    return true;
  }

  public boolean putLogToRocksDb(SegId segId, MutationLogEntry log)
      throws IOException, LogIdTooSmall, KvStoreException {
    boolean result;
    LogStorageRocksDbMetadata rocksDbColumnFamily = this.logRocksDbImpl.get(segId);
    if (rocksDbColumnFamily == null) {
      rocksDbColumnFamily = openOrCreateLogRocksDbMetadata(segId, true);
    }

    if (log.getLogId() < rocksDbColumnFamily.getMaxLogId()) {
      logger.warn("log id {} is too small to persist. The logStorageMetadata is {}, max log:{} ",
          log.getLogId(),
          this, rocksDbColumnFamily.getMaxLogId());
      throw new LogIdTooSmall();
    }

    result = rocksDbColumnFamily.put(log);
    if (!result) {
      logger.error("put log into rocks db failed, db path is {}, segment id is {}",
          cfg.getRocksDbPathConfig().getLogPersistRocksDbPath(), segId);
      for (ConcurrentHashMap.Entry<SegId, LogStorageRocksDbMetadata> entry : this.logRocksDbImpl
          .entrySet()) {
        logger.error("segment volume id is {} index id is {}, handle is {}",
            entry.getKey().getVolumeId(),
            entry.getKey().getIndex(), entry.getValue().getColumnFamilyHandle());
      }
    }
    return result;
  }

  /**
   * To determine whether a log ID.
   */
  public boolean logIdExists(SegId segId, long id) throws IOException, KvStoreException {
    LogStorageRocksDbMetadata rocksDbColumnFamily = this.logRocksDbImpl.get(segId);
    if (rocksDbColumnFamily == null) {
      try {
        rocksDbColumnFamily = openOrCreateLogRocksDbMetadata(segId, false);
      } catch (KvStoreException e) {
        logger.warn("open column family failed, create if missing is false", e);
        return false;
      }
      if (null == rocksDbColumnFamily) {
        return false;
      }
    }

    logger
        .debug("id {} >= minLogID {} && <= getMaxLogId {} ?", id, rocksDbColumnFamily.getMinLogId(),
            rocksDbColumnFamily.getMaxLogId());
    return id >= rocksDbColumnFamily.getMinLogId() && id <= rocksDbColumnFamily.getMaxLogId();
   
  }

  public List<MutationLogEntry> readLatestLogs(SegId segId, int maxNums)
      throws IOException, KvStoreException {
    LogStorageRocksDbMetadata rocksDbColumnFamily = this.logRocksDbImpl.get(segId);
    List<MutationLogEntry> listValues = new ArrayList<>();
    if (rocksDbColumnFamily == null) {
      try {
        rocksDbColumnFamily = openOrCreateLogRocksDbMetadata(segId, false);
      } catch (KvStoreException e) {
        logger.warn("open column family failed, create if missing is false", e);
        return listValues;
      }
    }

    if (rocksDbColumnFamily != null) {
      listValues = rocksDbColumnFamily.getLatestLogs(maxNums);
    }

    return listValues;
  }

  public List<MutationLogEntry> readLogsAfter(SegId segId, long id, int maxNum)
      throws IOException, LogIdNotFoundException, KvStoreException {
    return readLogsAfter(segId, id, maxNum, false);
  }

  public List<MutationLogEntry> readLogsAfter(SegId segId, long logId, int maxNum,
      boolean checkIdExists)
      throws IOException, LogIdNotFoundException, KvStoreException {
    LogStorageRocksDbMetadata rocksDbColumnFamily = this.logRocksDbImpl.get(segId);
    List<MutationLogEntry> listValues = new ArrayList<>();
    if (rocksDbColumnFamily == null) {
      try {
        rocksDbColumnFamily = openOrCreateLogRocksDbMetadata(segId, false);
      } catch (KvStoreException e) {
        logger.warn("open column family failed, create if missing is false", e);
        return listValues;
      }
    }

    if (null != rocksDbColumnFamily) {
      listValues = rocksDbColumnFamily.getLogsAfter(logId, maxNum);
    }
    if (listValues.size() == 0 && checkIdExists) {
      throw new LogIdNotFoundException("can not find the log: ", null, null);
    }
    return listValues;
  }

  protected void clearLogsInOneSegmentunit(LogStorageRocksDbMetadata rocksDbColumnFamily)
      throws IOException, KvStoreException {
    int numLogsNeedClear = cfg.getMaxNumLogsOfOneSegmentunit() / 5;
    rocksDbColumnFamily.clearOldLogs(numLogsNeedClear);
    return;
  }

  private LogStorageRocksDbMetadata openOrCreateLogRocksDbMetadata(SegId segId,
      boolean createIfMissing)
      throws IOException, KvStoreException {
    if (!openColumnFamily(segId, createIfMissing)) {
      logger.error("open rocks db column family ({}, {}) failed", segId, createIfMissing);
      if (createIfMissing) {
        throw new IOException("create rocks db column family failed");
      }
      return null;
    }

    return this.logRocksDbImpl.get(segId);
  }

  @Override
  public LogStorageMetadata getLogStorageToWrite(SegId segId) {
    return this.logRocksDbImpl.get(segId);
  }

  @Override
  public LogStorageMetadata createLogStorageToWrite(SegId segId, long id,
      LogStorageMetadata originalStorageMetadata)
      throws IOException {
    throw new IOException("LogRocksDBSystem has not support createLogStorageToWrite interface");
  }

  @Override
  public LogStorageMetadata getOrCreateLogStorageToWrite(SegId segId, long id) throws IOException {
    return getLogStorageToWrite(segId);
  }

  @Override
  public LogStorageMetadata getLatestLogStorageToRead(SegId segId) throws IOException {
    throw new IOException("LogRocksDBSystem has not support getLatestLogStorageToRead interface");
  }

  @Override
  public LogStorageMetadata getLogStorageToRead(SegId segId, long id)
      throws IOException, LogIdNotFoundException {
    throw new IOException("LogRocksDBSystem has not support getLogStorageToRead interface");
  }

  @Override
  public LogStorageMetadata createLogStorageWhenNoLogFileExist(SegId segId, long id)
      throws IOException, KvStoreException {
    LogStorageRocksDbMetadata metadata = openOrCreateLogRocksDbMetadata(segId, true);
    if (null == metadata) {
      throw new IOException("create rocks db column family failed");
    }
    return metadata;
  }

  @Override
  public synchronized void close() {
    try {
      if (null != rocksDbColumnFamilyHandleDefault) {
        rocksDbColumnFamilyHandleDefault.closeDb();
      }
    } catch (Exception e) {
      logger.error("close rocks db catch some exception", e);
    }
    for (Map.Entry<SegId, LogStorageRocksDbMetadata> entry : this.logRocksDbImpl.entrySet()) {
      LogStorageRocksDbMetadata logStorageRocksDbMetadata = entry.getValue();
      if (logStorageRocksDbMetadata != null) {
        RocksDbColumnFamilyHandle rocksDbColumnFamilyHandle = logStorageRocksDbMetadata
            .getRocksDbColumnFamilyHandle();
        if (null != rocksDbColumnFamilyHandle) {
          try {
            rocksDbColumnFamilyHandle.closeDb();
          } catch (Exception e) {
            logger.error("close rocks db catch some exception", e);
          }
        }

        logStorageRocksDbMetadata.closeAll();
      }
    }
    this.logRocksDbImpl.clear();
  }

  @Override
  public synchronized void removeLogStorage(SegId segId) throws IOException, KvStoreException {
    LogStorageRocksDbMetadata rocksDbColumnFamily = this.logRocksDbImpl.remove(segId);
    if (rocksDbColumnFamily == null) {
      return;
    }

    rocksDbColumnFamily.clearAllLogs();
    LogStorageSegmentAndColumnMapKey key = new LogStorageSegmentAndColumnMapKey(segId);
    byte[] bytes = new byte[key.size()];
    key.serialize(bytes);
    rocksDbColumnFamilyHandleDefault.delete(bytes);
  }

  protected synchronized boolean openColumnFamily(SegId segId, boolean createIfMissing)
      throws IOException, KvStoreException {
    if (null != this.logRocksDbImpl.get(segId)) {
      return true;
    }
    logger.info("begin open rocks db column family");

    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    RawArchive archive = segmentUnit.getArchive();
    long archiveId = segmentUnitManager.get(segId).getArchive().getArchiveId();
    LogStorageRocksDbMetadata rocksDbColumnFamily = new LogStorageRocksDbMetadata(cfg, factory,
        segId, archiveId);
    if (!rocksDbColumnFamily.init(createIfMissing)) {
      logger.error("open rocks db failed");
      return false;
    }

    LogStorageSegmentAndColumnMapKey key = new LogStorageSegmentAndColumnMapKey(segId);
    byte[] bytes = new byte[key.size()];
    key.serialize(bytes);
    LogStorageSegmentAndColumnMapValue value = new LogStorageSegmentAndColumnMapValue(archiveId);
    byte[] valuebytes = new byte[value.size()];
    value.serialize(valuebytes);
    rocksDbColumnFamilyHandleDefault.put(bytes, valuebytes);

    logRocksDbImpl.put(segId, rocksDbColumnFamily);

    return true;
  }
}
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
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.MutationLogEntry;
import py.third.rocksdb.KvStoreException;

public class LogDbPersister extends LogPersister {
  private static final Logger logger = LoggerFactory.getLogger(LogDbPersister.class);
  private LogRocksDbSystem logRocksDbSystem;
  private SegmentUnitManager segUnitManager;

  public LogDbPersister(LogStorageSystem logStorageSystem, SegmentUnitManager segmentUnitManager)
      throws IOException {
    super(null, segmentUnitManager);
    Validate.isTrue(logStorageSystem instanceof LogRocksDbSystem,
        "log db persister init by invalid log db system");
    this.logRocksDbSystem = (LogRocksDbSystem) logStorageSystem;
    this.segUnitManager = segmentUnitManager;

  }

  @Override
  public void persistLog(SegId segId, MutationLogEntry logEntry)
      throws IOException, LogIdTooSmall, KvStoreException {
    if (logEntry == null) {
      return;
    }

    List<MutationLogEntry> logs = new ArrayList<MutationLogEntry>();
    logs.add(logEntry);
    persistLog(segId, logs);
  }

  @Override
  public void persistLog(SegId segId, List<MutationLogEntry> logs)
      throws IOException, LogIdTooSmall, KvStoreException {
    if (logs == null || logs.isEmpty()) {
      return;
    }

    logger.debug("persistLog log list num is {}", logs.size());
    
    SegmentUnit segUnit = segUnitManager.get(segId);
    if (segUnit == null
        || segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleted) {
      logger.warn("segunit is null or deleted, no need to persist log, segId is {}", segId);
      return;
    }

    LogStorageRocksDbMetadata rocksDbColumnFamily = (LogStorageRocksDbMetadata) logRocksDbSystem
        .getLogStorageToWrite(segId);
    long firstLogId = logs.get(0).getLogId();
    if (rocksDbColumnFamily == null) {
     
      rocksDbColumnFamily = (LogStorageRocksDbMetadata) logRocksDbSystem
          .createLogStorageWhenNoLogFileExist(segId, firstLogId);
      logger.debug("create logStorageMetadata {} for segId {}", rocksDbColumnFamily, segId);
    }

    for (MutationLogEntry log : logs) {
      logger.debug("begin put log entry id {}", log.getLogId());
      if (!logRocksDbSystem.putLogToRocksDb(segId, log)) {
        logger.warn("put log to rocks db failed {}, {}", segId, log);
        throw new IOException(" put log to rocks db failed");
      }
    }
  }

  /**
   * flush rocks db cache in memory to disk for segment id.
   *
   */
  public void flush(SegId segId) throws IOException, KvStoreException {
    
    SegmentUnit segUnit = segUnitManager.get(segId);
    if (segUnit == null
        || segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleted) {
      logger.warn("segunit is null or deleted, no need to flush persist log, segId is {}", segId);
      return;
    }

    LogStorageRocksDbMetadata rocksDbColumnFamily = (LogStorageRocksDbMetadata) logRocksDbSystem
        .getLogStorageToWrite(segId);
    if (null == rocksDbColumnFamily) {
      logger.info("log storage has not been initialize, can not flush it. segment is {}", segId);
      return;
    }

    rocksDbColumnFamily.flush();
  }

  @Override
  public void close() throws IOException {
    this.logRocksDbSystem.close();
  }

  @Override
  public boolean reInit(SegId segId) throws IOException, KvStoreException {
    return this.logRocksDbSystem.initLogSystemForSeg(segId,
        segUnitManager.get(segId).getArchive().getArchiveId());
  }

  @Override
  public LogRocksDbSystem getLogStorageSystem() {
    return logRocksDbSystem;
  }

  @Override
  public void removeLogMetaData(SegId segId) {
    logger.warn("remove log meta data from disk and memory, segId: {}", segId);
    try {
      logRocksDbSystem.removeLogStorage(segId);
    } catch (Exception e) {
      logger.error("remove log storage fail: segId {}", segId, e);
    }
  }

}

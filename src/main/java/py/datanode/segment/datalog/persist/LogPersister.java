/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.datanode.segment.datalog.persist;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryWriter;
import py.third.rocksdb.KvStoreException;

public class LogPersister {
  private static final Logger logger = LoggerFactory.getLogger(LogPersister.class);
  private LogStorageSystem logStorageSystem;
  private SegmentUnitManager segUnitManager;

  public LogPersister(LogStorageSystem logStorageSystem, SegmentUnitManager segUnitManager)
      throws IOException {
    this.logStorageSystem = logStorageSystem;
    this.segUnitManager = segUnitManager;
  }

  public void persistLog(SegId segId, MutationLogEntry logEntry)
      throws IOException, LogIdTooSmall, KvStoreException {
    if (logEntry == null) {
      return;
    }

    List<MutationLogEntry> logs = new ArrayList<MutationLogEntry>();
    logs.add(logEntry);
    persistLog(segId, logs);
  }

  /**
   * Persisting the logs by index ascending.
   */
  public void persistLog(SegId segId, List<MutationLogEntry> logs)
      throws IOException, LogIdTooSmall, KvStoreException {
    if (logs == null || logs.isEmpty()) {
      return;
    }

    SegmentUnit segUnit = segUnitManager.get(segId);
    if (segUnit == null
        || segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleted) {
      logger.warn("segunit is null or deleted, no need to persist log, segId is {}", segId);
      return;
    }

    LogStorageMetadata logStorageMetadata = logStorageSystem.getLogStorageToWrite(segId);
    long firstLogId = logs.get(0).getLogId();
    if (logStorageMetadata == null) {
      logStorageMetadata = logStorageSystem.createLogStorageWhenNoLogFileExist(segId, firstLogId);
      logger.debug("create logStorageMetadata {} for segId {}", logStorageMetadata, segId);
    } else {
      if (firstLogId <= logStorageMetadata.getMaxLogId()) {
        logger.warn("log id {} is too small to persist. The logStorageMetadata is {}, max log:{} ",
            firstLogId,
            logStorageMetadata, logStorageMetadata.getMaxLogId());
        throw new LogIdTooSmall();
      }
    }

    int totalLogCount = logs.size();
    int leftLogCount = totalLogCount;
    int offset = 0;
    while (leftLogCount > 0) {
      if (!logStorageMetadata.moreSpace()) {
        logger.debug("switch log file to write: {}", logStorageMetadata);
        logStorageMetadata.close();
      }

      MutationLogEntryWriter writer = logStorageMetadata.getLogWriter();

      if (writer == null) {
        firstLogId = logs.get(offset).getLogId();
        logStorageMetadata = logStorageSystem
            .createLogStorageToWrite(segId, firstLogId, logStorageMetadata);
        writer = logStorageMetadata.getLogWriter();
      }

      int currentLogCount =
          leftLogCount <= logStorageMetadata.leftSpace() ? leftLogCount
              : logStorageMetadata.leftSpace();

      List<MutationLogEntry> currentList = logs.subList(offset, offset + currentLogCount);

      for (MutationLogEntry log : currentList) {
        writer.write(log);
      }

      logStorageMetadata.setMaxLogId(logs.get(offset + currentLogCount - 1).getLogId());
      logStorageMetadata.addNumLogs(currentLogCount);

      leftLogCount -= currentLogCount;
      offset += currentLogCount;
    }
  }

  public void close() throws IOException {
    this.logStorageSystem.close();
  }

  public boolean reInit(SegId segId) throws IOException, KvStoreException {
    return ((LogFileSystem) this.logStorageSystem).initLogSystemForSeg(segId);
  }

  protected LogStorageMetadata getLogStorageMetadata(SegId segId) {
    return logStorageSystem.getLogStorageToWrite(segId);
  }

  public LogStorageSystem getLogStorageSystem() {
    return logStorageSystem;
  }

  public void setLogStorageSystem(LogStorageSystem logStorageSystem) {
    this.logStorageSystem = logStorageSystem;
  }

  public void removeLogMetaData(SegId segId) {
    logger.warn("remove log meta data from disk and memory, segId: {}", segId);
    try {
      logStorageSystem.removeLogStorage(segId);
    } catch (Exception e) {
      logger.error("remove log storage fail: segId {}", segId, e);
    }
  }

  public void flush(SegId segId) throws IOException, KvStoreException {
    LogStorageMetadata metadata = getLogStorageMetadata(segId);
    if (metadata == null) {
      return;
    }
    MutationLogEntryWriter writer = metadata.getLogWriter();
    if (writer == null) {
      return;
    }

    OutputStream externalOutputStream = writer.getOutputStream();
    externalOutputStream.flush();
    if (externalOutputStream instanceof FileOutputStream) {
      FileOutputStream fileOutputStream = (FileOutputStream) externalOutputStream;
      logger.debug("going to flush log file for {}", segId);
      fileOutputStream.getFD().sync();
    }
  }
}

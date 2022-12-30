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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.exception.LogIdNotFoundException;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryReader;
import py.third.rocksdb.KvStoreException;

public class LogStorageReader {
  private static final Logger logger = LoggerFactory.getLogger(LogStorageReader.class);
  private LogStorageSystem logStorageSystem;

  public LogStorageReader(LogStorageSystem logStorageSystem) throws IOException {
    this.logStorageSystem = logStorageSystem;
  }

  /**
   * check if the specified id exists.
   *
   */
  public boolean logIdExists(SegId segId, long id) throws IOException, KvStoreException {
    try {
      logStorageSystem.getLogStorageToRead(segId, id);
      return true;
    } catch (LogIdNotFoundException e) {
      logger.debug("segment unit {} id {} does't exist in the log system", segId, id, e);
      return false;
    }
  }

  public List<MutationLogEntry> readLatestLogs(SegId segId, int maxNum)
      throws IOException, KvStoreException {
    LogStorageMetadata logStorageMetadata = logStorageSystem.getLatestLogStorageToRead(segId);
    List<MutationLogEntry> logs = new ArrayList<MutationLogEntry>();

    if (logStorageMetadata == null) {
      logger.warn("The latest log storage doesn't have a complete log yet.");
      return logs;
    } else if (logStorageMetadata.getLogReader() == null) {
      logger.error("Can't get the reader to read {} ", logStorageMetadata);
      throw new IOException();
    }
    logger.warn("getMaxLogId {}", logStorageMetadata.getMaxLogId());
    int numLogsInOneStorage = logStorageMetadata.getNumLogs();

    try {
      while (numLogsInOneStorage > 0) {
        MutationLogEntryReader logReader = logStorageMetadata.getLogReader();
        MutationLogEntry logEntry = logReader.read();
        logs.add(logEntry);
        numLogsInOneStorage--;
      }
    } catch (IOException e) {
      logger.error("catch an exception when read log, segId {}", segId, e);
      throw e;
    } finally {
      logStorageMetadata.close();
    }

    int fromIndex = logs.size() - maxNum;
    if (fromIndex < 0) {
      fromIndex = 0;
    }

    logStorageMetadata.close();

    return logs.subList(fromIndex, logs.size());
  }

  public List<MutationLogEntry> readLogsAfter(SegId segId, long id, int maxNum)
      throws IOException, LogIdNotFoundException, KvStoreException {
    return readLogsAfter(segId, id, maxNum, false);
  }

  public List<MutationLogEntry> readLogsAfter(SegId segId, long id, int maxNum,
      boolean checkIdExists)
      throws IOException, LogIdNotFoundException, KvStoreException {
    LogStorageMetadata logStorageMetadata = getLogStorageMetadataIncludingAndAfter(segId, id,
        checkIdExists);

    List<MutationLogEntry> logs = new ArrayList<MutationLogEntry>();
    if (logStorageMetadata == null) {
      logger.warn("The specified log id {} is too large or the segment doesn't exist ", id);
      if (checkIdExists) {
        throw new LogIdNotFoundException("can not find the log: " + id, null, null);
      }
      return logs;
    } else if (logStorageMetadata.getLogReader() == null) {
      logger.error("Can't get the reader to read {} ", logStorageMetadata);
      throw new IOException();
    }

    int totalNumLogs = maxNum;

    long maxLogId = id + 1;
    while (true) {
      int numLogsInOneStorage = logStorageMetadata.getNumLogs();

      while (numLogsInOneStorage > 0 && totalNumLogs > 0) {
        MutationLogEntryReader logReader = logStorageMetadata.getLogReader();
        try {
          MutationLogEntry logEntry = logReader.read();
          if (maxLogId <= logEntry.getLogId()) {
            maxLogId = logEntry.getLogId();
            if (logEntry != null && (logEntry.getLogId() == -1 || logEntry.getOffset() == -1)) {
              logger.error("logEntry: {} is invalid", logEntry);
            }
            logs.add(logEntry);
            totalNumLogs--;
          }
          numLogsInOneStorage--;
        } catch (IOException e) {
          logStorageMetadata.close();
          logger.error("catch an exception when read log", e);
          throw e;
        }
      }

      logStorageMetadata.getLogReader().close();
      if (totalNumLogs == 0) {
        logger.info("get all logs");
        break;
      }

      logger.debug("read another log storage file");
      if (logs.isEmpty()) {
        logStorageMetadata = getLogStorageMetadataIncludingAndAfter(segId, maxLogId, false);
      } else {
        logStorageMetadata = getLogStorageMetadataIncludingAndAfter(segId, maxLogId + 1, false);
      }

      if (logStorageMetadata == null || logStorageMetadata.getLogReader() == null) {
        logger.debug("Can't get a log storage that contain logs larger than {} reading is done ",
            maxLogId);
        break;
      }
    }

    if (logStorageMetadata != null) {
      logStorageMetadata.close();
    }

    return logs;
  }

  private LogStorageMetadata getLogStorageMetadataIncludingAndAfter(SegId segId, long id,
      boolean checkIdExists)
      throws IOException, LogIdNotFoundException, KvStoreException {
    LogStorageMetadata logStorageMetadata = null;
    try {
      logStorageMetadata = logStorageSystem.getLogStorageToRead(segId, id);
    } catch (LogIdNotFoundException e) {
      if (checkIdExists) {
        logger.warn("log {} in {} not exist in storage file", id, segId);
        throw e;
      }
      logStorageMetadata = e.getNextLogStorageMetadata();
    }
    return logStorageMetadata;
  }
}

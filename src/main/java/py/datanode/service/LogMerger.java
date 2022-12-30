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

package py.datanode.service;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.datanode.exception.LogMergingException;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.exception.NoAvailableBufferException;
import py.thrift.datanode.service.LogInfoThrift;
import py.thrift.datanode.service.LogSyncActionThrift;
import py.thrift.datanode.service.LogThrift;

public class LogMerger {
  private static final Logger logger = LoggerFactory.getLogger(LogMerger.class);
  private Multimap<Long, LogThrift> logsFromSecondaries = ArrayListMultimap.create();
  private TreeSet<Long> logIds = new TreeSet<Long>();
  private SegmentLogMetadata segmentLogMetadata;
  private int writeQuorumSize;
  
  
  public LogMerger(int writeQuorumSize, SegmentLogMetadata segmentLogMetadata) {
    this.writeQuorumSize = writeQuorumSize;
   
    this.segmentLogMetadata = segmentLogMetadata;

    LogImage logImage = segmentLogMetadata.getLogImageIncludeLogsAfterPcl();
   
    for (Long uncommittedLogId : logImage.getUncommittedLogIds()) {
      logIds.add(uncommittedLogId);
    }
  }

  /**
   * put all logs to the multiMap and return the max id received. If there is no log, return null.
   *
   */
  public Long addLogsFromResponse(List<LogThrift> logs) {
   
    Long maxIdReceived = null;
    if (logs != null && logs.size() > 0) {
      maxIdReceived = Long.MIN_VALUE;
      for (LogThrift log : logs) {
        long id = log.getLogId();
        logsFromSecondaries.put(id, log);
        logIds.add(id);
        if (id > maxIdReceived) {
          maxIdReceived = id;
        }
      }
    }
    return maxIdReceived;
  }

  public List<MutationLogEntry> merge(long maxLogIdToMerge) throws LogMergingException {
    Validate.isTrue(maxLogIdToMerge >= LogImage.INVALID_LOG_ID);

    List<MutationLogEntry> mergedLogs = new ArrayList<MutationLogEntry>();
    MergedLogIterator iterator = iterator(maxLogIdToMerge);
    try {
     
      while (iterator.hasNext()) {
        mergedLogs.add(iterator.next());
      }

      return mergedLogs;
    } catch (Exception e) {
     
      logger.error(
          "caught an exception when merging logs from "
              + "secondaries({}) and primary candidate. This should not happen",
          segmentLogMetadata.getSegId(), e);
      for (int i = 0; i < mergedLogs.size(); i++) {
        MutationLogEntryFactory.releaseLogData(mergedLogs.get(i));
      }
      throw new LogMergingException(e);
    }
  }

  /**
   * Merge all logs.
   *
   */
  public List<MutationLogEntry> merge() throws LogMergingException, NoAvailableBufferException {
    return merge(Long.MAX_VALUE);
  }

  public MergedLogIterator iterator() {
    return iterator(Long.MAX_VALUE);
  }

  public MergedLogIterator iterator(long maxLogIdToMerge) {
    return new MergedLogIterator(maxLogIdToMerge);
  }

  /**
   * clear all internal data and prepare for addLogsFromResponse() and merge() again.
   */
  public void clear() {
    logsFromSecondaries.clear();
    logIds.clear();
  }

  public static class LogWithSameIdMerger {
    public static MutationLogEntry merge(Collection<LogThrift> logs, int quorumSize)
        throws LogMergingException, NoAvailableBufferException {
      if (logs == null || logs.size() == 0) {
        logger.info("no log merged");
        return null;
      }

      Long logId = null;
      Set<Long> uuids = new HashSet<Long>(logs.size());
      Set<Long> logIds = new HashSet<Long>(logs.size());
      Set<Long> offsets = new HashSet<Long>(logs.size());
      Pair<Long, byte[]> oneCheckSumAndData = new Pair<Long, byte[]>();
      Set<Integer> lengths = new HashSet<>(logs.size());
      Multiset<LogStatus> statusSet = HashMultiset.create();
      final Set<Integer> snapshotVersions = new HashSet<Integer>();
      final Set<Long> cloneVolumeDataOffsets = new HashSet<Long>(logs.size());
      final Set<Integer> cloneVolumeDataLengths = new HashSet<Integer>(logs.size());
      for (LogThrift log : logs) {
        uuids.add(log.getLogUuid());
        logIds.add(log.getLogId());

        LogInfoThrift logInfo = log.getLogInfo();
        offsets.add(logInfo.getOffset());
        lengths.add(logInfo.getLength());

        if (oneCheckSumAndData.getFirst() == null && logInfo.getData() != null) {
          oneCheckSumAndData.setFirst(logInfo.getChecksum());
          oneCheckSumAndData.setSecond(logInfo.getData());
        }

        statusSet.add(DataNodeRequestResponseHelper.convertFromThriftStatus(log.getStatus()));
      }

      String errMsg = null;
      

      int numberAbortedConfirmed = statusSet.count(LogStatus.AbortedConfirmed);
      int numberCommitted = statusSet.count(LogStatus.Committed);
      int numberAborted = statusSet.count(LogStatus.Aborted);
      int numberCreated = statusSet.count(LogStatus.Created);
      LogStatus finalLogStatus = null;
      if (numberAbortedConfirmed > 0) {
        if (numberCommitted != 0) {
          errMsg = "Can't merge logs with the same id: " + logId 
              + " the reason is that there is an aborted " 
              + "confirmed log and at least one committed log";
          logger.error(errMsg);
          throw new LogMergingException(errMsg);
        } else {
          finalLogStatus = LogStatus.AbortedConfirmed;
        }
      } else if (numberCommitted > 0) {
        if (numberAbortedConfirmed != 0) {
          errMsg = "Can't merge logs with the same id: " 
              + logId 
              + " the reason is that there is an committed log and at" 
              + " least one aborted confirmed log";
          logger.error(errMsg);
          throw new LogMergingException(errMsg);
        } else {
          finalLogStatus = LogStatus.Committed;
        }
      } else if (numberAborted > 0) {
        
        finalLogStatus = LogStatus.Aborted;
      } else if (numberCreated >= quorumSize) {
        
        finalLogStatus = LogStatus.Committed;
      } else {
        finalLogStatus = LogStatus.Created;
      }

      if (uuids.size() != 1) {
        errMsg = "Can't merge logs" + logs + " because they have different log uuids";
        logger.error(errMsg);
        throw new LogMergingException(errMsg);
      }

      if (logIds.size() != 1) {
        errMsg = "Can't merge logs" + logs + " because they have different log ids";
        logger.error(errMsg);
        throw new LogMergingException(errMsg);
      }

      if (lengths.size() != 1) {
        errMsg = "Can't merge logs" + logs + " because they have different log length";
        logger.error(errMsg);
        throw new LogMergingException(errMsg);
      }

      if (offsets.size() != 1) {
        errMsg = "Can't merge logs" + logs + " because they have different offsets";
        logger.error(errMsg);
        throw new LogMergingException(errMsg);
      }

      if (snapshotVersions.size() != 1) {
        errMsg = "Can't merge logs" + logs + " because they have different snapshotId";
        logger.error(errMsg);
        throw new LogMergingException(errMsg);
      }

      if (cloneVolumeDataOffsets.size() != 1) {
        errMsg =
            "Can't merge logs" + logs + " because they have different clone volume data offset";
        logger.error(errMsg);
        throw new LogMergingException(errMsg);
      }

      if (cloneVolumeDataLengths.size() != 1) {
        errMsg =
            "Can't merge logs" + logs + " because they have different clone volume data length";
        logger.error(errMsg);
        throw new LogMergingException(errMsg);
      }

      byte[] finalData = null;
      long checksum = 0;
     
      if (oneCheckSumAndData.getFirst() == null) {
        errMsg = logs + " has no data";
        logger.info(errMsg);
       
      } else {
       
        checksum = oneCheckSumAndData.getFirst();
        finalData = oneCheckSumAndData.getSecond();
      }

      if (finalData == null && finalLogStatus == LogStatus.Created) {
        logger.warn("finalData:{}", finalData);
        errMsg = "Can't merge logs" + logs
            + " because the merged log doesn't have data while its status is Created";
        logger.error(errMsg);
        throw new LogMergingException(errMsg);
      }

      MutationLogEntry logEntry = MutationLogEntryFactory
          .createLogForSyncLog(uuids.iterator().next(), logIds.iterator().next(),
              offsets.iterator().next(),
              finalData, checksum);
      logEntry.setStatus(finalLogStatus);
      logEntry.setLength(lengths.iterator().next());
      return logEntry;
    }
  }

  public class MergedLogIterator {
    private final long maxLogIdToMerge;
    private Collection<LogThrift> thriftLogsToMerge = new ArrayList<>();

    protected MergedLogIterator(long maxLogIdToMerge) {
      this.maxLogIdToMerge = maxLogIdToMerge;
    }

    public boolean hasNext() {
      return logIds.floor(maxLogIdToMerge) != null;
    }

    public MutationLogEntry next() throws LogMergingException, NoAvailableBufferException {
      if (!hasNext()) {
       
        return null;
      }

      Long logIdToMerge = logIds.first();
      Validate.isTrue(logIdToMerge <= maxLogIdToMerge);
      thriftLogsToMerge.clear();

      Collection<LogThrift> secondaryLogsHavingSameId = logsFromSecondaries.get(logIdToMerge);
      thriftLogsToMerge.addAll(secondaryLogsHavingSameId);

      MutationLogEntry logEntryInPrimaryCandidate = segmentLogMetadata.getLog(logIdToMerge);
     
     
      if (logEntryInPrimaryCandidate != null) {
        byte[] data = logEntryInPrimaryCandidate.getData();

        if (data == null) {
          thriftLogsToMerge.add(DataNodeRequestResponseHelper
              .buildLogThriftFrom(segmentLogMetadata.getSegId(), logEntryInPrimaryCandidate,
                  LogSyncActionThrift.Add));
        } else {
          thriftLogsToMerge.add(DataNodeRequestResponseHelper
              .buildLogThriftFrom(segmentLogMetadata.getSegId(), logEntryInPrimaryCandidate, data,
                  LogSyncActionThrift.Add));
        }
      }

      MutationLogEntry mergedLogEntry = LogWithSameIdMerger
          .merge(thriftLogsToMerge, writeQuorumSize);
      logger.debug("merged log:  {} ", mergedLogEntry);
     
      logIds.pollFirst();
      logsFromSecondaries.removeAll(logIdToMerge);
      return mergedLogEntry;
    }
  }
}

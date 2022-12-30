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

package py.datanode.segment.datalog.algorithm.merger;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.datanode.segment.datalog.algorithm.ImmutableStatusDataLog;
import py.datanode.service.io.read.merge.MergeFailedException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;

public class BecomePrimaryLogsMerge implements DataLogsMerger {
  private static final Logger logger = LoggerFactory.getLogger(BecomePrimaryLogsMerge.class);
  private final SegmentMembership currentMemship;

  private final MergeLogCommunicator mergeLogCommunicator;

  private final InstanceId mySelfInstanceId;
  private final TreeMap<Long, LogsHolder> logs = new TreeMap<>();

  private final int votingCount;

  public BecomePrimaryLogsMerge(SegmentMembership currentMemship,
      MergeLogCommunicator mergeLogCommunicator, InstanceId mySelfInstanceId, int votingCount) {
    this.currentMemship = currentMemship;
    this.mergeLogCommunicator = mergeLogCommunicator;
    this.mySelfInstanceId = mySelfInstanceId;
    this.votingCount = votingCount;
  }

  @Override
  public void addLog(InstanceId instance, DataLog log) {
    Validate.isTrue(!log.isApplied());

    LogsHolder logsHolder = logs
        .computeIfAbsent(log.getLogId(), whatever -> new LogsHolder(log.getLogId()));
    logsHolder.allLog.put(instance, log);

  }

  @Override
  public MergeLogIterator iterator(long maxLogIdToMerge) {
    final Iterator<Entry<Long, LogsHolder>> mapIt = logs.entrySet().iterator();

    return new MergeLogIterator() {
      @Override
      public boolean hasNext() {
        return mapIt.hasNext() && logs.floorKey(maxLogIdToMerge) != null;
      }

      @Override
      public Pair<DataLog, Set<InstanceId>> next() throws MergeFailedException {
        Entry<Long, LogsHolder> entry = mapIt.next();
        LogsHolder log = entry.getValue();

        if (log.logId > maxLogIdToMerge) {
          logger.error("current log {} do not merge {}", log.logId, maxLogIdToMerge);
          throw new MergeFailedException();
        }
        Pair<DataLog, Set<InstanceId>> mergeResult = log.merge();
        mapIt.remove();
        return mergeResult;
      }
    };
  }

  private class LogsHolder {
    final long logId;
    final Map<InstanceId, DataLog> allLog = new HashMap<>();

    private LogsHolder(long logId) {
      this.logId = logId;
    }

    private Pair<DataLog, Set<InstanceId>> merge() throws MergeFailedException {
      while (true) {
        LogStatus findLogStatus = decideLogStatus();
        if (Objects.isNull(findLogStatus)) {
          throw new NullPointerException();
        }

        Set<InstanceId> successInstanceId = new HashSet<>();
        DataLog broadCastLog = null;
        switch (findLogStatus) {
          case Committed:
            for (Map.Entry<InstanceId, DataLog> entry : allLog.entrySet()) {
              LogStatus logStatus = entry.getValue().getLogStatus();
              if (logStatus == LogStatus.Committed || logStatus == LogStatus.Created) {
                successInstanceId.add(entry.getKey());
              }
              if (Objects.nonNull(entry.getValue().getData())) {
                broadCastLog = entry.getValue();
              }
            }

            if (!successInstanceId.contains(mySelfInstanceId)) {
              org.apache.commons.lang3.Validate.notNull(broadCastLog);
              successInstanceId.addAll(mergeLogCommunicator
                  .broadcastCommitLog(createLog(broadCastLog, LogStatus.Created)));
            } else {
              successInstanceId.addAll(mergeLogCommunicator.getOkInstance());
              broadCastLog = allLog.get(mySelfInstanceId);
            }

            if (successInstanceId.size() < votingCount) {
              throw new MergeFailedException();
            } else if (!successInstanceId.contains(mySelfInstanceId)) {
              throw new MergeFailedException();
            } else {
              return new Pair<>(createLog(broadCastLog, LogStatus.Committed), successInstanceId);
            }
          case Created:
            boolean statusChanged = false;
            for (Map.Entry<InstanceId, DataLog> entry : allLog.entrySet()) {
              if (Objects.nonNull(entry.getValue().getData())) {
                broadCastLog = entry.getValue();
              }

              LogStatus logStatus = entry.getValue().getLogStatus();
              InstanceId instanceId = entry.getKey();
              if (instanceId.equals(mySelfInstanceId)) {
                if (logStatus == LogStatus.Committed || logStatus == LogStatus.AbortedConfirmed) {
                  statusChanged = true;
                  logger.warn("a log {} in {} status changed {}", entry.getValue().getLogId(),
                      instanceId, logStatus);
                  break;
                }
              }
            }

            if (statusChanged) {
              continue;
            }
            if (broadCastLog == null) {
              logger.error("the log is {}", allLog);
              Validate.isTrue(false);
            }
            org.apache.commons.lang3.Validate.notNull(broadCastLog);
            successInstanceId.addAll(mergeLogCommunicator
                .broadcastCommitLog(createLog(broadCastLog, LogStatus.Created)));

            if (successInstanceId.size() < votingCount) {
              throw new MergeFailedException();
            } else if (!successInstanceId.contains(mySelfInstanceId)) {
              throw new MergeFailedException();
            } else {
              return new Pair<>(createLog(broadCastLog, LogStatus.Committed), successInstanceId);
            }
          case AbortedConfirmed:
          case Aborted:
            for (DataLog dataLog : allLog.values()) {
              if (dataLog.getLogStatus() == findLogStatus) {
                broadCastLog = dataLog;
                break;
              }
            }

            if (broadCastLog == null) {
              logger.error("the log is {}", allLog);
              Validate.isTrue(false);
            }
            org.apache.commons.lang3.Validate.notNull(broadCastLog);
            successInstanceId.addAll(mergeLogCommunicator
                .broadcastAbortLog(createLog(broadCastLog, LogStatus.Aborted)));
            if (successInstanceId.size() < votingCount) {
              throw new MergeFailedException();
            } else if (!successInstanceId.contains(mySelfInstanceId)) {
              throw new MergeFailedException();
            } else {
              return new Pair<>(createLog(broadCastLog, LogStatus.AbortedConfirmed),
                  successInstanceId);
            }
          default:
            throw new UnsupportedOperationException();
        }
      }

    }

    private DataLog createLog(DataLog originDataLog, LogStatus logStatus) {
      DataLog dataLog = new ImmutableStatusDataLog(originDataLog.getLogId(),
          originDataLog.getOffset(), originDataLog.getLength(), originDataLog.getLogUuid(),
          logStatus, originDataLog.isApplied(), originDataLog.isPersisted()) {
        @Override
        public void getData(ByteBuffer destination, int offset, int length) {
          originDataLog.getData(destination, offset, length);
        }

        @Override
        public void getData(ByteBuf destination, int offset, int length) {
          originDataLog.getData(destination, offset, length);
        }

        @Override
        public byte[] getData() {
          byte[] data = originDataLog.getData();
          if (Objects.isNull(data)) {
            logger.error("the data is null, {}", originDataLog);
          }

          return data;
        }

        @Override
        public long getCheckSum() {
          return originDataLog.getCheckSum();
        }
      };

      return dataLog;
    }

    private LogStatus decideLogStatus() {
      if (allLog.size() == 0) {
        return null;
      }

      if (allLog.size() == 1) {
        return allLog.values().iterator().next().getLogStatus();
      }

      if (allLog.get(mySelfInstanceId) == null) {
        logger.warn("myself {}, do not have log {}", mySelfInstanceId, allLog);
        for (Map.Entry<InstanceId, DataLog> entry : allLog.entrySet()) {
          DataLog dataLog = entry.getValue();
          switch (dataLog.getLogStatus()) {
            case AbortedConfirmed:
            case Committed:
            case Aborted:
              return dataLog.getLogStatus();
            case Created:
            default:
              break;
          }
        }

        return LogStatus.Created;
      }
      DataLog createdLog = null;

      for (Map.Entry<InstanceId, DataLog> entry : allLog.entrySet()) {
        if (currentMemship.isInactiveSecondary(entry.getKey()) || currentMemship
            .isJoiningSecondary(entry.getKey())) {
          continue;
        }
        DataLog dataLog = entry.getValue();
        switch (dataLog.getLogStatus()) {
          case AbortedConfirmed:
          case Committed:
            return dataLog.getLogStatus();
          case Created:
            createdLog = dataLog;
            break;
          case Aborted:
            break;
          default:
        }
      }

      if (Objects.nonNull(createdLog)) {
        return LogStatus.Created;
      } else {
        return LogStatus.Aborted;
      }
    }
  }
}

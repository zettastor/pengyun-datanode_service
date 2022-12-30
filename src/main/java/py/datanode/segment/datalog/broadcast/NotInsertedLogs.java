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

package py.datanode.segment.datalog.broadcast;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.NamedThreadFactory;
import py.common.struct.Pair;
import py.datanode.exception.ExceptionWithNoStack;
import py.datanode.exception.InvalidSegmentStatusException;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.broadcast.exception.HeIsBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.ImBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.WeDontHaveLogEntry;
import py.datanode.segment.datalog.broadcast.exception.WeDontHaveLogId;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.netty.core.AbstractMethodCallback;

public class NotInsertedLogs implements NotInsertedLogsInterface {
  private static final int DEFAULT_TIMEOUT_FOR_LISTENER = 200;
  private static final int DEFAULT_TIMEOUT_FOR_HOSTED_MAP = 60 * 1000;
  private static final Logger logger = LoggerFactory.getLogger(NotInsertedLogs.class);
  private static final int printPeriod = 10000;
  private static final AtomicBoolean threadStarted = new AtomicBoolean(false);
  private static int printCounter = 0;
  private static int logIdTooSmallPrintCounter = 0;
  private static io.netty.util.Timer drivingThread;

  private final int logCompletingTimeout;
  private final SegId segId;
  private final SegmentLogMetadata segmentLogMetadata;
  private final Map<Long, CompletingLog> mapLogsUuidToCompletingLogs = new ConcurrentHashMap<>();
  private Map<Long, Long> logsMissingAtPrimary = new ConcurrentHashMap<>();

  public NotInsertedLogs(SegId segId, SegmentLogMetadata segmentLogMetadata) {
    this.segId = segId;
    this.segmentLogMetadata = segmentLogMetadata;
    this.logCompletingTimeout = DEFAULT_TIMEOUT_FOR_LISTENER;
  }

  public NotInsertedLogs(SegId segId, SegmentLogMetadata segmentLogMetadata,
      int logCompletingTimeout) {
    this.segId = segId;
    this.segmentLogMetadata = segmentLogMetadata;
    this.logCompletingTimeout = logCompletingTimeout;
  }

  public static void startExpiredTaskDriving() {
    if (threadStarted.compareAndSet(false, true)) {
      drivingThread = new HashedWheelTimer(
          new NamedThreadFactory("not-inserted-logs-expired-driving"));
    }
  }

  public static void stopExpiredTaskDriving() {
    if (threadStarted.compareAndSet(true, false) && drivingThread != null) {
      drivingThread.stop();
      drivingThread = null;
    }
  }

  @Override
  public List<MutationLogEntry> addCompletingLog(PlalEngine plalEngine, boolean replaceOldValue,
      CompletingLog... completingLogs) {
    List<MutationLogEntry> logsToBeInserted = new LinkedList<>();
    for (CompletingLog completingLog : completingLogs) {
      if (completingLog == null) {
        continue;
      }
      MutationLogEntry completedLog;
      if (completingLog instanceof BrokenLogWithoutLogId) {
        completedLog = saveOrCompleteIncompleteLog(completingLog, null, replaceOldValue);
      } else if (completingLog instanceof LogWithoutLogId) {
        completedLog = saveOrCompleteIncompleteLog(completingLog,
            ((LogWithoutLogId) completingLog).getLogEntry(), replaceOldValue);
      } else if (completingLog instanceof LogWithoutLogEntry) {
        completedLog = saveOrCompleteIncompleteLog(completingLog, null, replaceOldValue);
      } else {
        throw new IllegalArgumentException();
      }
      if (completedLog != null) {
        logsToBeInserted.add(completedLog);
      }
    }

    insertCompleteLogs(logsToBeInserted, plalEngine, null);

    return logsToBeInserted;
  }

  private MutationLogEntry dealWithDifferentLog(MutationLogEntry previousValue,
      MutationLogEntry newValue) {
    Validate.isTrue(previousValue.getUuid() == newValue.getUuid());

    if (previousValue.getLogId() == newValue.getLogId()) {
      MutationLogEntryFactory.releaseLogData(newValue);
      if (!previousValue.isFinalStatus()) {
        previousValue.setStatus(newValue.getStatus());
      }
      return previousValue;
    }

    return newValue;
  }

  /**
   * Add complete logs to notify the listeners. Also those need to be inserted will be inserted.
   */
  @Override
  public Pair<List<CompletableFuture<Boolean>>, List<MutationLogEntry>> addCompleteLogs(
      PlalEngine plalEngine, List<MutationLogEntry> logs) {
    CompletableFuture<Boolean> futureOfInsertResult = new CompletableFuture<>();
    List<MutationLogEntry> logsToBeInserted = new LinkedList<>();
    List<MutationLogEntry> failedLogs = new LinkedList<>();
    List<CompleteLog> logsAlreadyInsertedButNotCompleted = new LinkedList<>();
    for (MutationLogEntry log : logs) {
      CompletingLog previousValue = mapLogsUuidToCompletingLogs.get(log.getUuid());
      if (previousValue != null) {
        CompleteLog newValue = previousValue.tryComplete(log);
        if (previousValue instanceof CompleteLog) {
          logger.debug("a complete log already exists old one {} , new one {}", previousValue,
              newValue);
          MutationLogEntry previousLogEntry = ((CompleteLog) previousValue).getLogEntry();

          MutationLogEntry logToBeInserted;
          if (previousLogEntry != log) {
            logToBeInserted = dealWithDifferentLog(previousLogEntry, log);
          } else {
            logToBeInserted = previousLogEntry;
          }

          if (logToBeInserted == previousLogEntry) {
            if (!previousValue.hasDone()) {
              logger.info("prev log not complete, prev {}, log {}, seg {}, please retry",
                  previousValue, log, segId);
              logsAlreadyInsertedButNotCompleted.add((CompleteLog) previousValue);
            }
          } else {
            logsToBeInserted.add(logToBeInserted);
          }

        } else if (mapLogsUuidToCompletingLogs.replace(log.getUuid(), previousValue, newValue)) {
          if (previousValue instanceof LogWithoutLogId) {
            MutationLogEntry previousLogEntry = ((LogWithoutLogId) previousValue).getLogEntry();
            if (previousLogEntry != null && previousLogEntry != log) {
              MutationLogEntryFactory.releaseLogData(previousLogEntry);
            }
          }
          logsToBeInserted.add(log);
        } else {
          failedLogs.add(log);
        }
      } else {
        CompleteLog newValue = new CompleteLog(log, null);
        previousValue = mapLogsUuidToCompletingLogs.putIfAbsent(log.getUuid(), newValue);
        if (previousValue != null) {
          logger.debug("add complete log fail, due to a completeing log enter the same time {} {}",
              previousValue, newValue);
          failedLogs.add(log);
        } else {
          logsToBeInserted.add(log);
        }
      }
    }

    insertCompleteLogs(logsToBeInserted, plalEngine, new AbstractMethodCallback() {
      @Override
      public void complete(Object object) {
        futureOfInsertResult.complete(true);
      }

      @Override
      public void fail(Exception e) {
        futureOfInsertResult.completeExceptionally(e);
      }
    });
    List<CompletableFuture<Boolean>> resultsOfInsertedLogs = new LinkedList<>();
    logsAlreadyInsertedButNotCompleted
        .forEach(completeLog -> resultsOfInsertedLogs.add(completeLog.getFuture()));
    resultsOfInsertedLogs.add(futureOfInsertResult);
    return new Pair<>(resultsOfInsertedLogs, failedLogs);
  }

  private MutationLogEntry saveOrCompleteIncompleteLog(CompletingLog newValue, MutationLogEntry log,
      boolean replaceOldValue) {
    CompletingLog previousValue = mapLogsUuidToCompletingLogs
        .putIfAbsent(newValue.getUuid(), newValue);
    if (previousValue == null) {
      if (segmentLogMetadata.getMapLogsUuidToLogId().containsKey(newValue.getUuid())) {
        long logId = segmentLogMetadata.getMapLogsUuidToLogId().get(newValue.getUuid());
        MutationLogEntry logEntry = segmentLogMetadata.getLog(logId);

        logger.info("a log {} has arrived data node twice, old is {}", newValue, logEntry);
        if (newValue instanceof LogWithoutLogEntry) {
          if (logId == ((LogWithoutLogEntry) newValue).getLogId()) {
            CompletingLog computeValue = mapLogsUuidToCompletingLogs
                .computeIfPresent(newValue.getUuid(),
                    (along, completingLog) -> {
                      if (completingLog == newValue) {
                        return null;
                      } else {
                        return completingLog;
                      }
                    });
            if (computeValue == null) {
              newValue.complete();
            }
          } else {
            addToDelayQueue((LogWithoutLogEntry) newValue);
          }
          return null;
        } else if (newValue instanceof LogWithoutLogId) {
          CompletingLog computeValue = mapLogsUuidToCompletingLogs
              .computeIfPresent(newValue.getUuid(),
                  (along, completingLog) -> {
                    if (completingLog == newValue) {
                      return null;
                    } else {
                      return completingLog;
                    }
                  });
          if (computeValue == null) {
            if (logEntry.getOffset() != log.getOffset()
                || logEntry.getLength() != log.getLength()) {
              logger.debug("the same log came twice but they are different instances"
                      + " we will release buffer of one of them! {} {}", logEntry,
                  log);
              newValue.fail(
                  new Exception(
                      "the same log came twice with different instances !"));
            } else {
              newValue.complete();
            }
            MutationLogEntryFactory.releaseLogData(log);
            return null;
          } else {
            return null;
          }
        } else {
          logger
              .error("what happen! a completed log {} call this function try to complete",
                  newValue);
          Validate.isTrue(false);
          return null;
        }
      } else {
        if (newValue instanceof LogWithoutLogEntry) {
          addToDelayQueue((LogWithoutLogEntry) newValue);
        }

        return null;
      }
    } else if (previousValue instanceof CompleteLog) {
      CompleteLog completeLog = (CompleteLog) previousValue;

      logger.debug("the log has been completed {}, log {}", completeLog, log);
      if (log == null) {
        if (completeLog.hasDone()) {
          logger.debug("the log has been inserted prev {} new {}", previousValue, newValue);
          newValue.complete();
        } else {
          if (completeLog.listener != null) {
            if (replaceOldValue) {
              completeLog.listener.fail(completeLog.getUuid(), new Exception("i am replaced"));

              completeLog.listener = newValue.listener;
              logger.debug("I give my listener to the complete log");
            } else {
              newValue.fail(new Exception("prev value exist" + completeLog.logUuid));
            }
          } else {
            completeLog.listener = newValue.listener;
            logger.debug("I give my listener to the complete log");
          }

          if (completeLog.hasDone()) {
            newValue.complete();
          }
        }

        return null;
      } else if (log != completeLog.getLogEntry()) {
        logger.debug("the same log came twice but they are different instances"
            + " we will release buffer of one of them! {} {}", log, completeLog);
        newValue.fail(new Exception("the same log came twice with different instances !"));
        MutationLogEntryFactory.releaseLogData(log);
        return null;
      } else {
        logger.warn("the same log came TWICE !!, I don't think this should happen -tyr. {}, {}",
            segId, log);
        return null;
      }
    } else {
      boolean retry;
      try {
        CompleteLog completeLog = previousValue.tryComplete(newValue);
        MutationLogEntry completeResult = completeLog.getLogEntry();
        if (mapLogsUuidToCompletingLogs.replace(newValue.getUuid(), previousValue, completeLog)) {
          return completeResult;
        } else {
          logger.info("previousValue {} newValue {}", previousValue, newValue);
          retry = true;
        }
      } catch (ImBrokenLogException e) {
        Validate.isTrue(previousValue instanceof BrokenLogWithoutLogId);
        logger.debug("going to replace a broken log");
        if (mapLogsUuidToCompletingLogs.replace(newValue.getUuid(), previousValue, newValue)) {
          previousValue.fail(e);
          BrokenLogWithoutLogId brokenLog = (BrokenLogWithoutLogId) previousValue;
          if (newValue instanceof LogWithoutLogEntry) {
            if (brokenLog.isSuccess()) {
              logger.warn("got a discarded broken log, complete {}", segId);
              newValue.complete();
            } else {
              newValue.fail(new Exception("log creation failed"));
              addToDelayQueue((LogWithoutLogEntry) newValue);
            }
          }
          return null;
        } else {
          retry = true;
        }
      } catch (HeIsBrokenLogException e) {
        Validate.isTrue(newValue instanceof BrokenLogWithoutLogId);
        BrokenLogWithoutLogId brokenLog = (BrokenLogWithoutLogId) newValue;
        logger.debug("new value is a broken log");
        newValue.fail(e);
        if (previousValue instanceof LogWithoutLogEntry) {
          if (brokenLog.isSuccess()) {
            logger.warn("got a discarded broken log, complete {}", segId);
            previousValue.complete();
          } else {
            previousValue.fail(new Exception("log creation failed"));
          }
        } else if (replaceOldValue) {
          throw new IllegalArgumentException("broken log can't replace anyone else");
        }
        return null;
      } catch (WeDontHaveLogId weDontHaveLogId) {
        Validate.isTrue(log != null);
        Validate.isTrue(newValue instanceof LogWithoutLogId);
        Validate.isTrue(previousValue instanceof LogWithoutLogId);
        MutationLogEntry preLogEntry = ((LogWithoutLogId) previousValue).getLogEntry();

        boolean sameLog = (preLogEntry == log);
        if (replaceOldValue) {
          if (mapLogsUuidToCompletingLogs.replace(newValue.getUuid(), previousValue, newValue)) {
            previousValue.fail(new Exception("I have been replaced !"));
            if (!sameLog) {
              MutationLogEntryFactory.releaseLogData(preLogEntry);
            }
            return null;
          } else {
            retry = true;
          }
        } else {
          newValue.fail(new Exception("same log already exists"));
          if (!sameLog) {
            MutationLogEntryFactory.releaseLogData(log);
          }
          return null;
        }
      } catch (WeDontHaveLogEntry weDontHaveLogEntry) {
        Validate.isTrue(log == null);
        Validate.isTrue(newValue instanceof LogWithoutLogEntry);
        Validate.isTrue(previousValue instanceof LogWithoutLogEntry);

        if (replaceOldValue) {
          if (mapLogsUuidToCompletingLogs.replace(newValue.getUuid(), previousValue, newValue)) {
            previousValue.fail(new Exception("I have been replaced !"));
            addToDelayQueue((LogWithoutLogEntry) newValue);
            return null;
          } else {
            retry = true;
          }
        } else {
          newValue.fail(new Exception("same log already exists"));
          return null;
        }
      }

      Validate.isTrue(retry);
      logger.info(
          "the previous {} value has been replaced to {} just now, try again recursively {} {}",
          previousValue, mapLogsUuidToCompletingLogs.get(newValue.getUuid()), segId, newValue);
      return saveOrCompleteIncompleteLog(newValue, log, replaceOldValue);
    }
  }

  private void insertLogs(List<MutationLogEntry> logsToBeInserted, PlalEngine plalEngine)
      throws LogIdTooSmall {
    Collections.sort(logsToBeInserted);
    List<MutationLogEntry> logsReadyToBeApplied = new ArrayList<>();
    try {
      segmentLogMetadata.insertOrUpdateLogs(logsToBeInserted, false, logsReadyToBeApplied);

      if (plalEngine != null) {
        logsReadyToBeApplied.forEach(log -> plalEngine.putLog(segId, log));
      }
    } catch (LogIdTooSmall logIdTooSmall) {
      if (logIdTooSmallPrintCounter++ % printPeriod == 0) {
        logger.warn("caught a log id too small exception {}", logIdTooSmall.getMessage());
      } else {
        logger.info("caught a log id too small exception {}", logIdTooSmall.getMessage());
      }
      logsToBeInserted.forEach(logEntry -> {
        CompletingLog removed = mapLogsUuidToCompletingLogs.remove(logEntry.getUuid());
        MutationLogEntryFactory.releaseLogData(logEntry);
      });
      throw logIdTooSmall;
    }
  }

  private void insertCompleteLogs(List<MutationLogEntry> logsToBeInserted, PlalEngine plalEngine,
      AbstractMethodCallback callback) {
    AbstractMethodCallback innerCallBack = new AbstractMethodCallback() {
      @Override
      public void complete(Object object) {
        Exception exception = null;
        logger.debug("going to insert {} logs {} ", logsToBeInserted.size(), logsToBeInserted);
        Collections.sort(logsToBeInserted);
        ArrayList<MutationLogEntry> tmpList = new ArrayList<>(1);

        for (MutationLogEntry log : logsToBeInserted) {
          CompletingLog completingLog = mapLogsUuidToCompletingLogs.get(log.getUuid());
          try {
            validateCompleteLogStatus(completingLog, log);
          } catch (InvalidSegmentStatusException e) {
            exception = e;
            continue;
          } catch (Exception e) {
            logger.error(
                "caught an unexpected exception here, completing log {}, mutation log {} seg {}",
                completingLog, log, segId);
            exception = e;
            continue;
          }

          tmpList.clear();
          tmpList.add(log);
          try {
            insertLogs(tmpList, plalEngine);
            completingLog.complete();
          } catch (Exception logIdTooSmall) {
            logger.info("logger already print warn before,", logIdTooSmall);
            exception = logIdTooSmall;

            completingLog.complete();
          }
        }

        if (exception == null) {
          outerComplete();
        } else {
          outerFail(exception);
        }
      }

      @Override
      public void fail(Exception e) {
        logger.error("", e);
        for (MutationLogEntry log : logsToBeInserted) {
          CompleteLog completeLog = (CompleteLog) mapLogsUuidToCompletingLogs.get(log.getUuid());
          if (completeLog != null) {
            completeLog.fail(e);
          }
        }
        outerFail(e);
      }

      private void validateCompleteLogStatus(CompletingLog completingLog, MutationLogEntry log)
          throws Exception {
        if (completingLog == null) {
          logger.warn("not exist in not insert logs {} seg {}", log, segId);
          if (!segmentLogMetadata.isDestroyed()) {
            logger.error("i do not think it will happen {}", segmentLogMetadata);
            throw new IllegalStateException();
          } else {
            throw new InvalidSegmentStatusException();
          }
        }

        if (!(completingLog instanceof CompleteLog)) {
          logger.error("what happened? exist {} insert logs {} seg {} image {}", completingLog, log,
              segId, segmentLogMetadata.getLogImageIncludeLogsAfterPcl());
          throw new IllegalStateException();
        }
      }

      private void outerComplete() {
        if (callback != null) {
          callback.complete(logsToBeInserted);
        }
      }

      private void outerFail(Exception e) {
        if (callback != null) {
          callback.fail(e);
        }
      }
    };

    innerCallBack.complete(logsToBeInserted);
  }

  private void addToDelayQueue(LogWithoutLogEntry logWithoutLogEntry) {
    ExpiredCompletingLog expiredCompletingLog = new ExpiredCompletingLog(logWithoutLogEntry,
        mapLogsUuidToCompletingLogs);
    expiredCompletingLog.setDelay(logCompletingTimeout);
    drivingThread
        .newTimeout(expiredCompletingLog, expiredCompletingLog.delayMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public Map<Long, MutationLogEntry> getAllNotInsertedLogs() {
    Set<Map.Entry<Long, CompletingLog>> set = mapLogsUuidToCompletingLogs.entrySet();
    Iterator<Map.Entry<Long, CompletingLog>> iterator = set.iterator();
    Map<Long, MutationLogEntry> allLogs = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<Long, CompletingLog> entry = iterator.next();
      Long logUuid = entry.getKey();
      CompletingLog completingLog = entry.getValue();
      if (completingLog != null && completingLog instanceof LogWithoutLogId
          && !(completingLog instanceof BrokenLogWithoutLogId)) {
        allLogs.put(logUuid, ((LogWithoutLogId) completingLog).getLogEntry());
      }
    }

    return allLogs;
  }

  @Override
  public int getAllNotInsertedLogsCount() {
    int count = 0;
    for (CompletingLog completingLog : mapLogsUuidToCompletingLogs.values()) {
      if (completingLog instanceof LogWithoutLogId
          && !(completingLog instanceof BrokenLogWithoutLogId)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public void checkPrimaryMissingLogs(List<Long> uuids, long timeOut) {
    for (Long uuid : uuids) {
      Long lastExistTime = logsMissingAtPrimary.putIfAbsent(uuid, System.currentTimeMillis());
      long timePassed = lastExistTime == null ? 0 : System.currentTimeMillis() - lastExistTime;
      if (timePassed > timeOut) {
        CompletingLog completingLog = mapLogsUuidToCompletingLogs.remove(uuid);
        logsMissingAtPrimary.remove(uuid);
        if (completingLog != null) {
          completingLog.fail(new Exception("primary missing log"));
          if (completingLog instanceof LogWithoutLogId) {
            MutationLogEntry logEntry = ((LogWithoutLogId) completingLog).getLogEntry();
            if (logEntry != null) {
              MutationLogEntryFactory.releaseLogData(logEntry);
            }
          } else if (completingLog instanceof CompleteLog) {
            logger.warn("the log has been completed {}", completingLog);
          }
        }
      }
    }

    List<Long> uuidsToRemove = new ArrayList<>();
    for (Map.Entry<Long, Long> uuidAndTimeout : logsMissingAtPrimary.entrySet()) {
      if (System.currentTimeMillis() - uuidAndTimeout.getValue() > timeOut) {
        CompletingLog completingLog = mapLogsUuidToCompletingLogs.get(uuidAndTimeout.getKey());
        if (completingLog == null || completingLog instanceof CompleteLog
            || completingLog instanceof LogWithoutLogEntry) {
          uuidsToRemove.add(uuidAndTimeout.getKey());
        }
      }
    }

    for (Long uuid : uuidsToRemove) {
      logsMissingAtPrimary.remove(uuid);
    }

  }

  @Override
  public void removeCompletingLogsAndCompletedLogsPriorTo(long newPclId) {
    List<Long> uuidsToRemove = new LinkedList<>();
    for (Map.Entry<Long, CompletingLog> mapEntry : mapLogsUuidToCompletingLogs.entrySet()) {
      long uuid = mapEntry.getKey();
      CompletingLog removed = mapEntry.getValue();
      if (!(removed instanceof CompleteLog)) {
        uuidsToRemove.add(uuid);
      } else {
        CompleteLog completeLog = (CompleteLog) removed;
        if (completeLog.getLogEntry().getLogId() < newPclId) {
          uuidsToRemove.add(uuid);
        }
      }
    }
    for (Long uuid : uuidsToRemove) {
      CompletingLog removed = mapLogsUuidToCompletingLogs.remove(uuid);
      if (!(removed instanceof CompleteLog)) {
        if (removed instanceof LogWithoutLogId) {
          MutationLogEntry logEntry = ((LogWithoutLogId) removed).getLogEntry();
          if (logEntry != null) {
            MutationLogEntryFactory.releaseLogData(logEntry);
          }
        }
        removed.fail(new Exception("log has been removed"));
      } else {
        CompleteLog completeLog = (CompleteLog) removed;
        if (completeLog.getLogEntry().getLogId() < newPclId) {
          MutationLogEntry logEntry = completeLog.getLogEntry();
          if (logEntry != null) {
            MutationLogEntryFactory.releaseLogData(logEntry);
          }
        }
      }
    }

    logsMissingAtPrimary.clear();
  }

  @Override
  public void clearAllLogs(boolean force) {
    List<Long> uuidsToRemove = new LinkedList<>();
    for (Map.Entry<Long, CompletingLog> mapEntry : mapLogsUuidToCompletingLogs.entrySet()) {
      long uuid = mapEntry.getKey();
      CompletingLog log = mapEntry.getValue();
      if (!(log instanceof CompleteLog)) {
        if (log instanceof LogWithoutLogId) {
          MutationLogEntry logEntry = ((LogWithoutLogId) log).getLogEntry();
          if (logEntry != null) {
            MutationLogEntryFactory.releaseLogData(logEntry);
          }
        }
        log.fail(new Exception("log has been removed"));
      } else {
        CompleteLog completeLog = (CompleteLog) log;
        if (force) {
          MutationLogEntry logEntry = completeLog.getLogEntry();
          if (logEntry != null) {
            MutationLogEntryFactory.releaseLogData(logEntry);
          }
        }
      }
      uuidsToRemove.add(uuid);
    }
    for (Long uuid : uuidsToRemove) {
      CompletingLog removed = mapLogsUuidToCompletingLogs.remove(uuid);
    }

    logsMissingAtPrimary.clear();
  }

  @Override
  public void removeLog(MutationLogEntry log, boolean completedOnly) {
    if (completedOnly) {
      CompletingLog completingLog = mapLogsUuidToCompletingLogs.get(log.getUuid());
      if (completingLog != null && completingLog instanceof CompleteLog) {
        if (!mapLogsUuidToCompletingLogs.remove(log.getUuid(), completingLog)) {
          removeLog(log, true);
        }
      }
    } else {
      CompletingLog completingLog = mapLogsUuidToCompletingLogs.remove(log.getUuid());
      if (completingLog != null) {
        if (!(completingLog instanceof CompleteLog)) {
          if (completingLog instanceof LogWithoutLogId) {
            MutationLogEntry localLog = ((LogWithoutLogId) completingLog).getLogEntry();
            if (localLog != log) {
              logger.warn("releasing a log data almost missing");
              MutationLogEntryFactory.releaseLogData(localLog);
            }
          }
          completingLog.fail(new Exception("log has been removed"));
        }
      }
    }
  }

  @Override
  public MutationLogEntry getLogEntry(long logUuid) {
    CompletingLog completingLog = mapLogsUuidToCompletingLogs.get(logUuid);
    if (completingLog instanceof LogWithoutLogId) {
      return ((LogWithoutLogId) completingLog).getLogEntry();
    } else if (completingLog instanceof CompleteLog) {
      return ((CompleteLog) completingLog).getLogEntry();
    } else {
      return null;
    }
  }

  @Override
  public CompletingLog getCompletingLog(long logUuid) {
    return mapLogsUuidToCompletingLogs.get(logUuid);
  }

  public void dumpMap() {
    logger.error("@@@@@@@@ dump not inserted logs for {} @@@@@@@ size : {} {}", segId,
        mapLogsUuidToCompletingLogs.size(), mapLogsUuidToCompletingLogs);
  }

  public void cleanUpTimeOutCompletingLog(long cleanCompletingLogPeriod) {
    Set<Map.Entry<Long, CompletingLog>> set = mapLogsUuidToCompletingLogs.entrySet();
    Iterator<Map.Entry<Long, CompletingLog>> iterator = set.iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, CompletingLog> entry = iterator.next();
      if (entry.getValue().getLogCreatedTime() + cleanCompletingLogPeriod < System
          .currentTimeMillis()) {
        if (entry.getValue() instanceof LogWithoutLogEntry) {
          logger.warn("a completing log has expire {}, crash out it {}", entry.getValue(), segId);
          entry.getValue().fail(ExceptionWithNoStack.INSTANCE);
          mapLogsUuidToCompletingLogs.remove(entry.getKey());
        } else if (entry.getValue() instanceof LogWithoutLogId) {
          logger.warn("a completing log has expire {}, crash out it {}", entry.getValue(), segId);
          MutationLogEntryFactory
              .releaseLogData(((LogWithoutLogId) entry.getValue()).getLogEntry());
          entry.getValue().fail(ExceptionWithNoStack.INSTANCE);
          mapLogsUuidToCompletingLogs.remove(entry.getKey());
        } else {
          if (printCounter++ % printPeriod == 0) {
            logger.warn("a completing log has expire {}, crash out it {}", entry.getValue(), segId);
          }
        }
      }
    }
  }

  static class ExpiredCompletingLog implements TimerTask {
    final LogWithoutLogEntry logWithoutLogEntry;
    final Map<Long, CompletingLog> hostedMap;
    ExpiredType expiredType;
    private long delayMs;
    
    ExpiredCompletingLog(LogWithoutLogEntry logWithoutLogEntry,
        Map<Long, CompletingLog> hostedMap) {
      this.logWithoutLogEntry = logWithoutLogEntry;
      this.hostedMap = hostedMap;
      this.expiredType = ExpiredType.EXPIRED_FOR_LISTENER;
    }

    @Override
    public void run(Timeout timeout) {
      ExpiredCompletingLog nextTask = expire();
      if (nextTask != null) {
        drivingThread.newTimeout(nextTask, nextTask.delayMs, TimeUnit.MILLISECONDS);
      }
    }

    ExpiredCompletingLog expire() {
      if (expiredType == ExpiredType.EXPIRED_FOR_LISTENER) {
        if (logWithoutLogEntry != null) {
          CompletingLog currentValue =
              hostedMap == null ? null : hostedMap.get(logWithoutLogEntry.getUuid());
          if (logWithoutLogEntry.fail(ExceptionWithNoStack.INSTANCE)) {
            logger.debug("a log timed out ! {}, current value in map {}",
                logWithoutLogEntry, currentValue);
          }
          this.expiredType = ExpiredType.EXPIRED_FOR_HOSTED_MAP;
          if (logWithoutLogEntry == currentValue) {
            setDelay(DEFAULT_TIMEOUT_FOR_HOSTED_MAP);
            return this;
          }
        }
        return null;
      } else if (expiredType == ExpiredType.EXPIRED_FOR_HOSTED_MAP) {
        if (hostedMap != null && logWithoutLogEntry != null) {
          if (hostedMap.remove(logWithoutLogEntry.getUuid(), logWithoutLogEntry)) {
            logger.debug("remove a expired completing log {}", logWithoutLogEntry);
          }
        }
        return null;
      } else {
        logger.error("a strange type {}", expiredType);
        return null;
      }
    }

    public void setDelay(long delayMills) {
      this.delayMs = delayMills;
    }

    private enum ExpiredType {
      EXPIRED_FOR_LISTENER,
      EXPIRED_FOR_HOSTED_MAP
    }

  }
}

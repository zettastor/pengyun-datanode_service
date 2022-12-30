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

package py.datanode.segment.datalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveOptions;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.common.DoubleLinkedList;
import py.common.DoubleLinkedList.DoubleLinkedListIterator;
import py.common.DoubleLinkedList.ListNode;
import py.common.RequestIdBuilder;
import py.common.struct.Pair;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.InappropriateLogStatusException;
import py.datanode.exception.InvalidLogStatusException;
import py.datanode.exception.LogExpiredException;
import py.datanode.exception.LogIdTooLarge;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.exception.LogNotFoundException;
import py.datanode.exception.LogsNotInRightOrderException;
import py.datanode.exception.LogsNotInSamePageException;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.broadcast.BrokenLogWithoutLogId;
import py.datanode.segment.datalog.broadcast.CompleteLog;
import py.datanode.segment.datalog.broadcast.CompletingLog;
import py.datanode.segment.datalog.broadcast.GiveYouLogIdEvent;
import py.datanode.segment.datalog.broadcast.LogWithoutLogEntry;
import py.datanode.segment.datalog.broadcast.LogWithoutLogId;
import py.datanode.segment.datalog.broadcast.NotInsertedLogs;
import py.datanode.segment.datalog.broadcast.NotInsertedLogsInterface;
import py.datanode.segment.datalog.broadcast.listener.CompletingLogListener;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogsBuilder;
import py.datanode.segment.datalogbak.catchup.PclDriver;
import py.exception.NoAvailableBufferException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog.PbBackwardSyncLogMetadata;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.thrift.share.BroadcastLogStatusThrift;
import py.thrift.share.BroadcastLogThrift;
import py.utils.BroadcastLogUuidGenerator;

public class SegmentLogMetadata {
  private static final Logger logger = LoggerFactory.getLogger(SegmentLogMetadata.class);
  private static final int PRINT_PERIOD = 10000;
  private static final int DEFAULT_WINDOW_SIZE = 20000;
  private static final int DEFAULT_SPLIT_COUNT = 50;
  private static int logIdTooSmallPrintCounter = 0;
  private static int MAX_PENDING_LOGS_FROM_PPL_TO_PLAL = 4096;

  private final SegId segId;
  private final int pageSize;
  private final int quorumSize;

  private final Map<Long, Long> mapLogsUuidToLogId = new ConcurrentHashMap<>();
  private final ReentrantLock createLogLock;
  private final ReentrantLock logIdLock;
  /**
   * this is for the secondaries only, we will get logs without log id. Save them here temporally.
   */
  private final NotInsertedLogsInterface notInsertedLogs;
  /**
   * this map record the last uuid removed by PSWL pointer for each coordinator the purpose is to
   * let segment log metadata sense that a duplicate log comes after the first log is removed from
   * log queue.
   */
  private final Map<Long, Pair<Long, Long>> lastRemovedUuid = new ConcurrentHashMap<>();
  /**
   * An instance of SegmentLogMetadata might be destroyed when copying pages, so we need a flag to
   * prevent logs submit to a destroyed instance which will cause a memory leak.
   */
  private boolean destroyed = false;
  // all logs for the segment
  private DoubleLinkedList<ListNode<MutationLogEntry>> segLogs;
  // total logs
  private AtomicLong segLogConut = new AtomicLong(0);
  // a group of double linked list. Each linked list contains all logs belonging to a page.
  // The key of the map is the
  // page offset at the segment
  private Map<Long, DoubleLinkedList<MutationLogEntry>> pageLogs = new HashMap<>();
  private Map<Long, ListNode<ListNode<MutationLogEntry>>> mapLogIdLogEntry =
      new ConcurrentHashMap<>();
  private int numLogsFromPlalToPcl;
  private int numLogsAfterPcl;
  private int numLogsFromPplToPlal;

  private ReentrantLock reentrantLock = new ReentrantLock(true);
  private AtomicBoolean freezeClInPrimary = new AtomicBoolean(false);

  private long swplLogId;

  private volatile long primarySwplLogId;

  private long swclLogId;

  @JsonIgnore
  private long primaryMaxLogIdWhenPsi;

  private volatile int snapshotVersionOfPcl;
  private int maxAppliedSnapId;

  private volatile DoubleLinkedListIterator<ListNode<MutationLogEntry>> pcl;

  private volatile DoubleLinkedListIterator<ListNode<MutationLogEntry>> plal;

  private volatile DoubleLinkedListIterator<ListNode<MutationLogEntry>> ppl;

  private Map<InstanceId, PeerLogIdAndExpirationTime> peerClIds;

  private Map<InstanceId, PeerLogIdAndExpirationTime> peerPlIds;

  private AtomicLong preparePplId = new AtomicLong();

  private AtomicLong latestPreparePplId = new AtomicLong();
  private PeerLogIdAndExpirationTime primaryClId;
  private SegmentUnit segmentUnit;
  private LogIdGenerator logIdGenerator;

  public SegmentLogMetadata(SegId segId, int pageSize, int quorumSize) {
    this(segId, pageSize, quorumSize, new LogIdWindow(DEFAULT_WINDOW_SIZE, DEFAULT_SPLIT_COUNT));
  }

  public SegmentLogMetadata(SegId segId, int pageSize, int quorumSize, LogIdWindow window) {
    this(segId, pageSize, quorumSize, Optional.empty(), window);
  }

  public SegmentLogMetadata(SegId segId, int pageSize, int quorumSize,
      Optional<Integer> giveYouLogIdTimeout,
      LogIdWindow window) {
    this.segId = segId;
    this.pageSize = pageSize;
    this.quorumSize = quorumSize;
    this.segLogs = new DoubleLinkedList<>();
    this.pageLogs = new HashMap<>();

    this.numLogsAfterPcl = 0;
    this.numLogsFromPplToPlal = 0;
    this.numLogsFromPlalToPcl = 0;

    this.swplLogId = LogImage.INVALID_LOG_ID;
    this.swclLogId = LogImage.INVALID_LOG_ID;
    this.peerClIds = new HashMap<>();
    this.peerPlIds = new HashMap<>();

    this.pcl = segLogs.head();
    this.plal = segLogs.head();
    this.ppl = segLogs.head();
    this.createLogLock = new ReentrantLock(true);
    this.logIdLock = new ReentrantLock(true);

    NotInsertedLogs notInsertedLogs = giveYouLogIdTimeout.isPresent()
        ? new NotInsertedLogs(segId, this, giveYouLogIdTimeout.get()) :
        new NotInsertedLogs(segId, this);
    this.notInsertedLogs = notInsertedLogs;

  }

  public static void finalizeLogStatus(MutationLogEntry logAtSecondary,
      LogStatus logStatusAtPrimary, SegmentLogMetadata segmentLogMetadata) {
    if (MutationLogEntry.isFinalStatus(logStatusAtPrimary)) {
      if (logAtSecondary.isFinalStatus()) {
        if (logAtSecondary.getStatus() != logStatusAtPrimary) {
          Validate.isTrue(false,
              "existing=" + logAtSecondary + ", primary status=" + logStatusAtPrimary);
        }
      } else {
        logAtSecondary.setStatus(logStatusAtPrimary);
      }
    }
  }

  public void removeAllLog(boolean destroy) {
    logger.warn("going to remove all {} logs for {}", segLogs == null ? 0 : segLogs.size(), segId);
    reentrantLock.lock();
    try {
      if (segLogs != null && !segLogs.isEmpty()) {
        while (!segLogs.isEmpty()) {
          DoubleLinkedList.ListNode<MutationLogEntry> mutationLogEntryNode = segLogs
              .removeFromTail();
          if (mutationLogEntryNode.content() != null) {
            MutationLogEntryFactory.releaseLogData(mutationLogEntryNode.content());
          }
        }
      }
      if (pageLogs != null && !pageLogs.isEmpty()) {
        for (DoubleLinkedList<MutationLogEntry> mutationLogEntryList : pageLogs.values()) {
          for (int count = 0; count < mutationLogEntryList.size(); count++) {
            MutationLogEntry mutationLogEntry = mutationLogEntryList.removeFromTail();
            MutationLogEntryFactory.releaseLogData(mutationLogEntry);
          }
        }
        pageLogs.clear();
      }
      if (mapLogIdLogEntry != null && !mapLogIdLogEntry.isEmpty()) {
        for (ListNode<ListNode<MutationLogEntry>> listOfMutationLogEntryList : mapLogIdLogEntry
            .values()) {
          ListNode<MutationLogEntry> mutationLogEntryList = listOfMutationLogEntryList.content();
          if (mutationLogEntryList != null) {
            MutationLogEntryFactory.releaseLogData(mutationLogEntryList.content());
          }
        }
        mapLogIdLogEntry.clear();
      }

      if (mapLogsUuidToLogId != null && !mapLogsUuidToLogId.isEmpty()) {
        mapLogsUuidToLogId.clear();
      }

      notInsertedLogs.clearAllLogs(true);

      if (destroy) {
        destroyed = true;
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public LogImage getLogImageIncludeLogsAfterPcl() {
    reentrantLock.lock();
    try {
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = new DoubleLinkedListIterator<>(
          pcl);
      List<Long> logIdsAfterPcl = new ArrayList<>();
      while (cursor.hasNext()) {
        ListNode<MutationLogEntry> uncommittedLog = cursor.next().content();
        MutationLogEntry log = uncommittedLog.content();
        if (log.getLogId() == LogImage.INVALID_LOG_ID) {
          continue;
        }

        logIdsAfterPcl.add(log.getLogId());
      }

      if (logIdsAfterPcl.size() != numLogsAfterPcl) {
        logger.warn("it is not equals logs after pcl, {}, {}", logIdsAfterPcl.size(),
            numLogsAfterPcl);
      }

      List<Long> uuidsOfLogsWithoutLogId = Lists
          .newLinkedList(notInsertedLogs.getAllNotInsertedLogs().keySet());

      return new LogImage(getSwplId(), getSwclId(), ppl, plal, pcl, numLogsFromPplToPlal,
          numLogsFromPlalToPcl,
          numLogsAfterPcl, logIdsAfterPcl, uuidsOfLogsWithoutLogId, getMaxLogId());
    } finally {
      reentrantLock.unlock();
    }
  }

  public LogImage getLogImage(int numOfAfterPcl) {
    reentrantLock.lock();
    try {
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = new DoubleLinkedListIterator<>(
          pcl);
      List<Long> logIdsAfterPcl = new ArrayList<>();
      int dealWithCount = 0;
      while (dealWithCount < numOfAfterPcl && cursor.hasNext()) {
        ListNode<MutationLogEntry> uncommittedLog = cursor.next().content();
        MutationLogEntry log = uncommittedLog.content();
        if (log.getLogId() == LogImage.INVALID_LOG_ID) {
          continue;
        }

        logIdsAfterPcl.add(log.getLogId());
        dealWithCount++;
      }

      List<Long> uuidsOfLogsWithoutLogId = null;
      if (0 != numOfAfterPcl) {
        Lists.newLinkedList(notInsertedLogs.getAllNotInsertedLogs().keySet());
      } else {
        uuidsOfLogsWithoutLogId = new LinkedList<>();
      }

      return new LogImage(getSwplId(), getSwclId(), ppl, plal, pcl, numLogsFromPplToPlal,
          numLogsFromPlalToPcl, numLogsAfterPcl, logIdsAfterPcl, uuidsOfLogsWithoutLogId,
          getMaxLogId());
    } finally {
      reentrantLock.unlock();
    }
  }

  public long getPlId() {
    reentrantLock.lock();
    try {
      if (segLogs.isEmpty()) {
        return LogImage.INVALID_LOG_ID;
      } else {
        return ppl.pointToHead() ? LogImage.INVALID_LOG_ID : ppl.content().content().getLogId();
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public long getLalId() {
    reentrantLock.lock();
    try {
      return plal.pointToHead() ? LogImage.INVALID_LOG_ID : plal.content().content().getLogId();
    } catch (Exception e) {
      if (plal == null) {
        logger.warn("segId: {}, numLogsAfterPCL: {}", segId, numLogsAfterPcl);
      } else {
        if (plal.content() == null) {
          logger.warn("segId: {}, numLogsAfterPCL: {}", segId, numLogsAfterPcl);
        } else {
          if (plal.content().content() == null) {
            logger.warn("segId: {}, numLogsAfterPCL: {}", segId, numLogsAfterPcl);
          }
        }
      }
      throw e;
    } finally {
      reentrantLock.unlock();
    }
  }

  public long getMaxLogId() {
    reentrantLock.lock();
    try {
      if (segLogs.isEmpty()) {
        return LogImage.INVALID_LOG_ID;
      }
      return segLogs.tail().content().content().getLogId();
    } finally {
      reentrantLock.unlock();
    }
  }

  private MutationLogEntry getClLogEntry() {
    reentrantLock.lock();
    try {
      return pcl.pointToHead() ? MutationLogEntry.INVALID_MUTATION_LOG : pcl.content().content();
    } catch (Exception e) {
      if (pcl == null) {
        logger.warn("segId: {}, numLogsAfterPCL: {}", segId, numLogsAfterPcl);
      } else {
        if (pcl.content() == null) {
          logger.warn("segId: {}, numLogsAfterPCL: {}", segId, numLogsAfterPcl);
        } else {
          if (pcl.content().content() == null) {
            logger.warn("segId: {}, numLogsAfterPCL: {}", segId, numLogsAfterPcl);
          }
        }
      }
      throw e;
    } finally {
      reentrantLock.unlock();
    }

  }

  public long getClId() {
    return getClLogEntry().getLogId();
  }

  public SegId getSegId() {
    return segId;
  }

  public void updateLogStatus(long logId, MutationLogEntry.LogStatus newStatus)
      throws InappropriateLogStatusException, LogNotFoundException {
    ListNode<ListNode<MutationLogEntry>> node = mapLogIdLogEntry.get(logId);
    if (node != null && node.content() != null && node.content().content() != null) {
      MutationLogEntry log = node.content().content();
      log.setStatus(newStatus);
      if (log.getStatus() == MutationLogEntry.LogStatus.Aborted
          || log.getStatus() == MutationLogEntry.LogStatus.AbortedConfirmed) {
        MutationLogEntryFactory.releaseLogData(log);
      }
    } else {
      throw new LogNotFoundException("log with " + logId + " cant' be found");
    }
  }

  public MutationLogEntry removeLog(long logId) throws LogIdTooSmall {
    reentrantLock.lock();
    try {
      Long logIdObject = logId;
      ListNode<ListNode<MutationLogEntry>> node = mapLogIdLogEntry.get(logIdObject);
      if (node == null || node.isHead() || node.content() == null || node.content().isHead()
          || node.content().content() == null) {
        logger.debug("a missing log with logId {}  not exists. return it silently", logId);
        return null;
      }

      ListNode<MutationLogEntry> cl = pcl.content();
      if (cl == null) {
        logger.error("cl is null for segId: {}, logId: {}", segId, logId);
      }

      if (cl != null && logId <= cl.content().getLogId()) {
        throw new LogIdTooSmall(
            "cl log id: " + cl.content().getLogId() + ". The new log's id: " + logId)
            .setCurrentSmallestId(cl.content().getLogId());
      }

      ListNode<MutationLogEntry> nodeInPageList = node.content();
      MutationLogEntry logToRemove = nodeInPageList.content();
      Long pageAddress = PageAddressGenerator
          .calculatePageOffsetInSegUnit(logToRemove.getOffset(), pageSize);
      DoubleLinkedList<MutationLogEntry> logsForPage = pageLogs.get(pageAddress);
      if (logsForPage != null && !logsForPage.isEmpty()) {
        logsForPage.removeNode(nodeInPageList);
      }
      if (logsForPage != null && logsForPage.isEmpty()) {
        pageLogs.remove(pageAddress);
      }

      segLogs.removeNode(node);

      mapLogIdLogEntry.remove(logIdObject);
      MutationLogEntryFactory.releaseLogData(logToRemove);

      mapLogsUuidToLogId.remove(logToRemove.getUuid());
      notInsertedLogs.removeLog(logToRemove, true);

      this.numLogsAfterPcl--;

      return logToRemove;
    } finally {
      reentrantLock.unlock();
    }
  }

  public void removeLogs(List<MutationLogEntry> logs)
      throws LogIdTooSmall, InappropriateLogStatusException {
    reentrantLock.lock();
    try {
      for (MutationLogEntry log : logs) {
        removeLog(log.getLogId());
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public MutationLogEntry appendLog(MutationLogEntry mutationLog)
      throws LogIdTooSmall, InappropriateLogStatusException {
    reentrantLock.lock();
    try {
      return addLog(mutationLog, false);
    } finally {
      reentrantLock.unlock();
    }
  }

  public void insertOrUpdateLogs(Collection<MutationLogEntry> logs, boolean firstSearchForward,
      List<MutationLogEntry> logsThatCanBeApplied) throws LogIdTooSmall {
    if (logs.isEmpty()) {
      return;
    }

    reentrantLock.lock();
    try {
      validateNotDestroyed();
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = null;
      long firstLogId = logs.iterator().next().getLogId();

      boolean positionFound = false;
      ListNode<ListNode<MutationLogEntry>> position = this.mapLogIdLogEntry.get(firstLogId - 1);
      if (position != null) {
        cursor = new DoubleLinkedListIterator<>(position);
        positionFound = true;
      } else {
        if (firstSearchForward) {
          cursor = new DoubleLinkedListIterator<>(pcl);
        } else {
          cursor = segLogs.head();
        }
      }

      try {
        long clId = getClId();
        for (MutationLogEntry entry : logs) {
          MutationLogEntry existing = getAndUpdateExistMutationLog(entry);
          if (null == existing) {
            if (entry.getLogId() <= clId) {
              throw new LogIdTooSmall(
                  "cl log id: " + clId + ". The new log: " + entry + ". The existing log: " + null
                      + ", segId: " + segId).setCurrentSmallestId(clId);
            }

            cursor = insertMutationLogToDoubleLinkedList(entry,
                positionFound ? true : firstSearchForward,
                true, cursor);
            entry.insert();

            if (logsThatCanBeApplied != null && entry.canBeApplied()) {
              logsThatCanBeApplied.add(entry);
            }
            positionFound = true;
          } else {
            if (entry.getLogId() <= clId) {
              logger.info("the log:{} has been persist, existing: {}", entry, existing);
            }
            if (logsThatCanBeApplied != null && existing.canBeApplied()) {
              logsThatCanBeApplied.add(existing);
            }
          }
        }

      } catch (LogIdTooSmall e) {
        if (logIdTooSmallPrintCounter++ % PRINT_PERIOD == 0) {
          logger.warn("caught an exception", e);
        } else {
          logger.info("caught an exception", e);
        }
        for (MutationLogEntry entry : logs) {
          MutationLogEntryFactory.releaseLogData(entry);
        }
        throw e;
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public void searchContinuousPagesToApply(PageAddress currentPageAddress,
      PageAddress lastPageAddress,
      int maxNumContPagesToApply, List<PageAddress> continuousPagesToBeAppliedTogether) {
    long pageLogicOffsetInSegment = currentPageAddress.getLogicOffsetInSegment(pageSize);
    long lastPageLogicOffsetInSegment =
        lastPageAddress != null ? lastPageAddress.getLogicOffsetInSegment(pageSize)
            : Long.MAX_VALUE;
    SegId segId = currentPageAddress.getSegId();
    int numPagesAdded = 0;
    reentrantLock.lock();
    boolean found = false;
    try {
      while (numPagesAdded < maxNumContPagesToApply) {
        pageLogicOffsetInSegment += pageSize;
        if (pageLogicOffsetInSegment >= lastPageLogicOffsetInSegment) {
          return;
        }
        DoubleLinkedList<MutationLogEntry> logNodesForPage = pageLogs.get(pageLogicOffsetInSegment);
        if (logNodesForPage != null) {
          DoubleLinkedListIterator<MutationLogEntry> cursor = logNodesForPage.head();
          while (cursor.hasNext()) {
            MutationLogEntry logEntry = cursor.next().content();
            Validate.notNull(logEntry);
            if (logEntry.canBeApplied()) {
              PageAddress pageAddress = PageAddressGenerator
                  .generate(segId, currentPageAddress.getSegUnitOffsetInArchive(),
                      pageLogicOffsetInSegment, currentPageAddress.getStorage(), pageSize);
              continuousPagesToBeAppliedTogether.add(pageAddress);
              numPagesAdded++;
              found = true;
              break;
            }
          }

          if (!found) {
            return;
          } else {
            found = false;
          }
        } else {
          return;
        }
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  private DoubleLinkedListIterator<ListNode<MutationLogEntry>> insertMutationLogToDoubleLinkedList(
      MutationLogEntry mutationLog, boolean searchForward, boolean searchForwardForPageList,
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor) {
    long logId = mutationLog.getLogId();
    Long pageAddress = PageAddressGenerator
        .calculatePageOffsetInSegUnit(mutationLog.getOffset(), pageSize);
    DoubleLinkedList<MutationLogEntry> logsForPage = pageLogs.get(pageAddress);
    if (logsForPage == null) {
      MutationLogEntry head = MutationLogEntryFactory.createHeadLog();
      logsForPage = new DoubleLinkedList<>(head);
      pageLogs.put(pageAddress, logsForPage);
    }

    DoubleLinkedListIterator<MutationLogEntry> pageCursor = logsForPage.head();
    Pair<DoubleLinkedListIterator<MutationLogEntry>, Boolean> howToInsertInPageList = null;
    if (pageCursor.hasPrevious()) {
      DoubleLinkedListIterator<MutationLogEntry> previous = pageCursor.previous();
      if (previous.content().getLogId() == logId - 1) {
        howToInsertInPageList = new Pair<>(previous, true);
      }
    }

    if (howToInsertInPageList == null) {
      howToInsertInPageList = searchPositionToAddLog(searchForwardForPageList, logsForPage.head(),
          new Function<MutationLogEntry, Long>() {
            public Long apply(MutationLogEntry log) {
              Validate.notNull(log);
              return log.getLogId();
            }
          }, logId);
    }

    ListNode<MutationLogEntry> newNode;
    if (howToInsertInPageList.getSecond()) {
      newNode = logsForPage.insertAfter(mutationLog, howToInsertInPageList.getFirst());
    } else {
      newNode = logsForPage.insertBefore(mutationLog, howToInsertInPageList.getFirst());
    }

    Pair<DoubleLinkedListIterator<ListNode<MutationLogEntry>>, Boolean> howToInsertInList =
        searchPositionToAddLog(
        searchForward, cursor, new Function<ListNode<MutationLogEntry>, Long>() {
          public Long apply(ListNode<MutationLogEntry> logNode) {
            Validate.notNull(logNode.content());
            return logNode.content().getLogId();
          }
        }, logId);

    boolean insertAfter = howToInsertInList.getSecond();
    DoubleLinkedListIterator<ListNode<MutationLogEntry>> insertPosition = howToInsertInList
        .getFirst();
    ListNode<ListNode<MutationLogEntry>> newNodePosition;
    if (insertAfter) {
      newNodePosition = segLogs.insertAfter(newNode, insertPosition);
      segLogConut.incrementAndGet();
    } else {
      newNodePosition = segLogs.insertBefore(newNode, insertPosition);
      segLogConut.incrementAndGet();
    }

    mapLogIdLogEntry.put(logId, newNodePosition);
    numLogsAfterPcl++;
    return new DoubleLinkedListIterator<>(newNodePosition);

  }

  private MutationLogEntry getAndUpdateExistMutationLog(MutationLogEntry mutationLog)
      throws LogIdTooSmall {
    long logId = mutationLog.getLogId();

    MutationLogEntry existingLogEntry = findLog(logId);
    if (existingLogEntry == null) {
      return null;
    }

    if (existingLogEntry == mutationLog) {
      logger.info("they are referencing to the same log object.");
      return mutationLog;
    }

    logger.debug("log with logId {} exists. Update its status from {} to {} ", logId,
        existingLogEntry.getStatus(),
        mutationLog.getStatus());

    try {
      if (!mutationLog.isDummy() && mutationLog.getOffset() != existingLogEntry.getOffset()) {
        Validate.isTrue(false, "current: " + mutationLog + ", existing: " + existingLogEntry);
      }

      MutationLogEntry.LogStatus newStatus = mutationLog.getStatus();
      if (existingLogEntry.isFinalStatus()) {
        if (newStatus != existingLogEntry.getStatus()) {
          if (mutationLog.isFinalStatus()) {
            logger.warn("existing: {}, out log: {}", existingLogEntry, mutationLog);
          } else {
            logger.info("existing: {}, out log: {}", existingLogEntry, mutationLog);
          }
        }
      } else {
        existingLogEntry.setStatus(newStatus);
        if (newStatus == MutationLogEntry.LogStatus.AbortedConfirmed) {
          logger.warn("release an log {}", existingLogEntry);
          existingLogEntry.apply();
          MutationLogEntryFactory.releaseLogData(existingLogEntry);
        }
      }

      if (mutationLog.isApplied() && !existingLogEntry.isApplied()) {
        logger.warn("current log: {}, existing: {}, clId: {}, lalId: {}", mutationLog,
            existingLogEntry,
            getLalId(), getClId(), new Exception());
      }

      if (mutationLog.isPersisted() && !existingLogEntry.isPersisted()) {
        if (newStatus != MutationLogEntry.LogStatus.Aborted
            && newStatus != MutationLogEntry.LogStatus.AbortedConfirmed) {
          Validate
              .isTrue(false, "existing log: " + existingLogEntry + ", current log: " + mutationLog);
        } else {
          if (existingLogEntry.isFinalStatus()) {
            existingLogEntry.setPersisted();
          }
        }
      }

    } finally {
      logger.debug("i have the log {},so i will release the new one {} ", existingLogEntry,
          mutationLog);
      MutationLogEntryFactory.releaseLogData(mutationLog);
    }

    return existingLogEntry;
  }

  public MutationLogEntry insertLog(MutationLogEntry mutationLog)
      throws LogIdTooSmall, InappropriateLogStatusException {
    reentrantLock.lock();
    try {
      return addLog(mutationLog, true);
    } finally {
      reentrantLock.unlock();
    }
  }

  private void validateNotDestroyed() {
    if (destroyed) {
      throw new IllegalStateException("this segment log metadata has been destroyed");
    }
  }

  public boolean isDestroyed() {
    reentrantLock.lock();
    try {
      return destroyed;
    } finally {
      reentrantLock.unlock();
    }
  }

  private MutationLogEntry addLog(MutationLogEntry newMutationLog, boolean searchForward)
      throws LogIdTooSmall, InappropriateLogStatusException {
    validateNotDestroyed();

    try {
      long clId = getClId();
      if (newMutationLog.getLogId() < clId) {
        MutationLogEntryFactory.releaseLogData(newMutationLog);
        throw new LogIdTooSmall(
            "cl log id: " + clId + ". The new log: " + newMutationLog + ", segId: " + segId)
            .setCurrentSmallestId(clId);
      }

      MutationLogEntry existingLogEntry = this.getAndUpdateExistMutationLog(newMutationLog);
      if (null != existingLogEntry) {
        return existingLogEntry;
      }

      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor;
      long priorId = newMutationLog.getLogId() - 1;
      ListNode<ListNode<MutationLogEntry>> position = this.mapLogIdLogEntry.get(priorId);
      if (position != null) {
        cursor = new DoubleLinkedListIterator<>(position);

        searchForward = true;
      } else {
        if (searchForward) {
          cursor = new DoubleLinkedListIterator<>(pcl);
        } else {
          cursor = segLogs.head();
        }
      }

      insertMutationLogToDoubleLinkedList(newMutationLog, searchForward, searchForward, cursor);
      newMutationLog.insert();
      return newMutationLog;
    } finally {
      logger.info("nothing need to do here");
    }
  }

  public List<MutationLogEntry> moveClToInPrimary() {
    Predicate<List<MutationLogEntry>> logEntryPredicate = mutationLogEntry -> true;
    return moveClToInPrimary(logEntryPredicate);
  }

  public List<MutationLogEntry> moveClToInPrimary(
      Predicate<List<MutationLogEntry>> isLogCanAbortConfirm) {
    List<MutationLogEntry> mutationLogEntryArrayList = new ArrayList<>();
    List<MutationLogEntry> mutationLogEntries = new ArrayList<>();
    DoubleLinkedListIterator<ListNode<MutationLogEntry>> oldPcl = pcl;
    DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor =
        new DoubleLinkedListIterator<ListNode<MutationLogEntry>>(
        oldPcl, null, null);

    ListNode<ListNode<MutationLogEntry>> previous = cursor.getCurrentPosition();
    ListNode<ListNode<MutationLogEntry>> previousWithFirstAbortLog = cursor.getCurrentPosition();
    DoubleLinkedListIterator<ListNode<MutationLogEntry>> next;
    int numSteps = 0;
    int numStepsWithFirtAbortLog = 0;
    List<MutationLogEntry> abortLogs = new ArrayList<>();
    List<MutationLogEntry> logEntries = new ArrayList<>();
    logger.debug("begin cursor all logs after pcl {}", cursor.content());
    boolean firstFind = true;
    while (cursor.hasNext()) {
      try {
        next = cursor.next();
        MutationLogEntry curLog = next.content().content();
        if (!curLog.isFinalStatus()) {
          if (curLog.isExpired()) {
            if (firstFind) {
              firstFind = false;
              numStepsWithFirtAbortLog = numSteps;
            }
            logger.warn(
                "there is a log not in final status, and it's expired, "
                    + "confirm finish it, log={}, segId={}",
                curLog, segId);

            reentrantLock.lock();
            try {
              if (curLog.getStatus() != LogStatus.Aborted) {
                curLog.abort();
                logEntries.add(curLog);
              }
              abortLogs.add(curLog);
            } finally {
              reentrantLock.unlock();
            }
          } else {
            logger.info(
                "there is a log:{} has created/aborted status."
                    + " Move pcl to this log instead, segId={}",
                curLog, segId);
            break;
          }
        }

        previous = next.getCurrentPosition();
        if (firstFind) {
          previousWithFirstAbortLog = next.getCurrentPosition();
        }
        numSteps++;
      } catch (NullPointerException e) {
        logger.warn("now move the pcl for segId({}), pcl: {}, current: {}, previous: {}",
            segId, pcl, cursor,
            previous.content());
        break;
      }
    }

    if (abortLogs.size() > 0) {
      if (isLogCanAbortConfirm.test(abortLogs)) {
        for (MutationLogEntry log : abortLogs) {
          log.confirmAbortAndApply();
        }
        mutationLogEntryArrayList.addAll(mutationLogEntries);
      } else {
        logger.warn("there are some log {}  can not be abort Confirm", abortLogs);
        numSteps = numStepsWithFirtAbortLog;
        previous = previousWithFirstAbortLog;
      }
    }

    if (numSteps == 0) {
      logger.debug("no need move pcl, because some not final status at the first");
      return mutationLogEntryArrayList;
    }

    reentrantLock.lock();
    try {
      if (freezeClInPrimary.get()) {
        logger.warn("{} primary cl freezed, skip cl moving", segId);
      } else {
        if (oldPcl == pcl) {
          LogImage logImage = this.getLogImage(0);
          logger.debug("begin update log image {}", logImage);
          Validate.isTrue(previous != null && previous.content() != null);
          pcl = new DoubleLinkedListIterator<>(previous);
          snapshotVersionOfPcl = 0;
          numLogsAfterPcl -= numSteps;
          numLogsFromPlalToPcl += numSteps;
          logImage = this.getLogImage(0);
          logger.debug("after update log image {}", logImage);
          if (mutationLogEntryArrayList.size() > 0) {
            getLogsAfterWithSameSnapshotVersion(getClId(), true,
                mutationLogEntryArrayList);
          }
        } else {
          logger.warn("1 old pcl: {}; new pcl: {}", oldPcl.content().content(),
              pcl.content().content());
        }
      }
    } finally {
      reentrantLock.unlock();
    }
    logger.info("new pcl pointer: {}, step: {}", previous.content().content(), numSteps);
    return mutationLogEntryArrayList;
  }

  public void resetFreezeClInPrimary() {
    reentrantLock.lock();
    try {
      freezeClInPrimary.set(false);
    } finally {
      reentrantLock.unlock();
    }
  }

  public boolean freezeClInPrimary(long expectedCl) {
    reentrantLock.lock();
    try {
      if (expectedCl == getClId()) {
        freezeClInPrimary.set(true);
        return true;
      } else {
        logger.warn("{} some one has moved cl just now, not freezing cl in primary", segId);
        return false;
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public long forceMovePclAndRemoveBefore(long pclId) throws LogNotFoundException, LogIdTooSmall {
    reentrantLock.lock();
    try {
      notInsertedLogs.removeCompletingLogsAndCompletedLogsPriorTo(pclId);
      removeLogsPriorTo(pclId, true);
      return moveClTo(pclId).getFirst();
    } finally {
      reentrantLock.unlock();
    }
  }

  public Pair<Long, List<MutationLogEntry>> moveClTo(long newClId)
      throws LogNotFoundException, LogIdTooSmall {
    int numSteps = 0;
    List<MutationLogEntry> logsToRemoveFromNotInsertedLogs = new ArrayList<>();
    List<MutationLogEntry> logEntryListNeedToResubmitToPlal = new ArrayList<>();
    reentrantLock.lock();
    try {
      if (newClId == LogImage.INVALID_LOG_ID) {
        if (pcl.pointToHead()) {
          return new Pair<>(0L, logEntryListNeedToResubmitToPlal);
        } else {
          throw new LogIdTooSmall(
              "cl log id: " + pcl.content().content().getLogId() + ". The new log's id: "
                  + newClId);
        }
      }

      ListNode<ListNode<MutationLogEntry>> dest = mapLogIdLogEntry.get(newClId);
      if (dest == null) {
        throw new LogNotFoundException(
            "log with logId " + newClId + " does not exists, segId: " + segId);
      }

      ListNode<MutationLogEntry> cl = pcl.content();

      if (!pcl.pointToHead() && newClId < cl.content().getLogId()) {
        long clId = cl.content().getLogId();
        throw new LogIdTooSmall(
            "cl log id: " + clId + ". The new log's id: " + newClId + ", segId: " + segId)
            .setCurrentSmallestId(clId);
      }

      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = new DoubleLinkedListIterator<>(
          pcl, null,
          dest);

      ListNode<ListNode<MutationLogEntry>> previous = cursor.getCurrentPosition();
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> next;
      while (cursor.hasNext()) {
        try {
          next = cursor.next();
          ListNode<MutationLogEntry> curNode = next.content();
          if (!curNode.content().isFinalStatus()) {
            logger.info(
                "there is a log:{} has created/aborted status. Move pcl to this log instead",
                curNode.content());
            break;
          }

          previous = next.getCurrentPosition();
          logsToRemoveFromNotInsertedLogs.add(curNode.content());
          numSteps++;
        } catch (NullPointerException e) {
          logger
              .warn("now move the pcl for segId({}), pcl: {}, current: {}, previous: {}",
                  segId, pcl,
                  cursor, previous.content());
          break;
        }
      }

      if (numSteps == 0) {
        return new Pair<>(getClId(), logEntryListNeedToResubmitToPlal);
      }

      Validate.isTrue(previous != null && previous.content() != null);

      pcl = new DoubleLinkedListIterator<>(previous);
      snapshotVersionOfPcl = 0;
      numLogsAfterPcl -= numSteps;
      numLogsFromPlalToPcl += numSteps;
      logger.info("new pcl pointer: {}, numStep: {}, segId: {}", previous.content().content(),
          numSteps, segId);

      if (logEntryListNeedToResubmitToPlal.size() > 0) {
        getLogsAfterWithSameSnapshotVersion(getClId(), true,
            logEntryListNeedToResubmitToPlal);
      }

      return new Pair<>(previous.content().content().getLogId(),
          logEntryListNeedToResubmitToPlal);
    } finally {
      reentrantLock.unlock();
      for (MutationLogEntry log : logsToRemoveFromNotInsertedLogs) {
        mapLogsUuidToLogId.put(log.getUuid(), log.getLogId());
        notInsertedLogs.removeLog(log, false);
      }

    }
  }

  public MutationLogEntry getLog(Long logId) {
    return findLog(logId);
  }

  public MutationLogEntry findLog(Long logId) {
    ListNode<ListNode<MutationLogEntry>> node = mapLogIdLogEntry.get(logId);
    if (node == null) {
      return null;
    } else {
      return node.content().content();
    }
  }

  public boolean logExist(long logId) {
    return findLog(logId) == null ? false : true;
  }

  public void movePlToWithoutCheck(long newPlId, int step)
      throws LogNotFoundException, LogIdTooSmall, LogIdTooLarge {
    reentrantLock.lock();
    try {
      if (newPlId == LogImage.INVALID_LOG_ID) {
        if (ppl.pointToHead()) {
          return;
        } else {
          logger
              .error("The PPL points to a valid log but the new pl id is invalid. Abort the move");
          throw new LogIdTooSmall(
              "lal log id: " + ppl.content().content().getLogId() + ". The new log's id: "
                  + newPlId);
        }
      }

      ListNode<ListNode<MutationLogEntry>> dest = mapLogIdLogEntry.get(newPlId);
      if (dest == null) {
        throw new LogNotFoundException("log with logId" + newPlId + " exists");
      }

      ListNode<MutationLogEntry> pl = ppl.content();
      if (!ppl.pointToHead() && newPlId < pl.content().getLogId()) {
        throw new LogIdTooSmall(
            "pl log id: " + pl.content().getLogId() + ". The new log's id: " + newPlId);
      }

      if (!plal.pointToHead() && plal.content().content().getLogId() < newPlId) {
        throw new LogIdTooLarge(
            "lal log id: " + plal.content().content().getLogId() + ". The new log's id: "
                + newPlId);
      }

      ppl = new DoubleLinkedListIterator<>(dest);
      numLogsFromPplToPlal -= step;
    } finally {
      reentrantLock.unlock();
    }
  }

  public int movePlTo(long newPlId) throws LogNotFoundException, LogIdTooSmall, LogIdTooLarge {
    reentrantLock.lock();
    try {
      if (newPlId == LogImage.INVALID_LOG_ID) {
        if (ppl.pointToHead()) {
          return 0;
        } else {
          logger
              .error("The PPL points to a valid log but the new pl id is invalid. Abort the move");
          throw new LogIdTooSmall(
              "lal log id: " + ppl.content().content().getLogId() + ". The new log's id: "
                  + newPlId);
        }
      }

      ListNode<ListNode<MutationLogEntry>> dest = mapLogIdLogEntry.get(newPlId);
      if (dest == null) {
        throw new LogNotFoundException("log with logId" + newPlId + " exists");
      }

      ListNode<MutationLogEntry> pl = ppl.content();
      if (!ppl.pointToHead() && newPlId < pl.content().getLogId()) {
        throw new LogIdTooSmall(
            "pl log id: " + pl.content().getLogId() + ". The new log's id: " + newPlId);
      }

      if (!plal.pointToHead() && plal.content().content().getLogId() < newPlId) {
        throw new LogIdTooLarge(
            "lal log id: " + plal.content().content().getLogId() + ". The new log's id: "
                + newPlId);
      }

      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = new DoubleLinkedListIterator<>(
          ppl, null,
          dest);

      int numSteps = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ListNode<MutationLogEntry> curNode = cursor.content();
        if (!curNode.content().isPersisted()) {
          logger.info("before move PPL to the log whose logid is {} , there is a log {} "
                  + " has data not being persisted yet. Move ppl to this log instead", newPlId,
              curNode.content());
          cursor.previous();
          break;
        }
        numSteps++;
      }

      ppl = new DoubleLinkedListIterator<>(cursor, null, null);
      numLogsFromPplToPlal -= numSteps;
      return numSteps;
    } finally {
      reentrantLock.unlock();
    }
  }

  public void moveLalToWithoutCheck(long newLalId, int numSteps)
      throws LogNotFoundException, LogIdTooSmall, LogIdTooLarge {
    reentrantLock.lock();
    try {
      if (newLalId == LogImage.INVALID_LOG_ID) {
        if (plal.pointToHead()) {
          return;
        } else {
          logger.error("The PLAL points to a valid log but the new lal is invalid. Abort the move");
          throw new LogIdTooSmall(
              "lal log id: " + plal.content().content().getLogId() + ". The new log's id: "
                  + newLalId);
        }
      }

      ListNode<ListNode<MutationLogEntry>> dest = mapLogIdLogEntry.get(newLalId);
      if (dest == null) {
        throw new LogNotFoundException("log with logId" + newLalId + " not exists");
      }

      ListNode<MutationLogEntry> lal = plal.content();
      if (!plal.pointToHead() && newLalId < lal.content().getLogId()) {
        throw new LogIdTooSmall(
            "lal log id: " + lal.content().getLogId() + ". The new log's id: " + newLalId);
      }

      if (!pcl.pointToHead() && pcl.content().content().getLogId() < newLalId) {
        throw new LogIdTooLarge(
            "cl log id: " + pcl.content().content().getLogId() + ". The new log's id: " + newLalId);
      }

      plal = new DoubleLinkedListIterator<>(dest);
      numLogsFromPlalToPcl -= numSteps;
      numLogsFromPplToPlal += numSteps;

      logger.debug("moved {} steps and numLogsFromPLALToPCL {}", numSteps, numLogsFromPlalToPcl);
      if (!plal.pointToHead()) {
        logger.debug("SegId {}; plal {} ", segId, plal.content().content().getLogId());
      } else {
        logger.debug("plal is 0");
      }
      if (!pcl.pointToHead()) {
        logger.debug("pcl {} ", pcl.content().content().getLogId());
      } else {
        logger.debug("pcl is 0");
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public int moveLalTo(long newLalId) throws LogNotFoundException, LogIdTooSmall, LogIdTooLarge {
    reentrantLock.lock();
    try {
      if (newLalId == LogImage.INVALID_LOG_ID) {
        if (plal.pointToHead()) {
          return 0;
        } else {
          logger.error("The PLAL points to a valid log but the new lal is invalid. Abort the move");
          throw new LogIdTooSmall(
              "lal log id: " + plal.content().content().getLogId() + ". The new log's id: "
                  + newLalId);
        }
      }

      ListNode<ListNode<MutationLogEntry>> dest = mapLogIdLogEntry.get(newLalId);
      if (dest == null) {
        throw new LogNotFoundException("log with logId" + newLalId + " not exists");
      }

      ListNode<MutationLogEntry> lal = plal.content();
      if (!plal.pointToHead() && newLalId < lal.content().getLogId()) {
        throw new LogIdTooSmall(
            "lal log id: " + lal.content().getLogId() + ". The new log's id: " + newLalId);
      }

      if (!pcl.pointToHead() && pcl.content().content().getLogId() < newLalId) {
        throw new LogIdTooLarge(
            "cl log id: " + pcl.content().content().getLogId() + ". The new log's id: " + newLalId);
      }

      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = new DoubleLinkedListIterator<>(
          plal, null,
          dest);

      int numSteps = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ListNode<MutationLogEntry> curNode = cursor.content();
        if (!curNode.content().isApplied()) {
          logger.debug(
              "before move PLAL to the log whose logid is {} "
                  + "there is a log data not being applied yet. "
                  + "Move plal to the log prior to the current one instead", newLalId);

          cursor.previous();
          break;
        }
        numSteps++;
      }

      plal = new DoubleLinkedListIterator<>(cursor, null, null);
      numLogsFromPlalToPcl -= numSteps;
      numLogsFromPplToPlal += numSteps;
      logger.debug("moved {} steps and numLogsFromPLALToPCL {}", numSteps, numLogsFromPlalToPcl);
      if (!plal.pointToHead()) {
        logger.debug("SegId {}; plal {} ", segId, plal.content().content().getLogId());
      } else {
        logger.debug("plal is 0");
      }
      if (!pcl.pointToHead()) {
        logger.debug("pcl {} ", pcl.content().content().getLogId());
      } else {
        logger.debug("pcl is 0");
      }
      return numSteps;
    } finally {
      reentrantLock.unlock();
    }
  }

  public Pair<Boolean, List<MutationLogEntry>> getLogsAfterAndCheckCompletion(long logId,
      int maxNumLogs,
      boolean committedLogOnly) {
    reentrantLock.lock();
    try {
      List<MutationLogEntry> logs = new ArrayList<MutationLogEntry>();
      getLogsAfter(logId, maxNumLogs, committedLogOnly, logs);

      if (logs.isEmpty() || segLogs.isEmpty() || ppl.pointToHead()) {
        return new Pair<>(true, logs);
      } else {
        MutationLogEntry firstLog = segLogs.head().next().content().content();
        if (logId >= firstLog.getLogId()) {
          return new Pair<>(Boolean.TRUE, logs);
        } else {
          return new Pair<>(Boolean.FALSE, logs);
        }
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public Pair<Boolean, List<MutationLogEntry>> getLogsBetweenClAndCheckCompletion(
      long secondaryCl, long primaryCl, int maxNumLogs) {
    reentrantLock.lock();
    try {
      List<MutationLogEntry> logs = getLogIntervalsWithMaxNumberLogs(secondaryCl, primaryCl,
          maxNumLogs);

      if (logs.isEmpty() || segLogs.isEmpty() || ppl.pointToHead()) {
        return new Pair<>(true, logs);
      } else {
        MutationLogEntry firstLog = segLogs.head().next().content().content();
        if (secondaryCl + 1 >= firstLog.getLogId()) {
          return new Pair<>(Boolean.TRUE, logs);
        } else {
          return new Pair<>(Boolean.FALSE, logs);
        }
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public List<MutationLogEntry> getLogsForPageAfter(long pageLogicOffsetInSegment, long minId) {
    List<MutationLogEntry> logs = new ArrayList<MutationLogEntry>();
    reentrantLock.lock();
    try {
      DoubleLinkedList<MutationLogEntry> logNodesForPage = pageLogs.get(pageLogicOffsetInSegment);
      if (logNodesForPage != null) {
        DoubleLinkedListIterator<MutationLogEntry> cursor = logNodesForPage.head();
        while (cursor.hasNext()) {
          MutationLogEntry logEntry = cursor.next().content();
          Validate.notNull(logEntry);
          if (logEntry.getLogId() > minId) {
            logs.add(logEntry);
          }
        }
      }
    } finally {
      reentrantLock.unlock();
    }
    return logs;
  }

  public void getCommittedYetAppliedLogsForPage(long pageLogicOffsetInSegment,
      List<MutationLogEntry> logs) {
    reentrantLock.lock();
    try {
      DoubleLinkedList<MutationLogEntry> logNodesForPage = pageLogs.get(pageLogicOffsetInSegment);
      if (logNodesForPage == null) {
        return;
      }

      DoubleLinkedListIterator<MutationLogEntry> cursor = logNodesForPage.head();
      while (cursor.hasNext()) {
        MutationLogEntry logEntry = cursor.next().content();
        Validate.notNull(logEntry);
        if (logEntry.canBeApplied()) {
          logger.info("current log : {}", logEntry);
          logs.add(logEntry);
        }
      }

      if (!logs.isEmpty()) {
        logger.info("need apply logs:{} for read", logs);
      }

    } finally {
      reentrantLock.unlock();
    }
  }

  public int movePlalTo() {
    LogImage logImage = getLogImage(0);

    int numSteps = 0;
    if (logImage.getClId() != LogImage.INVALID_LOG_ID) {
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> plalCursor = logImage
          .getCursorFromLalToCl();
      long newLalId = logImage.getLalId();

      while (plalCursor.hasNext()) {
        MutationLogEntry currLog = plalCursor.next().content().content();
        Validate.isTrue(currLog.isFinalStatus());

        if (!currLog.isApplied()) {
          if (currLog.getStatus() == MutationLogEntry.LogStatus.AbortedConfirmed) {
            currLog.apply();
            MutationLogEntryFactory.releaseLogData(currLog);
          } else {
            logger.info("{} is not applied yet. Move PLAL to {}", currLog, newLalId);
            break;
          }
        }
        newLalId = currLog.getLogId();
        numSteps++;
      }

      if (numSteps > 0) {
        try {
          moveLalToWithoutCheck(newLalId, numSteps);
        } catch (Exception e) {
          logger.warn("can't move plal pointer?");
        }
      }
    }

    return numSteps;
  }

  public Set<Integer> movePlalAndGetCommittedYetAppliedLogs(int maxNumLogsToApply) {
    LogImage logImage = getLogImage(0);

    DoubleLinkedListIterator<ListNode<MutationLogEntry>> plalCursor = logImage
        .getCursorFromLalToCl();
    if (logImage.getClId() != LogImage.INVALID_LOG_ID) {
      long newLalId = logImage.getLalId();
      int numSteps = 0;

      while (plalCursor.hasNext()) {
        MutationLogEntry currLog = plalCursor.next().content().content();
        Validate.isTrue(currLog.isFinalStatus());

        if (!currLog.isApplied()) {
          if (currLog.getStatus() == MutationLogEntry.LogStatus.AbortedConfirmed) {
            currLog.apply();
            MutationLogEntryFactory.releaseLogData(currLog);
          } else {
            logger.info("{} is not applied yet. Move PLAL to {}", currLog, newLalId);
            break;
          }
        }
        newLalId = currLog.getLogId();
        numSteps++;
      }

      if (numSteps > 0) {
        try {
          moveLalToWithoutCheck(newLalId, numSteps);
        } catch (Exception e) {
          logger.warn("can't move plal pointer?");
        }
      }
    }

    Set<Integer> pagesUnappliedLogsBelongTo = new HashSet<>();
    DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = new DoubleLinkedListIterator<>(
        plal,
        plal.getCurrentPosition(), pcl.getCurrentPosition());

    while (cursor.hasNext() && maxNumLogsToApply > pagesUnappliedLogsBelongTo.size()) {
      MutationLogEntry log = cursor.next().content().content();
      if (log.getStatus() == MutationLogEntry.LogStatus.Created) {
        logger.warn("log: {}, pcl: {}", log, pcl);
        Validate.isTrue(false,
            "segId: " + segId + ", log: " + log + ", pcl: " + pcl.getCurrentPosition().content()
                .content());
      }

      if (log.isApplied()) {
        continue;
      }

      int pageIndex = (int) (
          PageAddressGenerator.calculatePageOffsetInSegUnit(log.getOffset(), pageSize)
              / pageSize);
      pagesUnappliedLogsBelongTo.add(pageIndex);
    }
    return pagesUnappliedLogsBelongTo;
  }

  private void getLogsAfter(long logId, int maxNumLogs, boolean committedLogOnly,
      List<MutationLogEntry> out) {
    ListNode<ListNode<MutationLogEntry>> node = findNode(logId);
    int n = 0;
    while (node != null && !node.isHead()) {
      MutationLogEntry log = node.content().content();
      if (committedLogOnly && (log.getStatus() == MutationLogEntry.LogStatus.Created
          || log.getStatus() == MutationLogEntry.LogStatus.Aborted)) {
        break;
      }

      out.add(log);
      node = node.getNext();

      n++;
      if (n == maxNumLogs) {
        break;
      }
    }
  }

  public List<MutationLogEntry> getLogsAfter(long logId, int maxNumLogs) {
    reentrantLock.lock();
    try {
      List<MutationLogEntry> logs = new ArrayList<>();
      getLogsAfter(logId, maxNumLogs, false, logs);
      return logs;
    } finally {
      reentrantLock.unlock();
    }
  }

  public List<MutationLogEntry> getLogsAfterPcl() {
    reentrantLock.lock();
    try {
      List<MutationLogEntry> logs = new ArrayList<>();
      MutationLogEntry pclLog = pcl.pointToHead()
          ? MutationLogEntry.INVALID_MUTATION_LOG :
          pcl.content().content();
      logs.add(pclLog);
      getLogsAfter(pclLog.getLogId(), Integer.MAX_VALUE, false, logs);
      return logs;
    } finally {
      reentrantLock.unlock();
    }
  }

  public List<MutationLogEntry> getLogsAfterPpl() {
    reentrantLock.lock();
    try {
      List<MutationLogEntry> logs = new ArrayList<>();
      MutationLogEntry pplLog = ppl.pointToHead()
          ? MutationLogEntry.INVALID_MUTATION_LOG :
          ppl.content().content();
      logs.add(pplLog);
      getLogsAfter(pplLog.getLogId(), Integer.MAX_VALUE, false, logs);
      return logs;
    } finally {
      reentrantLock.unlock();
    }
  }

  private ListNode<ListNode<MutationLogEntry>> findNode(long logId) {
    ListNode<ListNode<MutationLogEntry>> node = mapLogIdLogEntry.get(logId);
    if (node == null) {
      logger.info(
          "Can't find the log with log id : {} in the memory, "
              + "searching for the first log that is larger than it for segId: {}",
          logId, segId);

      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = segLogs.head();
      while (cursor.hasNext()) {
        cursor.next();
        Validate.notNull(cursor.content());
        if (cursor.content().content().getLogId() > logId) {
          node = cursor.getCurrentPosition();
          logger.info("logId: {}, current node: {}, segId: {}", logId,
              node.content().content(), segId);
          break;
        }
      }
    } else {
      logger.debug("Found the log with log id : {} in the memory. Node:{} ", logId,
          node.content().content());
      node = node.getNext();
    }

    return node;
  }

  private void getLogsAfterWithSameSnapshotVersion(long logId, boolean committedLogOnly,
      List<MutationLogEntry> out) {
    ListNode<ListNode<MutationLogEntry>> node = findNode(logId);

    if (node == null || node.isHead()) {
      return;
    }
    while (node != null && !node.isHead()) {
      MutationLogEntry log = node.content().content();

      if (committedLogOnly && (log.getStatus() == MutationLogEntry.LogStatus.Created
          || log.getStatus() == MutationLogEntry.LogStatus.Aborted)) {
        node = node.getNext();
        continue;
      }

      out.add(log);
      node = node.getNext();
    }
  }

  public List<MutationLogEntry> getLogIntervals(long fromLogId, long toLogId) {
    return getLogIntervalsWithMaxNumberLogs(fromLogId, toLogId, Integer.MAX_VALUE);
  }

  public List<MutationLogEntry> getLogIntervalsWithMaxNumberLogs(long fromLogId, long toLogId,
      int maxNumLogs) {
    logger
        .debug("begin get log intervals from {} to {} with max number logs {}", fromLogId, toLogId,
            maxNumLogs);
    List<MutationLogEntry> logsIntervals = new ArrayList<>();
    ListNode<ListNode<MutationLogEntry>> node = findNode(fromLogId);

    while (node != null && !node.isHead() && maxNumLogs-- > 0) {
      MutationLogEntry log = node.content().content();
      if (log.getLogId() > toLogId) {
        break;
      }
      logsIntervals.add(log);
      node = node.getNext();
    }

    return logsIntervals;
  }

  public int removeLogsPriorTo(long logId) throws LogNotFoundException {
    logger.info("remove logs prior to {}, segment id {}", logId, segId);
    reentrantLock.lock();
    try {
      return removeLogsPriorTo(logId, false);
    } finally {
      reentrantLock.unlock();
    }
  }

  public int removeLogsPriorTo(long logId, boolean force) throws LogNotFoundException {
    reentrantLock.lock();
    try {
      if (mapLogIdLogEntry.get(logId) == null) {
        throw new LogNotFoundException("log with logId: [" + logId + "] not found");
      }

      long clId = getClId();
      long lalId = getLalId();
      long plId = getPlId();
      int numRemovedLogsNotLessThanCl = 0;
      int numRemovedLogsNotLessThanLal = 0;
      int numRemovedLogsNotLessThanPl = 0;
      int numLogsRemoved = 0;

      while (!segLogs.isEmpty()) {
        DoubleLinkedListIterator<ListNode<MutationLogEntry>> ppFirstLog = segLogs.head().next();
        if (ppFirstLog.pointToHead()) {
          continue;
        }

        ListNode<MutationLogEntry> pfirstlog = ppFirstLog.content();
        MutationLogEntry firstLog = pfirstlog.content();
        long firstLogId = firstLog.getLogId();

        if (firstLog.getLogId() >= logId) {
          break;
        }

        if (!force && (firstLog.getStatus() == MutationLogEntry.LogStatus.Created
            || firstLog.getStatus() == MutationLogEntry.LogStatus.Aborted
            || firstLogId >= plId)) {
          logger.warn("first log: {}, plId: {}, logid: {}", firstLog, plId, logId);
          break;
        }

        segLogs.removeFromHead();

        updateLastRemoveUuid(firstLog.getUuid());
        mapLogsUuidToLogId.remove(firstLog.getUuid());

        notInsertedLogs.removeLog(firstLog, true);
        MutationLogEntryFactory.releaseLogData(firstLog);

        numLogsRemoved++;

        Long pageAddress = PageAddressGenerator
            .calculatePageOffsetInSegUnit(firstLog.getOffset(), pageSize);
        DoubleLinkedList<MutationLogEntry> logsForPage = pageLogs.get(pageAddress);
        Validate.notNull(logsForPage);
        Validate.isTrue(!logsForPage.isEmpty());

        MutationLogEntry firstLogAtPage = logsForPage.removeFromHead();

        Validate.isTrue(firstLog == firstLogAtPage);

        if (logsForPage.size() == 0) {
          pageLogs.remove(pageAddress);
        }

        mapLogIdLogEntry.remove(firstLogId);

        if (firstLogId >= clId) {
          numRemovedLogsNotLessThanCl++;
        }

        if (firstLogId >= lalId) {
          numRemovedLogsNotLessThanLal++;
        }

        if (firstLogId >= plId) {
          numRemovedLogsNotLessThanPl++;
        }
      }

      if (numRemovedLogsNotLessThanCl > 0) {
        pcl = segLogs.head();
        numLogsAfterPcl -= numRemovedLogsNotLessThanCl;
        numLogsAfterPcl = numLogsAfterPcl >= 0 ? numLogsAfterPcl : 0;
      }

      if (numRemovedLogsNotLessThanLal > 0) {
        plal = segLogs.head();
        numLogsFromPlalToPcl -= numRemovedLogsNotLessThanLal;
        numLogsFromPlalToPcl = numLogsFromPlalToPcl >= 0 ? numLogsFromPlalToPcl : 0;
      }

      if (numRemovedLogsNotLessThanPl > 0) {
        ppl = segLogs.head();
        numLogsFromPplToPlal -= numRemovedLogsNotLessThanPl;
        numLogsFromPplToPlal = numLogsFromPplToPlal >= 0 ? numLogsFromPplToPlal : 0;
      }

      return numLogsRemoved;
    } finally {
      reentrantLock.unlock();
    }
  }

  public int updateSwplAndremoveLogsPriorToSwpl() {
    reentrantLock.lock();
    int numLogs = 0;
    long swplLogId = this.swplLogId;
    long plId = LogImage.INVALID_LOG_ID;
    try {
      plId = getPlId();
      this.swplLogId = Math.min(primarySwplLogId, plId);
      if (this.swplLogId != LogImage.INVALID_LOG_ID) {
        numLogs = removeLogsPriorTo(this.swplLogId);
      }
    } catch (LogNotFoundException e) {
      logger.warn(
          "swpl id: {} is not found. Can't remove any logs prior to {} for segId: {}, "
              + "primarySwplLogId: {}, swplLogId: {}",
          this.swplLogId, plId, segId, primarySwplLogId, swplLogId, e);
    } finally {
      reentrantLock.unlock();
    }
    return numLogs;
  }

  public void abortLogs(Collection<MutationLogEntry> logsToAbort) {
    reentrantLock.lock();
    try {
      if (logsToAbort == null || logsToAbort.size() == 0) {
        return;
      }

      for (MutationLogEntry logToAbort : logsToAbort) {
        if (logToAbort.getStatus() != MutationLogEntry.LogStatus.Created) {
          logger.debug("{} status is not created. No finish action was applied", logToAbort);
        }

        if (logToAbort.getLogId() == -1 || logToAbort.getOffset() == -1) {
          Validate.isTrue(false, "current log: " + logToAbort + ", logs: " + logsToAbort);
        }

        logToAbort.abort();

      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public long confirmAbortLogs(Collection<MutationLogEntry> logsToConfirmAbort) {
    long currentConfirmAbortLogId = 0;
    if (logsToConfirmAbort == null || logsToConfirmAbort.isEmpty()) {
      return currentConfirmAbortLogId;
    }
    reentrantLock.lock();
    try {
      for (MutationLogEntry logToConfirmAbort : logsToConfirmAbort) {
        currentConfirmAbortLogId = logToConfirmAbort.getLogId();
        if (logToConfirmAbort.isFinalStatus()) {
          logger.info("{} status is not created. No confirm finish action was applied",
              logToConfirmAbort);
          continue;
        }

        try {
          logToConfirmAbort.confirmAbortAndApply();
          MutationLogEntryFactory.releaseLogData(logToConfirmAbort);
        } catch (InappropriateLogStatusException e) {
          logger.warn("going to confirm finish log:{}, but it was committed by coordinator",
              logToConfirmAbort.getLogId(), e);
        }
      }
    } finally {
      reentrantLock.unlock();
    }
    return currentConfirmAbortLogId;
  }

  public boolean commitLog(long logId) {
    boolean success = false;
    reentrantLock.lock();
    try {
      MutationLogEntry logEntry = findLog(logId);
      if (logEntry == null) {
        logger.warn("can not find a log with logId={}", logId);
        return false;
      }
      if (logEntry.getStatus() != MutationLogEntry.LogStatus.Created) {
        logger.info("log={} status is not created. No finish action was applied", logEntry);
        if (logEntry.getStatus() == MutationLogEntry.LogStatus.Committed) {
          return true;
        }
      } else {
        logEntry.commit();
        if (logEntry.isCommitted()) {
          int pageIndex = (int) (logEntry.getOffset() / ArchiveOptions.PAGE_SIZE);
          segmentUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
        }
        success = true;
      }
    } finally {
      reentrantLock.unlock();
    }
    return success;
  }

  public List<Pair<MutationLogEntry, BroadcastLogThrift>> commitThriftLogs(
      List<BroadcastLogThrift> logsToCommit,
      boolean isPrimary) {
    if (logsToCommit == null || logsToCommit.size() == 0) {
      return new ArrayList<>();
    }

    reentrantLock.lock();
    try {
      return commitLogs(logsToCommit, isPrimary);
    } finally {
      reentrantLock.unlock();
    }
  }

  public List<Pair<MutationLogEntry, PbBroadcastLog>> commitPbLogs(
      List<PbBroadcastLog> logsToCommit,
      boolean isPrimary) {
    if (logsToCommit == null || logsToCommit.size() == 0) {
      return new ArrayList<>();
    }

    reentrantLock.lock();
    try {
      return commitLogs(logsToCommit, isPrimary);
    } finally {
      reentrantLock.unlock();
    }
  }

  private <T> List<Pair<MutationLogEntry, T>> commitLogs(List<T> logsToCommit, boolean isPrimary) {
    validateNotDestroyed();
    logger.debug("commit logs (is priamry : {}) {}, ", isPrimary, logsToCommit);
    List<Pair<MutationLogEntry, T>> committedLogs = new ArrayList<>();
    long clId = getClId();
    for (T broadcastLog : logsToCommit) {
      if (ThriftPbLogUtil.isFinalStatus(broadcastLog)) {
        committedLogs.add(new Pair<>(null, broadcastLog));
        continue;
      }

      long logId = ThriftPbLogUtil.getLogId(broadcastLog);
      if (logId <= clId) {
        logger.info("current log:{} to be committed, but less than clId: {}", broadcastLog, clId);
        T tmpLog = broadcastLog;
        if (ThriftPbLogUtil.isLogCreated(broadcastLog)) {
          tmpLog = ThriftPbLogUtil.changeLogStatusToCommitt(broadcastLog);
        } else if (ThriftPbLogUtil.isLogAborted(broadcastLog)) {
          tmpLog = ThriftPbLogUtil.changeLogStatusToAbortConfirmed(broadcastLog);
        } else {
          logger.error("commit the log failure: {}", broadcastLog);
        }
        committedLogs.add(new Pair<>(null, tmpLog));
        continue;
      }

      MutationLogEntry entry = findLog(logId);
      if (entry == null) {
        if (ThriftPbLogUtil.isLogAborted(broadcastLog)) {
          MutationLogEntry missLog = MutationLogEntryFactory
              .createEmptyLog(ThriftPbLogUtil.getUuid(broadcastLog),
                  ThriftPbLogUtil.getLogId(broadcastLog), ThriftPbLogUtil.getOffset(broadcastLog),
                  ThriftPbLogUtil.getChecksum(broadcastLog),
                  ThriftPbLogUtil.getLength(broadcastLog));
          logger.warn("now, insert the finish log: {} for segId: {}", missLog, segId);
          T tmpLog = broadcastLog;
          try {
            missLog.confirmAbortAndApply();
            insertLog(missLog);
            tmpLog = ThriftPbLogUtil.changeLogStatusToAbortConfirmed(broadcastLog);
          } catch (InappropriateLogStatusException | LogIdTooSmall e) {
            logger.error("can not insert finish confirm log: {}", missLog);
            missLog = null;
          }
          committedLogs.add(new Pair<>(missLog, tmpLog));
        } else {
          if (isPrimary) {
            logger.error("oh my god, i am primary but i have not the log={} for segId={}, cl id={}",
                broadcastLog, segId, clId);
            Validate.isTrue(false);
          }

          if (!ThriftPbLogUtil.isLogCreated(broadcastLog)) {
            logger.error("not created status, log: {}", broadcastLog);
          }
          committedLogs
              .add(new Pair<>(null, ThriftPbLogUtil.changeLogStatusToCommitt(broadcastLog)));
        }
        continue;
      }

      if (ThriftPbLogUtil.isLogCreated(broadcastLog)) {
        try {
          entry.commit();
          if (entry.isCommitted()) {
            int pageIndex = (int) (entry.getOffset() / ArchiveOptions.PAGE_SIZE);
            segmentUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
          }
        } catch (InappropriateLogStatusException e) {
          logger.warn(
              "the situation: get latest logs want to commit log:{}, "
                  + "but after increase log id, also confirm finish",
              entry.getLogId());
        }

        committedLogs
            .add(new Pair<>(entry, ThriftPbLogUtil.changeLogStatusToCommitt(broadcastLog)));
      } else if (ThriftPbLogUtil.isLogAborted(broadcastLog)) {
        try {
          if (entry.isFinalStatus()) {
            logger.warn("log entry: {} is final status when aborting", entry);
          } else {
            logger.debug("abort log {}", entry.getLogId());
            entry.confirmAbortAndApply();
          }
        } catch (InappropriateLogStatusException e) {
          logger.warn("can't abortcomfirm the log, entry: {}", entry);
        }

        committedLogs
            .add(new Pair<>(entry, ThriftPbLogUtil.changeLogStatusToAbortConfirmed(broadcastLog)));
      } else {
        logger.error("Log {} exists but the coordinator has a wrong log status: {}", broadcastLog,
            logsToCommit);
        committedLogs.add(new Pair<>(entry, broadcastLog));
      }
    }
    return committedLogs;
  }

  public void abortLog(long logId) {
    reentrantLock.lock();
    try {
      MutationLogEntry logEntry = getLog(logId);
      if (logEntry == null || logEntry.getStatus() != MutationLogEntry.LogStatus.Created) {
        logger.debug(
            "There is no log having id of {} or log status is not created. "
                + "No finish action was applied",
            logId);
        return;
      }

      logEntry.abort();
    } finally {
      reentrantLock.unlock();
    }
  }

  public MutationLogEntry getFirstNode() {
    if (segLogs.isEmpty()) {
      return null;
    } else {
      return segLogs.head().next().content().content();
    }
  }

  public int size() {
    return segLogs.size();
  }

  public SegmentLogMetadata duplicate() {
    SegmentLogMetadata newSegmentLogMetadata = new SegmentLogMetadata(this.segId, this.pageSize,
        this.quorumSize);
    List<MutationLogEntry> logs = getLogsAfter(-1, Integer.MAX_VALUE);
    logger.debug("Done with getting all logs. The size of logs is {} ", logs.size());
    try {
      for (MutationLogEntry log : logs) {
        MutationLogEntry newLog = MutationLogEntryFactory.createLog(log);
        newSegmentLogMetadata.insertLog(newLog);
      }

      logger.debug("Done with adding logs to the new metadata. LogImage is {}",
          newSegmentLogMetadata.getLogImageIncludeLogsAfterPcl());

      newSegmentLogMetadata.moveClTo(getClId());
      newSegmentLogMetadata.moveLalTo(getLalId());
      newSegmentLogMetadata.movePlTo(getPlId());
      newSegmentLogMetadata.setSwclIdTo(getSwclId());
      newSegmentLogMetadata.setSwplIdTo(getSwplId());
      logger.debug("Done with move");
      return newSegmentLogMetadata;
    } catch (Exception e) {
      logger.warn("failed to duplicate segment log metadata to a new one", e);
      return null;
    }
  }

  public void updateLogIdGenerator(LogIdGenerator newLogIdGenerator) {
    reentrantLock.lock();
    try {
      if (this.logIdGenerator == null || newLogIdGenerator == null
          || this.logIdGenerator.currentLogId() < newLogIdGenerator.currentLogId()) {
        this.logIdGenerator = newLogIdGenerator;
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public MutationLogEntry createLog(long uuid, long logId, long offset, byte[] bytes, long checksum)
      throws NoAvailableBufferException, LogIdTooSmall, InappropriateLogStatusException {
    return createLog(uuid, logId, 0, offset, bytes, checksum, Long.MAX_VALUE, 0);
  }

  public MutationLogEntry createLog(PbWriteRequestUnit writeUnit, ByteBuf data, long archiveId,
      boolean isPrimary,
      int timeout) throws NoAvailableBufferException {
    MutationLogEntry entry;
    if (isPrimary) {
      entry = MutationLogEntryFactory
          .createLogForPrimary(writeUnit.getLogUuid(), writeUnit.getLogId(), archiveId,
              writeUnit.getOffset(),
              writeUnit.getLength(), writeUnit.getChecksum(), timeout);
    } else {
      entry = MutationLogEntryFactory
          .createLogForSecondary(writeUnit.getLogUuid(), writeUnit.getLogId(), archiveId,
              writeUnit.getOffset(), writeUnit.getLength(), writeUnit.getChecksum(),
              timeout);
    }

    entry.put(data);
    return entry;
  }

  public MutationLogEntry createLog(long uuid, long logId, long archiveId, long offset,
      byte[] bytes, long checksum,
      long maxSynchronizeTimeForCreateLogMs, int snapshotVersion)
      throws NoAvailableBufferException, LogIdTooSmall, InappropriateLogStatusException {
    boolean success = false;
    try {
      success = createLogLock.tryLock(maxSynchronizeTimeForCreateLogMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("can not lock when creating a log");
    }

    if (!success) {
      throw new NoAvailableBufferException("can not get a lock when creating a log");
    }

    try {
      MutationLogEntry entry = MutationLogEntryFactory
          .createLogForPrimary(uuid, logId, archiveId, offset, bytes, checksum);

      reentrantLock.lock();
      try {
        entry = addLog(entry, false);
      } finally {
        reentrantLock.unlock();
      }

      return entry;
    } finally {
      createLogLock.unlock();
    }
  }

  public MutationLogEntry getLogWithoutLogId(long logUuid) {
    return notInsertedLogs.getLogEntry(logUuid);
  }

  public boolean insertLogsForSecondary(PlalEngine plalEngine, List<MutationLogEntry> logs,
      int timeoutMs) {
    logger.debug("sync logs {}", logs);
    Pair<List<CompletableFuture<Boolean>>, List<MutationLogEntry>> addCompleteLogsResult = new Pair(
        Lists.newArrayList(), logs);
    List<CompletableFuture<Boolean>> resultsOfInsertedLogs = new LinkedList<>();
    while (addCompleteLogsResult.getSecond().size() > 0) {
      reentrantLock.lock();
      try {
        validateNotDestroyed();
        addCompleteLogsResult = notInsertedLogs
            .addCompleteLogs(plalEngine, addCompleteLogsResult.getSecond());
        resultsOfInsertedLogs.addAll(addCompleteLogsResult.getFirst());
      } finally {
        reentrantLock.unlock();
      }
    }

    for (CompletableFuture<Boolean> future : resultsOfInsertedLogs) {
      long startTime = System.currentTimeMillis();
      try {
        future.get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        logger.error("", e);
        return false;
      } catch (Exception e) {
        logger.error("", e);
        return false;
      }
      timeoutMs -= System.currentTimeMillis() - startTime;
    }

    logger.debug("done insertLogsForSecondary logs {}", logs);
    return true;
  }

  public void markLogBroken(long logUuid, BrokenLogWithoutLogId.BrokenReason reason) {
    BrokenLogWithoutLogId brokenLogWithoutLogId = new BrokenLogWithoutLogId(logUuid, reason);
    Validate
        .isTrue((notInsertedLogs.addCompletingLog(null, false, brokenLogWithoutLogId).size() == 0));
  }

  public void saveLogsWithoutLogId(List<MutationLogEntry> logsWithoutLogId, PlalEngine plalEngine)
      throws LogIdTooSmall {
    LogWithoutLogId[] logWithoutLogIdList = new LogWithoutLogId[logsWithoutLogId.size()];
    for (int i = 0; i < logsWithoutLogId.size(); i++) {
      MutationLogEntry logEntry = logsWithoutLogId.get(i);
      if (logHasBeenCrashedOut(logEntry.getUuid())) {
        logger.info("delete this log {} because it has passed pcl {}", logEntry, getClId());
        MutationLogEntryFactory.releaseLogData(logEntry);
        logWithoutLogIdList[i] = null;
      } else {
        logWithoutLogIdList[i] = new LogWithoutLogId(logEntry);
      }
    }
    int count = notInsertedLogs.addCompletingLog(plalEngine, false, logWithoutLogIdList).size();
  }

  public void giveYouLogId(GiveYouLogIdEvent giveYouLogIdEvent, PlalEngine plalEngine) {
    Map<Long, Long> mapLogUuidToLogId = giveYouLogIdEvent.getMapLogUuidToLogId();
    int count = internalGiveYouLogId(mapLogUuidToLogId, plalEngine, true, giveYouLogIdEvent).size();
  }

  public void giveYouLogId(Map<Long, Long> mapLogsUuidToLogId, PlalEngine plalEngine,
      boolean syncOrCommit,
      boolean comeWithWrite) {
    int count = internalGiveYouLogId(mapLogsUuidToLogId, plalEngine, false, null).size();
  }

  public void giveYouLogId(long beginLogId, long maxLogId,
      List<PbBackwardSyncLogMetadata> backwardLogs, PlalEngine plalEngine,
      List<PbBackwardSyncLogMetadata> missingLogs, long logIdOfCatchUp,
      List<CompletingLog> toBeInsertLogs,
      Map<MutationLogEntry, LogStatus> toBeCompletedLogs,
      List<CompleteLog> logsAlreadyInsertedButNotCompleted,
      List<PbBackwardSyncLogMetadata> toBeInstertCompleteLogs)
      throws LogIdTooSmall {
    missingLogs.addAll(
        internalGiveYouLogId(beginLogId, maxLogId, backwardLogs, plalEngine,
            null, logIdOfCatchUp, toBeInsertLogs, toBeCompletedLogs,
            logsAlreadyInsertedButNotCompleted, toBeInstertCompleteLogs));
  }

  private List<PbBackwardSyncLogMetadata> internalGiveYouLogId(long beginLogId, long maxLogId,
      List<PbBackwardSyncLogMetadata> backwardLogs, PlalEngine plalEngine,
      CompletingLogListener listener, long logIdOfCatchUp, List<CompletingLog> toBeInsertLogs,
      Map<MutationLogEntry, LogStatus> toBeCompletedLogs,
      List<CompleteLog> logsAlreadyInsertedButNotCompleted,
      List<PbBackwardSyncLogMetadata> toBeInsertCompleteLogs) throws LogIdTooSmall {
    logger.debug("give you log id {}, begin log Id {} , max log Id {}", backwardLogs, beginLogId,
        maxLogId);

    reentrantLock.lock();
    try {
      if (beginLogId != this.getClId()) {
        logger.warn("cl id not equal(old is {}, new is {}), maybe it has been modify",
            beginLogId, this.getClId());
        Validate.isTrue(false);
      }
      validateNotDestroyed();
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = null;
      long firstLogId = backwardLogs.get(0).getLogId();

      ListNode<ListNode<MutationLogEntry>> position = pcl.getCurrentPosition();
      Validate.notNull(position);

      cursor = new DoubleLinkedListIterator<>(position).next();

      List<CompletingLog> beInsertLogsWithOutLogEntry = new ArrayList<>();
      List<PbBackwardSyncLogMetadata> missingLogs = new ArrayList<>();
      int backwardCursor = 0;
      final SegmentLogMetadata segmentLogMetadata = this;

      if (firstLogId < beginLogId) {
        logger.debug("found a invalid log id for sync log, segment {}, request log metadata {}"
                + "i got the first log id {} from sync log request letter than my pcl log id {}, ",
            segId, backwardLogs, firstLogId, beginLogId);
        while (firstLogId < beginLogId && backwardCursor < backwardLogs.size()) {
          PbBackwardSyncLogMetadata logMetadata = backwardLogs.get(backwardCursor);
          if (logMetadata.getLogId() < getFirstNode().getLogId()) {
            logger
                .info(
                    "a log {} which sync log by primary, "
                        + "but secondary has remove it in ppl driver {}",
                    logMetadata.getLogId(), segId);
            firstLogId = getFirstNode().getLogId();
          } else {
            Validate.notNull(getLog(logMetadata.getLogId()));
            firstLogId = backwardLogs.get(backwardCursor).getLogId();
          }

          backwardCursor++;
        }

        if (backwardCursor >= backwardLogs.size()) {
          logger.info(
              "no need deal any logs at this request, "
                  + "because our cl {} grater than the max log id of request {}",
              getClId(), backwardLogs);
          return new ArrayList<>();
        }
      }

      do {
        PbBackwardSyncLogMetadata logMetadata = backwardLogs.get(backwardCursor);
        firstLogId = logMetadata.getLogId();
        MutationLogEntry logEntry = null == cursor.content() ? null : cursor.content().content();

        CompletingLogListener statusListener = new CompletingLogListener() {
          @Override
          public void complete(long logUuid) {
            logger.debug("log uuid {} has been complete", logUuid);
            MutationLogEntry mutationLogEntry = segmentLogMetadata.getLog(logMetadata.getLogId());
            if (mutationLogEntry == null) {
              logger.error("can not found any mutation log by {}, ppl {}, pcl {}",
                  logMetadata.getLogId(), getPlId(), getClId());
              if (listener != null) {
                listener.fail(logUuid, new NullPointerException());
              }
            } else {
              LogStatus status = logMetadata.hasStatus()
                  ? LogStatus.convertPbStatusToLogStatus(logMetadata.getStatus())
                  : LogStatus.Committed;
              try {
                finalizeLogStatus(mutationLogEntry, status, segmentLogMetadata);
              } catch (Exception e) {
                logger.warn(
                    "System exit, caught an exception,"
                        + " backward sync log status={}, local log status ={}",
                    logMetadata,
                    mutationLogEntry, e);

                System.exit(0);
              }
              if (mutationLogEntry.isCommitted()) {
                int pageIndex = (int) (mutationLogEntry.getOffset()
                    / ArchiveOptions.PAGE_SIZE);
                segmentUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
              }
              plalEngine.putLog(segId, mutationLogEntry);
              if (listener != null) {
                listener.complete(logUuid);
              }
            }
          }

          @Override
          public void fail(long logUuid, Exception e) {
            if (listener != null) {
              listener.fail(logUuid, e);
            }
          }
        };

        LogStatus newStatus = LogStatus.convertPbStatusToLogStatus(
            logMetadata.hasStatus() ? logMetadata.getStatus() : PbBroadcastLogStatus.COMMITTED);

        if (logEntry == null || logEntry.getLogId() > firstLogId) {
          logger.debug("a missing log be found, log metadata {}", logMetadata);

          CompletingLog completingLog = notInsertedLogs.getCompletingLog(logMetadata.getUuid());
          if (null != completingLog && completingLog instanceof CompleteLog) {
            CompleteLog completeLog = (CompleteLog) completingLog;
            if (completeLog.getLogEntry().getLogId() != logMetadata.getLogId()) {
              logger.error("remove different {} {}", completingLog, logMetadata);
              notInsertedLogs.removeLog(completeLog.getLogEntry(), true);
              completingLog = notInsertedLogs.getCompletingLog(logMetadata.getUuid());
            }
          }

          if (null == completingLog) {
            if (logMetadata.hasLogData()
                || logMetadata.getLogId() <= logIdOfCatchUp
                || logMetadata.getStatus() == PbBroadcastLogStatus.ABORT
                || logMetadata.getStatus() == PbBroadcastLogStatus.ABORT_CONFIRMED) {
              toBeInsertCompleteLogs.add(logMetadata);
            } else {
              beInsertLogsWithOutLogEntry
                  .add(new LogWithoutLogEntry(logMetadata.getUuid(), logMetadata.getLogId(),
                      statusListener));
              missingLogs.add(logMetadata);
            }
          } else if (completingLog instanceof LogWithoutLogEntry) {
            logger.debug("a completing with out entry log be found, log metadata {}", logMetadata);
            if (logMetadata.hasLogData()
                || logMetadata.getLogId() <= logIdOfCatchUp
                || logMetadata.getStatus() == PbBroadcastLogStatus.ABORT
                || logMetadata.getStatus() == PbBroadcastLogStatus.ABORT_CONFIRMED) {
              toBeInsertCompleteLogs.add(logMetadata);
            } else {
              missingLogs.add(logMetadata);
            }
          } else if (completingLog instanceof LogWithoutLogId) {
            logger.debug("a completing with out id log be found, log metadata {}", logMetadata);
            toBeInsertLogs.add(new LogWithoutLogEntry(logMetadata.getUuid(), logMetadata.getLogId(),
                statusListener));
          } else if (completingLog instanceof CompleteLog) {
            logger.debug("a complete log be found, log metadata {}", logMetadata);
            logsAlreadyInsertedButNotCompleted.add((CompleteLog) completingLog);
          }
        } else if (logEntry.getLogId() == firstLogId) {
          logger
              .debug("the same log be found, set finalize status for log metadata {}", logMetadata);
          toBeCompletedLogs.put(logEntry, newStatus);
          cursor.next();
        } else if (logEntry.getLogId() < firstLogId) {
          logger.error("mutation log {} not in primary but found it in secondary log double queue",
              logEntry);
          LogIdTooSmall tooSmall = new LogIdTooSmall();
          tooSmall.setCurrentSmallestId(logEntry.getLogId());
          throw tooSmall;
        }
        backwardCursor++;
      } while (backwardCursor < backwardLogs.size() && maxLogId >= backwardLogs.get(backwardCursor)
          .getLogId());

      if (toBeInsertLogs.size() > 0 || beInsertLogsWithOutLogEntry.size() > 0) {
        CompletingLog[]  completingLog = new CompletingLog[toBeInsertLogs.size()
            + beInsertLogsWithOutLogEntry.size()];

        int index = 0;
        for (int i = 0; i < toBeInsertLogs.size(); i++) {
          completingLog[index++] = toBeInsertLogs.get(i);
        }
        for (int i = 0; i < beInsertLogsWithOutLogEntry.size(); i++) {
          completingLog[index++] = beInsertLogsWithOutLogEntry.get(i);
        }
        notInsertedLogs.addCompletingLog(plalEngine, false, completingLog);
      }
      logger.debug("found missing logs {}", missingLogs);
      return missingLogs;
    } finally {
      reentrantLock.unlock();
    }
  }

  private List<MutationLogEntry> internalGiveYouLogId(Map<Long, Long> mapLogsUuidToLogId,
      PlalEngine plalEngine,
      boolean replaceOldValue, CompletingLogListener listener) {
    logger.debug("give you log id {}", mapLogsUuidToLogId);
    LogWithoutLogEntry[] logsWithoutLogEntry = new LogWithoutLogEntry[mapLogsUuidToLogId.size()];
    int index = 0;
    for (Map.Entry<Long, Long> uuidAndLogId : mapLogsUuidToLogId.entrySet()) {
      if (uuidAndLogId.getValue() <= getClId()) {
        logger.info("delete this log {} because it has passed pcl {}", uuidAndLogId, getClId());
        if (listener != null) {
          listener.complete(uuidAndLogId.getKey());
        }
        logsWithoutLogEntry[index] = null;
      } else {
        logsWithoutLogEntry[index] = new LogWithoutLogEntry(uuidAndLogId.getKey(),
            uuidAndLogId.getValue(),
            listener);
      }
      index++;
    }
    return notInsertedLogs.addCompletingLog(plalEngine, replaceOldValue, logsWithoutLogEntry);
  }

  public boolean discardLogIdForLog(long logUuid, long logId) {
    logIdLock.lock();
    try {
      return mapLogsUuidToLogId.remove(logUuid, logId);
    } finally {
      logIdLock.unlock();
    }
  }

  public Pair<Long, Boolean> getNewOrOldLogId(long uuid, boolean isPrimary) {
    logIdLock.lock();
    try {
      Pair<Long, Boolean> logIdAndAlreadyExists = new Pair<>();
      Long logId = mapLogsUuidToLogId.get(uuid);
      if (logId == null) {
        if (isPrimary) {
          logId = logIdGenerator.newId();
          saveLogUuidToLogId(uuid, logId);
        } else {
          logId = LogImage.INVALID_LOG_ID;
        }
        logIdAndAlreadyExists.setFirst(logId);
        logIdAndAlreadyExists.setSecond(false);
      } else {
        logIdAndAlreadyExists.setFirst(logId);
        logIdAndAlreadyExists.setSecond(true);
      }
      return logIdAndAlreadyExists;
    } finally {
      logIdLock.unlock();
    }
  }

  public void saveLogUuidToLogId(long uuid, long logId) {
    mapLogsUuidToLogId.put(uuid, logId);
  }

  public Map<Long, Long> getMapLogsUuidToLogId() {
    return mapLogsUuidToLogId;
  }

  @Deprecated
  public SortedMap<MutationLogEntry, Boolean> createLogs(
      List<Pair<PbWriteRequestUnit, ByteBuf>> writeRequests,
      boolean isPrimary, int timeoutMillsForEachLog) throws LogExpiredException {
    List<Pair<PbWriteRequestUnit, MutationLogEntry>> writeUnitsAndLogWithoutData =
        new ArrayList<>();
    SortedMap<MutationLogEntry, Boolean> mapLogsToNewlyCreated = new TreeMap<>();

    for (Pair<PbWriteRequestUnit, ByteBuf> writeRequest : writeRequests) {
      PbWriteRequestUnit writeUnit = writeRequest.getFirst();
      MutationLogEntry logEntry = null;
      long logUuid = writeUnit.getLogUuid();

      Pair<Long, Boolean> logIdAndAlreadyExists = getNewOrOldLogId(logUuid, isPrimary);
      long logId = logIdAndAlreadyExists.getFirst();
      if (logIdAndAlreadyExists.getSecond()) {
        logEntry = getLog(logId);
        if (logEntry == null) {
          if (isPrimary) {
            logger.warn(
                "primary has the log id {} for uuid {}, "
                    + "but doesn't have the log entry, log image {}",
                logId, logUuid, getLogImageIncludeLogsAfterPcl());
          }
          logEntry = notInsertedLogs.getLogEntry(logUuid);
        }
      }

      if (logEntry != null) {
        logger.info("log : {} already exists, not newly created", logEntry);
        if (logEntry.getOffset() != writeUnit.getOffset() || logEntry.getLength() != writeUnit
            .getLength()) {
          String errMsg = "got two logs with the same uuid but different content !";
          logger.error("{} : log : {}, write unit : {}", errMsg, logEntry, writeUnit);
          Validate.isTrue(false, errMsg);
        } else if (logEntry.getChecksum() != 0 && logEntry.getChecksum() != writeUnit
            .getChecksum()) {
          logger.error("checksum mismatch log's {}, write unit's {}", logEntry, writeUnit);
          Validate.isTrue(false);
        } else {
          mapLogsToNewlyCreated.put(logEntry, false);
        }
      } else {
        try {
          if (isPrimary) {
            logEntry = MutationLogEntryFactory
                .createLogForPrimary(writeUnit.getLogUuid(), logId, writeUnit.getOffset(),
                    writeUnit.getLength(), writeUnit.getChecksum(),
                    timeoutMillsForEachLog);
          } else {
            logEntry = MutationLogEntryFactory
                .createLogForSecondary(writeUnit.getLogUuid(), logId, writeUnit.getOffset(),
                    writeUnit.getLength(), writeUnit.getChecksum(),
                    timeoutMillsForEachLog);
          }
          logger.debug("a newly created log : {}", logEntry);
          writeUnitsAndLogWithoutData.add(new Pair<>(writeUnit, logEntry));
          mapLogsToNewlyCreated.put(logEntry, true);
        } catch (NoAvailableBufferException e) {
          logger.debug(
              "can not create log, uuid={}, log-id={}, offset={}, "
                  + "length={} in timeout {}. but we have created {} logs already.",
              logUuid, logId, writeUnit.getOffset(), writeUnit.getLength(), timeoutMillsForEachLog,
              writeUnitsAndLogWithoutData.size());
          if (isPrimary) {
            discardLogIdForLog(logUuid, logId);
          }
          break;
        }
      }
    }
    return mapLogsToNewlyCreated;
  }

  public List<MutationLogEntry> createLogs(List<Pair<PbWriteRequestUnit, ByteBuf>> writeRequests)
      throws NoAvailableBufferException {
    return createLogs(writeRequests, true);
  }

  public List<MutationLogEntry> createLogs(
      Collection<Pair<PbWriteRequestUnit, ByteBuf>> writeRequestUnits,
      boolean isPrimary) throws NoAvailableBufferException {
    List<MutationLogEntry> newLogs = new LinkedList<>();
    for (Pair<PbWriteRequestUnit, ByteBuf> writeUnitPair : writeRequestUnits) {
      try {
        MutationLogEntry entry = null;
        PbWriteRequestUnit writeUnit = writeUnitPair.getFirst();
        if (isPrimary) {
          entry = MutationLogEntryFactory
              .createLogForPrimary(writeUnit.getLogUuid(), writeUnit.getLogId(),
                  writeUnit.getOffset(), 0,
                  writeUnit.getLength(), writeUnit.getChecksum());
        } else {
          entry = MutationLogEntryFactory
              .createLogForSecondary(writeUnit.getLogUuid(), writeUnit.getLogId(), 0,
                  writeUnit.getOffset(), writeUnit.getLength(), writeUnit.getChecksum());
        }
        newLogs.add(entry);
      } catch (NoAvailableBufferException e) {
        logger.warn("caught an exception", e);
        break;
      }
    }

    return newLogs;
  }

  public long setSwplIdTo(long newSwplId) {
    reentrantLock.lock();
    try {
      long myPlId = getPlId();
      if (swplLogId > newSwplId) {
        logger.info(
            "The new swpl id: {} is smaller than old: {},  PL id: {}, segId: {}, "
                + "primarySwplLogId: {}",
            newSwplId, swplLogId, myPlId, segId, primarySwplLogId, new Exception());
        newSwplId = swplLogId;
      }

      this.primarySwplLogId = newSwplId;
      swplLogId = Math.min(newSwplId, myPlId);
      return swplLogId;
    } finally {
      reentrantLock.unlock();
    }
  }

  public long getSwplId() {
    reentrantLock.lock();
    try {
      if (segLogs.isEmpty()) {
        return LogImage.INVALID_LOG_ID;
      }
      return swplLogId != LogImage.INVALID_LOG_ID ? swplLogId : calculateId(peerPlIds, getPlId());
    } finally {
      reentrantLock.unlock();
    }
  }

  public long setSwclIdTo(long newSwclId) {
    reentrantLock.lock();
    try {
      long myclid = getClId();
      if (myclid < newSwclId) {
        logger.info("The new swcl id {} is larger than my CL id: {}, segId: {}", newSwclId, myclid,
            segId);
      }

      swclLogId = Math.min(newSwclId, myclid);
      return swclLogId;
    } finally {
      reentrantLock.unlock();
    }
  }

  public long getSwclId() {
    reentrantLock.lock();
    try {
      if (segLogs.isEmpty()) {
        return LogImage.INVALID_LOG_ID;
      }
      return swclLogId != LogImage.INVALID_LOG_ID ? swclLogId : calculateId(peerClIds, getClId());
    } finally {
      reentrantLock.unlock();
    }
  }

  public long getPeerClId(InstanceId instanceId) {
    PeerLogIdAndExpirationTime peerLogIdAndExpirationTime = peerClIds.get(instanceId);
    if (peerLogIdAndExpirationTime == null) {
      return LogImage.INVALID_LOG_ID;
    } else {
      return peerLogIdAndExpirationTime.peerLogId;
    }

  }

  public long removePeerClId(InstanceId peer) {
    reentrantLock.lock();
    try {
      return removeLogId(peerClIds, peer, getClId());
    } finally {
      reentrantLock.unlock();
    }
  }

  public long removePeerPlId(InstanceId peer) {
    reentrantLock.lock();
    try {
      return removeLogId(peerPlIds, peer, getPlId());
    } finally {
      reentrantLock.unlock();
    }
  }

  public long peerUpdateClId(long peerClId, InstanceId peer, long timeout, PeerStatus status) {
    reentrantLock.lock();
    try {
      return peerUpdateId(getClId(), peerClIds, peerClId, peer, timeout, status);
    } finally {
      reentrantLock.unlock();
    }
  }

  public long arbiterUpdateClId(InstanceId arbiter, long timeout) {
    reentrantLock.lock();
    try {
      return arbiterUpdateId(getClId(), peerClIds, arbiter, timeout);
    } finally {
      reentrantLock.unlock();
    }
  }

  public long peerUpdatePlId(long peerPlId, InstanceId peer, long timeout, PeerStatus status) {
    reentrantLock.lock();
    try {
      return peerUpdateId(getPlId(), peerPlIds, peerPlId, peer, timeout, status);
    } finally {
      reentrantLock.unlock();
    }
  }

  public long arbiterUpdatePlId(InstanceId arbiter, long timeout) {
    reentrantLock.lock();
    try {
      return arbiterUpdateId(getClId(), peerPlIds, arbiter, timeout);
    } finally {
      reentrantLock.unlock();
    }
  }

  public void clearPeerClIds() {
    reentrantLock.lock();
    try {
      peerClIds.clear();
    } finally {
      reentrantLock.unlock();
    }
  }

  public void clearPeerPlIds() {
    reentrantLock.lock();
    try {
      peerPlIds.clear();
    } finally {
      reentrantLock.unlock();
    }
  }

  public long getPrimaryClId() {
    return primaryClId == null ? LogImage.INVALID_LOG_ID : primaryClId.peerLogId;
  }

  public void setPrimaryClId(long primaryClId) {
    if (this.primaryClId == null) {
      this.primaryClId = new PeerLogIdAndExpirationTime(primaryClId,
          System.currentTimeMillis() + 10, PeerStatus.Active);
    } else {
      this.primaryClId.peerLogId = primaryClId;
    }
  }

  private long arbiterUpdateId(long myId, Map<InstanceId, PeerLogIdAndExpirationTime> peerIds,
      InstanceId arbiter,
      long timeout) {
    PeerLogIdAndExpirationTime peerIdAndTime = new PeerLogIdAndExpirationTime(true,
        System.currentTimeMillis() + timeout);
    peerIds.put(arbiter, peerIdAndTime);
    return calculateId(peerIds, myId);
  }

  private long peerUpdateId(long myId, Map<InstanceId, PeerLogIdAndExpirationTime> peerIds,
      long peerLogId,
      InstanceId peer, long timeout, PeerStatus status) {
    PeerLogIdAndExpirationTime peerIdAndTime = new PeerLogIdAndExpirationTime(peerLogId,
        System.currentTimeMillis() + timeout, status);
    peerIds.put(peer, peerIdAndTime);
    return calculateId(peerIds, myId);
  }

  private long removeLogId(Map<InstanceId, PeerLogIdAndExpirationTime> peerIds, InstanceId peer,
      long myId) {
    PeerLogIdAndExpirationTime peerIdAndTime = peerIds.remove(peer);
    if (peerIdAndTime != null && System.currentTimeMillis() < peerIdAndTime.expirationTime) {
      logger.warn(
          "Instance {} is being removed from the membership however its "
              + "log id is not expired (expiration: {} ). Nothing needs to be done",
          peer, peerIdAndTime.expirationTime);
    }
    if (peerIdAndTime != null) {
      return peerIdAndTime.peerLogId;
    } else {
      return LogImage.IMPOSSIBLE_LOG_ID;
    }
  }

  private long calculateId(Map<InstanceId, PeerLogIdAndExpirationTime> peerIds, long myId) {
    long currentTime = System.currentTimeMillis();
    LinkedList<Pair<Long, PeerStatus>> validIds = new LinkedList<>();

    validIds.add(new Pair<>(myId, PeerStatus.Active));
    int arbiterCount = 0;
    for (InstanceId instanceId : peerIds.keySet()) {
      PeerLogIdAndExpirationTime idAndTime = peerIds.get(instanceId);
      if (idAndTime.expirationTime > currentTime) {
        if (idAndTime.status == PeerStatus.Arbiter) {
          arbiterCount++;
        } else {
          validIds.add(new Pair<>(idAndTime.peerLogId, idAndTime.status));
        }
      }
    }

    int validIdSize = validIds.size();
    if (validIdSize + arbiterCount >= quorumSize) {
      validIds.sort((p1, p2) -> Long.compare(p2.getFirst(), p1.getFirst()));
      long value = LogImage.INVALID_LOG_ID;
      int index = 0;
      for (Pair<Long, PeerStatus> validId : validIds) {
        if (validId.getSecond() == PeerStatus.Active) {
          index++;
        }
        value = validId.getFirst();
        if (index == quorumSize) {
          break;
        }
      }
      return value;

    } else {
      return LogImage.INVALID_LOG_ID;
    }
  }

  private <T> Pair<DoubleLinkedListIterator<T>, Boolean> searchPositionToAddLog(
      boolean searchForward,
      DoubleLinkedListIterator<T> cursor, Function<T, Long> getLogId, long idOfLogToInsert) {
    boolean insertAfter;
    if (searchForward) {
      insertAfter = true;
      while (cursor.hasNext()) {
        cursor.next();
        Validate.notNull(cursor.content());
        if (getLogId.apply(cursor.content()) > idOfLogToInsert) {
          insertAfter = false;
          break;
        }
      }
    } else {
      insertAfter = false;
      while (cursor.hasPrevious()) {
        cursor.previous();
        Validate.notNull(cursor.content());
        if (idOfLogToInsert > getLogId.apply(cursor.content())) {
          insertAfter = true;
          break;
        }
      }
    }

    return new Pair<>(cursor, insertAfter);
  }

  public void dump() {
    reentrantLock.lock();
    try {
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = segLogs.head();
      int count = 0;
      try {
        logger.warn("++ pcl: {}, plal: {}, ppl: {}",
            pcl.content() == null ? null : pcl.content().content(),
            plal.content() == null ? null : plal.content().content(),
            ppl.content() == null ? null : ppl.content().content());
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }

      while (cursor.hasNext()) {
        DoubleLinkedListIterator<ListNode<MutationLogEntry>> ppFirstLog = cursor.next();
        logger.warn("content: {}", ppFirstLog.content().content());
        count++;
      }

      logger.warn("-- total count: {}", count);
    } finally {
      reentrantLock.unlock();
    }
  }

  private DoubleLinkedListIterator<MutationLogEntry> getLogList(PageAddress pageAddress) {
    reentrantLock.lock();
    try {
      DoubleLinkedList<MutationLogEntry> logsForPage = pageLogs
          .get(pageAddress.getLogicOffsetInSegment(pageSize));
      if (logsForPage != null && !logsForPage.isEmpty()) {
        DoubleLinkedListIterator<MutationLogEntry> pageCursor = logsForPage.head();
        return pageCursor;
      }
      return null;
    } finally {
      reentrantLock.unlock();
    }
  }

  public long getContinuousAppliedLogId(PageAddress pageAddress) {
    reentrantLock.lock();
    long continousAppliedLogId = 0;
    try {
      DoubleLinkedListIterator<MutationLogEntry> pageCursor = getLogList(pageAddress);

      long clId = getClId();
      while (pageCursor != null && pageCursor.hasNext()) {
        MutationLogEntry log = pageCursor.next().content();
        if (log.isApplied() && log.getLogId() <= clId) {
          continousAppliedLogId = log.getLogId();
        } else {
          break;
        }
      }
      return continousAppliedLogId;
    } finally {
      reentrantLock.unlock();
    }
  }

  public boolean hasLogNotApply(PageAddress pageAddress) {
    reentrantLock.lock();
    try {
      DoubleLinkedListIterator<MutationLogEntry> pageCursor = getLogList(pageAddress);
      while (pageCursor != null && pageCursor.hasNext()) {
        MutationLogEntry log = pageCursor.next().content();

        if (log.isCommitted() && !log.isApplied()) {
          return true;
        }
      }
    } finally {
      reentrantLock.unlock();
    }
    return false;
  }

  public List<MutationLogEntry> getLogsInPageToApply(PageAddress pageAddress) {
    List<MutationLogEntry> logs = new LinkedList<>();
    reentrantLock.lock();
    try {
      DoubleLinkedList<MutationLogEntry> logsForPage = pageLogs
          .get(pageAddress.getLogicOffsetInSegment(pageSize));
      if (logsForPage != null && !logsForPage.isEmpty()) {
        DoubleLinkedListIterator<MutationLogEntry> pageCursor = logsForPage.head();

        while (pageCursor.hasNext()) {
          MutationLogEntry log = pageCursor.next().content();

          if (log.getStatus() == MutationLogEntry.LogStatus.Committed && !log.isApplied()) {
            logger.info("the committed log haven't been applied yet {} ", log);
            logs.add(log);
            break;
          }
        }

        while (pageCursor.hasNext()) {
          MutationLogEntry log = pageCursor.next().content();
          if (log.getStatus() == MutationLogEntry.LogStatus.AbortedConfirmed) {
            continue;
          }

          if (log.getStatus() == MutationLogEntry.LogStatus.Committed || log.isApplied()) {
            logger.info("add a log {}", log);
            logs.add(log);
          }
        }
      }

    } finally {
      reentrantLock.unlock();
    }

    return Lists.reverse(logs);
  }

  public List<MutationLogEntry> getContinuityPersistLogAfterPpl() {
    List<MutationLogEntry> logs = new LinkedList<>();

    reentrantLock.lock();
    try {
      if (segLogs.isEmpty()) {
        return logs;
      }

      LogImage logImage = getLogImage(0);
      int numLogsFromPplToPlal = logImage.getNumLogsFromPplToPlal();
      if (numLogsFromPplToPlal == 0) {
        return logs;
      }

      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = logImage.getCursorFromPlToLal();
      logger.info("current logImage: {}, numLogsFromPplToPlal {} for PPL driver", logImage,
          numLogsFromPplToPlal);
      while (cursor.hasNext()) {
        MutationLogEntry logToPersisted = cursor.next().content().content();
        if (!logToPersisted.isPersisted()) {
          if (logToPersisted.getStatus() == MutationLogEntry.LogStatus.Committed) {
            break;

          } else {
            Validate
                .isTrue(logToPersisted.getStatus() == MutationLogEntry.LogStatus.AbortedConfirmed);
            logToPersisted.setPersisted();
          }
        }
        logs.add(logToPersisted);
      }
    } finally {
      reentrantLock.unlock();
    }

    return logs;
  }

  public List<MutationLogEntry> getNotPersistLogBeforePlal(int maxNumberOfLogsToPersistPerDrive,
      int maxLogs) {
    List<MutationLogEntry> logs = new LinkedList<>();

    reentrantLock.lock();
    try {
      if (segLogs.isEmpty()) {
        return logs;
      }

      LogImage logImage = getLogImage(0);
      int numLogsFromPplToPlal = logImage.getNumLogsFromPplToPlal();
      if (numLogsFromPplToPlal == 0) {
        return logs;
      }

      DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor = logImage.getCursorFromPlToLal();
      logger.info("current logImage: {}, numLogsFromPplToPlal {} for PPL driver", logImage,
          numLogsFromPplToPlal);
      int totalDealWithLogs = 0;
      while (cursor.hasNext() && totalDealWithLogs < maxNumberOfLogsToPersistPerDrive
          && logs.size() <= maxLogs) {
        MutationLogEntry logToPersisted = cursor.next().content().content();
        if (!logToPersisted.isPersisted()) {
          if (logToPersisted.getStatus() == MutationLogEntry.LogStatus.Committed) {
            if (numLogsFromPplToPlal - totalDealWithLogs <= MAX_PENDING_LOGS_FROM_PPL_TO_PLAL) {
              break;
            }
            logs.add(logToPersisted);

          } else {
            Validate
                .isTrue(logToPersisted.getStatus() == MutationLogEntry.LogStatus.AbortedConfirmed);
            logToPersisted.setPersisted();
          }
        }

        totalDealWithLogs++;
      }
    } finally {
      reentrantLock.unlock();
    }

    return logs;
  }

  public PlansToApplyLogs generatePlans(PageAddress pageAddress, DataNodeConfiguration cfg)
      throws LogsNotInRightOrderException, LogsNotInSamePageException, InvalidLogStatusException {
    List<MutationLogEntry> logs = getLogsInPageToApply(pageAddress);
    return PlansToApplyLogsBuilder.generate(pageAddress, logs, pageSize, segmentUnit);
  }

  public int getNumLogsFromPlalToPcl() {
    return numLogsFromPlalToPcl;
  }

  public int getNumLogsAfterPcl() {
    return numLogsAfterPcl;
  }

  public int getNumLogsFromPplToPlal() {
    return numLogsFromPplToPlal;
  }

  public Map<Long, MutationLogEntry> getMapLogUuidToLogsWithoutLogId() {
    return notInsertedLogs.getAllNotInsertedLogs();
  }

  public void checkPrimaryMissingLogs(List<Long> logUuids, long timeOut) {
    notInsertedLogs.checkPrimaryMissingLogs(logUuids, timeOut);
  }

  public Pair<Long, Long> getValueOfLogIdGeneratorAndMaxLogId() {
    reentrantLock.lock();
    try {
      long valueOfLogIdGenerator =
          logIdGenerator == null ? LogImage.INVALID_LOG_ID : logIdGenerator.currentLogId();
      long maxLogId = getMaxLogId();
      return new Pair<>(valueOfLogIdGenerator, maxLogId);

    } finally {
      reentrantLock.unlock();
    }
  }

  public void clearNotInsertedLogs() {
    notInsertedLogs.clearAllLogs(false);
  }

  public MutationLogEntry generateOneDummyLogWhileBecomingPrimary() {
    long dummyLogUuid = RequestIdBuilder.get();
    MutationLogEntry dummyLog;
    reentrantLock.lock();
    try {
      MutationLogEntry pclLog = pcl.content() == null ? null : pcl.content().content();
      if (pclLog != null) {
        dummyLog = MutationLogEntryFactory
            .createEmptyLog(dummyLogUuid, getNewOrOldLogId(dummyLogUuid, true).getFirst(), 0, 0, 0);
      } else {
        dummyLog = MutationLogEntryFactory
            .createEmptyLog(dummyLogUuid, getNewOrOldLogId(dummyLogUuid, true).getFirst(), 0, 0);
      }

      dummyLog.confirmAbortAndApply();
      dummyLog.setPersisted();
      dummyLog.setDummy(true);
      return dummyLog;
    } finally {
      reentrantLock.unlock();
    }
  }

  public int getPageSize() {
    return pageSize;
  }

  public long getSegLogConut() {
    return segLogConut.longValue();
  }

  public int getNotInsertedLogsCount() {
    return notInsertedLogs.getAllNotInsertedLogsCount();
  }

  @JsonIgnore
  public long getPrimaryMaxLogIdWhenPsi() {
    return primaryMaxLogIdWhenPsi;
  }

  @JsonIgnore
  public void setPrimaryMaxLogIdWhenPsi(long primaryMaxLogIdWhenPsi) {
    this.primaryMaxLogIdWhenPsi = primaryMaxLogIdWhenPsi;
  }

  public void updatePreparePplId() {
    reentrantLock.lock();
    try {
      ListNode<MutationLogEntry> maxLogBeforePlal = plal.content();
      if (maxLogBeforePlal != null) {
        updatePreparePplId(maxLogBeforePlal.content().getLogId());
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public void updatePreparePplId(long prepareLogId) {
    this.preparePplId.set(prepareLogId);
  }

  public void updateLatestPreparePplId() {
    this.latestPreparePplId.set(preparePplId.get());
  }

  public long getLatestPreparePplId() {
    return latestPreparePplId.get();
  }

  public int getMaxAppliedSnapId() {
    return maxAppliedSnapId;
  }

  public void setMaxAppliedSnapId(int maxAppliedSnapId) {
    this.maxAppliedSnapId = maxAppliedSnapId;
  }

  public void setSegmentUnit(SegmentUnit segmentUnit) {
    this.segmentUnit = segmentUnit;
  }

  public int gapOfPclLogIds() {
    if (primaryClId != null) {
      if (this.pcl.content() != null) {
        return (int) (primaryClId.peerLogId - this.pcl.content().content().getLogId());
      }
      return Integer.MAX_VALUE;
    }
    return 0;
  }

  public void updateLastRemoveUuid(long uuid) {
    long highVal = BroadcastLogUuidGenerator.getInstance().parseHighPartial(uuid);
    long lowerVal = BroadcastLogUuidGenerator.getInstance().parseLowerPartial(uuid);

    lastRemovedUuid.compute(highVal, (k, v) -> {
      if (v == null) {
        return new Pair<>(lowerVal, System.currentTimeMillis());
      }

      int result = BroadcastLogUuidGenerator.getInstance().compare(lowerVal, v.getFirst());
      if (result > 0) {
        return new Pair<>(lowerVal, System.currentTimeMillis());
      }

      return v;
    });
  }

  public boolean logHasBeenCrashedOut(long uuid) {
    if (mapLogsUuidToLogId.containsKey(uuid)) {
      logger.info(
          "a log which is uuid {} has inserted segment log metadata,"
              + " it may be crash out by pcl id {}",
          uuid, getClId());
      return true;
    }
    long highVal = BroadcastLogUuidGenerator.getInstance().parseHighPartial(uuid);
    long lowerVal = BroadcastLogUuidGenerator.getInstance().parseLowerPartial(uuid);

    Pair<Long, Long> v = lastRemovedUuid.get(highVal);
    if (v == null) {
      return false;
    }

    int result = BroadcastLogUuidGenerator.getInstance().compare(lowerVal, v.getFirst());

    if (result < 0) {
      logger.info(
          "a log which is uuid {} lower than last removed uuid, "
              + "it may be crash out by pswl pointer {}",
          uuid, v);
    }

    return result < 0;
  }

  public void cleanUpTimeOutUuid(long cleanUpUuidPeriod, long cleanCompletingLogPeriod) {
    Set<Map.Entry<Long, Pair<Long, Long>>> set = lastRemovedUuid.entrySet();
    Iterator<Map.Entry<Long, Pair<Long, Long>>> iterator = set.iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Pair<Long, Long>> entry = iterator.next();
      if (entry.getValue().getSecond() + cleanUpUuidPeriod < System.currentTimeMillis()) {
        lastRemovedUuid.remove(entry.getKey());
      }
    }

    if (cleanCompletingLogPeriod != 0) {
      notInsertedLogs.cleanUpTimeOutCompletingLog(cleanCompletingLogPeriod);
    }
  }

  public CompletingLog getCompleteLog(long uuId) {
    return notInsertedLogs.getCompletingLog(uuId);
  }

  public enum PeerStatus {
    Arbiter((byte) 0),
    Inactive((byte) 1),
    BecomingActive((byte) 2),
    Active((byte) 3),
    ;

    byte value;

    PeerStatus(byte value) {
      this.value = value;
    }

    public static PeerStatus getPeerStatusInMembership(SegmentMembership membership,
        InstanceId instanceId) {
      if (membership.isArbiter(instanceId)) {
        return Arbiter;
      }

      if (membership.isPrimary(instanceId)) {
        return Active;
      }

      if (membership.isSecondary(instanceId)) {
        return Active;
      }

      if (membership.isJoiningSecondary(instanceId)) {
        return BecomingActive;
      }

      return Inactive;
    }

    byte getValue() {
      return value;
    }
  }

  private static class ThriftPbLogUtil {
    static <T> long getUuid(T log) {
      if (isPbLog(log)) {
        return ((PbBroadcastLog) log).getLogUuid();
      } else {
        return ((BroadcastLogThrift) log).getLogUuid();
      }
    }

    static <T> long getLogId(T log) {
      if (isPbLog(log)) {
        return ((PbBroadcastLog) log).getLogId();
      } else {
        return ((BroadcastLogThrift) log).getLogId();
      }
    }

    static <T> boolean isLogAborted(T log) {
      if (isPbLog(log)) {
        return ((PbBroadcastLog) log).getLogStatus() == PbBroadcastLogStatus.ABORT;
      } else {
        return ((BroadcastLogThrift) log).getLogStatus() == BroadcastLogStatusThrift.Abort;
      }
    }

    static <T> boolean isLogCreated(T log) {
      if (isPbLog(log)) {
        return ((PbBroadcastLog) log).getLogStatus() == PbBroadcastLogStatus.CREATED;
      } else {
        return ((BroadcastLogThrift) log).getLogStatus() == BroadcastLogStatusThrift.Created;
      }
    }

    static <T> boolean isFinalStatus(T log) {
      if (isPbLog(log)) {
        return ((PbBroadcastLog) log).getLogStatus() == PbBroadcastLogStatus.ABORT_CONFIRMED
            || ((PbBroadcastLog) log).getLogStatus() == PbBroadcastLogStatus.COMMITTED;
      } else {
        return
            ((BroadcastLogThrift) log).getLogStatus() == BroadcastLogStatusThrift.AbortConfirmed
                || ((BroadcastLogThrift) log).getLogStatus()
                == BroadcastLogStatusThrift.Committed;
      }
    }

    static <T> long getChecksum(T log) {
      if (isPbLog(log)) {
        return ((PbBroadcastLog) log).getChecksum();
      } else {
        return ((BroadcastLogThrift) log).getChecksum();
      }
    }

    static <T> long getOffset(T log) {
      if (isPbLog(log)) {
        return ((PbBroadcastLog) log).getOffset();
      } else {
        return ((BroadcastLogThrift) log).getOffset();
      }
    }

    static <T> int getLength(T log) {
      if (isPbLog(log)) {
        return ((PbBroadcastLog) log).getLength();
      } else {
        return ((BroadcastLogThrift) log).getLength();
      }
    }

    @SuppressWarnings("unchecked")
    static <T> T changeLogStatusToCommitt(T log) {
      if (isPbLog(log)) {
        return (T) ((PbBroadcastLog) log).toBuilder().setLogStatus(PbBroadcastLogStatus.COMMITTED)
            .build();
      } else {
        return (T) ((BroadcastLogThrift) log).setLogStatus(BroadcastLogStatusThrift.Committed);
      }
    }

    @SuppressWarnings("unchecked")
    static <T> T changeLogStatusToAbortConfirmed(T log) {
      if (isPbLog(log)) {
        return (T) ((PbBroadcastLog) log).toBuilder()
            .setLogStatus(PbBroadcastLogStatus.ABORT_CONFIRMED).build();
      } else {
        return (T) ((BroadcastLogThrift) log)
            .setLogStatus(BroadcastLogStatusThrift.AbortConfirmed);
      }
    }

    private static <T> boolean isPbLog(T t) {
      if (t instanceof PbBroadcastLog) {
        return true;
      } else if (t instanceof BroadcastLogThrift) {
        return false;
      } else {
        throw new RuntimeException("wrong log type");
      }
    }

  }

  private class PeerLogIdAndExpirationTime {
    long peerLogId;
    long expirationTime;
    PeerStatus status;

    PeerLogIdAndExpirationTime(long logId, long expiration, PeerStatus status) {
      this.peerLogId = logId;
      this.expirationTime = expiration;
      this.status = status;
    }

    PeerLogIdAndExpirationTime(boolean isArbiter, long expiration) {
      Validate.isTrue(isArbiter);
      this.peerLogId = LogImage.INVALID_LOG_ID;
      this.expirationTime = expiration;
      this.status = PeerStatus.Arbiter;
    }
  }
}

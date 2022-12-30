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

package py.datanode.segment.datalogbak.catchup;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageManager;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.context.SinglePageContextFactory;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.persist.LogPersister;
import py.storage.Storage;
import py.third.rocksdb.KvStoreException;

public class PplDriver implements ChainedLogDriver {
  private static final Logger logger = LoggerFactory.getLogger(PplDriver.class);
  private static long lastFlushTime = System.currentTimeMillis();
  private static int DEFAULT_INTERVAL_FLUSH_PAGE = 100;
  private static int MAX_PENDING_LOGS_FROM_PPL_TO_PLAL = 4096;
  private final SegmentUnitManager segmentUnitManager;
  private final DataNodeConfiguration cfg;
  private SegmentLogMetadata segLogMetadata;
  private SegmentUnit segmentUnit;
  private LogPersister logPersister;
  private SegId segId;
  private PageManager<Page> pageManager;

  public PplDriver(SegmentLogMetadata segLogMetadata, SegmentUnit segmentUnit,
      SegmentUnitManager segmentUnitManager, PageManager<Page> pageManager,
      LogPersister logPersister, DataNodeConfiguration cfg) {
    this.segmentUnitManager = segmentUnitManager;
    this.segLogMetadata = segLogMetadata;
    this.segId = segLogMetadata.getSegId();
    this.segmentUnit = segmentUnit;
    this.logPersister = logPersister;
    this.pageManager = pageManager;
    this.cfg = cfg;
  }

  public static void setMaxPendingLogs(int maxPendingLogs) {
    MAX_PENDING_LOGS_FROM_PPL_TO_PLAL = maxPendingLogs;
  }

  @Override
  public ExecuteLevel drive() throws Exception {
    int moveSteps = 0;
    try {
      moveSteps = schdulePpl();
    } catch (Throwable t) {
     
      logger.warn("caught an throwable while driving ppl for {}", segId, t);
    }

    if (moveSteps > MAX_PENDING_LOGS_FROM_PPL_TO_PLAL
        || segLogMetadata.getNumLogsFromPplToPlal() > MAX_PENDING_LOGS_FROM_PPL_TO_PLAL) {
      return ExecuteLevel.QUICKLY;
    } else {
      return ExecuteLevel.SLOWLY;
    }
  }

  private int schdulePpl() throws Exception {
   
    segLogMetadata.updatePreparePplId();

    return pplDrive();
  }

  private int pplDrive() throws Exception {
   
   
   
    SegmentUnitStatus currentStatus = segmentUnit.getSegmentUnitMetadata().getStatus();
    if (currentStatus == SegmentUnitStatus.Deleted) {
      logger.warn("current segment unit is deleted, not need to process PPL log: {}", segmentUnit);
      return 0;
    }

    int pageSize = segmentUnit.getArchive().getArchiveMetadata().getPageSize();
    long startOffset = segmentUnit.getStartLogicalOffset();
    Storage storage = segmentUnit.getArchive().getStorage();
    if (storage.isBroken()) {
      logger.warn("storage has broken: {}, for segId: {}", storage, segId);
      return 0;
    }

    LogImage logImage = segLogMetadata.getLogImage(0);
    logger.debug("begin applying, the new log image is {} ", logImage);

    List<MutationLogEntry> persistLogs = segLogMetadata.getContinuityPersistLogAfterPpl();
    try {
      if (!persistLogs.isEmpty()) {
        logPersister.persistLog(segId, persistLogs);
       
        long idOfLatestPersistedLog = persistLogs.get(persistLogs.size() - 1).getLogId();
        try {
          logger
              .info("move ppl pointer to: {},{} for segId:{}", idOfLatestPersistedLog, persistLogs,
                  segId);
          segLogMetadata.movePlToWithoutCheck(idOfLatestPersistedLog, persistLogs.size());
        } catch (Exception e) {
          logger.error("can't move to {}", idOfLatestPersistedLog, e);
        }

      } else {
        logger.info("there is no logs({}) to persist: {} in segment: {}", segId, logImage, segId);
      }

      int numRemovedLogs = moveSwplPointer();
      logger.debug("removed {} prior to {} for {} ", numRemovedLogs, logImage.getSwplId(), segId);
      logger.debug(" after applying, the new log image is {} ", segLogMetadata.getLogImage(0));
    } catch (IOException e) {
      boolean reinit = logPersister.reInit(segId);
      logger.error(
          "Caught an IOException while persisting from to {} . " 
              + "Let's close the LogPersister and reinit({}) it again",
          persistLogs.size(), reinit, e);
      return 0;
    } catch (KvStoreException e) {
      boolean reinit = logPersister.reInit(segId);
      logger.error(
          "Caught an KvStoreException while persisting from to {}." 
              + " Let's close the LogPersister and reinit({}) it again",
          persistLogs.size(), reinit, e);
      return 0;
    } catch (LogIdTooSmall e) {
      logger.warn(
          "It is weird that has smaller id than those persisted logs." 
              + " Skip persist this log from to {}",
          persistLogs.size(), e);
      return 0;
    } catch (Exception e) {
      if (persistLogs.size() > 0) {
        logger.error(
            "Caught an unknown exception when persisting" 
                + " log from {} to {} to file log system. Quit the PPL Driver thread",
            persistLogs.get(0), persistLogs.get(persistLogs.size() - 1), e);
      } else {
        logger.error(
            "Caught an unknown exception persisting " 
                + "log to file log system. Quit the PPL Driver thread",
            e);
      }
      return 0;
    }

    List<MutationLogEntry> notPersistLog = segLogMetadata
        .getNotPersistLogBeforePlal(cfg.getMaxNumberOfLogsToPersistPerDrive(),
            cfg.getMaxNumberOfPlalWorkerPendingPage());

    if (!notPersistLog.isEmpty()
        && System.currentTimeMillis() - lastFlushTime >= DEFAULT_INTERVAL_FLUSH_PAGE) {
      Map<PageAddress, PageContext<Page>> mapPageToFlush = null;
      for (MutationLogEntry logToPersisted : notPersistLog) {
       
        PageAddress address = PageAddressGenerator
            .generate(segId, startOffset, logToPersisted.getOffset(), storage, pageSize);
        PageContextFactory<Page> contextFactory = SinglePageContextFactory.getInstance();
        if (mapPageToFlush == null) {
          mapPageToFlush = new HashMap<>();
          mapPageToFlush.put(address, contextFactory.generatePageFlushContext(address));
        } else {
          if (!mapPageToFlush.containsKey(address)) {
            mapPageToFlush.put(address, contextFactory.generatePageFlushContext(address));
          }
        }
      }
      lastFlushTime = System.currentTimeMillis();
      try {
        logger.info("flush pages : {}", mapPageToFlush.keySet());
        pageManager.flushPage(
            SinglePageContextFactory.getInstance()
                .generatePageContextWrapper(mapPageToFlush.values()));
      } finally {
        logger.info("nothing need to do here");
      }
    }

    return persistLogs.size();
  }

  private int moveSwplPointer() {
    int numRemovedLogs = segLogMetadata.updateSwplAndremoveLogsPriorToSwpl();
    return numRemovedLogs;
  }

  @Override
  public ChainedLogDriver getNextDriver() {
    return null;
  }

  @Override
  public void setNextDriver(ChainedLogDriver driver) {
  }
}

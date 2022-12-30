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

package py.datanode.segment.datalog.plal.engine;

import com.google.common.collect.RangeSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.archive.page.PageAddressComparator;
import py.archive.page.PageAddressImpl;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.common.NamedThreadFactory;
import py.common.struct.Pair;
import py.consumer.AbstractSortedConsumerService;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.FailToDealWithPlansException;
import py.datanode.exception.InvalidLogStatusException;
import py.datanode.exception.LogsNotInRightOrderException;
import py.datanode.exception.LogsNotInSamePageException;
import py.datanode.exception.PageHasBeenMigratedJustNowException;
import py.datanode.page.MultiChunkAddressHelper;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageHelper;
import py.datanode.page.PageListener;
import py.datanode.page.PageManager;
import py.datanode.page.TaskType;
import py.datanode.page.context.AsyncPageContextWaiter;
import py.datanode.page.context.AsyncShadowPageContextImpl;
import py.datanode.page.context.BogusPageContext;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.context.SinglePageContextFactory;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs.WhatToDo;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs.WhatToSave;
import py.datanode.service.DataNodeServiceImpl;
import py.datanode.service.PageErrorCorrector;
import py.exception.ChecksumMismatchedException;
import py.membership.SegmentMembership;
import py.storage.Storage;
import py.storage.impl.StorageUtils;

public class PlalWorker extends AbstractSortedConsumerService<PageAddress> {
  private static final Logger logger = LoggerFactory.getLogger(PlalWorker.class);

  private final DataNodeConfiguration cfg;

  // actually it's not elegant to pass the whole service into PLALWorker
  // the main reason it has not been removed is PageErrorCorrector
  private final DataNodeServiceImpl service;
  private final Storage storage;

  private final SegmentUnitManager segmentUnitManager;
  private final PageContextFactory<Page> pageContextFactory;
  private final PageManager<Page> pageManager;

  // for metric
  private final String prefix;

  // page system can't take too many requests.
  // Use a semaphore to control the number of requests submitted to the page system.
  // this semaphore is released after the requested page has been flushed to storage or discarded.
  private final Semaphore availableSlotsForPageSystemRequestsTillPersisted;
  // this semaphore is release after the requested page has been checked in.
  private final Semaphore availableSlotsForPageSystemRequestsTillCheckedOut;
  private final AsyncPageContextWaiter<Page> waiter;
  private final Map<MultiPageAddress, List<PlansToApplyLogs>> chunkAddressToPlans;
  private final Map<MultiPageAddress, AtomicInteger> chunkAddressToPlansWorkingCount;
  //    private final Map<PageAddress, PageAddress> physicalAddressToLogicalAddress;
  private final Map<PageContext<Page>, PageAddress> pageContextToLogicalAddress;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final ThreadPoolExecutor failToPersistThreadPoolExecutor;
  // page system callback processing thread, applying logs to page gotten from page system
  private Thread processThread;
  private int lockFailedCount = 0;

  public PlalWorker(DataNodeServiceImpl service, Storage storage,
      ThreadPoolExecutor failToPersistThreadPoolExecutor) {
    super(new PageAddressComparator(),
        Storage.getDeviceName(storage.identifier()) + "-" + "PLAL Driver");
    this.service = service;
    this.storage = storage;
    this.cfg = service.getCfg();
    this.segmentUnitManager = service.getSegmentUnitManager();
    this.pageManager = service.getPageManager();
    this.failToPersistThreadPoolExecutor = failToPersistThreadPoolExecutor;
    this.pageContextFactory = SinglePageContextFactory.getInstance();

    if (StorageUtils.isSsd(storage) || StorageUtils.isPcie(storage)) {
      this.availableSlotsForPageSystemRequestsTillPersisted = new Semaphore(
          cfg.getMaxNumberOfPlalWorkerPendingPageForSsd());
    } else {
      this.availableSlotsForPageSystemRequestsTillPersisted = new Semaphore(
          cfg.getMaxNumberOfPlalWorkerPendingPage());
    }
    this.availableSlotsForPageSystemRequestsTillCheckedOut = new Semaphore(
        cfg.getConcurrentPageContextsPerPlalWorker());

    this.chunkAddressToPlans = new ConcurrentHashMap<>();
    this.pageContextToLogicalAddress = new ConcurrentHashMap<>();
    this.chunkAddressToPlansWorkingCount = new ConcurrentHashMap<>();
    this.prefix = name;

    this.waiter = new AsyncPageContextWaiter<>();
  }

  public boolean add(PageAddress logicalPageAddress) {
    MultiPageAddress chunkAddress = MultiChunkAddressHelper
        .getChunkAddressFromChildAddress(logicalPageAddress);
    if (!chunkAddressToPlans.containsKey(chunkAddress)) {
      return super.submit(logicalPageAddress);
    } else {
      return false;
    }
  }

  @Override
  protected void consume(Collection<PageAddress> logicalPageAddresses) {
    logger.debug("scheduling {}", logicalPageAddresses.size());
    List<PageContext<Page>> pageContexts = new ArrayList<>();
    MultiPageAddress prevChunkPageAddress = null;
    int prevChunkIndex = -1;

    for (PageAddress logicalPageAddress : logicalPageAddresses) {
      if (logicalPageAddress instanceof ExitRequestForScheduler) {
        Thread tmp = Thread.currentThread();
        logger.warn("receive an exit request, for plal driver={}, name: {}, id: {}", prefix,
            tmp.getName(),
            tmp.getId());
        super.shutdown();
        continue;
      } else if (logicalPageAddress instanceof CheckPageAddress) {
        logger.debug("check request got");
        waiter.increment();
        waiter.completed(new CheckPageContext(logicalPageAddress));
        continue;
      }

      SegId segId = logicalPageAddress.getSegId();
      SegmentUnit segmentUnit = segmentUnitManager.get(segId);
      if (segmentUnit == null) {
        logger.warn("can not get segment unit manager for segId: {}", segId);
        continue;
      }

      SegmentUnitStatus status = segmentUnit.getSegmentUnitMetadata().getStatus();
      if (status == SegmentUnitStatus.Deleted) {
        logger.warn("current segment unit is {}, not need to process PLAL log: {}", status,
            segmentUnit);
        continue;
      }

      int chunkIndex = MultiChunkAddressHelper.calculateChunkIndex(logicalPageAddress);
      if (chunkIndex != prevChunkIndex || !logicalPageAddress.getSegId().equals(
          prevChunkPageAddress.getStartPageAddress().getSegId())) {
        MultiPageAddress chunkPageAddress = MultiChunkAddressHelper
            .getChunkAddressFromChildAddress(logicalPageAddress);
        if (chunkAddressToPlans.containsKey(chunkPageAddress)) {
          logger.info("this chunk {} is processing {}", chunkPageAddress,
              chunkAddressToPlans.get(chunkPageAddress));
        } else {
          pageContexts = scheduleMultiPagesInChunk(chunkPageAddress, segmentUnit);
          checkoutPages(pageContexts);
        }
        prevChunkIndex = chunkIndex;
        prevChunkPageAddress = chunkPageAddress;
      } else {
        logger.debug("this page {} belong to the same chunk {}", logicalPageAddress,
            prevChunkPageAddress);
      }

    }
  }

  private void checkoutPages(List<PageContext<Page>> pageContexts) {
    if (pageContexts.isEmpty()) {
      return;
    }

    int count = pageContexts.size();
    if (count > 1) {
      pageManager
          .checkout(pageContextFactory.generatePageContextWrapper(new ArrayList<>(pageContexts)));
    } else if (count == 1) {
      pageManager.checkout(pageContexts.get(0));
    } else {
      throw new RuntimeException("no way getting here");
    }
    logger.debug("have successfully submitted context related to page {} to the page system",
        pageContexts);
  }

  private List<PageContext<Page>> scheduleMultiPagesInChunk(MultiPageAddress chunkPageAddress,
      SegmentUnit segmentUnit) {
    List<PageAddress> pageAddressList = chunkPageAddress.convertToPageAddresses();
    List<PlansToApplyLogs> plansList = new ArrayList<>(pageAddressList.size());
    List<PageContext<Page>> pageContexts = new ArrayList<>();

    for (int i = 0; i < pageAddressList.size(); i++) {
      PageAddress logicalPageAddress = pageAddressList.get(i);
      logger.info("scheduling {}", logicalPageAddress);
      PlansToApplyLogs plans;
      try {
        plans = segmentUnit.getSegmentLogMetadata().generatePlans(logicalPageAddress, cfg);
      } catch (LogsNotInRightOrderException | LogsNotInSamePageException 
          | InvalidLogStatusException e) {
        logger.error("caught an exception generating plans {}, {}", segmentUnit, logicalPageAddress,
            e);
        plansList.add(new PlansToApplyLogsBuilder.EmptyPlans());
        continue;
      }
      Validate.notNull(plans);
      logger.info("got plans. logicalPageAddress: {}, plans: {}", logicalPageAddress, plans);

      if (plans instanceof PlansToApplyLogsBuilder.EmptyPlans) {
        logger.debug(
            "empty plans for the page address {}. " 
                + "Don't need to submit the page address to the page system",
            logicalPageAddress);
        plansList.add(plans);
        continue;
      } else {
        int pageIndex = (int) (logicalPageAddress.getOffsetInSegment() / cfg.getPhysicalPageSize());
        if (!dealWithCopyPageStatus(plans, segmentUnit, pageIndex)) {
          plansList.add(new PlansToApplyLogsBuilder.EmptyPlans());
          continue;
        } else {
          plansList.add(plans);
          PageContext<Page> context = generatePageContexts(plans,
              new MultiPageAddress(logicalPageAddress, 1), segmentUnit);
          logger.debug("generate context {} , plan is {}", context, plans);
          pageContexts.add(context);
        }
      }

    }

    if (pageContexts.size() == 0) {
      logger.info("no plan at all");
      return new ArrayList<>(0);
    }

    logger.info("chunk {} plans {}, page {} ", chunkPageAddress, plansList, pageContexts);

    chunkAddressToPlansWorkingCount.put(chunkPageAddress, new AtomicInteger(pageContexts.size()));
    chunkAddressToPlans.put(chunkPageAddress, plansList);
    return pageContexts;
  }

  private PageContext<Page> generatePageContexts(PlansToApplyLogs pagePlan,
      MultiPageAddress pageAddress, SegmentUnit segmentUnit) {
    PageContext<Page> context;
    int pageCount = 1;
    long timeout = service.getCfg().getDefaultPageRequestTimeoutMs();
    PageAddress logicalPageAddress = pageAddress.convertToPageAddresses().get(0);
    PageAddress physicalPageAddress = segmentUnit.getAddressManager()
        .getOriginPhysicalPageAddressByLogicalAddress(logicalPageAddress);
    if (pagePlan.getWhatToDo() == WhatToDo.NoNeedToLoadPage) {
      context = pageContextFactory.generateAsyncCheckoutContext(
          physicalPageAddress, TaskType.CHECK_OUT_FOR_EXTERNAL_CORRECTION, waiter, timeout);
    } else {
      context = pageContextFactory.generateAsyncCheckoutContext(physicalPageAddress,
          TaskType.CHECK_OUT_FOR_EXTERNAL_WRITE, waiter, timeout);
    }
    waiter.increment();
    pageContextToLogicalAddress.put(context, pageAddress.getStartPageAddress());
    logger.info("plan is {} context {}", pagePlan, context);

    logger.debug("acquire availableSlotsForPageSystem {}",
        availableSlotsForPageSystemRequestsTillPersisted.availablePermits());
    try {
      availableSlotsForPageSystemRequestsTillCheckedOut.acquire(pageCount);
      availableSlotsForPageSystemRequestsTillPersisted.acquire(pageCount);
    } catch (InterruptedException e) {
      logger.error("interrupted !", e);
    }
    logger.debug("done acquire {}",
        availableSlotsForPageSystemRequestsTillPersisted.availablePermits());

    return context;
  }

  private List<Pair<MultiPageAddress, Boolean>> splitByFirstWrite(MultiPageAddress multiPageAddress,
      SegmentUnit segmentUnit) {
    List<Pair<MultiPageAddress, Boolean>> multiPageAddresses = new ArrayList<>();
    segmentUnit.setPageHasBeenWrittenAndFirstWritePageIndex(
        PageAddressGenerator.calculatePageIndex(multiPageAddress.getStartPageAddress()));
    boolean currentPageIsFirstWrite = segmentUnit.isPageIndexFristWrite(
        PageAddressGenerator.calculatePageIndex(multiPageAddress.getStartPageAddress()));
    int count = 1;
    PageAddress startPageAddress = multiPageAddress.getStartPageAddress();
    for (PageAddress pageAddress : multiPageAddress.convertToPageAddresses()) {
      if (pageAddress.equals(startPageAddress)) {
        continue;
      }
      int pageIndex = PageAddressGenerator.calculatePageIndex(pageAddress);

      segmentUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
      if (segmentUnit.isPageIndexFristWrite(pageIndex)) {
        if (currentPageIsFirstWrite) {
          count++;
        } else {
          multiPageAddresses.add(new Pair<>(new MultiPageAddress(startPageAddress, count),
              currentPageIsFirstWrite));
          startPageAddress = pageAddress;
          count = 1;
          currentPageIsFirstWrite = true;
        }
      } else {
        if (!currentPageIsFirstWrite) {
          count++;
        } else {
          multiPageAddresses.add(new Pair<>(new MultiPageAddress(startPageAddress, count),
              currentPageIsFirstWrite));
          startPageAddress = pageAddress;
          count = 1;
          currentPageIsFirstWrite = false;
        }
      }

    }

    multiPageAddresses.add(
        new Pair<>(new MultiPageAddress(startPageAddress, count), currentPageIsFirstWrite));

    return multiPageAddresses;
  }

  private boolean dealWithCopyPageStatus(PlansToApplyLogs plans, SegmentUnit segmentUnit,
      int pageIndex) {
    SecondaryCopyPageManager copyManager = segmentUnit.getSecondaryCopyPageManager();
    if (copyManager == null) {
      return true;
    }

    if (copyManager.getCatchUpLog() == null) {
      logger.warn("{} status is {}, just wait catchup log, log count {}", segmentUnit.getSegId(),
          copyManager.getCopyPageStatus(),
          segmentUnit.getSegmentLogMetadata().getMaxLogId() - segmentUnit.getSegmentLogMetadata()
              .getLalId());
      return false;
    }

    Consumer<MutationLogEntry> logReleaser = log -> {
      log.commitAndApply();
      log.setPersisted();
      MutationLogEntryFactory.releaseLogData(log);
    };

    int countLogDiscard = 0;
    long catchUpLogId = copyManager.getCatchUpLog().getLogId();
    long firstLogId = plans.getPlans().get(0).getLog().getLogId();
    long maxId = copyManager.getMaxLogId(pageIndex);
    logger.info("first is {} max is {}", firstLogId, maxId);

    for (PlansToApplyLogs.PlanToApplyLog plan : plans.getPlans()) {
      MutationLogEntry log = plan.getLog();
      if (log.getLogId() <= catchUpLogId) {
        logger.info("discard the log={} before catchup log", log);
        logReleaser.accept(log);
        countLogDiscard++;
        copyManager.getCopyPageBitmap().clear(pageIndex);
        if (countLogDiscard == 0) {
          logger.info("logs in plan are smaller than catch up log, mark copy {}", plan);
          copyManager.getCopyPageBitmap().clear(pageIndex);
        }
      }
    }
    logger.info("logs smaller than catch up logs {}", countLogDiscard);

    if (segmentUnit.getSegmentLogMetadata().getLalId() < catchUpLogId) {
      logger.info("wait for catch up log {} {}", catchUpLogId,
          segmentUnit.getSegmentLogMetadata().getLalId());
      return false;
    }

    if (copyManager.isProcessed(pageIndex)) {
      logger
          .info("it is ok to apply the log {}, due to page no need copy or already copied", plans);
      return true;
    } else if (copyManager.isProcessing(pageIndex)) {
      return false;
    } else {
      long maxLogId = plans.getMaxLogId();
      if (maxLogId < maxId) {
        logger.warn("discard the plan {} due to log smaller than max id {}", plans, maxId);
        plans.getPlans().forEach(plan -> logReleaser.accept(plan.getLog()));
        return false;
      }

      if (!copyManager.markMaxLogId(pageIndex, plans.getMaxLogId())) {
        return false;
      }

      if (copyManager.getCopyPageBitmap().get(pageIndex)) {
        logger.info("just clear the page {} and wait the chunk be copied", pageIndex);
        copyManager.getCopyPageBitmap().clear(pageIndex);
        plans.getPlans().forEach(plan -> logReleaser.accept(plan.getLog()));
        return false;
      }

      if (plans.getWhatToDo() == WhatToDo.NoNeedToLoadPage) {
        plans.canApplyWhenMigrating(true);
        return true;
      } else {
        plans.getPlans().forEach(plan -> logReleaser.accept(plan.getLog()));
        logger.info("discard the plan ={} and expect the page been copied , hasSnap {}",
            plans);
        return false;
      }
    }
  }

  private void processResult() {
    PageContext<Page> context;
    PageAddress logicalPageAddress;
    PlansToApplyLogs plans;

    while (true) {
      try {
        context = waiter.take1(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error("interrupted", e);
        return;
      }

      if (context == null) {
        logger.info("no task gotten in 60s, waiter {}", waiter);
        continue;
      }

      if (context instanceof ExitRequestForProcessor) {
        logger.warn("processor exit {}", Thread.currentThread());

        return;
      } else if (context instanceof CheckPageContext) {
        CheckPageAddress tmp = (CheckPageAddress) context.getPageAddressForIo();
        tmp.notifyFor();
        continue;
      }

      logicalPageAddress = pageContextToLogicalAddress.remove(context);
      MultiPageAddress chunkPageAddress =
          MultiChunkAddressHelper.getChunkAddressFromChildAddress(logicalPageAddress);
      int pageIndexInChunk = PageAddressGenerator.calculatePageIndex(logicalPageAddress) 
          - PageAddressGenerator.calculatePageIndex(chunkPageAddress.getStartPageAddress());
      List<PlansToApplyLogs> plansList = chunkAddressToPlans.get(chunkPageAddress);
      logger.info("chunk {} plans {} ", chunkPageAddress, plansList);
      plans = plansList.get(pageIndexInChunk);

      logger
          .info("Got a page context from the processing queue context {}, plan {}", context, plans);
      try {
        processResultForOnePage(context, logicalPageAddress, plans);
      } catch (Throwable throwable) {
        logger.error("caught a throwable", throwable);
      } finally {
        if (!(context instanceof AsyncShadowPageContextImpl) && !context.getPage().isDirty()) {
          chunkProcessDone(logicalPageAddress);
        }
        availableSlotsForPageSystemRequestsTillCheckedOut.release();
        PageHelper.checkIn(pageManager, context, logicalPageAddress.getSegId());
      }

    }
  }

  private void chunkProcessDone(PageAddress logicalPageAddress) {
    MultiPageAddress chunkPageAddress =
        MultiChunkAddressHelper.getChunkAddressFromChildAddress(logicalPageAddress);
    AtomicInteger count = chunkAddressToPlansWorkingCount.get(chunkPageAddress);
    if (count.decrementAndGet() == 0) {
      chunkAddressToPlansWorkingCount.remove(chunkPageAddress);
      List<PlansToApplyLogs> logsList = chunkAddressToPlans.remove(chunkPageAddress);

      logger.debug("a chunk is processed {} {} remain {} {}", chunkPageAddress,
          logsList, chunkAddressToPlansWorkingCount,
          chunkAddressToPlans);
      List<PageAddress> pageAddresses = chunkPageAddress.convertToPageAddresses();

      for (PageAddress pageAddress : pageAddresses) {
        if (segmentUnitManager.get(pageAddress.getSegId()).getSegmentLogMetadata()
            .hasLogNotApply(pageAddress)) {
          add(pageAddress);
        }
      }
    }
  }

  private void processResultForOnePage(PageContext<Page> context, PageAddress logicalPageAddress,
      PlansToApplyLogs plans) {
    Validate.notNull(plans, "this is impossible that can not get plans for the page");

    SegmentUnit segmentUnit = segmentUnitManager.get(logicalPageAddress.getSegId());
    int pageIndex = PageAddressGenerator.calculatePageIndex(logicalPageAddress);

    boolean success = false;
    if (plans.getChunkLogsCollection() != null && !plans.getChunkLogsCollection().isAllSuccess()) {
      success = false;
    } else if (context.isSuccess()) {
      if (!logicalPageAddress.equals(segmentUnit.getLogicalPageAddressToApplyLog(pageIndex))) {
        context.setCause(new PageHasBeenMigratedJustNowException());
        success = false;
      } else {
        SecondaryCopyPageManager copyPageManager = segmentUnit.getSecondaryCopyPageManager();
        if (copyPageManager != null && !copyPageManager.isProcessed(pageIndex)
            && !plans.isMigrating()) {
          success = false;
          logger.warn(
              "the plan is not marked as migrating before checkout, but segment is migrating now, "
                  + "do nothing {} {}", logicalPageAddress, plans);
        } else {
          success = dealWithPlans(logicalPageAddress, context, plans);
        }
      }
    }

    if (!success) {
      try {
        failToCheckoutForPage(segmentUnit, context, plans);
      } catch (PageHasBeenMigratedJustNowException e) {
        logger.warn("page has been migrated just now, just skip it");
      } catch (ChecksumMismatchedException e) {
        logger.error(
            "Caught a checksum mismatched exception. " 
                + "PageAddress: {} submitting a request to correct the page",
            logicalPageAddress, e);

        if (logicalPageAddress.getSegId() != null) {
          SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
          PageErrorCorrector corrector = service
              .buildPageErrorCorrector(logicalPageAddress, membership, segmentUnit);

          corrector.correctPage();
        }
      } catch (Exception e) {
        logger.error("caught an exception when checking out some page, context: {}", context, e);
      }
    }
  }

  private boolean dealWithPlans(PageAddress logicalPageAddress, PageContext<Page> context,
      PlansToApplyLogs plans) {
    WhatToSave whatToSave = plans.getWhatToSave();
    PageContext<Page> contextToApplyLogs = null;
    switch (whatToSave) {
      case Nothing:
        contextToApplyLogs = context;
        break;
      default:
        throw new IllegalArgumentException();
    }
    SegmentUnit segmentUnit = segmentUnitManager.get(logicalPageAddress.getSegId());
    List<MutationLogEntry> justAppliedLogs = new ArrayList<>();
    if (applyLogsToPage(logicalPageAddress, contextToApplyLogs, plans, justAppliedLogs)) {
      for (MutationLogEntry appliedLog : justAppliedLogs) {
        appliedLog.apply();
        if (cfg.isEnableIoTracing()) {
          logger.warn("log applied uuid={} id={}", appliedLog.getUuid(), appliedLog.getLogId());
        }
        segmentUnit.removeFirstWritePageIndex(
            PageAddressGenerator.calculatePageIndex(logicalPageAddress));
      }
      return true;
    } else {
      return false;
    }
  }

  private boolean applyLogsToPage(PageAddress logicalPageAddress,
      PageContext<Page> contextToApplyLogs,
      PlansToApplyLogs plansToApplyLogs, List<MutationLogEntry> justAppliedLogs) {
    PageAddress physicalPageAddress = contextToApplyLogs.getPageAddressForIo();
    SegmentUnit segmentUnit = segmentUnitManager.get(logicalPageAddress.getSegId());

    int numLogs = plansToApplyLogs.getPlans().size();
    Page page = contextToApplyLogs.getPage();

    logger.info("there are {} logs in the same page {}", justAppliedLogs, physicalPageAddress);
    long maxLogId = -1;
    int pageSize = service.getCfg().getPageSize();
    for (PlansToApplyLogs.PlanToApplyLog plan : plansToApplyLogs.getPlans()) {
      MutationLogEntry logToApply = plan.getLog();

      Validate.notNull(logToApply);
      maxLogId = maxLogId < logToApply.getLogId() ? logToApply.getLogId() : maxLogId;

      RangeSet<Integer> rangeSet = plan.getRangesToApply();
      if (rangeSet == null || rangeSet.isEmpty()) {
        logger.debug("range of the plan {} for log is empty", plan);
        if (!logToApply.isApplied()) {
          logToApply.apply();
        }

        logToApply.setPersisted();
        MutationLogEntryFactory.releaseLogData(logToApply);
      }

      if (!logToApply.isApplied()) {
        try {
          if (cfg.isEnableIoTracing()) {
            logger.warn("applying log {}", logToApply);
          } else {
            logger.debug("applying log {}", logToApply.getLogId());
          }
          logToApply.applyLogData(page, pageSize, rangeSet);
          justAppliedLogs.add(logToApply);
        } catch (Exception e) {
          logger.error("Fail to applying log {} to the page {} and the ranges are {}, plans={}",
              logToApply,
              physicalPageAddress, rangeSet, plansToApplyLogs, e);
          return false;
        }
      }
      logger.info("log has been applied. The plan is {} ", plan);
    }

    int pageIndex = (int) (logicalPageAddress.getOffsetInSegment() / service.getCfg()
        .getPhysicalPageSize());

    markPageWritten(segmentUnit, pageIndex);

    if (plansToApplyLogs.isMigrating() && segmentUnit.getSecondaryCopyPageManager() != null) {
      segmentUnit.getSecondaryCopyPageManager().pageWrittenByNewLogs(pageIndex);
      logger.info("set the page done logs={}, pageIndex={}", plansToApplyLogs, pageIndex);
    }

    boolean needToPersistLogs = !justAppliedLogs.isEmpty();
    if (!needToPersistLogs) {
      logger.debug("no need to persist logs");
      availableSlotsForPageSystemRequestsTillPersisted.release();
    } else {
      logger.debug("need to persist logs");
      Validate.isTrue(page.isDirty());
      PageListener listener = new PageListener() {
        @Override
        public void successToPersist() {
          logger.debug(
              "release availableSlotsForPageSystem. Its availablleSlotsForPageSystemRequests {}",
              availableSlotsForPageSystemRequestsTillPersisted.availablePermits());
          availableSlotsForPageSystemRequestsTillPersisted.release();

          if (plansToApplyLogs.getWhatToSave() == WhatToSave.ROW) {
            Validate.isTrue(plansToApplyLogs.getChunkLogsCollection() != null);
            for (MutationLogEntry log : justAppliedLogs) {
              plansToApplyLogs.getChunkLogsCollection().logPersisted(log);
            }
          } else {
            for (MutationLogEntry log : justAppliedLogs) {
              log.setPersisted();
              MutationLogEntryFactory.releaseLogData(log);
            }
          }

          chunkProcessDone(logicalPageAddress);
        }

        @Override
        public void failToPersist(Exception e) {
          logger.error("fail to persist the page={}, for logs={}", page,
              justAppliedLogs, e);
          availableSlotsForPageSystemRequestsTillPersisted.release();

          if (!segmentUnit.getArchive().getArchiveStatus().canDoIo()) {
            chunkProcessDone(logicalPageAddress);
            return;
          }

          for (MutationLogEntry log : justAppliedLogs) {
            log.resetNotApply();
          }

          chunkProcessDone(logicalPageAddress);
          service.getSegmentUnitCanDeletingCheck()
              .deleteSegmentUnitWithCheck(segmentUnit.getSegId());
        }
      };

      page.addPageListener(listener);
      logger.debug("plans {}, page {}", plansToApplyLogs, page);
      if (plansToApplyLogs.getWhatToDo() == WhatToDo.NoNeedToLoadPage) {
        page.setPageLoaded(true);
      } else {
        Validate.isTrue(page.isPageLoaded());
      }
    }

    return true;
  }

  private void failToCheckoutForPage(SegmentUnit segmentUnit, PageContext<Page> context,
      PlansToApplyLogs plans) throws Exception {
    logger.warn("fail to check out the page, context: {}, plans: {}, available permit: {}", context,
        plans,
        availableSlotsForPageSystemRequestsTillPersisted.availablePermits());
    availableSlotsForPageSystemRequestsTillPersisted.release();
    if (!context.isSuccess()) {
      throw context.getCause();
    } else {
      throw new FailToDealWithPlansException();
    }
  }

  private void markPageWritten(SegmentUnit segmentUnit, int pageIndex) {
    segmentUnit.setPageHasBeenWritten(pageIndex);
  }

  @Override
  public void start() {
    super.start();

    processThread = new Thread(this::processResult, prefix + "-" + "PLAL Callback");
    processThread.start();
    logger.info("started a PLALWorker {}", prefix);

    if (cfg.isEnablePerformanceReport()) {
      ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1,
          new NamedThreadFactory(prefix + "PLAL-metric-reporter"));
      scheduler.scheduleAtFixedRate(this::printMetrics, 200, 200, TimeUnit.MILLISECONDS);
    }
  }

  private void printMetrics() {
    int logsFromPlalToPcl = 0;
    int logsAfterPcl = 0;
    Collection<SegmentUnit> segmentUnits = segmentUnitManager
        .get(s -> s.getArchive().getStorage().equals(storage));
    for (SegmentUnit segmentUnit : segmentUnits) {
      SegmentLogMetadata logMetadata = segmentUnit.getSegmentLogMetadata();
      if (logMetadata != null) {
        logsFromPlalToPcl += logMetadata.getNumLogsFromPlalToPcl();
        logsAfterPcl += logMetadata.getNumLogsAfterPcl();
      }
    }
    logger.warn("{} available slots for page system requests till checked out {}", prefix,
        availableSlotsForPageSystemRequestsTillCheckedOut.availablePermits());
    logger.warn("{} available slots for page system requests till persisted {}", prefix,
        availableSlotsForPageSystemRequestsTillPersisted.availablePermits());
    logger.warn("{} logs plal to pcl and after pcl : {}, {}", prefix, logsFromPlalToPcl,
        logsAfterPcl);
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      logger.warn("this PLAL has been stopped already", new Exception());
      return;
    }
    logger.info("stopping PLALWorker {}", prefix);

    super.submit(new ExitRequestForScheduler());
    super.join();

    while (waiter.getPendingTaskCount() != 0) {
      logger.warn("{} page waiter not clear: {}", prefix, waiter);
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        logger.error("interrupted", e);
      }
    }

    waiter.completed(new ExitRequestForProcessor());
    logger.info("added a bogus page context to the waiter {}", waiter);
    try {
      processThread.join();
      logger.info("successfully to wait the process thread to die");
    } catch (InterruptedException e) {
      logger.error("interrupted", e);
    }
  }

  private class ExitRequestForProcessor extends BogusPageContext<Page> {
    ExitRequestForProcessor() {
      super();
    }
  }

  private class ExitRequestForScheduler extends PageAddressImpl {
    ExitRequestForScheduler() {
      super(0L, 0, Long.MAX_VALUE, 0, storage);
    }
  }
}

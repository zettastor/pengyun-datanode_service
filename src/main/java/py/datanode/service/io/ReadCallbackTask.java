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

package py.datanode.service.io;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegmentUnitStatus;
import py.consumer.ConsumerService;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.FailToCorrectPageException;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageContextCallback;
import py.datanode.page.TaskType;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.impl.BogusPageAddress;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs;
import py.datanode.service.DataNodeServiceImpl;
import py.datanode.service.PageErrorCorrector;
import py.datanode.service.io.ReadScheduleTask.Builder;
import py.exception.ChecksumMismatchedException;
import py.exception.StorageException;
import py.membership.SegmentMembership;
import py.netty.datanode.NettyExceptionHelper;
import py.netty.datanode.PyReadResponse;
import py.netty.exception.ServerProcessException;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.ReadCause;

public class ReadCallbackTask extends IoTask {
  private static final Logger logger = LoggerFactory.getLogger(ReadCallbackTask.class);

  private final Builder builder;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicInteger counter;
  private final List<Integer> unitsIndex;

  private final ReadCause readCause;
  private final DataNodeServiceImpl service;
  private final SegmentMembership membership;
  private final DataNodeConfiguration cfg;

  private boolean hasPhysicalPageAddress = false;
  private PageAddress logicalPageAddress;
  private PageContext<Page> pageContext;
  private ConsumerService<IoTask> readEngine;

  public ReadCallbackTask(Builder builder, AtomicInteger counter, PageAddress logicalPageAddress,
      boolean hasPhysicalPageAddress, List<Integer> unitsIndex,
       ReadCause readCause,
      DataNodeServiceImpl service, SegmentMembership membership,
      ConsumerService<IoTask> readEngine,
      DataNodeConfiguration cfg) {
    super(builder.segmentUnit);
    this.builder = builder;
    this.counter = counter;
    this.logicalPageAddress = logicalPageAddress;
    this.unitsIndex = unitsIndex;
    this.readCause = readCause;
    this.service = service;
    this.membership = membership;
    this.readEngine = readEngine;
    this.cfg = cfg;
    this.hasPhysicalPageAddress = hasPhysicalPageAddress;
  }

  public void pageLoaded(PageContext<Page> pageContext) {
    this.pageContext = pageContext;
  }

  public PageContext<Page> getPageContext() {
    return pageContext;
  }

  @Override
  public void run() {
    if (!started.compareAndSet(false, true)) {
      if (pageContext != null && pageContext.isSuccess()) {
        pageContext.updateSegId(segmentUnit.getSegId());
        builder.pageManager.checkin(pageContext);
      } else {
        logger.warn("anything wrong ? {}, {}", pageContext, builder.segmentUnit);
      }
      return;
    }

    try {
      internalRun();
    } catch (Throwable t) {
      logger.error("caught an throwable {}", this, t);
    }
  }

  public void runOnTimeout() {
    if (!started.compareAndSet(false, true)) {
      return;
    }

    try {
      logger.warn("page not loaded, storage read must be timed out, read from secondaries ! {}",
          logicalPageAddress);
      mergeReadFromSecondaries();
    } catch (Throwable t) {
      logger.error("caught an throwable {}", this, t);
      builder.callback.fail(NettyExceptionHelper.buildServerProcessException(t));
    }
  }

  private void warningCost(long timeNanos, WarningType type) {
    long timeMillis = TimeUnit.NANOSECONDS.toMillis(timeNanos);
    if (cfg.isEnableIoCostWarning() && timeMillis > cfg.getIoTaskThresholdMs()) {
      logger.warn("a read request for {} {} takes too long {}", segmentUnit.getSegId(), type,
          timeMillis);
    }
  }

  private void internalRun() {
    if (cfg.isEnableIoTracing()) {
      logger.warn("get from page system for read, page context:{}, requestId: {}",
          pageContext, builder.getRequestId());
    } else {
      logger.debug("get from page system for read, page context:{}, requestId: {}",
          pageContext, builder.getRequestId());
    }

    SegmentUnit segmentUnit = builder.segmentUnit;

    if (pageContext != null && !pageContext.isSuccess()) {
      if (pageContext.getCause() instanceof ChecksumMismatchedException) {
        logger.error(
            "Caught a checksum mismatched exception. PageAddress: {} submitting a request to "
                + "correct the page and wait for it", pageContext);
        if (readCause != ReadCause.FETCH || !cfg.isEnableMergeReadOnTimeout()) {
          logger.warn("Don't try to fix the checksum issue for page: {}", pageContext);
          builder.processExceptionStatus(unitsIndex, PbIoUnitResult.CHECKSUM_MISMATCHED);
        } else {
          logger.warn("correct the page: {} for mismatch exception", pageContext);
          PageErrorCorrector corrector = service
              .buildPageErrorCorrector(pageContext.getPageAddressForIo(), membership, segmentUnit);
          ReadCallbackTask newTask = new ReadCallbackTask(builder, counter, logicalPageAddress,
              hasPhysicalPageAddress, unitsIndex,  readCause, service, membership, readEngine,
              cfg);
          newTask.pageLoaded(pageContext);
          corrector.setReadTask(newTask);
          try {
            corrector.correctPage();

            return;
          } catch (Exception e) {
            logger.warn("can not correct the page={}", pageContext, e);

            builder.processExceptionStatus(unitsIndex, PbIoUnitResult.CHECKSUM_MISMATCHED);
          }
        }
      } else if (!cfg.isEnableMergeReadOnTimeout()) {
        logger.error("something wrong {}", pageContext);
        builder.processExceptionStatus(unitsIndex, PbIoUnitResult.SKIP);
      } else if (pageContext.getCause() instanceof StorageException) {
        PageErrorCorrector corrector = service
            .buildPageErrorCorrector(pageContext.getPageAddressForIo(), membership,
                segmentUnit);
        logger.error("build corrector success: {} for storage exception", corrector);
        ReadCallbackTask newTask = new ReadCallbackTask(builder, counter, logicalPageAddress,
            hasPhysicalPageAddress, unitsIndex,  readCause, service, membership, readEngine,
            cfg);
        newTask.pageLoaded(pageContext);
        corrector.setReadTask(newTask);
        try {
          corrector.correctPage();

          return;
        } catch (Exception e) {
          logger.warn("can not correct the page={}", pageContext, e);

          builder.processExceptionStatus(unitsIndex, PbIoUnitResult.SKIP);
        }
      } else if (pageContext.getCause() instanceof FailToCorrectPageException) {
        logger.error("something wrong {}", pageContext);
        builder.processExceptionStatus(unitsIndex, PbIoUnitResult.CHECKSUM_MISMATCHED);
      } else {
        logger.error("something wrong {}", pageContext);
        builder.processExceptionStatus(unitsIndex, PbIoUnitResult.SKIP);
      }
    } else {
      try {
        processOnePageContext();

      } catch (Exception e) {
        logger.warn("caught an exception", e);
        builder.processExceptionStatus(unitsIndex, PbIoUnitResult.SKIP);
      } finally {
        if (pageContext != null) {
          pageContext.updateSegId(segmentUnit.getSegId());
          builder.pageManager.checkin(pageContext);
        }
      }
    }

    doneWithOnePage();

  }

  private void doneWithOnePage() {
    if (counter.decrementAndGet() != 0) {
      logger.debug("wait for some other pages");
      return;
    }

    PyReadResponse response = builder.build();
    logger.debug("read over, response: {}", response.getMetadata());
    builder.callback.complete(response);
  }

  private boolean thingsHaveBeenChangedJustNow(PageAddress addressAgain) {
    if (!hasPhysicalPageAddress) {
      if (addressAgain != null && !GarbagePageAddress
          .isGarbagePageAddress(addressAgain) && !BogusPageAddress.isAddressBogus(addressAgain)) {
        logger.warn("we got a valuable physical address this time logical : {}, physical : {}",
            logicalPageAddress, addressAgain);
        return true;
      } else {
        return false;
      }
    }

    if (!pageContext.getPageAddressForIo().equals(addressAgain)) {
      logger.warn("address has been changed from {} to {}", pageContext.getPageAddressForIo(),
          addressAgain);
      return true;
    }

    return false;
  }

  private void processOnePageContext() throws Exception {
    MigrationStatus migrationStatus = builder.segmentUnit.getSegmentUnitMetadata()
        .getMigrationStatus();
    SegmentUnitStatus statusBefore = builder.segmentUnit.getSegmentUnitMetadata().getStatus();
    if (migrationStatus.isMigratedStatus()) {
      logger.warn("it is migration status for segment unit={}, {}, {}",
          builder.segmentUnit.getSegId(),
          migrationStatus, statusBefore);
      builder.cleanPcl();
    }

    SegmentLogMetadata metadata = builder.segmentUnit.getSegmentLogMetadata();

    builder.addLogsToMerge(metadata, logicalPageAddress, unitsIndex);

    PlansToApplyLogs plans = null;
    plans = metadata
          .generatePlans(logicalPageAddress, builder.pageManager.getCfg());

    builder.copyDataFromPage(unitsIndex, logicalPageAddress, hasPhysicalPageAddress, plans,
        pageContext);

    PageAddress physicalAddressAgain = builder.segmentUnit.getAddressManager()
        .getPhysicalPageAddress(builder.getUnitOffset(unitsIndex.get(0)));
    if (thingsHaveBeenChangedJustNow(physicalAddressAgain)) {
      logger.warn("things have been changed just now !!");

      if (cfg.isSkippingReadDoubleCheck()) {
        logger.error(
            "things have been changed, but we are skipping "
                + "double check, a data corruption is expeted !! ");
      } else {
        cleanProgress();

        if (BogusPageAddress.isAddressBogus(physicalAddressAgain) || GarbagePageAddress
            .isGarbagePageAddress(physicalAddressAgain)) {
          logger.warn("physical address become bogus or garbage");
          ReadCallbackTask readTask = new ReadCallbackTask(builder, counter,
              logicalPageAddress, false, unitsIndex,
              readCause, service, membership, readEngine, cfg);
          readEngine.submit(readTask);
          return;
        }

        PageContextCallback<Page> contextCallback = new PageContextCallback<Page>() {
          @Override
          public void completed(PageContext<Page> pageContext) {
            ReadCallbackTask task = new ReadCallbackTask(builder, counter,
                logicalPageAddress, true,
                unitsIndex, readCause, service, membership, readEngine, cfg);
            task.pageLoaded(pageContext);
            readEngine.submit(task);
          }
        };

        PageContext<Page> pageContext = new PageContextFactory<>()
            .generateAsyncCheckoutContext(physicalAddressAgain,
                TaskType.CHECK_OUT_FOR_EXTERNAL_READ, contextCallback,
                cfg.getDefaultPageRequestTimeoutMs());
        builder.pageManager.checkout(pageContext);
        counter.incrementAndGet();
        return;
      }

    }

    SegmentUnitStatus statusAfter = builder.segmentUnit.getSegmentUnitMetadata().getStatus();
    migrationStatus = builder.segmentUnit.getSegmentUnitMetadata().getMigrationStatus();
    if (statusBefore != statusAfter) {
      logger.warn(
          "read request {} will fail! segment unit status={}," 
              + " {}, migration status={}, for segId={}",
          builder, statusBefore, statusAfter, migrationStatus, builder.segmentUnit.getSegId());
      cleanProgress();
      throw new ServerProcessException("read fail! due to migration");
    }

    if (migrationStatus.isMigratedStatus()) {
      logger.warn("segment unit {} is migrating now, clean pcl", segmentUnit.getSegId());
      builder.cleanPcl();
    }

  }

  private void mergeReadFromSecondaries() {
    Validate.isTrue(hasPhysicalPageAddress);
    service.buildPageErrorCorrector(logicalPageAddress, membership, segmentUnit)
        .mergeReadFromSecondaries().thenAccept(mergeReadResponse -> {
          if (mergeReadResponse == null) {
            builder.processExceptionStatus(unitsIndex, PbIoUnitResult.SKIP);
            doneWithOnePage();
          } else if (
              mergeReadResponse.getMetadata().getResponseUnits(0).getResult() 
                  == PbIoUnitResult.MERGE_OK
                  || mergeReadResponse.getMetadata().getResponseUnits(0).getResult()
                  == PbIoUnitResult.OK) {
            builder.copyDataFromSecondaries(unitsIndex, mergeReadResponse, logicalPageAddress);
            mergeReadResponse.release();
          } else {
            logger.warn("read failed {} for page {}", mergeReadResponse.getMetadata(),
                logicalPageAddress);
            mergeReadResponse.release();
            builder.processExceptionStatus(unitsIndex, PbIoUnitResult.SKIP);
          }
          doneWithOnePage();
        }).exceptionally(e -> {
          logger.warn("read failed {}", builder.segmentUnit, e);
          builder.processExceptionStatus(unitsIndex, PbIoUnitResult.SKIP);
          doneWithOnePage();
          return null;
        });
  }

  private void cleanProgress() {
    builder.cleanProgress(unitsIndex);
  }

  public PageAddress getLogicalPageAddress() {
    return logicalPageAddress;
  }

  private enum WarningType {
    Wait, Process, Overall
  }
}

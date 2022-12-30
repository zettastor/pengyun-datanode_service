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

package py.datanode.segment.copy.task;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.consumer.ConsumerService;
import py.datanode.exception.CopyPageAbortException;
import py.datanode.exception.SnapshotPageInGcException;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageManager;
import py.datanode.page.TaskType;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.context.SinglePageContextFactory;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPage;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.copy.bitmap.CopyPageBitmap;
import py.datanode.segment.membership.statemachine.processors.PageContentUtil;
import py.datanode.service.io.DelayedIoTask;
import py.netty.core.MethodCallback;
import py.netty.datanode.PyCopyPageRequest;
import py.proto.Broadcastlog.PbCopyPageRequest;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Broadcastlog.PbCopyPageStatus;
import py.proto.Broadcastlog.PbPageRequest;
import py.proto.Broadcastlog.PbPageResponse;

public class SecondaryCopyPageTask extends DelayedIoTask {
  private static final Logger logger = LoggerFactory.getLogger(SecondaryCopyPageTask.class);

  private final PageManager<Page> pageManager;
  private final SecondaryCopyPageManager secondaryCopyPageManager;
  private final MethodCallback callback;
  private final PbCopyPageResponse.Builder responseBuilder;
  private final int primaryTaskId;
  private final int unitIndex;
  private final List<PbPageResponse> pageResponses;
  private final PyCopyPageRequest request;
  private final PbCopyPageRequest metadata;
  private final CopyPageBitmap copyPageUnitBitmap;
  private final CopyPage[] copyPageUnit;
  private final Map<PageAddress, PbPageRequest> mapPageRequest;
  private final AtomicBoolean completed;
  private final ConsumerService<DelayedIoTask> consumerService;

  public SecondaryCopyPageTask(int delayMs, PyCopyPageRequest request, MethodCallback callback,
      PbCopyPageResponse.Builder responseBuilder, SegmentUnit segmentUnit,
      PageManager<Page> pageManager, ConsumerService<DelayedIoTask> consumerService) {
    super(delayMs, segmentUnit);
    this.request = request;
    this.metadata = request.getMetadata();
    this.secondaryCopyPageManager = segmentUnit.getSecondaryCopyPageManager();
    this.callback = callback;
    this.pageManager = pageManager;
    this.responseBuilder = responseBuilder;
    this.primaryTaskId = (int) metadata.getTaskId();
    this.unitIndex = metadata.getCopyPageUnitIndex();
    this.consumerService = consumerService;
    mapPageRequest = new ConcurrentHashMap<>();
    completed = new AtomicBoolean();
    pageResponses = new CopyOnWriteArrayList<>();
    copyPageUnitBitmap = secondaryCopyPageManager.getCopyUnitBitmap(primaryTaskId);
    copyPageUnit = secondaryCopyPageManager.getCopyUnit(primaryTaskId);

  }

  private void processOnePageContext(Page page, PbPageRequest pbPageRequest) {
    SegId segId = segmentUnit.getSegId();
    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    int bufferIndex = pbPageRequest.getBufferIndex();
    ByteBuffer data =
        bufferIndex == -1 ? null : request.getRequestUnitData(bufferIndex).nioBuffer();

    int pageIndex = pbPageRequest.getPageIndex();
    if (data != null && !PageContentUtil.isChecksumMatch(data, pbPageRequest.getChecksum())) {
      logger.error("checksum mismatch for segId: {}, index: {}", segId, pageIndex);
      abort();
      return;
    }

    int pageIndexInCopyPageUnit = pageIndex - copyPageUnitBitmap.offset();
    PbPageResponse.Builder pagebuilder = PbPageResponse.newBuilder();
    pagebuilder.setPageIndex(pageIndex);
    CopyPage copyPage = copyPageUnit[pageIndexInCopyPageUnit];
    CopyPage.PageNode pageNode = copyPage.content();

    boolean isOriginalPage = copyPage.isLastOriginPageNode();
    if (isOriginalPage) {
      logger.info("copy the origin data {} {}", copyPage);
      segmentUnit.setPageHasBeenWritten(pageIndex);
      segmentUnit.removeFirstWritePageIndex(pageIndex);
    }

    if (page != null) {
      page.write(0, data);
      page.setPageLoaded(true);
      logger.debug("write data to page system");
    }

    if (!copyPage.hasNext()) {
      copyPage.setDone();
      copyPageUnitBitmap.set(pageIndexInCopyPageUnit);
      logger.info("page has done {} {}", pageIndex, copyPage);
    } else {
      pageNode = copyPage.moveToNext();
      logger.info("page has not done {}, snapshot id to copy {}, page {}", pageIndex, copyPage);
    }

    logger.info("page {} snap current {} next {}", pageIndex);
    pageResponses.add(pagebuilder.build());
  }

  @Override
  public void run() {
    try {
      boolean abortCopy = false;
      if (secondaryCopyPageManager == null || secondaryCopyPageManager.getSessionId() != metadata
          .getSessionId()) {
        logger.error("null or session has changed, {}, {}", secondaryCopyPageManager,
            metadata.getSessionId());
        abortCopy = true;
      }

      int position = secondaryCopyPageManager.getCopyUnitPosition(primaryTaskId);
      if (unitIndex != position) {
        logger.error("unit index mismatch : P {} S {}", unitIndex, position);
        abortCopy = true;
      }

      if (abortCopy) {
        abort();
        return;
      }

      int delayMs = secondaryCopyPageManager.getIoThrottleManager().throttle(segmentUnit.getSegId(),
          metadata.getPageRequestsCount());
      if (delayMs > 0) {
        this.updateDelay(delayMs);
        consumerService.submit(this);
        return;
      }

      secondaryCopyPageManager.setLastPushTime(System.currentTimeMillis());

      if (unitIndex == -1) {
        logger.warn(
            "primary {} don't know the first copy-page-unit that the secondary wants to request, " 
                + "so the new copy-page-unit information should be returned.", primaryTaskId);
        complete();
        return;
      }

      if (request.getMetadata().getErrorCount() == -1) {
        logger.warn("primary tell me nothing to copy , just move next {}", copyPageUnitBitmap);
        copyPageUnitBitmap.set(0, copyPageUnitBitmap.size());
        complete();
        return;
      }
      loadPages();
    } catch (Throwable t) {
      logger.error("", t);
      abort();
    }
  }

  private void loadPages() {
    PageContextFactory<Page> pageContextFactory = SinglePageContextFactory.getInstance();
    List<PbPageRequest> pbPageRequests = metadata.getPageRequestsList();

    final AtomicInteger counter = new AtomicInteger(0);
    List<PageContext<Page>> contexts = new ArrayList<PageContext<Page>>();

    if (pbPageRequests.size() > 0) {
      try {
        secondaryCopyPageManager.allocatePageAddressAtTheFirstTime(pbPageRequests, primaryTaskId);
      } catch (CopyPageAbortException e) {
        logger.error("", e);
        abort();
        return;
      } catch (SnapshotPageInGcException e) {
        consumerService.submit(this);
        return;
      }
    }

    for (PbPageRequest pbPage : pbPageRequests) {
      logger
          .debug("request page index: {}, snapshot id: {}, first index: {}", pbPage.getPageIndex(),
               copyPageUnitBitmap.offset());

      int pageIndex = pbPage.getPageIndex();

      int pageIndexInUnit = pageIndex - copyPageUnitBitmap.offset();
      CopyPage copyPage = copyPageUnit[pageIndexInUnit];
      Validate.isTrue(copyPage.getPageIndex() == pageIndex);

      if (copyPage.isDone() || copyPageUnitBitmap.get(pageIndexInUnit)) {
        logger
            .error("why current CopyPage={} {} already done, primary resend? {}", copyPage, pbPage);
        abort();
        return;
      }

      logger.debug("current page node: {}, snapId : {}, shadow table: {}", copyPage);

      PageAddress pageAddress = copyPage.content().physicalPageAddress;
      if (GarbagePageAddress.isGarbagePageAddress(pageAddress)) {
        logger.info("this is a garbage address {}", pageAddress);
        processOnePageContext(null, pbPage);
        continue;
      }
      mapPageRequest.put(pageAddress, pbPage);
      PageContext<Page> pageContext = pageContextFactory
          .generateAsyncCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_INTERNAL_CORRECTION,
              context -> pageLoaded(context), 10000);
      contexts.add(pageContext);
    }

    if (contexts.size() == 0) {
      boolean isDone = copyPageUnitBitmap.isFull();
      logger.info(
          "bitmap {}, request={}, unit index={}, segId={}," 
              + " primary give me nothing due to wait plal",
          copyPageUnitBitmap, metadata.getRequestId(), metadata.getCopyPageUnitIndex(),
          segmentUnit.getSegId());
      complete();
    } else {
      counter.set(contexts.size());

      if (contexts.size() > 0) {
        pageManager.checkout(pageContextFactory.generatePageContextWrapper(contexts));
      }
    }
  }

  private void pageLoaded(PageContext<Page> pageContext) {
    try {
      PbPageRequest pbPageRequest = mapPageRequest.get(pageContext.getPageAddressForIo());
      if (pageContext.isSuccess()) {
        try {
          processOnePageContext(pageContext.getPage(), pbPageRequest);
        } catch (Exception e) {
          responseBuilder.setStatus(PbCopyPageStatus.COPY_PAGE_ABORT);
          logger.error("caught an exception", e);
        } finally {
          pageContext.updateSegId(segmentUnit.getSegId());
          pageManager.checkin(pageContext);
        }
      } else {
        logger.warn("can not load the page from page system: {} for copy page", pageContext);
      }

      mapPageRequest.remove(pageContext.getPageAddressForIo());
      if (mapPageRequest.size() != 0) {
        logger.debug("there are still some requests: {}", mapPageRequest);
        return;
      }

      complete();
    } catch (Throwable t) {
      logger.error("", t);
      abort();
    }
  }

  private void complete() {
    if (!completed.compareAndSet(false, true)) {
      return;
    }

    if (copyPageUnitBitmap == null || copyPageUnitBitmap.isFull()) {
      logger.info("current bitmap {} is done going to build next for worker {}", copyPageUnitBitmap,
          primaryTaskId);
      secondaryCopyPageManager.buildNextCopyPageUnit(responseBuilder, primaryTaskId);
    } else {
      logger.info("current bitmap {} is not done worker {}\n ", copyPageUnitBitmap, primaryTaskId);
      secondaryCopyPageManager.buildCurrentCopyPageUnit(responseBuilder, primaryTaskId);
    }

    logger.debug("secondary complete for {}", copyPageUnitBitmap);
    if (metadata.getPageRequestsCount() > 0) {
      secondaryCopyPageManager.getIoThrottleManager()
          .addAlready(segmentUnit.getSegId(), metadata.getPageRequestsCount());
    }
    if (secondaryCopyPageManager.isDone()) {
      responseBuilder.setStatus(PbCopyPageStatus.COPY_PAGE_DONE);
      secondaryCopyPageManager.getIoThrottleManager()
          .finish(segmentUnit.getArchive(), segmentUnit.getSegId());
    }

    pageResponses.forEach(pageResponse -> responseBuilder.addPageResponses(pageResponse));
    callback.complete(responseBuilder.build());
  }

  private void abort() {
    if (!completed.compareAndSet(false, true)) {
      return;
    }
    responseBuilder.setStatus(PbCopyPageStatus.COPY_PAGE_ABORT);
    secondaryCopyPageManager.removeTask(primaryTaskId);
    callback.complete(responseBuilder.build());
  }
}

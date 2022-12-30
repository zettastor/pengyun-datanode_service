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

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.consumer.ConsumerService;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.TaskType;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.context.SinglePageContextFactory;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPage;
import py.datanode.segment.copy.PrimaryCopyPageManager;
import py.datanode.segment.copy.bitmap.CopyPageBitmap;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.service.io.DelayedIoTask;
import py.netty.core.AbstractMethodCallback;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.PyCopyPageRequest;
import py.proto.Broadcastlog.PbCopyPageRequest;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Broadcastlog.PbCopyPageStatus;
import py.proto.Broadcastlog.PbPageRequest;
import py.proto.Broadcastlog.PbPageResponse;

public class PrimaryCopyPageTask extends DelayedIoTask {
  private static final Logger logger = LoggerFactory.getLogger(PrimaryCopyPageTask.class);
  private static final int DELAY_FOR_RETRY = 100;

  private final Map<PageAddress, CopyPage> mapPageAddressToPage;
  private int workerId;
  private TaskStatus taskStatus;
  private PrimaryCopyPageManager copyPageManager;
  private ConsumerService consumerService;
  private AtomicBoolean completed;
  private boolean pageAddressInited;
  private int unitPos;

  public PrimaryCopyPageTask(int workerId, SegmentUnit segmentUnit, ConsumerService consumerService,
      PrimaryCopyPageManager copyPageManager) {
    super(0, segmentUnit);
    this.taskStatus = TaskStatus.Init;
    this.consumerService = consumerService;
    this.copyPageManager = copyPageManager;
    this.workerId = workerId;
    this.mapPageAddressToPage = new ConcurrentHashMap<>();
    completed = new AtomicBoolean();

  }

  @Override
  public void run() {
    try {
      unitPos = copyPageManager.getCopyUnitPosition(workerId);
      if (unitPos == -1) {
        logger.warn(
            "copy page start {}, push an empty request to S, and expect him response me a bitmap");
        pushPages();
        return;
      }

      switch (taskStatus) {
        case Init:
        case LoadPages:
          loadPages();
          break;
        case PushPages:
          pushPages();
          break;
        case Done:
          break;
        case Cancel:
          break;
        default:
      }
    } catch (Throwable t) {
      logger.error("", t);
      cancel();
    }
  }

  private void loadPages() {
    completed.set(false);

    CopyPage[] copyPages = copyPageManager.getCopyUnit(workerId);

    PageContext<Page> pageContext;
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    long lalId = segmentLogMetadata.getLalId();

    List<PageContext<Page>> pageContexts = new ArrayList<>();
    PageContextFactory<Page> factory = SinglePageContextFactory.getInstance();
    int pageCount = 0;
    List<CopyPage> pagesNeedCopy = new ArrayList<>();
    for (CopyPage copyPage : copyPages) {
      if (copyPage == null || copyPage.isDone()) {
        continue;
      }

      if (lalId < copyPage.getMaxLogId()) {
        logger.warn("primary plal id={} less than the secondary last log id={}, {}", lalId,
            copyPage.getMaxLogId(), copyPage.getPageIndex());
        taskStatus = TaskStatus.PushPages;
        this.updateDelay(DELAY_FOR_RETRY);
        consumerService.submit(this);
        return;
      }
      pagesNeedCopy.add(copyPage);
    }

    if (!pageAddressInited) {
      try {
        copyPageManager.allocatePageAddressAtTheFirstTime(copyPages, workerId);
        pageAddressInited = true;
      } catch (Exception e) {
        logger.error("", e);
        cancel();
        return;
      }
    }

    for (CopyPage copyPage : pagesNeedCopy) {
      logger.info("copy page: {}", copyPage);

      CopyPage.PageNode pageNode = copyPage.content();
      if (copyPage.isDone() || pageNode.pageLoaded()) {
        continue;
      }

      CopyPage old = mapPageAddressToPage.put(pageNode.physicalPageAddress, copyPage);
      if (old != null) {
        logger.error(
            "current content {}, \n old copy page: {}, \n new copy page: {}, \n copy page task={}",
            pageNode, old, copyPage, this);
        Validate.isTrue(false);
      }

      pageContext = factory
          .generateAsyncCheckoutContext(pageNode.physicalPageAddress, TaskType.CHECK_OUT_FOR_READ,
              donePageContext -> pageLoaded(donePageContext), 10000);
      logger.debug("submit to page system for address={}, copy page={}",
          pageNode.physicalPageAddress, copyPage);
      pageContexts.add(pageContext);
      pageCount++;
    }

    if (pageCount == 0) {
      logger.info("no pages need load {}", this);
      pushPages();
    } else {
      if (pageCount > 0) {
        copyPageManager.getPageManager().checkout(factory.generatePageContextWrapper(pageContexts));
      }
    }
  }

  private void pageLoaded(PageContext<Page> pageContext) {
    PageAddress physicalPageAddress = pageContext.getPageAddressForIo();
    CopyPage copyPage = mapPageAddressToPage.get(physicalPageAddress);

    try {
      if (!pageContext.isSuccess()) {
        logger.error("can not load the page from page system, context={}, copy page context={}",
            pageContext,
            copyPageManager);
        copyPageManager.incErrCount();
      } else {
        logger.debug("load a page from page system, context={}", pageContext);
        Page page = pageContext.getPage();
        ByteBuffer buffer = copyPageManager.getFromBufferPool(workerId, copyPage.getPageIndex());
        page.getData(0, buffer);
        copyPage.content().buffer = buffer;
        pageContext.updateSegId(segmentUnit.getSegId());
      }
    } catch (Exception e) {
      logger
          .warn("fail to load page:{}, task={}, CopyPage={}", physicalPageAddress, this, copyPage,
              e);
      cancel();
    } finally {
      mapPageAddressToPage.remove(physicalPageAddress);
      copyPageManager.getPageManager().checkin(pageContext);
    }

    if (mapPageAddressToPage.size() == 0) {
      logger.info("all pages are checkout for task {}", this);
      taskStatus = TaskStatus.PushPages;
      if (completed.compareAndSet(false, true)) {
        consumerService.submit(this);
      }
    } else {
      logger.debug("there are still some pages to be loaded from page system, map={}",
          mapPageAddressToPage);
    }
  }

  private void pushPages() {
    PbCopyPageRequest.Builder requestBuilder = PbCopyPageRequest.newBuilder();
    requestBuilder.setRequestId(RequestIdBuilder.get());
    requestBuilder.setSessionId(copyPageManager.getSessionId());
    requestBuilder.setMembership(PbRequestResponseHelper
        .buildPbMembershipFrom(segmentUnit.getSegmentUnitMetadata().getMembership()));
    SegId segId = segmentUnit.getSegId();
    requestBuilder.setVolumeId(segId.getVolumeId().getId());

    requestBuilder.setSegIndex(segId.getIndex());
    requestBuilder.setCopyPageUnitIndex(unitPos);
    List<PbPageRequest> pageRequests = new ArrayList<PbPageRequest>();

    logger.debug("now start work {}", this);

    int currentCount = 0;
    SegmentUnitStatus status = segmentUnit.getSegmentUnitMetadata().getStatus();
    PbCopyPageStatus pbCopyPageStatus = PbCopyPageStatus.COPY_PAGE_PROCESSING;
    CopyPageCallback callback = new CopyPageCallback();
    CompositeByteBuf compositeByteBuf = new CompositeByteBuf(callback.getAllocator(), true,
        copyPageManager.getCopyUnitSize());
    if (status == SegmentUnitStatus.Primary || status == SegmentUnitStatus.PrePrimary) {
      PbPageRequest.Builder builder = PbPageRequest.newBuilder();
      CopyPage[] copyPageToSend = copyPageManager.getCopyUnit(workerId);
      logger.debug("copy pages: {}", copyPageToSend);
      for (CopyPage copyPage : copyPageToSend) {
        if (copyPage == null || copyPage.isDone() || copyPage.content() == null
            || !copyPage.content().pageLoaded()) {
          continue;
        }
        builder.clear();
        CopyPage.PageNode pageNode = copyPage.content();
        logger.info("page index={} to push to secondary, cursor={} ", copyPage.getPageIndex(),
            pageNode);

        if (GarbagePageAddress.isGarbagePageAddress(pageNode.physicalPageAddress)) {
          logger.info("it is a garbage pageAddress {}", pageNode);
          builder.setChecksum(0);

          builder.setBufferIndex(-1);
        } else {
          ByteBuffer buffer = pageNode.buffer;
          builder.setChecksum(0);
          builder.setBufferIndex(compositeByteBuf.numComponents());
          compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(pageNode.buffer));
        }
        builder.setPageIndex(copyPage.getPageIndex());
        builder.setLastLogId(copyPage.getMaxLogId());
        pageRequests.add(builder.build());
        currentCount++;
      }

    } else {
      logger
          .warn("primary going to abort the segment ={} for session={}, due to my status {}", segId,
              copyPageManager.getSessionId(), status);
      pbCopyPageStatus = PbCopyPageStatus.COPY_PAGE_ABORT;
    }

    logger.info(
        "CopyPageRequest, sessionId:{}, requestId: {}, segId: {}, copy page unit: {}, " 
            + "size: {}, S={}",
        copyPageManager.getSessionId(), requestBuilder.getRequestId(), segId, unitPos, currentCount,
        copyPageManager.getPeer());

    requestBuilder.setStatus(pbCopyPageStatus);
    requestBuilder.addAllPageRequests(pageRequests);
    requestBuilder.setTaskId(workerId);
    requestBuilder.setErrorCount(0);
    CopyPageBitmap bitmap = copyPageManager.getCopyUnitBitmap(workerId);
    if (currentCount == 0 && bitmap != null && bitmap.isFull()) {
      logger.warn("bitmap is done, just tell S nothing to copy {}", bitmap);

      requestBuilder.setErrorCount(-1);
    }
    PbCopyPageRequest request = requestBuilder.build();
    AsyncDataNode.AsyncIface client = copyPageManager.getSecondaryClient();
    client.copy(new PyCopyPageRequest(request, compositeByteBuf), callback);
    logger.info("done copy request");
  }

  private void retryWhenError() {
    copyPageManager.incErrCount();
    if (copyPageManager.reachMaxErrCount()) {
      cancel();
    } else {
      updateDelay(DELAY_FOR_RETRY);
      consumerService.submit(PrimaryCopyPageTask.this);
    }
  }

  private void processResponse(PbCopyPageResponse pbCopyPageResponse) {
    int copyPageUnitIndex = pbCopyPageResponse.getCopyPageUnitIndex();
    if (copyPageUnitIndex == -1) {
      logger.warn("secondary response me at the first time {}", segmentUnit.getSegId());
    }
    if (unitPos != copyPageUnitIndex) {
      logger.error("primary unit index={}, secondary unit index={} for segId={}",
          unitPos, copyPageUnitIndex, segmentUnit.getSegId());
      cancel();
      return;
    }

    CopyPage[] copyPages = copyPageManager.getCopyUnit(workerId);
    CopyPageBitmap copyPageUnitBitmap = copyPageManager.getCopyUnitBitmap(workerId);
    try {
      for (PbPageResponse pageResponse : pbCopyPageResponse.getPageResponsesList()) {
        int index = pageResponse.getPageIndex() - copyPageUnitBitmap.offset();
        CopyPage copyPage = copyPages[index];
        logger.info("secondary response {} copy page: {}", pageResponse, copyPage);

        if (copyPage.hasNext()) {
          failToDealResponse(copyPage, pageResponse);
        }
        copyPage.setDone();
        logger.info("set done for pageIndex={}", copyPage.getPageIndex());
        copyPageUnitBitmap.set(copyPage.getPageIndex() - copyPageUnitBitmap.offset());
      }

      if (pbCopyPageResponse.hasNextCopyPageUnitIndex()) {
        int nextUnitPos = pbCopyPageResponse.getNextCopyPageUnitIndex();
        if (nextUnitPos == copyPageManager.getCopyUnitPosition(workerId)) {
          logger.error("resend unit pos {} current bitmap is not done {} ", unitPos,
              copyPageUnitBitmap.isFull());
          cancel();
          return;
        } else {
          copyPageManager
              .updateNextCopyPageUnit(workerId, pbCopyPageResponse.getNextBitmap().toByteArray(),
                  nextUnitPos);
          pageAddressInited = false;
        }
      }

      pbCopyPageResponse.getNexLogUnitsList().forEach(logUnit ->
          copyPageManager.getCopyUnit(workerId)[logUnit.getPageIndexInUnit()]
              .setMaxLogId(logUnit.getLastLogId()));
    } catch (Exception e) {
      logger
          .error("can't not deal with page: {}, unit: {}", pbCopyPageResponse, copyPageManager, e);
      cancel();
      return;
    }
  }

  private void failToDealResponse(CopyPage copyPage, PbPageResponse response) {
    logger.error("err, copy page {} response {}", copyPage, response);
    throw new RuntimeException();
  }

  public boolean isCancel() {
    return taskStatus == TaskStatus.Cancel;
  }

  public void cancel() {
    taskStatus = TaskStatus.Cancel;
    copyPageManager.removeTask(workerId);
  }

  public boolean isDone() {
    return taskStatus == TaskStatus.Done;
  }

  public void done() {
    taskStatus = TaskStatus.Done;
    copyPageManager.removeTask(workerId);
    if (copyPageManager.workerCount() == 0) {
      logger.warn("done copy page {}", segmentUnit.getSegId());
      segmentUnit.setPrimaryCopyPageManager(null);
      copyPageManager.getIoThrottleManager()
          .finish(segmentUnit.getArchive(), segmentUnit.getSegId());
    }
  }

  @Override
  public String toString() {
    return "CopyPageTask{copyPageManger=" + copyPageManager + ", workerId = " + workerId
        + ", status =" + taskStatus
        + '}';
  }

  enum TaskStatus {
    Init,
    LoadPages,
    PushPages,
    Done,
    Cancel
  }

  private class CopyPageCallback extends AbstractMethodCallback<PbCopyPageResponse> {
    @Override
    public void complete(PbCopyPageResponse pbCopyPageResponse) {
      logger.info("receive copy page response: {}", pbCopyPageResponse);
      PbCopyPageStatus status = pbCopyPageResponse.getStatus();
      if (status == PbCopyPageStatus.COPY_PAGE_PROCESSING) {
        processResponse(pbCopyPageResponse);
        taskStatus = TaskStatus.LoadPages;
        consumerService.submit(PrimaryCopyPageTask.this);
      } else if (status == PbCopyPageStatus.COPY_PAGE_DONE) {
        processResponse(pbCopyPageResponse);
        done();
      } else if (status == PbCopyPageStatus.COPY_PAGE_ABORT) {
        cancel();
      }
    }

    @Override
    public void fail(Exception e) {
      logger.error("", e);
      retryWhenError();
    }
  }
}

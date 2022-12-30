/*
 * Copyright (c) 2022-2022. PengYunNetWork
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

package py.datanode.segment.copy.unused;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPage;
import py.datanode.segment.copy.bitmap.CopyPageBitmap;
import py.datanode.segment.copy.bitmap.CopyPageBitmapImpl;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.engine.AbstractTask;
import py.engine.Result;
import py.engine.TaskListener;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.AbstractMethodCallback;
import py.netty.datanode.AsyncDataNode;
import py.proto.Broadcastlog.PbCopyPageRequest;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Broadcastlog.PbCopyPageStatus;
import py.proto.Broadcastlog.PbLogUnit;
import py.proto.Broadcastlog.PbPageRequest;
import py.proto.Broadcastlog.PbPageResponse;

public class CopyPageTask extends AbstractTask {
  private static final Logger logger = LoggerFactory.getLogger(CopyPageTask.class);

  private final GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory;
  private final CopyPageContext copyPageContext;
  private final Map<PageAddress, CopyPage> mapPageAddressToPage;
  private final PageManager<Page> pageManager;
  private List<CopyPage> copyPageToSend;
  private IoThrottleManager ioThrottleManager;
  private long taskId;

  public CopyPageTask(TaskListener listener, CopyPageContext copyPageContext,
      GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory,
      PageManager<Page> pageManager,
      IoThrottleManager ioThrottleManager, long taskId) {
    super(listener);
    this.copyPageContext = copyPageContext;
    this.clientFactory = clientFactory;
    this.mapPageAddressToPage = new ConcurrentHashMap<>();
    this.pageManager = pageManager;
    this.copyPageToSend = new ArrayList<>();
    this.ioThrottleManager = ioThrottleManager;
    this.taskId = taskId;
  }

  public List<CopyPage> getCopyPageToSend() {
    return copyPageToSend;
  }

  public void setCopyPageToSend(List<CopyPage> copyPageToSend) {
    this.copyPageToSend = copyPageToSend;
  }

  public SegId getSegId() {
    return copyPageContext.getSegmentUnit().getSegId();
  }

  public PageManager<Page> getPageManager() {
    return pageManager;
  }

  public Map<PageAddress, CopyPage> getMapPageAddressToPage() {
    return mapPageAddressToPage;
  }

  public long getSessionId() {
    return copyPageContext.getSessionId();
  }

  public Result work() {
    throw new NotImplementedException("it is a copy page task");
  }

  @Override
  public void doWork() {
    SegmentUnit segmentUnit = copyPageContext.getSegmentUnit();
    SegId segId = segmentUnit.getSegId();

    try {
      AsyncDataNode.AsyncIface client = clientFactory
          .generate(copyPageContext.getSecondaryEndPoint());
      PbCopyPageRequest.Builder requestBuilder = PbCopyPageRequest.newBuilder();
      requestBuilder.setRequestId(RequestIdBuilder.get());
      requestBuilder.setSessionId(copyPageContext.getSessionId());
      requestBuilder.setMembership(PbRequestResponseHelper
          .buildPbMembershipFrom(segmentUnit.getSegmentUnitMetadata().getMembership()));
      requestBuilder.setVolumeId(segId.getVolumeId().getId());

      requestBuilder.setSegIndex(segId.getIndex());
      requestBuilder.setCopyPageUnitIndex(copyPageContext.getCurrentCopyPageUnitIndex());
      requestBuilder.setErrorCount(copyPageContext.getErrorCount());
      List<PbPageRequest> pageRequests = new ArrayList<PbPageRequest>();

      logger.debug("now start work {}", this);

      int currentCount = 0;
      SegmentUnitStatus status = segmentUnit.getSegmentUnitMetadata().getStatus();
      PbCopyPageStatus pbCopyPageStatus = PbCopyPageStatus.COPY_PAGE_PROCESSING;
      if (status == SegmentUnitStatus.Primary || status == SegmentUnitStatus.PrePrimary) {
        for (CopyPage copyPageUnit : copyPageToSend) {
          logger.info("page index={} to push to secondary, cursor={}", copyPageUnit.getPageIndex(),
              copyPageUnit.content());
          pageRequests.add(buildPageRequestFrom(copyPageUnit));
          currentCount++;
        }

      } else {
        logger.warn("primary going to abort the segment ={} for session={}, due to my status {}",
            segId, getSessionId(), status);
        pbCopyPageStatus = PbCopyPageStatus.COPY_PAGE_ABORT;
      }

      logger.info(
          "PbCopyPageRequest, sessionId:{}, requestId: {}, segId: {}, copy page unit: {}, " 
              + "size: {}, status={}",
          copyPageContext.getSessionId(), requestBuilder.getRequestId(), segId,
          copyPageContext.getCurrentCopyPageUnitIndex(), currentCount, pbCopyPageStatus);

      requestBuilder.setStatus(pbCopyPageStatus);
      requestBuilder.addAllPageRequests(pageRequests);
      requestBuilder.setTaskId(taskId);

    } catch (Exception t) {
      logger.error("", t);
      getTaskListener().response(new CopyPageResult(this, t));
    } finally {
      copyPageToSend.clear();
    }
  }

  private boolean processOnePage(PbPageResponse pageResponse, CopyPageContext copyPageContext) {
    final CopyPageBitmap copyPageUnitBitmap = copyPageContext.getCopyPageUnitBitmap();
    CopyPage copyPage = copyPageContext.getCopyPage(pageResponse.getPageIndex());
    logger.debug("secondary response {} copy page: {}", pageResponse, copyPage);
    CopyPage.PageNode pageNode = copyPage.content();
    boolean isOriginalPage = true;

    Validate.isTrue(pageResponse.hasLastLogId());
    if (copyPage.getMaxLogId() >= pageResponse.getLastLogId()) {
      validateFalse(pageResponse, copyPage);
    } else {
      logger.warn("new logs have been applied to this copy page: {}, new log id: {}", copyPage,
          pageResponse.getLastLogId());
      logger.info("minus total for shadow pages pageIndex {}", copyPage.getPageIndex());
      copyPage.setMaxLogId(pageResponse.getLastLogId());
      return false;
    }

    logger.debug("move cursor for copy page {} cursor {}", copyPage, copyPage.content());
    copyPage.moveToNext();
    if (!copyPage.isDone()) {
      validateFalse(pageResponse, copyPage);
    }
    logger.debug("set for pageIndex={}", copyPage.getPageIndex());
    copyPageUnitBitmap.set(copyPage.getPageIndex() - copyPageUnitBitmap.offset());
    return true;
  }

  private void processResponse(PbCopyPageResponse pbCopyPageResponse) {
    SegmentUnit segmentUnit = copyPageContext.getSegmentUnit();
    CopyPageBitmap copyPageUnitBitmap = copyPageContext.getCopyPageUnitBitmap();
    int copyPageUnitIndex = pbCopyPageResponse.getCopyPageUnitIndex();
    logger.info("the bitmap of this copy page unit is {}", copyPageUnitBitmap);
    if (copyPageUnitBitmap == null) {
      Validate.isTrue(-1 == copyPageUnitIndex);
      logger.warn(
          "secondary response copyPageUnitBitmap is null means start, lets see next unit index");
    } else {
      if (copyPageContext.getCurrentCopyPageUnitIndex() != copyPageUnitIndex) {
        logger.warn("primary unit index={}, secondary unit index={} for segId={}",
            copyPageContext.getCurrentCopyPageUnitIndex(), copyPageUnitIndex,
            segmentUnit.getSegId());
        Validate.isTrue(false);
      }
    }

    for (PbPageResponse pageResponse : pbCopyPageResponse.getPageResponsesList()) {
      try {
        processOnePage(pageResponse, copyPageContext);
      } catch (Exception e) {
        logger.warn("can't not deal with page: {}, unit: {}", pageResponse.getPageIndex(),
            copyPageContext, e);
        copyPageContext.incrementErrorCount();
      }
    }

    if (pbCopyPageResponse.hasNextCopyPageUnitIndex()) {
      int nextCopyPageUnitIndex = pbCopyPageResponse.getNextCopyPageUnitIndex();

      if (copyPageUnitBitmap != null && !copyPageUnitBitmap.isFull()) {
        logger.warn("copy page context={}, next={}, for segId={}", copyPageUnitBitmap,
            nextCopyPageUnitIndex,
            segmentUnit.getSegId());
      }

      logger.info("secondary let me change to next copy page unit: {}, hasNextBitMap {}",
          nextCopyPageUnitIndex,
          pbCopyPageResponse.hasNextBitmap());
      CopyPageBitmap bitmap = new CopyPageBitmapImpl(
          pbCopyPageResponse.getNextBitmap().toByteArray(),
          copyPageContext.getCopyPageCountInUnit());

      try {
        copyPageContext.updateNextCopyPageUnit(bitmap, nextCopyPageUnitIndex);
      } catch (Exception e) {
        logger.error("", e);
        cancel();
        return;
      }

      if (pbCopyPageResponse.getNexLogUnitsCount() > 0) {
        for (PbLogUnit logUnit : pbCopyPageResponse.getNexLogUnitsList()) {
          int pageIndex = logUnit.getPageIndexInUnit() + bitmap.offset();
          CopyPage copyPage = copyPageContext.getCopyPage(pageIndex);
          copyPage.setMaxLogId(logUnit.getLastLogId());
        }
      }
    }
  }

  private void validateFalse(PbPageResponse pageResponse, CopyPage copyPageUnit) {
    Validate.isTrue(false, "pageUnit: ", copyPageUnit);
  }

  private PbPageRequest buildPageRequestFrom(CopyPage copyPage) {
    PbPageRequest.Builder builder = PbPageRequest.newBuilder();
    CopyPage.PageNode pageNode = copyPage.content();

    logger.info("going to set current snapshot id for pageNode {} pageIndex: {}", pageNode,
        copyPage.getPageIndex());
    builder.setChecksum(0);
    builder.setPageIndex(copyPage.getPageIndex());
    builder.setLastLogId(copyPage.getMaxLogId());
    return builder.build();
  }

  public CopyPageContext getCopyPageContext() {
    return copyPageContext;
  }

  public boolean isCancel() {
    return super.isCancel() || copyPageContext.reachMaxErrorCount();
  }

  @Override
  public String toString() {
    return "CopyPageTask{copyPageContext=" + copyPageContext + ", copyPageToSend=" + copyPageToSend
        + ", taskId = " + taskId
        + '}';
  }

  private class CopyPageCallback extends AbstractMethodCallback<PbCopyPageResponse> {
    private final CopyPageTask task;

    public CopyPageCallback(CopyPageTask task) {
      this.task = task;
    }

    @Override
    public void complete(PbCopyPageResponse pbCopyPageResponse) {
      CopyPageResult copyPageResult = new CopyPageResult(task, null);
      logger.info("receive copy page response: {}", pbCopyPageResponse);
      try {
        PbCopyPageStatus status = pbCopyPageResponse.getStatus();
        if (status == PbCopyPageStatus.COPY_PAGE_PROCESSING) {
          processResponse(pbCopyPageResponse);
        } else if (status == PbCopyPageStatus.COPY_PAGE_DONE) {
          processResponse(pbCopyPageResponse);
          copyPageResult.setDone(true);
        } else if (status == PbCopyPageStatus.COPY_PAGE_ABORT) {
          cancel();
        } else {
          logger.warn("get a bad response={} for segId={}", pbCopyPageResponse,
              copyPageContext.getSegmentUnit().getSegId());
          copyPageResult.setCause(new Exception("response status not right"));
        }
      } finally {
        getTaskListener().response(copyPageResult);
      }
    }

    @Override
    public void fail(Exception e) {
      logger.error("", e);
      getTaskListener().response(new CopyPageResult(task, e));
    }
  }
}

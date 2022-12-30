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

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.FailToCorrectPageException;
import py.datanode.exception.InvalidMembershipException;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageContextCallback;
import py.datanode.page.TaskType;
import py.datanode.page.context.SinglePageContextFactory;
import py.datanode.segment.SegmentUnit;
import py.datanode.service.io.MergeReadAlgorithmBuilder;
import py.datanode.service.io.ReadCallbackTask;
import py.datanode.service.io.read.merge.MergeReadAlgorithm;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbReadRequestUnit;
import py.proto.Broadcastlog.ReadCause;
import py.utils.ReadMergeHelper;

public class PageErrorCorrector {
  private static final Logger logger = LoggerFactory.getLogger(PageErrorCorrector.class);
  private static final int DEFAULT_TIMEOUT_GET_PAGE_MS = 3000;
  private final PageAddress pageAddress;
  private final SegmentMembership membership;
  private final DataNodeServiceImpl service;
  private final SegmentUnit segmentUnit;
  private ReadCallbackTask readTask;

  public PageErrorCorrector(DataNodeServiceImpl ioService, PageAddress pageAddress,
      SegmentMembership membership,  SegmentUnit segmentUnit) {
    this.pageAddress = pageAddress;
    Validate.isTrue(pageAddress != null);
    this.service = ioService;
    this.membership = membership;
    this.segmentUnit = segmentUnit;
  }

  public void correctPage() throws InvalidMembershipException {
   
    InstanceId myself = service.getContext().getInstanceId();

    PageContext<Page> pageContext = SinglePageContextFactory.getInstance()
        .generateAsyncCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_INTERNAL_CORRECTION,
            new CorrectionPageContext(), DEFAULT_TIMEOUT_GET_PAGE_MS);
    logger.info("going to checkout");
    service.getPageManager().checkout(pageContext);
  }

  public CompletableFuture<PyReadResponse> mergeReadFromSecondaries() {
    MergeReadAlgorithm mergeReadAlgorithm = MergeReadAlgorithmBuilder
        .build(service.getInstanceStore(), service.getContext().getInstanceId(),
            segmentUnit, membership,
            service.getCfg().getPageSize(), buildReadRequest(),
            service.getIoService().getAsyncClientFactory(), service.getByteBufAllocator());
    return mergeReadAlgorithm.process();
  }

  public PbReadRequest buildReadRequest() {
    Broadcastlog.PbReadRequest.Builder readRequestBuilder = PbReadRequest.newBuilder();
    readRequestBuilder.setRequestId(RequestIdBuilder.get());
    SegId segId = pageAddress.getSegId();
    readRequestBuilder.setVolumeId(segId.getVolumeId().getId());
    readRequestBuilder.setSegIndex(segId.getIndex());
    readRequestBuilder.setFailTimes(0);

    readRequestBuilder.setReadCause(ReadCause.CORRECTION);
    readRequestBuilder.addAllLogsToCommit(new ArrayList<Long>());
    List<PbReadRequestUnit> readRequestUnitList = new ArrayList<PbReadRequestUnit>();

    PbReadRequestUnit.Builder unitBuilder = PbReadRequestUnit.newBuilder();
   

    DataNodeConfiguration cfg = service.getCfg();
    long logicOffsetInVolume = pageAddress.getLogicOffsetInSegment(cfg.getPageSize());
    unitBuilder.setOffset(logicOffsetInVolume);
    unitBuilder.setLength(cfg.getPageSize());
    readRequestUnitList.add(unitBuilder.build());
    readRequestBuilder.addAllRequestUnits(readRequestUnitList);
    readRequestBuilder.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership));
    return readRequestBuilder.build();
  }

  public List<Instance> getTargetsToRead(SegmentMembership membership, InstanceId myself,
      SegId segId)
      throws InvalidMembershipException {
    if (membership == null) {
      throw new InvalidMembershipException();
    }

    if (!membership.contain(myself)) {
      throw new InvalidMembershipException(
          "my: " + myself + " no in membership " + membership + " segId=" + segId);
    }

    List<Instance> instances = new ArrayList<Instance>();
    boolean isPrimary = membership.isPrimary(myself);
    InstanceStore instanceStore = service.getInstanceStore();
    if (isPrimary) {
      Set<InstanceId> secondaries = membership.getSecondaries();
      for (InstanceId secondary : secondaries) {
        Instance instance = instanceStore.get(secondary);
        if (instance == null) {
          logger.warn("can not get instance of secondary id={}, segId={}", secondary, segId);
          continue;
        }
        instances.add(instanceStore.get(secondary));
      }

      if (instances.size() == 0 || instances.size() != secondaries.size()) {
        throw new InvalidMembershipException(
            "can not get at least one secondary, membership={}" + membership + ", segId=" + segId);
      }
      return instances;
    }

    if (membership.isSecondary(myself)) {
     
      Instance instance = instanceStore.get(membership.getPrimary());
      if (instance == null) {
        throw new InvalidMembershipException(
            "can not get instance of primary id=" + membership.getPrimary() + ", segId=" + segId);
      }
      instances.add(instanceStore.get(membership.getPrimary()));
      return instances;
    } else {
      throw new InvalidMembershipException(
          "my status is not correct in membership: " + membership + ", segId=" + segId);
    }
  }

  public ReadCallbackTask getReadTask() {
    return readTask;
  }

  public void setReadTask(ReadCallbackTask readTask) {
    this.readTask = readTask;
  }

  private class CorrectionPageContext implements PageContextCallback<Page> {
    @Override
    public void completed(PageContext<Page> pageContext) {
      logger.info("checkout for correction complete {}", pageContext);
      if (!pageContext.isSuccess()) {
        logger.warn("can not checkout the page={} for correction when reading", pageContext);
        if (readTask != null) {
          readTask.run();
        }
        return;
      }

      logger
          .warn("get the page for correction, page address={}", pageContext.getPageAddressForIo());

      mergeReadFromSecondaries()
          .thenAccept(pyReadResponse -> mergeReadDone(pyReadResponse, pageContext))
          .exceptionally(throwable -> {
            logger.error("merge read failed {}", pageContext, throwable);
            mergeReadDone(null, pageContext);
            return null;
          });
    }

    private void mergeReadDone(PyReadResponse mergeReadResponse, PageContext<Page> pageContext) {
      if (mergeReadResponse == null) {
        pageContext.updateSegId(pageAddress.getSegId());
        logger.warn("not success, check in pageContext: {}", pageContext);
        service.getPageManager().checkin(pageContext);

        logger.warn("now we return to the ReadCallBack do work with new exception");
        readTask.getPageContext().setPage(null);
        readTask.getPageContext().setCause(
            new FailToCorrectPageException(
                "can not get quorum response when correcting " + pageContext));
        if (readTask != null) {
          readTask.run();
        }
        return;
      }

      if (mergeReadResponse.getMetadata().getResponseUnits(0).getResult() == PbIoUnitResult.MERGE_OK
          || mergeReadResponse.getMetadata().getResponseUnits(0).getResult() == PbIoUnitResult.OK) {
        ByteBuf byteBuf = mergeReadResponse.getResponseUnitData(0);
        ByteBuffer[] byteBuffers = byteBuf.nioBuffers();
        int offset = 0;
        for (ByteBuffer byteBuffer : byteBuffers) {
          int remaining = byteBuffer.remaining();
          pageContext.getPage().write(offset, byteBuffer);
          offset += remaining;
        }
        pageContext.getPage().setPageLoaded(true);
        byteBuf.release();
        logger.warn("the page has been corrected, update the page context: {}", pageContext);
        readTask.pageLoaded(pageContext);
      } else {
       
       
       
        logger.warn("can not merge: {}", pageContext);
        pageContext.updateSegId(pageAddress.getSegId());
        service.getPageManager().checkin(pageContext);
        readTask.getPageContext().setPage(null);
        readTask.getPageContext().setCause(
            new FailToCorrectPageException(
                "can not merge quorum response when correcting " + pageContext));
      }

      if (readTask != null) {
        readTask.run();
      }
    }
  }

  private class MergeReadCallback {
    private PyReadResponse[] responses;

    public MergeReadCallback(PyReadResponse[] responses) {
      this.responses = responses;
    }

    private boolean isSuccess() {
      for (int i = 0; i < responses.length; i++) {
       
        if (responses[i] == null || responses[i].getMetadata().getPclId() < 0) {
          return false;
        }
      }

      return true;
    }

    public PyReadResponse processResult() {
     
      if (!isSuccess()) {
        logger.warn("there are some bad responses");
       
        for (PyReadResponse response : responses) {
          if (response != null) {
            response.release();
          }
        }
        return null;
      }

      logger.info("there are some enough good responses");
      int pageSize = service.getCfg().getPageSize();
      return ReadMergeHelper.merge(responses, pageSize);

    }
  }
}

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

package py.datanode.service.io;

import com.google.common.collect.Multimap;
import com.google.protobuf.ByteStringHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.consumer.ConsumerService;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.MultiChunkAddressHelper;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageContextCallback;
import py.datanode.page.PageManager;
import py.datanode.page.TaskType;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.impl.BogusPageAddress;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs;
import py.datanode.service.DataLogUtils;
import py.datanode.service.DataNodeServiceImpl;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.netty.core.MethodCallback;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbReadRequestUnit;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.proto.Broadcastlog.ReadCause;
import py.volume.VolumeType;

public class ReadScheduleTask extends IoTask {
  private static final Logger logger = LoggerFactory.getLogger(ReadScheduleTask.class);
  private static AtomicLong requestIdCount = new AtomicLong(0);

  private final DataNodeConfiguration cfg;
  private final Multimap<Long, ReadCallbackTask> readRequestsMap;
  private final MethodCallback<PyReadResponse> callback;
  private final SegmentUnit segUnit;
  private final PageManager<Page> pageManager;
  private final DataNodeServiceImpl service;
  private final ConsumerService<IoTask> readEngine;
  private final io.netty.util.Timer readTimeoutEngine;
  private final PageContextFactory<Page> pageContextFactory;
  private final PbReadRequest request;
  private final long uniqueReadRequestId;
  private Long pcl;

  ReadScheduleTask(
      DataNodeConfiguration cfg,
      Multimap<Long, ReadCallbackTask> readRequestsMap,
      MethodCallback<PyReadResponse> callback, SegmentUnit segUnit,
      PageManager<Page> pageManager, DataNodeServiceImpl service,
      ConsumerService<IoTask> readEngine,
      io.netty.util.Timer readTimeoutEngine,
      PageContextFactory<Page> pageContextFactory, PbReadRequest request,
      long uniqueReadRequestId) {
    super(segUnit);
    this.cfg = cfg;
    this.readRequestsMap = readRequestsMap;
    this.callback = callback;
    this.segUnit = segUnit;
    this.pageManager = pageManager;
    this.service = service;
    this.readEngine = readEngine;
    this.readTimeoutEngine = readTimeoutEngine;
    this.pageContextFactory = pageContextFactory;
    this.request = request;
    this.uniqueReadRequestId = uniqueReadRequestId;
  }

  private static boolean canMergeReadOnTimeout(PbReadRequest request, DataNodeConfiguration cfg,
      SegmentMembership membership, VolumeType volumeType, InstanceId myself) {
    if (request.getReadCause() != ReadCause.FETCH) {
      return false;
    }
    if (cfg.isEnableMergeReadOnTimeout()) {
      return false;
    }

    Set<InstanceId> secondaries = membership.getSecondaries();
    if (secondaries == null || secondaries.size() < volumeType.getNumSecondaries()) {
      return false;
    }

    if (membership.getTempPrimary() != null) {
      return false;
    }

    if (!membership.isPrimary(myself)) {
      return false;
    }

    return true;
  }

  public static long uniqueReadRequestId() {
    return requestIdCount.incrementAndGet();
  }

  @Override
  public void run() {
    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    if (cfg.isEnableIoTracing()) {
      logger.warn("read request:{}, segId:{}", request, segId);
    }
    logger.debug("read request:{}, segId:{}", request, segId);

    PbReadResponse.Builder responseBuilder = PbReadResponse.newBuilder();
    responseBuilder.setRequestId(request.getRequestId());

    SegmentMembership membership = segUnit.getSegmentUnitMetadata().getMembership();

    List<PageContext<Page>> pageContexts = new ArrayList<>();

    List<ReadCallbackTask> noPageTasks = new ArrayList<>();
    Map<Integer, List<Integer>> mapLogicalPageIndexToUnits = new HashMap<>();

    int numberOfRequestUnit = request.getRequestUnitsCount();
    Builder builder = new Builder(responseBuilder, numberOfRequestUnit);
    builder.withMethodCallback(callback).withPageManager(pageManager).withSegmentUnit(segUnit)
        .setSnapshotId(0).setAllocator(callback.getAllocator());

    SegmentUnitMetadata segmentUnitMetadata = segUnit.getSegmentUnitMetadata();
    final AtomicInteger counter = new AtomicInteger(0);
    List<PbReadRequestUnit> requestUnitsList = request.getRequestUnitsList();
    List<PageAddress> physicalAddressList = new ArrayList<>(numberOfRequestUnit);
    List<PageAddress> logicalAddressList = new ArrayList<>(numberOfRequestUnit);

    List<PageAddress> tmpPhysicalAddressList = new ArrayList<>(numberOfRequestUnit);
    List<PageAddress> tmpLogicalAddressList = new ArrayList<>(numberOfRequestUnit);

    PageAddress startPageAddress = null;
    final PageAddress nullAddressNoNeedRead = null;
    PageAddress placeHolderAddress = new BogusPageAddress();

    for (int i = 0; i < numberOfRequestUnit; i++) {
      PbReadRequestUnit readUnit = requestUnitsList.get(i);
      final long logicOffsetInSeg = readUnit.getOffset();
      final int dataLength = readUnit.getLength();
      builder.setResponseUnit(i, new ResponseUnit(readUnit));

      if (cfg.isEnableIoTracing()) {
        logger.warn("processing unit {}", readUnit);
      }
      logger.debug("unit is {}", readUnit);

      PbIoUnitResult result = IoValidator
          .validateReadInput(logicOffsetInSeg, dataLength, cfg.getPageSize(),
              cfg.getSegmentUnitSize());
      if (result != PbIoUnitResult.OK) {
        builder.setResponseUnitStatus(i, result);
        physicalAddressList.add(nullAddressNoNeedRead);
        logicalAddressList.add(nullAddressNoNeedRead);
        continue;
      }

      int logicalPageIndex = PageAddressGenerator
          .calculatePageIndex(logicOffsetInSeg, cfg.getPageSize());
      List<Integer> existing = mapLogicalPageIndexToUnits.get(logicalPageIndex);
      if (existing != null) {
        builder.setResponseUnitStatus(i, builder.getResponseUnitStatus(existing.get(0)));
        existing.add(i);
        logger.debug("page {} existing {}", logicalPageIndex, existing);
        physicalAddressList.add(nullAddressNoNeedRead);
        logicalAddressList.add(nullAddressNoNeedRead);
        continue;
      }
      if (segmentUnitMetadata.isPageFree(logicalPageIndex)
          && request.getReadCause() == ReadCause.FETCH) {
        if (cfg.isEnableIoTracing()) {
          logger.warn("Got a clean page while reading, just return 0s , logicalPageIndex {}",
              logicalPageIndex);
        } else {
          logger.info("Got a clean page while reading, just return 0s , logicalPageIndex {}",
              logicalPageIndex);
        }
        builder.setResponseUnitStatus(i, PbIoUnitResult.FREE);
        physicalAddressList.add(nullAddressNoNeedRead);
        logicalAddressList.add(nullAddressNoNeedRead);
        continue;
      }

      builder.setResponseUnitStatus(i, PbIoUnitResult.SKIP);
      final List<Integer> newIndex = new ArrayList<>();
      newIndex.add(i);
      mapLogicalPageIndexToUnits.put(logicalPageIndex, newIndex);

      PageAddress logicalAddress = PageAddressGenerator
          .generate(segId, segUnit.getStartLogicalOffset(), logicOffsetInSeg, segUnit.getStorage(),
              cfg.getPageSize());

      logicalAddressList.add(logicalAddress);
      tmpLogicalAddressList.add(logicalAddress);
      physicalAddressList.add(placeHolderAddress);
    }

    int multiCount = 0;
    long prevOffset = -1;
    int prevChunkIndex = -1;
    for (int i = 0; i < tmpLogicalAddressList.size() + 1; i++) {
      boolean isEndHint = i == tmpLogicalAddressList.size();
      PageAddress logicalAddress = isEndHint ? placeHolderAddress : tmpLogicalAddressList.get(i);
      long logicOffsetInSeg =
          isEndHint ? -1 : logicalAddress.getLogicOffsetInSegment(cfg.getPageSize());
      int chunkIndex = isEndHint ? -1 : MultiChunkAddressHelper.calculateChunkIndex(logicalAddress);

      if (multiCount == 0) {
        startPageAddress = logicalAddress;
        prevOffset = logicOffsetInSeg;
        prevChunkIndex = chunkIndex;
        multiCount = 1;
        continue;
      }
      if (prevChunkIndex == chunkIndex
          && PageAddressGenerator.isConnected(prevOffset, logicOffsetInSeg)) {
        prevOffset = logicOffsetInSeg;
        multiCount++;
        continue;
      }
      MultiPageAddress multiPageAddress = segUnit.getAddressManager()
          .getPhysicalPageAddress(new MultiPageAddress(startPageAddress, multiCount));
      tmpPhysicalAddressList.addAll(multiPageAddress.convertToPageAddresses());
      if (!isEndHint) {
        multiCount = 1;
        startPageAddress = logicalAddress;
        prevOffset = logicOffsetInSeg;
        prevChunkIndex = chunkIndex;
      }
    }

    if (tmpPhysicalAddressList.size() != tmpLogicalAddressList.size()) {
      logger.error("error logical {} \n physical {} {} {}", tmpLogicalAddressList,
          tmpPhysicalAddressList,
          numberOfRequestUnit, requestUnitsList);
    }

    if (physicalAddressList.size() > tmpPhysicalAddressList.size()) {
      List<PageAddress> mergedList = new ArrayList<>(numberOfRequestUnit);
      while (physicalAddressList.size() > 0) {
        PageAddress address = physicalAddressList.remove(0);
        if (address != nullAddressNoNeedRead) {
          mergedList.add(tmpPhysicalAddressList.remove(0));
        } else {
          mergedList.add(nullAddressNoNeedRead);
        }
      }
      physicalAddressList = mergedList;
    } else if (physicalAddressList.size() == tmpLogicalAddressList.size()) {
      physicalAddressList = tmpPhysicalAddressList;
    }

    for (int i = 0; i < numberOfRequestUnit; i++) {
      PbReadRequestUnit readUnit = requestUnitsList.get(i);
      final long logicOffsetInSeg = readUnit.getOffset();
      int dataLength = readUnit.getLength();
      PageAddress physicalPageAddress = physicalAddressList.get(i);
      PageAddress logicalPageAddress = logicalAddressList.get(i);

      if (physicalPageAddress == nullAddressNoNeedRead) {
        continue;
      }

      logger
          .debug("begin logical offset in segment {}, {}", logicalPageAddress, physicalPageAddress);

      if (GarbagePageAddress.isGarbagePageAddress(physicalPageAddress)) {
        logger.debug("Got a clean page while reading garbage address, just return 0s {}",
            physicalPageAddress);
      }

      int logicalPageIndex = PageAddressGenerator
          .calculatePageIndex(logicOffsetInSeg, cfg.getPageSize());

      List<Integer> newIndex = mapLogicalPageIndexToUnits.get(logicalPageIndex);

      if (BogusPageAddress.isAddressBogus(physicalPageAddress) || GarbagePageAddress
          .isGarbagePageAddress(physicalPageAddress)) {
        ReadCallbackTask readTask = new ReadCallbackTask(builder, counter, logicalPageAddress,
            false, newIndex, request.getReadCause(), service,
            membership,
            readEngine, cfg);

        noPageTasks.add(readTask);
        continue;
      }

      long pageStartTime = System.currentTimeMillis();

      ReadCallbackTask task = new ReadCallbackTask(builder, counter, logicalPageAddress, true,
          newIndex, request.getReadCause(), service,
          membership, readEngine, cfg);

      if (canMergeReadOnTimeout(request, cfg, membership,
          segmentUnit.getSegmentUnitMetadata().getVolumeType(),
          service.getContext().getInstanceId())) {
        readRequestsMap.put(uniqueReadRequestId, task);
        long taskOffsetInArchive = logicalPageAddress.getPhysicalOffsetInArchive();
        readTimeoutEngine.newTimeout(timeout -> {
          Iterator<ReadCallbackTask> allTimedOutTaskIterator =
              readRequestsMap.get(uniqueReadRequestId).iterator();
          while (allTimedOutTaskIterator.hasNext()) {
            ReadCallbackTask timedOutTask = allTimedOutTaskIterator.next();
            if (timedOutTask.getLogicalPageAddress().getPhysicalOffsetInArchive()
                != taskOffsetInArchive) {
              continue;
            }

            if (timedOutTask.segmentUnit.getSegId() != getSegmentUnit().getSegId()) {
              logger.error("read request id maybe duplicated !! {} {}",
                  timedOutTask.segmentUnit.getSegId(), segmentUnit.getSegId());
            }
            timedOutTask.runOnTimeout();
            break;
          }
        }, cfg.getReadPageTimeoutMs(), TimeUnit.MILLISECONDS);
      }

      PageContextCallback<Page> contextCallback = new PageContextCallback<Page>() {
        @Override
        public void completed(PageContext<Page> pageContext) {
          logger.debug("page load done {}", pageContext);
          if (cfg.isEnableFrontIoCostWarning()) {
            long checkoutTime = System.currentTimeMillis() - pageStartTime;
            if (checkoutTime > cfg.getFrontIoTaskThresholdMs()) {
              logger.warn(
                  "[IO Cost Warning] read request checking out" 
                      + " page cost too long: [{}] MS, request id [{}]",
                  checkoutTime, request.getRequestId());
            }
          }
          task.pageLoaded(pageContext);
          readEngine.submit(task);
        }
      };

      PageContext<Page> pageContext = pageContextFactory
          .generateAsyncCheckoutContext(physicalPageAddress, TaskType.CHECK_OUT_FOR_EXTERNAL_READ,
              contextCallback, cfg.getDefaultPageRequestTimeoutMs());

      pageContexts.add(pageContext);
      logger.debug("put page context:{} to IO scheduler", pageContext);
    }

    counter.set(pageContexts.size() + noPageTasks.size());

    noPageTasks.forEach(readEngine::submit);
    if (pageContexts.size() != 0) {
      pageManager.checkout(pageContextFactory.generatePageContextWrapper(pageContexts));
    } else  if (noPageTasks.isEmpty()) {
      callback.complete(builder.build());
    }
  }

  public class ResponseUnit {
    private final PbReadRequestUnit requestUnit;
    private final List<PbBroadcastLog> logsToMerge = new ArrayList<>();
    private ByteBuf data;
    private PbIoUnitResult status = PbIoUnitResult.SKIP;

    ResponseUnit(PbReadRequestUnit requestUnit) {
      this.requestUnit = requestUnit;
    }

    PbIoUnitResult getStatus() {
      return status;
    }

    void setStatus(PbIoUnitResult status) {
      this.status = status;
    }
  }

  public class Builder {
    private final ResponseUnit[] responseUnits;
    SegmentUnit segmentUnit;
    PageManager<Page> pageManager;
    MethodCallback<PyReadResponse> callback;
    private PbReadResponse.Builder responseBuilder;
    private int snapshotId;
    private ByteBufAllocator allocator;

    private Builder(PbReadResponse.Builder responseBuilder, int unitCount) {
      this.responseBuilder = responseBuilder;
      this.responseUnits = new ResponseUnit[unitCount];
    }

    private Builder withPageManager(PageManager<Page> pageManager) {
      this.pageManager = pageManager;
      return this;
    }

    private Builder withSegmentUnit(SegmentUnit segmentUnit) {
      this.segmentUnit = segmentUnit;
      return this;
    }

    private Builder withMethodCallback(MethodCallback<PyReadResponse> callback) {
      this.callback = callback;
      return this;
    }

    long getUnitOffset(int index) {
      return responseUnits[index].requestUnit.getOffset();
    }

    long getRequestId() {
      return responseBuilder.getRequestId();
    }

    synchronized void cleanPcl() {
      pcl = -1L;
    }

    synchronized void setResponseUnit(int index, ResponseUnit unit) {
      responseUnits[index] = unit;
    }

    synchronized PbIoUnitResult getResponseUnitStatus(int index) {
      return responseUnits[index].getStatus();
    }

    synchronized void setResponseUnitStatus(int index, PbIoUnitResult result) {
      responseUnits[index].setStatus(result);
    }

    synchronized void processExceptionStatus(Collection<Integer> unitIndices,
        PbIoUnitResult status) {
      for (int index : unitIndices) {
        responseUnits[index].setStatus(status);
      }
    }

    synchronized void cleanProgress(Collection<Integer> unitIndices) {
      for (Integer index : unitIndices) {
        if (responseUnits[index].data != null) {
          responseUnits[index].data.release();
          responseUnits[index].data = null;
        }

        responseUnits[index].setStatus(PbIoUnitResult.SKIP);
        responseUnits[index].logsToMerge.clear();
      }
    }

    synchronized void copyDataFromSecondaries(Collection<Integer> unitsIndex,
        PyReadResponse mergeReadResponse, PageAddress logicalPageAddress) {
      ByteBuf byteBuf = mergeReadResponse.getResponseUnitData(0);
      logger.info("merge result is {}, for page {}", mergeReadResponse.getMetadata(),
          logicalPageAddress);

      for (Integer index : unitsIndex) {
        PbReadRequestUnit readRequestUnit = responseUnits[index].requestUnit;
        logger.debug("read request unit: {}, in page: {}", readRequestUnit, logicalPageAddress);

        long logicOffsetInSeg = readRequestUnit.getOffset();
        int offsetAtPage = (int) (logicOffsetInSeg % cfg.getPageSize());
        int length = readRequestUnit.getLength();

        ByteBuf buffer = allocator.buffer(length);
        buffer.writeBytes(byteBuf, offsetAtPage, length);

        responseUnits[index].data = buffer;
        responseUnits[index].setStatus(PbIoUnitResult.OK);
      }

      byteBuf.release();
    }

    synchronized void copyDataFromPage(Collection<Integer> unitsIndex,
        PageAddress logicalPageAddress,
        boolean hasPhysicalPageAddress, PlansToApplyLogs plans, PageContext<Page> page)
        throws Exception {
      SegmentLogMetadata metadata = segmentUnit.getSegmentLogMetadata();

      for (Integer index : unitsIndex) {
        PbReadRequestUnit readRequestUnit = responseUnits[index].requestUnit;
        logger.debug("read request unit: {}, in page: {}", readRequestUnit, logicalPageAddress);

        long logicOffsetInSeg = readRequestUnit.getOffset();
        int offsetAtPage = (int) (logicOffsetInSeg % cfg.getPageSize());
        int length = readRequestUnit.getLength();

        ByteBuf buffer = null;
        if (!hasPhysicalPageAddress) {
          if (null != plans && plans.getPlans().size() != 0) {
            buffer = allocator.buffer(length);
            byte[] zeros = new byte[length];
            buffer.writeBytes(zeros);
          }
        } else {
          buffer = allocator.buffer(length);
          buffer.writeBytes(page.getPage().getReadOnlyView(offsetAtPage, length));
        }

        if (buffer != null) {
          DataLogUtils.applyLogs(buffer.duplicate(), offsetAtPage, length, cfg.getPageSize(), plans,
              logicalPageAddress, segmentUnit);
          responseUnits[index].data = buffer;
          responseUnits[index].setStatus(PbIoUnitResult.OK);
        } else {
          responseUnits[index].setStatus(PbIoUnitResult.FREE);
        }
      }
    }

    synchronized void addLogsToMerge(SegmentLogMetadata metadata,
        PageAddress logicalPageAddress, List<Integer> unitsIndex) {
      if (request.getReadCause() == ReadCause.FETCH) {
        logger.debug("not merge read, no need to add logs");
        return;
      }

      if (pcl == null) {
        if (segUnit.getSegmentUnitMetadata().getMigrationStatus().isMigratedStatus()) {
          pcl = -1L;
        } else {
          pcl = metadata.getClId();
        }
      }

      long startLogId = pcl == -1L ? metadata.getClId() : pcl;

      for (Integer index : unitsIndex) {
        PbReadRequestUnit readRequestUnit = responseUnits[index].requestUnit;
        logger.debug("read request unit: {}, in page: {}", readRequestUnit, logicalPageAddress);

        long logicOffsetInSeg = readRequestUnit.getOffset();
        List<MutationLogEntry> logsToMerge = metadata
            .getLogsForPageAfter(logicOffsetInSeg, startLogId);
        logger.warn("{} merge reading, we got {} logs after cl {}", segUnit.getSegId(),
            logsToMerge.size(), startLogId);
        if (cfg.isEnableIoTracing()) {
          logger.warn("logsToMerge after pcl in this page {} are {}", logicalPageAddress,
              logsToMerge);
        }
        logger.debug("logsToMerge after pcl in this page {} are {}", logicalPageAddress,
            logsToMerge);
        for (MutationLogEntry log : logsToMerge) {
          PbBroadcastLog.Builder logBuilder = PbBroadcastLog.newBuilder();
          logBuilder.setLogUuid(log.getUuid());
          logBuilder.setLogId(log.getLogId());
          logBuilder.setOffset(log.getOffset());
          logBuilder.setChecksum(log.getChecksum());
          logBuilder.setLength(log.getLength());
          logBuilder.setLogStatus(log.getStatus().getPbLogStatus());
          if (log.getStatus() == LogStatus.Created) {
            logBuilder.setData(ByteStringHelper.wrap(log.getData()));
          }
          responseUnits[index].logsToMerge.add(logBuilder.build());
        }
      }
    }

    public PyReadResponse build() {
      ByteBuf data = null;
      CompositeByteBuf compositeByteBuf = null;

      if (pcl != null) {
        responseBuilder.setPclId(pcl);
      }

      int numUnits = responseUnits.length;
      for (int i = 0; i < responseUnits.length; i++) {
        ResponseUnit responseUnit = responseUnits[i];

        logger.debug("logsToMerge:{}", responseUnit.logsToMerge);
        if (responseUnit.status == PbIoUnitResult.OK) {
          responseBuilder.addResponseUnits(PbRequestResponseHelper
              .buildPbReadResponseUnitFrom(responseUnit.requestUnit, responseUnit.data,
                  responseUnit.logsToMerge));
          if (data == null) {
            data = responseUnit.data;
          } else {
            if (compositeByteBuf == null) {
              compositeByteBuf = new CompositeByteBuf(allocator, true, numUnits);
              compositeByteBuf.addComponent(true, data);
            }
            compositeByteBuf.addComponent(true, responseUnit.data);
          }
        } else {
          responseBuilder.addResponseUnits(PbRequestResponseHelper
              .buildPbReadResponseUnitFrom(responseUnit.requestUnit, responseUnit.status));
          Validate.isTrue(responseUnit.data == null);
        }
      }

      if (responseUnits.length == 0) {
        responseBuilder.addAllResponseUnits(new ArrayList<PbReadResponseUnit>());
      }

      if (compositeByteBuf != null) {
        data = compositeByteBuf;
      }
      return new PyReadResponse(responseBuilder.build(), data, false);
    }

    public int getSnapshotId() {
      return snapshotId;
    }

    public Builder setSnapshotId(int snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public ByteBufAllocator getAllocator() {
      return allocator;
    }

    public void setAllocator(ByteBufAllocator allocator) {
      this.allocator = allocator;
    }
  }

}

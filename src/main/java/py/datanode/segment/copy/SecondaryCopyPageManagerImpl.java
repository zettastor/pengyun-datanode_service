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

package py.datanode.segment.copy;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveOptions;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitType;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.CopyPageAbortException;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.bitmap.CopyPageBitmap;
import py.datanode.segment.copy.bitmap.CopyPageBitmapImpl;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.instance.Instance;
import py.proto.Broadcastlog;
import py.storage.Storage;
import py.third.rocksdb.KvStoreException;

public class SecondaryCopyPageManagerImpl implements SecondaryCopyPageManager {
  private static final Logger logger = LoggerFactory.getLogger(SecondaryCopyPageManagerImpl.class);

  private final int segmentPageCount;
  private final int copyUnitSize;
  private final int copyUnitCount;
  private final CopyPageBitmap bitmap;
  private final Map<Integer, CopyPageBitmap> copyUnitBitmapMap = new ConcurrentHashMap<>();
  private final Map<Integer, CopyPage[]> copyUnitMap = new ConcurrentHashMap<>();
  private final Map<Integer, Long> pageMaxLogIdMap = new ConcurrentHashMap<>();
  private final DataNodeConfiguration cfg;
  private int copyUnitPosition = -1;
  private SegmentUnit segmentUnit;
  private IoThrottleManager ioThrottleManager;
  private AtomicBoolean hasMarkOriginBrick = new AtomicBoolean(false);

  private long sessionId;
  private long lastPushTime;
  private Instance primary;
  private long primaryMaxLogId;
  private boolean fullCopy;
  private int totalCountOfPageToCopy;

  private volatile CopyPageStatus copyPageStatus;
  private volatile MutationLogEntry catchUpLog;

  public SecondaryCopyPageManagerImpl(DataNodeConfiguration cfg, SegmentUnit segmentUnit,
      Instance primary,
      long sessionId) {
    this.copyUnitSize = cfg.getPageCountInRawChunk();
    this.segmentPageCount = ArchiveOptions.PAGE_NUMBER_PER_SEGMENT;
    this.copyUnitCount = segmentPageCount / cfg.getPageCountInRawChunk();
    this.bitmap = new CopyPageBitmapImpl(segmentPageCount);

    this.bitmap.set(0, segmentPageCount);
    this.lastPushTime = System.currentTimeMillis();
    this.cfg = cfg;
    this.copyPageStatus = CopyPageStatus.None;
    this.segmentUnit = segmentUnit;
    this.primary = primary;
    this.sessionId = sessionId;
  }

  public IoThrottleManager getIoThrottleManager() {
    return ioThrottleManager;
  }

  public void setIoThrottleManager(IoThrottleManager ioThrottleManager) {
    this.ioThrottleManager = ioThrottleManager;
  }

  @Override
  public int getCopyUnitSize() {
    return copyUnitSize;
  }

  @Override
  public CopyPageStatus getCopyPageStatus() {
    return copyPageStatus;
  }

  @Override
  public void setCopyPageStatus(CopyPageStatus newStatus) {
    this.copyPageStatus = newStatus;
    if (newStatus == CopyPageStatus.InitiateCopyPage && totalCountOfPageToCopy == 0) {
      totalCountOfPageToCopy = bitmap.size() - bitmap.cardinality();
    }
  }

  @Override
  public Instance getPeer() {
    return primary;
  }

  @Override
  public long getPrimaryMaxLogId() {
    return primaryMaxLogId;
  }

  @Override
  public void setPrimaryMaxLogId(long maxLogId) {
    this.primaryMaxLogId = maxLogId;
  }

  public CopyPageBitmap getCopyPageBitmap() {
    return bitmap;
  }

  @Override
  public CopyPage[] getCopyUnit(int workerId) {
    return copyUnitMap.get(workerId);
  }

  @Override
  public synchronized boolean markMaxLogId(int pageIndex, long newLogId) {
    if (isProcessing(pageIndex) || isProcessed(pageIndex)) {
      logger.warn("fail to mark max log for page {}", pageIndex, this);
      return false;
    }
    pageMaxLogIdMap.put(pageIndex, newLogId);
    return true;
  }

  @Override
  public long getMaxLogId(int pageIndex) {
    Long logId = pageMaxLogIdMap.get(pageIndex);
    return logId == null ? catchUpLog.getLogId() : logId;
  }

  @Override
  public void allocatePageAddressAtTheFirstTime(
      List<Broadcastlog.PbPageRequest> pbPageRequests, int workerId)
      throws CopyPageAbortException {
    CopyPageBitmap copyPageBitmap = getCopyUnitBitmap(workerId);
    CopyPage[] copyPages = getCopyUnit(workerId);

    logger.debug("begin allocate page address for work id {}, copy unit position {}", workerId,
        getCopyUnitPosition(workerId));
    List<PageAddress> successPageAddress = new ArrayList<>();
    for (CopyPage copyPage : copyPages) {
      if (Objects.isNull(copyPage)) {
        continue;
      }
      successPageAddress.add(copyPage.getLogicalPageAddress());
    }
    getIoThrottleManager().addTotal(segmentUnit.getSegId(), pbPageRequests.size());
    try {
      for (Broadcastlog.PbPageRequest pbPageRequest : pbPageRequests) {
        int pageIndexInUnit = pbPageRequest.getPageIndex() - copyPageBitmap.offset();
        Validate.isTrue(pageIndexInUnit < copyUnitSize);
        CopyPage copyPage = copyPages[pageIndexInUnit];
        PageAddress pageAddress = segmentUnit.getAddressManager()
            .getOriginPhysicalPageAddressByLogicalAddress(
                copyPage.getLogicalPageAddress());
        logger.info("save origin for {}, {}", pageAddress, pageIndexInUnit);
        copyPages[pageIndexInUnit].addOriginalPageNode(pageAddress);
      }

      for (int i = 0; i < copyUnitSize; i++) {
        if (copyPages[i] != null) {
          if (!copyPages[i].hasNext()) {
            logger.info("this page no need copy {}", copyPages[i]);
            copyPages[i].setDone();
            copyPageBitmap.set(i);
          } else {
            copyPages[i].moveToNext();
          }
        }
      }
    } catch (Exception e) {
      logger.warn("catch a exception when allocate shadow page for copy page", e);
      throw new CopyPageAbortException(e);
    }
  }

  @Override
  public void removeTask(int workerId) {
    CopyPageBitmap bitmap = copyUnitBitmapMap.remove(workerId);
    CopyPage[] copyPages = copyUnitMap.remove(workerId);
    logger.warn("a task {} finish his job {}", workerId, bitmap);
  }

  @Override
  public int workerCount() {
    return copyUnitBitmapMap.size();
  }

  @Override
  public synchronized boolean moveToNextCopyPageUnit(int workerId) {
    CopyPageBitmap currentBitmapUnit = copyUnitBitmapMap.get(workerId);
    if (currentBitmapUnit != null && !currentBitmapUnit.isFull()) {
      throw new RuntimeException("wrong :" + currentBitmapUnit);
    }

    long offsetInArchive = segmentUnit.getStartLogicalOffset();
    Storage storage = segmentUnit.getArchive().getStorage();
    SegId segId = segmentUnit.getSegId();

    CopyPage[] nextCopyPageUnit = new CopyPage[copyUnitSize];
    CopyPageBitmap subBitmap = null;
    int firstIndex = 0;
    while (++copyUnitPosition < copyUnitCount) {
      firstIndex = copyUnitPosition * copyUnitSize;
      subBitmap = bitmap.subBitmap(firstIndex, firstIndex + copyUnitSize);
      if (!subBitmap.isFull()) {
        logger.info("the next unit is {} bitmap {} for worker {}", copyUnitPosition, subBitmap,
            workerId);
        break;
      }
    }

    if (copyUnitPosition >= copyUnitCount) {
      logger.info("primary has pushing all pages to myself: {}", this);
      return false;
    }

    for (int index = 0; index < copyUnitSize; index++) {
      if (!subBitmap.get(index)) {
        int pageIndex = firstIndex + index;
        PageAddress logicalPageAddress = PageAddressGenerator
            .generate(segId, offsetInArchive, pageIndex, storage, cfg.getPageSize());
        nextCopyPageUnit[index] = new CopyPage(pageIndex, logicalPageAddress);
        Long maxLogId = pageMaxLogIdMap.remove(pageIndex);
        if (maxLogId != null) {
          nextCopyPageUnit[index].setMaxLogId(maxLogId);
        }
      }
    }

    logger.info("set the next bitmap unit {} to current task {}", subBitmap, workerId);
    copyUnitBitmapMap.put(workerId, subBitmap);
    copyUnitMap.put(workerId, nextCopyPageUnit);
    return true;
  }

  @Override
  public void buildCurrentCopyPageUnit(Broadcastlog.PbCopyPageResponse.Builder responseBuilder,
      int workerId) {
    List<Broadcastlog.PbLogUnit> logUnits = new ArrayList<>();
    CopyPageBitmap copyPageUnitBitmap = getCopyUnitBitmap(workerId);
    CopyPage[] copyPageUnit = getCopyUnit(workerId);

    int unitIndex = getCopyUnitPosition(workerId);
    logger.info("buildCurrentCopyPageUnit index: {}", unitIndex);

    Broadcastlog.PbLogUnit.Builder builder = Broadcastlog.PbLogUnit.newBuilder();
    for (int i = 0; i < copyPageUnitBitmap.size(); i++) {
      if (copyPageUnitBitmap.get(i)) {
        continue;
      }

      long maxLogId = copyPageUnit[i].getMaxLogId();
      if (maxLogId >= 0) {
        builder.clear();
        builder.setPageIndexInUnit(i);
        builder.setLastLogId(maxLogId);
        logUnits.add(builder.build());
      }
    }

    responseBuilder.addAllNexLogUnits(logUnits);
  }

  @Override
  public void buildNextCopyPageUnit(Broadcastlog.PbCopyPageResponse.Builder responseBuilder,
      int workerId) {
    if (!moveToNextCopyPageUnit(workerId)) {
      if (!isDone()) {
        int leftCount = workerCount();
        if (leftCount > 1) {
          removeTask(workerId);
          logger.warn("a task finish his job {} {} left task count {}", workerId,
              segmentUnit.getSegId(), leftCount - 1);
          responseBuilder.setStatus(Broadcastlog.PbCopyPageStatus.COPY_PAGE_DONE);
          return;
        } else {
          logger
              .error("i am the last task, why it is  not done and cannot move next {} {}", workerId,
                  this);
          responseBuilder.setStatus(Broadcastlog.PbCopyPageStatus.COPY_PAGE_ABORT);
          return;
        }
      } else {
        removeTask(workerId);
        logger.info("copy page has done, manager: {}", this);
        responseBuilder.setStatus(Broadcastlog.PbCopyPageStatus.COPY_PAGE_DONE);
        return;
      }
    }
    responseBuilder.setAfterNextCopyPageUnitIndex(0);
    responseBuilder.setNextCopyPageUnitIndex(getCopyUnitPosition(workerId));
    responseBuilder.setNextBitmap(ByteString.copyFrom(getCopyUnitBitmap(workerId).array()));
    buildCurrentCopyPageUnit(responseBuilder, workerId);
  }

  @Override
  public boolean isDone() {
    return bitmap.isFull();
  }

  @Override
  public boolean isProcessing(int pageIndex) {
    for (CopyPageBitmap copyingSubBitmap : copyUnitBitmapMap.values()) {
      if (copyingSubBitmap.offset() / copyUnitSize == pageIndex / copyUnitSize) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isProcessed(int pageIndex) {
    int unitIndex = pageIndex / copyUnitSize;
    CopyPageBitmap subBitmap = bitmap
        .subBitmap(unitIndex * copyUnitSize, (unitIndex + 1) * copyUnitSize);
    boolean done = subBitmap.isFull();

    if (bitmap.get(pageIndex) && !done) {
      logger.info("even page {} done , but the unit is not {}", pageIndex, subBitmap);
    }
    return done;

  }

  @Override
  public void pageWrittenByNewLogs(int pageIndex) {
    if (!bitmap.get(pageIndex)) {
      bitmap.set(pageIndex);
      ioThrottleManager.addAlready(segmentUnit.getSegId(), 1);
      logger.info("set the page done {}", pageIndex);
    }
  }

  public SegmentUnit getSegmentUnit() {
    return segmentUnit;
  }

  public void setSegmentUnit(SegmentUnit segmentUnit) {
    this.segmentUnit = segmentUnit;
  }

  @Override
  public double progress() {
    int total = totalCountOfPageToCopy;
    int left = bitmap.size() - bitmap.cardinality();
    if (left > total) {
      total = totalCountOfPageToCopy = left;
    }
    return total == 0 ? 0 : (double) (total - left) / (double) total;
  }

  @Override
  public MutationLogEntry getCatchUpLog() {
    return catchUpLog;
  }

  @Override
  public void setCatchUpLog(MutationLogEntry entry) {
    this.catchUpLog = entry;
  }

  @Override
  public long getSessionId() {
    return sessionId;
  }

  @Override
  public CopyPageBitmap getCopyUnitBitmap(int workerId) {
    return copyUnitBitmapMap.get(workerId);
  }

  @Override
  public int getCopyUnitPosition(int workerId) {
    CopyPageBitmap bitmap = getCopyUnitBitmap(workerId);
    return bitmap == null ? -1 : bitmap.offset() / copyUnitSize;
  }

  public long getLastPushTime() {
    return lastPushTime;
  }

  public void setLastPushTime(long lastPushTime) {
    this.lastPushTime = lastPushTime;
  }

  @Override
  public boolean isFullCopy() {
    return fullCopy;
  }

  @Override
  public void setFullCopy(boolean fullCopy) {
    this.fullCopy = fullCopy;
  }

  @Override
  public int getTotalCountOfPageToCopy() {
    return totalCountOfPageToCopy;
  }

  @Override
  public String toString() {
    return "SecondaryCopyPageManagerImpl{" + "copyUnitSize=" + copyUnitSize
        + ", segmentPageCount=" + segmentPageCount + ", copyUnitCount="
        + copyUnitCount + ", bitmap=" + bitmap + ", copyUnitPosition=" + copyUnitPosition
        + ", sessionId=" + sessionId + ", currentCopyPageBitmapUnit=" + copyUnitBitmapMap
        + ", copyPageUnit=" + copyUnitMap + ", segmentUnit=" + segmentUnit + ", lastPushTime="
        + lastPushTime + ", status=" + copyPageStatus + ", catchUpLog=" + catchUpLog
        + ", fullCopy=" + fullCopy + '}';
  }

}

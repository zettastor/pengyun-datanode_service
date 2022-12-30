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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveOptions;
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitType;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.page.impl.BogusPageAddress;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.bitmap.CopyPageBitmap;
import py.datanode.segment.copy.bitmap.CopyPageBitmapImpl;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.instance.Instance;
import py.instance.PortType;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.datanode.AsyncDataNode;
import py.storage.Storage;

/**
 * a primary manager allow several worker run in parallel, and use three maps to manager them.
 */
public class PrimaryCopyPageManagerImpl implements PrimaryCopyPageManager {
  private static final  Logger logger = LoggerFactory.getLogger(PrimaryCopyPageManagerImpl.class);
  private final GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory;
  private final SegmentUnit segmentUnit;
  private final Instance secondary;
  private final long sessionId;
  private final CopyPageBitmap bitmap;
  private final Map<Integer, CopyPageBitmap> copyUnitBitmapMap = new ConcurrentHashMap<>();
  private final Map<Integer, CopyPage[]> copyUnitMap = new ConcurrentHashMap<>();
  private final Map<Integer, ByteBuffer[]> buffersMap = new ConcurrentHashMap<>();
  private final Map<Integer, List<PageAddress>> lockPageMap = new ConcurrentHashMap<>();
  private final int maxFailNumberOfCopyPage = 5;
  private final int copyPageCountInUnit;
  private int currentUnitPosition = -1;
  private AtomicInteger errorCounter;
  private PageManager<Page> pageManager;
  private IoThrottleManager ioThrottleManager;

  public PrimaryCopyPageManagerImpl(SegmentUnit segmentUnit, int copyUnitSize, long sessionId,
      Instance secondary, PageManager<Page> pageManager,
      GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory) {
    this.clientFactory = clientFactory;
    this.copyPageCountInUnit = copyUnitSize;
    this.segmentUnit = segmentUnit;
    this.sessionId = sessionId;
    this.secondary = secondary;
    this.pageManager = pageManager;
    this.errorCounter = new AtomicInteger(0);
    this.bitmap = new CopyPageBitmapImpl(ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
   
    this.bitmap.set(0, ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
  }

  public void incErrCount() {
    errorCounter.incrementAndGet();
  }

  public int getErrorCount() {
    return errorCounter.get();
  }

  public boolean reachMaxErrCount() {
    return errorCounter.get() >= maxFailNumberOfCopyPage;
  }

  @Override
  public CopyPage[] getCopyUnit(int workerId) {
    CopyPage[] copyPages = copyUnitMap.get(workerId);
    return copyPages == null ? new CopyPage[0] : copyPages;
  }

  @Override
  public CopyPageBitmap getCopyPageBitmap() {
    return bitmap;
  }

  @Override
  public CopyPageBitmap getCopyUnitBitmap(int workerId) {
    return copyUnitBitmapMap.get(workerId);
  }

  @Override
  public int getCopyUnitPosition(int workerId) {
    CopyPageBitmap unitBitmap = getCopyUnitBitmap(workerId);
    return unitBitmap == null ? -1 : unitBitmap.offset() / getCopyUnitSize();
  }

  @Override
  public long getLastPushTime() {
    return 0;
  }

  @Override
  public void setLastPushTime(long lastPushTime) {
  }

  @Override
  public IoThrottleManager getIoThrottleManager() {
    return ioThrottleManager;
  }

  @Override
  public void setIoThrottleManager(IoThrottleManager ioThrottleManager) {
    this.ioThrottleManager = ioThrottleManager;
  }

  @Override
  public void removeTask(int workerId) {
    copyUnitBitmapMap.remove(workerId);
    List<PageAddress> pageAddresses = lockPageMap.remove(workerId);
  }

  @Override
  public int workerCount() {
    return copyUnitBitmapMap.size();
  }

  @Override
  public void updateNextCopyPageUnit(int workerId, byte[] bitArray, int unitPos) {
    logger.info("secondary let me change to next copy page unit: {} max snap {}, worker {}",
        unitPos, workerId);
    CopyPageBitmap tmpBitmap = new CopyPageBitmapImpl(bitArray, copyPageCountInUnit);
    int firstIndex = unitPos * copyPageCountInUnit;
    CopyPageBitmap cpBitmap = bitmap.subBitmap(firstIndex, firstIndex + copyPageCountInUnit);
    CopyPage[] copyPages = new CopyPage[copyPageCountInUnit];
    this.currentUnitPosition = unitPos;
    SegId segId = segmentUnit.getSegId();
    Storage storage = segmentUnit.getArchive().getStorage();
    long offsetInArchive = segmentUnit.getStartLogicalOffset();

    logger.info("bitmap {} start page index {} worker {}", tmpBitmap, firstIndex, workerId);
    for (int i = 0; i < copyPageCountInUnit; i++) {
      if (tmpBitmap.get(i)) {
        copyPages[i] = null;
        logger.debug("has Done pageIndex {}", i + firstIndex);
        continue;
      }

      cpBitmap.clear(i);
      int pageIndex = firstIndex + i;
      PageAddress logicalPageAddress = PageAddressGenerator
          .generate(segId, offsetInArchive, pageIndex, storage, ArchiveOptions.PAGE_SIZE);
      copyPages[i] = new CopyPage(pageIndex, logicalPageAddress);
    }

    logger.debug("updateNextCopyPageUnit {}", cpBitmap);
    copyUnitBitmapMap.put(workerId, cpBitmap);
    copyUnitMap.put(workerId, copyPages);
    lockPageMap.remove(workerId);
  }

  @Override
  public void allocatePageAddressAtTheFirstTime(CopyPage[] copyPages, int workerId)
      throws Exception {
    int unitPos = getCopyUnitPosition(workerId);
    int firstIndex = unitPos * copyPageCountInUnit;
    SegId segId = segmentUnit.getSegId();
    Storage storage = segmentUnit.getArchive().getStorage();
    List<PageAddress> copyPageAdderss = new ArrayList<>();
    for (CopyPage copyPage : copyPages) {
      if (Objects.isNull(copyPage)) {
        continue;
      }
      copyPageAdderss.add(copyPage.getLogicalPageAddress());
    }
    lockPageMap.put(workerId, copyPageAdderss);

    BitSet bitSet = new BitSet(copyPageCountInUnit);
    bitSet.set(0, copyPageCountInUnit);

    for (int i = 0; i < copyPageCountInUnit; i++) {
      int pageIndex = firstIndex + i;
      if (copyPages[i] != null) {
        long physicalOffsetInArchive = segmentUnit.getStartPhysicalOffset();
        PageAddress physicalPageAddress = PageAddressGenerator
            .generate(segId, physicalOffsetInArchive, pageIndex, storage,
                ArchiveOptions.PAGE_SIZE);
        copyPages[i].addOriginalPageNode(physicalPageAddress);
        logger.info("for normal segment, the page address {} of snapId 0 should calculate",
            physicalPageAddress);
      }
    }

    for (int i = 0; i < copyPageCountInUnit; i++) {
      if (copyPages[i] != null) {
        if (!copyPages[i].hasNext()) {
          logger.info("this page no need copy {}", copyPages[i]);
          copyPages[i].setDone();
          getCopyUnitBitmap(workerId).set(i);
        } else {
          copyPages[i].moveToNext();
        }
      }
    }
  }

  @Override
  public ByteBuffer getFromBufferPool(int workerId, int pageIndex) {
    ByteBuffer[] byteBuffers = buffersMap.get(workerId);
    if (byteBuffers == null) {
      byteBuffers = new ByteBuffer[copyPageCountInUnit];
      for (int i = 0; i < byteBuffers.length; i++) {
        byteBuffers[i] = ByteBuffer.wrap(new byte[(int) ArchiveOptions.PAGE_SIZE]);
      }
      buffersMap.put(workerId, byteBuffers);
    }
    ByteBuffer buffer = byteBuffers[pageIndex - getCopyUnitBitmap(workerId).offset()].duplicate();
    buffer.clear();
    return buffer;
  }

  public SegmentUnit getSegmentUnit() {
    return segmentUnit;
  }

  @Override
  public int getCopyUnitSize() {
    return copyPageCountInUnit;
  }

  @Override
  public CopyPageStatus getCopyPageStatus() {
    return null;
  }

  @Override
  public void setCopyPageStatus(CopyPageStatus newStatus) {
  }

  @Override
  public Instance getPeer() {
    return secondary;
  }

  @Override
  public boolean isFullCopy() {
    throw new RuntimeException("not used");
  }

  @Override
  public void setFullCopy(boolean fullCopy) {
  }

  public long getSessionId() {
    return sessionId;
  }

  @Override
  public boolean markMaxLogId(int pageIndex, long newLogId) {
    throw new RuntimeException("not used");
  }

  @Override
  public boolean isDone() {
    throw new RuntimeException("not used");
  }

  @Override
  public boolean isProcessing(int pageIndex) {
    throw new RuntimeException("not used");
  }

  @Override
  public boolean isProcessed(int pageIndex) {
    throw new RuntimeException("not used");
  }

  public PageManager<Page> getPageManager() {
    return pageManager;
  }

  public AsyncDataNode.AsyncIface getSecondaryClient() {
    return clientFactory.generate(secondary.getEndPointByServiceName(PortType.IO));
  }

  @Override
  public String toString() {
    return "PrimaryCopyPageMangerImpl{"
        +  ", segmentUnit=" + segmentUnit.getSegId()
        +  ", secondary=" + secondary
        +  ", sessionId=" + sessionId
        +  ", errorCounter=" + errorCounter
        +  ", bitmap=" + copyUnitBitmapMap
        +  '}';
  }
}

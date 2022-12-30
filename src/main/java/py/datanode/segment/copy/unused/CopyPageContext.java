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

package py.datanode.segment.copy.unused;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.common.struct.EndPoint;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.impl.BogusPageAddress;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPage;
import py.datanode.segment.copy.bitmap.CopyPageBitmap;
import py.storage.Storage;

public class CopyPageContext {
  private static final Logger logger = LoggerFactory.getLogger(CopyPageContext.class);
  private final SegmentUnit segmentUnit;
  private final EndPoint secondaryEndPoint;
  private final long sessionId;
  private final DataNodeConfiguration cfg;
  private final int maxFailNumberOfCopyPage = 5;
  private final int copyPageCountInUnit;
  private final int secondaryMaxSnapId;
  int workingCount = 0;
  private volatile CopyPageBitmap copyPageUnitBitmap;
  private CopyPage[] copyPages;
  private int currentUnitPosition = -1;
  private AtomicInteger errorCounter;
  private ByteBuffer[] byteBuffers;

  public CopyPageContext(SegmentUnit segmentUnit, DataNodeConfiguration cfg, long sessionId,
      EndPoint secondaryEndPoint, int secondaryMaxSnapId) {
    this.cfg = cfg;
    this.copyPageCountInUnit = cfg.getPageCountInRawChunk();
    this.segmentUnit = segmentUnit;
    this.sessionId = sessionId;
    this.secondaryEndPoint = secondaryEndPoint;
    this.secondaryMaxSnapId = secondaryMaxSnapId;
    init();
  }

  public CopyPageContext duplicate() {
    return new CopyPageContext(segmentUnit, cfg, sessionId, secondaryEndPoint, secondaryMaxSnapId);
  }

  /**
   * the init step should seperate from constructor.
   */
  private void init() {
    this.copyPages = new CopyPage[copyPageCountInUnit];
    this.byteBuffers = new ByteBuffer[copyPageCountInUnit];
    for (int i = 0; i < byteBuffers.length; i++) {
      this.byteBuffers[i] = ByteBuffer.wrap(new byte[cfg.getPageSize()]);
      this.byteBuffers[i].clear();
    }

    this.errorCounter = new AtomicInteger(0);
  }

  public CopyPage getCopyPage(int pageIndex) {
    return copyPages[pageIndex - copyPageUnitBitmap.offset()];
  }

  public int getCopyPageCountInUnit() {
    return copyPageCountInUnit;
  }

  public List<CopyPage> getCopyPageUnitToRead() {
    List<CopyPage> copyPages = new ArrayList<CopyPage>();
    if (copyPageUnitBitmap == null) {
      return copyPages;
    }

    for (int i = 0; i < this.copyPages.length; i++) {
      if (copyPageUnitBitmap.get(i) || this.copyPages[i] == null) {
        continue;
      }

      if (!this.copyPages[i].content().pageLoaded()) {
        copyPages.add(this.copyPages[i]);
      }
    }
    return copyPages;
  }

  public void incrementErrorCount() {
    errorCounter.incrementAndGet();
  }

  public int getErrorCount() {
    return errorCounter.get();
  }

  public boolean reachMaxErrorCount() {
    return errorCounter.get() >= maxFailNumberOfCopyPage;
  }

  public List<CopyPage> getCopyPageUnitToSend() {
    List<CopyPage> copyPages = new ArrayList<CopyPage>();
    if (copyPageUnitBitmap == null) {
      return copyPages;
    }

    for (int i = 0; i < this.copyPages.length; i++) {
      if (copyPageUnitBitmap.get(i) || this.copyPages[i] == null) {
        continue;
      }

      CopyPage.PageNode pageNode = this.copyPages[i].content();
     
      if (pageNode.pageLoaded()) {
        copyPages.add(this.copyPages[i]);
      }
    }
    logger.debug("pages to copy {}", copyPages.size());
    return copyPages;
  }

  public void updateNextCopyPageUnit(CopyPageBitmap cpBitmap, int currentUnitPosition)
      throws Exception {
    this.copyPageUnitBitmap = cpBitmap;
    this.currentUnitPosition = currentUnitPosition;
    Validate.isTrue(cpBitmap.size() == copyPageCountInUnit);
    int firstIndex = cpBitmap.offset();
    SegId segId = segmentUnit.getSegId();
    long offsetInArchive = segmentUnit.getStartLogicalOffset();
    Storage storage = segmentUnit.getArchive().getStorage();

    for (int i = 0; i < copyPageCountInUnit; i++) {
      if (cpBitmap.get(i)) {
        copyPages[i] = null;
        logger.debug("has Done pageIndex {}", i + firstIndex);
        continue;
      }

      int pageIndex = firstIndex + i;
      PageAddress logicalPageAddress = PageAddressGenerator
          .generate(segId, offsetInArchive, pageIndex, storage, cfg.getPageSize());
      copyPages[i] = new CopyPage(pageIndex, logicalPageAddress);
      copyPages[i].addOriginalPageNode(logicalPageAddress);
    }
    logger.debug("updateNextCopyPageUnit {}", cpBitmap);

  }

  public int getCurrentCopyPageUnitIndex() {
    return currentUnitPosition;
  }

  public CopyPageBitmap getCopyPageUnitBitmap() {
    return copyPageUnitBitmap;
  }

  public ByteBuffer getPageByteBuffer(int pageIndex) {
    return byteBuffers[pageIndex - copyPageUnitBitmap.offset()].duplicate();
  }

  public EndPoint getSecondaryEndPoint() {
    return secondaryEndPoint;
  }

  public SegmentUnit getSegmentUnit() {
    return segmentUnit;
  }

  public long getSessionId() {
    return sessionId;
  }

  @Override
  public String toString() {
    return "CopyPageContext{"
        + "copyPageUnitBitmap=" + copyPageUnitBitmap
        + ", segmentUnit=" + segmentUnit.getSegId()
        + ", secondaryEndPoint=" + secondaryEndPoint
        + ", sessionId=" + sessionId
        + ", copyPageCountInUnit=" + copyPageCountInUnit
        + ", errorCounter=" + errorCounter
        + ", secondaryMaxSnapId=" + secondaryMaxSnapId
        + ", workingCount=" + workingCount
        + '}';
  }
}

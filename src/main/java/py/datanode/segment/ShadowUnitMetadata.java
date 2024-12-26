/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.datanode.segment;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractSegmentUnitMetadata;
import py.archive.ArchiveOptions;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitBitmap;
import py.datanode.page.impl.PageAddressGenerator;
import py.exception.NotEnoughSpaceException;
import py.storage.Storage;
import py.volume.VolumeId;

public class ShadowUnitMetadata extends AbstractSegmentUnitMetadata {
  public static final VolumeId FAKE_VOLUME_ID_FOR_SHADOW_UNIT = new VolumeId(-123456789);
  private static final Logger logger = LoggerFactory.getLogger(ShadowUnitMetadata.class);

  public ShadowUnitMetadata(SegId segId, long metadataOffset, long dataOffset, Storage storage) {
    super(segId, metadataOffset, dataOffset, storage);
  }

  public ShadowUnitMetadata(@JsonProperty("segId") SegId segId,
      @JsonProperty("offset") long offset) {
    super(segId, offset);
  }

  @Override
  public boolean isUnitReusable() {
    return super.getFreePageCount() == super.getPageCount();
  }

  @JsonIgnore
  public PageAddress getShadowPage() throws NotEnoughSpaceException {
    if (super.getFreePageCount() <= 0) {
      logger.warn("I don't have any free pages ! ");
      throw new NotEnoughSpaceException();
    }
    int freeIndex = super.getFirstFreePageIndex();
    super.setPage(freeIndex);
    PageAddress address = PageAddressGenerator
        .generate(SegId.SYS_RESERVED_SEGID, super.getLogicalDataOffset(), freeIndex, storage,
            ArchiveOptions.PAGE_SIZE);
    logger.debug("new allocate address is {}", address);
    isBitmapNeedPersisted = true;
    return address;
  }

  public void freeShadowPage(PageAddress pageAddress) {
    logger.debug("free a shadow page {}", pageAddress);
    Validate.isTrue(pageAddress.getStorage() == this.storage);
    Validate.isTrue(pageAddress.getSegUnitOffsetInArchive() == super.getLogicalDataOffset());
    int pageIndex = (int) (pageAddress.getOffsetInSegment() / ArchiveOptions.PAGE_PHYSICAL_SIZE);
    super.clearPage(pageIndex);
    logger.debug("free shadow page {}, and the cardinality {}, free page count {}", pageIndex,
        bitmap.cardinality(SegmentUnitBitmap.SegmentUnitBitMapType.Data), super.getFreePageCount());
    isBitmapNeedPersisted = true;
  }

  public void markPageUsed(PageAddress pageAddress) {
    Validate.isTrue(pageAddress.getSegUnitOffsetInArchive() == super.getLogicalDataOffset());
    Validate.isTrue(pageAddress.getStorage() == this.storage);
    int pageIndex = (int) (pageAddress.getOffsetInSegment() / ArchiveOptions.PAGE_PHYSICAL_SIZE);
    Validate.isTrue(super.isPageFree(pageIndex));
    super.setPage(pageIndex);
    isBitmapNeedPersisted = true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ShadowUnitMetadata[offset=").append(this.dataOffset).append(", metadata Offset=")
        .append(this.metadataOffsetInArchive).append(",storage=").append(this.storage)
        .append(",FreeSpaceCount=").append(this.getFreePageCount()).append("]");
    return sb.toString();
  }

}

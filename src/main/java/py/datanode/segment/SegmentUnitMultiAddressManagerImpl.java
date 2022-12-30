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

package py.datanode.segment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveOptions;
import py.archive.brick.BrickMetadata;
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.archive.segment.SegmentUnitType;
import py.datanode.page.impl.PageAddressGenerator;
import py.storage.Storage;
import py.third.rocksdb.KvStoreException;

public class SegmentUnitMultiAddressManagerImpl implements SegmentUnitMultiAddressManager {
  private static final Logger logger = LoggerFactory
      .getLogger(SegmentUnitMultiAddressManagerImpl.class);

  private final SegmentUnit segmentUnit;

  public SegmentUnitMultiAddressManagerImpl(SegmentUnit segmentUnit) {
    this.segmentUnit = segmentUnit;
  }

  public MultiPageAddress getPhysicalPageAddress(MultiPageAddress logicalPageAddress) {
    PageAddress pageAddress = getOriginPhysicalPageAddressByLogicalAddress(
        logicalPageAddress.getStartPageAddress());
    return new MultiPageAddress(pageAddress, logicalPageAddress.getPageCount());
  }

  public PageAddress getPhysicalPageAddress(long logicalOffsetInSegment) {
    Storage storage = segmentUnit.getArchive().getStorage();
    long pageSize = ArchiveOptions.PAGE_SIZE;
    PageAddress logicalAddress = PageAddressGenerator
        .generate(segmentUnit.getSegId(), segmentUnit.getStartLogicalOffset(),
            logicalOffsetInSegment, storage,
            pageSize);
    logger.debug("logical address by logical offset in segment {}, {}", logicalOffsetInSegment,
        logicalAddress);
    return getOriginPhysicalPageAddressByLogicalAddress(logicalAddress);
  }

  public PageAddress getOriginPhysicalPageAddressByLogicalAddress(PageAddress logicalPageAddress) {
    Validate.isTrue(
        !segmentUnit.getSegmentUnitMetadata().getSegmentUnitType().equals(SegmentUnitType.Arbiter)
            && !segmentUnit.getSegmentUnitMetadata().getSegmentUnitType()
                .equals(SegmentUnitType.Flexible));

    BrickMetadata brickMetadata = segmentUnit.getSegmentUnitMetadata().getBrickMetadata();
    PageAddress physicalAddress = PageAddressGenerator.generate(segmentUnit.getSegId(),
        brickMetadata.getDataOffset(), logicalPageAddress.getOffsetInSegment(),
        brickMetadata.getStorage());
    return physicalAddress;
  }
}

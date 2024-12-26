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

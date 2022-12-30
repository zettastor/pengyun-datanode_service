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
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.third.rocksdb.KvStoreException;

public interface SegmentUnitMultiAddressManager {
  MultiPageAddress getPhysicalPageAddress(MultiPageAddress logicalPageAddress);

  PageAddress getPhysicalPageAddress(long logicalOffsetInSegment);

  /**
   * get origin physical page address.
   */
  PageAddress getOriginPhysicalPageAddressByLogicalAddress(PageAddress logicalPageAddress);

}

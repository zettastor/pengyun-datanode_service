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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.MigratingSegmentUnitMetadata;
import py.volume.CacheType;
import py.volume.VolumeType;

public class MigratingSegmentUnitMetadataAccessor {
  private static final Logger logger = LoggerFactory.getLogger(MigratingSegmentUnitMetadata.class);

  public static void writeMigratingSegmentUnitMetaToDisk(MigratingSegmentUnitMetadata newMetadata)
      throws Exception {
    SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(newMetadata.getSegId(),
        newMetadata.getLogicalDataOffset(), null, SegmentUnitStatus.Deleted, VolumeType.REGULAR,
        SegmentUnitType.Normal);
    SegmentUnitMetadataAccessor.writeSegmentUnitMetaToDisk(segmentUnitMetadata);
  }

}

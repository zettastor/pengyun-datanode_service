
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


package py.datanode.segment;

import py.archive.segment.SegmentUnitMetadata;
import py.datanode.archive.RawArchive;

public class SegmentUnitFactory {
  public static SegmentUnit build(SegmentUnitMetadata metadata, RawArchive archive) {
   
   
    return new SegmentUnit(metadata, archive);
  }
}

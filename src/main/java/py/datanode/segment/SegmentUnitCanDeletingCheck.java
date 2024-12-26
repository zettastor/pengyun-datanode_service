
package py.datanode.segment;

import py.archive.segment.SegId;

public interface SegmentUnitCanDeletingCheck {
  void deleteSegmentUnitWithOutCheck(SegId segId, boolean syncPersist) throws Exception;

  void deleteSegmentUnitWithCheck(SegId segId);

  boolean checkSegmentDeletingSync(SegId segId);

  void stop();
}

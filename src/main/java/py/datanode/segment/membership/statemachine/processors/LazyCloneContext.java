
package py.datanode.segment.membership.statemachine.processors;

import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.StateProcessingContextKey;

public class LazyCloneContext extends StateProcessingContext {
  public LazyCloneContext(SegId segId, SegmentUnitStatus status, SegmentUnit segmentUnit) {
    super(new StateProcessingContextKey(segId, "LazyClone"), status, segmentUnit);
  }
}

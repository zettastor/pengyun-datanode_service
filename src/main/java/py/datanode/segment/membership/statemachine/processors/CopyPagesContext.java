
package py.datanode.segment.membership.statemachine.processors;

import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.StateProcessingContextKey;

/**
 * This class is used to record CatchupLogsFromPrimaryProcess process information.
 *
 */
public class CopyPagesContext extends StateProcessingContext {
  // max times of failures
  private static final int MAX_FAILURE_TIMES = 10;

  public CopyPagesContext(SegId segId, SegmentUnit segmentUnit) {
    super(new StateProcessingContextKey(segId, "CopyPage"), SegmentUnitStatus.PreSecondary,
        segmentUnit);
  }

  public boolean reachMaxEndurance() {
    return getFailureTimes() >= MAX_FAILURE_TIMES;
  }

}

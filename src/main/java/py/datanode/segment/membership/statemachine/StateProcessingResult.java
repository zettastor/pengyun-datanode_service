
package py.datanode.segment.membership.statemachine;

import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.recurring.SegmentUnitProcessResult;
import py.datanode.segment.membership.statemachine.processors.StateProcessingContext;

public class StateProcessingResult extends SegmentUnitProcessResult {
  private final SegmentUnitStatus newStatus;

  public StateProcessingResult(StateProcessingContext context, SegmentUnitStatus newStatus) {
    super(context);
    this.newStatus = newStatus;
  }

  public SegmentUnitStatus getNewStatus() {
    return newStatus;
  }

  public StateProcessingContext getContext() {
    return (StateProcessingContext) super.getContext();
  }

  @Override
  public String toString() {
    return "StateProcessingResult [newStatus=" + newStatus + ", toString()=" + super.toString()
        + "]";
  }
}

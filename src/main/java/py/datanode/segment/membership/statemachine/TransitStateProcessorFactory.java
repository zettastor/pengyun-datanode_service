

package py.datanode.segment.membership.statemachine;

import py.datanode.segment.membership.statemachine.processors.StateProcessingContext;

public interface TransitStateProcessorFactory {
  public StateProcessor generate(StateProcessingContext context);
}

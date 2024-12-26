
package py.datanode.segment.datalog.sync.log.reduce;

import py.instance.InstanceId;
import py.netty.core.MethodCallback;
import py.proto.Broadcastlog.PbBackwardSyncLogRequestUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;

public class BackwardSyncLogRequestReduceBuilderFactory extends
    AbstractPbSyncLogReduceBuilderFactory<PbBackwardSyncLogRequestUnit, PbBackwardSyncLogsRequest> {
  @Override
  public BackwardSyncLogRequestReduceBuilder generate(InstanceId instanceId, int max) {
    return new BackwardSyncLogRequestReduceBuilder(instanceId, max);
  }

  @Override
  public BackwardSyncLogRequestReduceBuilder generate(long requestId, int unitsCount,
      InstanceId instanceId, MethodCallback<PbBackwardSyncLogsRequest> callback) {
   
    return null;
  }
}

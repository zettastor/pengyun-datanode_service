
package py.datanode.segment.datalog.sync.log.reduce;

import py.instance.InstanceId;
import py.netty.core.MethodCallback;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;

public class PbAsyncLogsBatchRequestReduceBuilderFactory extends
    AbstractPbSyncLogReduceBuilderFactory<PbAsyncSyncLogBatchUnit, PbAsyncSyncLogsBatchRequest> {
  @Override
  public PbAsyncLogsBatchRequestReduceBuilder generate(InstanceId instanceId, int max) {
    return new PbAsyncLogsBatchRequestReduceBuilder(instanceId, max);
  }

  @Override
  public PbAsyncLogsBatchRequestReduceBuilder generate(long requestId, int unitsCount,
      InstanceId instanceId, MethodCallback<PbAsyncSyncLogsBatchRequest> callback) {
    return null;
  }
}


package py.datanode.segment.datalog.sync.log.reduce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.instance.InstanceId;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;

public class PbAsyncLogsBatchRequestReduceBuilder extends
    PbSyncLogReduceBuilder<PbAsyncSyncLogBatchUnit, PbAsyncSyncLogsBatchRequest> {
  private static final Logger logger = LoggerFactory
      .getLogger(BackwardSyncLogRequestReduceBuilder.class);
  private final PbAsyncSyncLogsBatchRequest.Builder builder = PbAsyncSyncLogsBatchRequest
      .newBuilder();

  public PbAsyncLogsBatchRequestReduceBuilder(InstanceId destination,
      int maxReduceCacheLength) {
    super(destination, maxReduceCacheLength);
  }

  @Override
  synchronized void enqueue(PbAsyncSyncLogBatchUnit unit) {
    builder.addSegmentUnits(unit);
  }

  @Override
  public PbAsyncSyncLogsBatchRequest build() {
    builder.setRequestId(RequestIdBuilder.get());
    return builder.build();
  }
}



package py.datanode.service.io.read.merge;

import java.util.concurrent.CompletableFuture;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.instance.InstanceId;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog.PbReadRequest;

public interface MergeReadCommunicator {
  CompletableFuture<PyReadResponse> read(InstanceId target, PbReadRequest request);

  CompletableFuture<Void> addOrUpdateLogs(InstanceId target, Iterable<DataLog> logs);

}

/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

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

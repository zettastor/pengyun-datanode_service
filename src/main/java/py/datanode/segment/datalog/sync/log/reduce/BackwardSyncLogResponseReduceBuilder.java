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
import py.proto.Broadcastlog.PbBackwardSyncLogResponseUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;

public class BackwardSyncLogResponseReduceBuilder extends
    PbSyncLogReduceBuilder<PbBackwardSyncLogResponseUnit, PbBackwardSyncLogsResponse> {
  private final long requestId;
  private final PbBackwardSyncLogsResponse.Builder builder = PbBackwardSyncLogsResponse
      .newBuilder();
  private final MethodCallback callback;

  public BackwardSyncLogResponseReduceBuilder(long requestId, int unitsCount,
      InstanceId destination, MethodCallback callback) {
    super(destination, unitsCount);
    this.requestId = requestId;
    this.callback = callback;
  }

  public MethodCallback getCallback() {
    return callback;
  }

  @Override
  synchronized void enqueue(PbBackwardSyncLogResponseUnit unit) {
    builder.addUnits(unit);
  }

  @Override
  public PbBackwardSyncLogsResponse build() {
    builder.setRequestId(requestId);
    builder.setInstanceId(getDestination().getId());
    return builder.build();
  }

  @Override
  public boolean submitUnit(PbBackwardSyncLogResponseUnit unit) {
    if (hasDone()) {
      return false;
    } else if (fillMessage(1)) {
      enqueue(unit);
      return true;
    } else {
      return false;
    }
  }
}

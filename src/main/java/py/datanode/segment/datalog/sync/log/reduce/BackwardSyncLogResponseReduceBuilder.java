/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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

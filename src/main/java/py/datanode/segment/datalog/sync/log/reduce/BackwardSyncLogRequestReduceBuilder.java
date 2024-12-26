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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.instance.InstanceId;
import py.proto.Broadcastlog.PbBackwardSyncLogRequestUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;

public class BackwardSyncLogRequestReduceBuilder extends
    PbSyncLogReduceBuilder<PbBackwardSyncLogRequestUnit, PbBackwardSyncLogsRequest> {
  private static final Logger logger = LoggerFactory
      .getLogger(BackwardSyncLogRequestReduceBuilder.class);
  private final PbBackwardSyncLogsRequest.Builder builder = PbBackwardSyncLogsRequest.newBuilder();

  public BackwardSyncLogRequestReduceBuilder(InstanceId destination,
      int maxReduceCacheLength) {
    super(destination, maxReduceCacheLength);
  }

  @Override
  synchronized void enqueue(PbBackwardSyncLogRequestUnit unit) {
    builder.addUnits(unit);
  }

  @Override
  public PbBackwardSyncLogsRequest build() {
    builder.setRequestId(RequestIdBuilder.get());
    return builder.build();
  }
}

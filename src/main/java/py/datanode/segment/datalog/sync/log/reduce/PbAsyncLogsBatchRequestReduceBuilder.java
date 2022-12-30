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

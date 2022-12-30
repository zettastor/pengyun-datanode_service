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

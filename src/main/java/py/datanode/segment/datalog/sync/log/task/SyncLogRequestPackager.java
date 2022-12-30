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

package py.datanode.segment.datalog.sync.log.task;

import py.PbRequestResponseHelper;
import py.app.context.AppContext;
import py.archive.segment.SegId;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPageStatus;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogReduceCollector;
import py.datanode.segment.datalogbak.catchup.PclDrivingType;
import py.instance.InstanceId;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;

public class SyncLogRequestPackager implements SyncLogTask {
  private final SyncLogReduceCollector<PbAsyncSyncLogBatchUnit, 
      PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest> reduceCollector;
  private final SegmentUnit segmentUnit;
  private final InstanceId destination;
  private final AppContext context;

  public SyncLogRequestPackager(
      SyncLogReduceCollector<PbAsyncSyncLogBatchUnit, PbAsyncSyncLogsBatchRequest,
          PbAsyncSyncLogsBatchRequest> reduceCollector,
      SegmentUnit segmentUnit, InstanceId destination, AppContext context) {
    this.reduceCollector = reduceCollector;
    this.segmentUnit = segmentUnit;
    this.destination = destination;
    this.context = context;
  }

  @Override
  public SyncLogTaskType type() {
    return SyncLogTaskType.PACKAGER;
  }

  @Override
  public SegId getSegId() {
    return segmentUnit.getSegId();
  }

  @Override
  public boolean process() {
    return true;
  }

  @Override
  public boolean reduce() {
    PbAsyncSyncLogBatchUnit.Builder builder = PbAsyncSyncLogBatchUnit.newBuilder();
    builder.setVolumeId(segmentUnit.getSegId().getVolumeId().getId());
    builder.setSegIndex(segmentUnit.getSegId().getIndex());
    builder.setPpl(segmentUnit.getSegmentLogMetadata().getPlId());
    builder.setPcl(segmentUnit.getSegmentLogMetadata().getClId());
    builder.setMembership(PbRequestResponseHelper
        .buildPbMembershipFrom(segmentUnit.getSegmentUnitMetadata().getMembership()));
    PclDrivingType pclDrivingType = segmentUnit.getPclDrivingType(context.getInstanceId());
    if (pclDrivingType == PclDrivingType.VotingSecondary) {
      builder.setPreprimarySid(segmentUnit.getPreprimaryDrivingSessionId());
    }
    builder.setMyself(context.getInstanceId().getId());
    builder.setSegmentStatus(
        segmentUnit.getSegmentUnitMetadata().getStatus().getPbSegmentUnitStatus());
    if (isCatchingUpLog()) {
      builder.setCatchUpLogId(segmentUnit.getSecondaryCopyPageManager().getCatchUpLog().getLogId());
    }

    reduceCollector.submit(destination, builder.build());
    return true;
  }

  private boolean isCatchingUpLog() {
    SecondaryCopyPageManager manager = segmentUnit.getSecondaryCopyPageManager();
    if (manager != null && manager.getCopyPageStatus() == CopyPageStatus.CatchupLog) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return "SyncLogRequestPackager{"
        + "reduceCollector=" + reduceCollector
        + ", segmentUnit=" + segmentUnit
        + ", destination=" + destination
        + ", context=" + context
        + '}';
  }

  @Override
  public InstanceId homeInstanceId() {
    return context.getInstanceId();
  }
}

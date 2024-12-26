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

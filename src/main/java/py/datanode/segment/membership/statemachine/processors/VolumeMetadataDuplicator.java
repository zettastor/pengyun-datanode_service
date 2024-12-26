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

package py.datanode.segment.membership.statemachine.processors;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.struct.EndPoint;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.BroadcastRequest;

public class VolumeMetadataDuplicator extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(VolumeMetadataDuplicator.class);
  private long defaultFixedDelay;

  public VolumeMetadataDuplicator(StateProcessingContext context) {
    super(context);

  }

  public StateProcessingResult process(StateProcessingContext context) {
    defaultFixedDelay = cfg.getRateOfDuplicatingVolumeMetadataJsonMs();
    SegmentUnit segmentUnit = context.getSegmentUnit();
    if (segmentUnit == null) {
      logger.warn(" segment unit {} not exist ", context.getSegId());
      return getFailureResultWithRandomizedDelay(context, context.getStatus(), defaultFixedDelay);
    }

    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    SegmentUnitStatus currentStatus = metadata.getStatus();

    if (!SegmentUnitStatus.Primary.equals(currentStatus)) {
      logger.info(
          "context: {} corresponding segment unit is not in Primary status. abandon the context ",
          context);

      return getAbandonedResult(context);
    }

    SegId segId = context.getSegId();
    SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();

    InstanceId primaryInstanceId = currentMembership.getPrimary();

    if (!primaryInstanceId.equals(appContext.getInstanceId())) {
      logger.warn(
          "the segment unit {} is a primary but its membership indicates "
              + "that the primary is a different instance:{} {} abandon the current context",
          segId, primaryInstanceId);
      return getAbandonedResult(context);
    }

    String volumeMetadataJson = metadata.getVolumeMetadataJson();

    if (volumeMetadataJson == null) {
      return getSuccesslResultWithFixedDelay(context, SegmentUnitStatus.Primary,
          defaultFixedDelay);
    }

    List<EndPoint> endpoints = DataNodeRequestResponseHelper
        .buildEndPoints(instanceStore, currentMembership, true, appContext.getInstanceId());
    int quorumSize = metadata.getVolumeType().getVotingQuorumSize();

    int quorumExcludingPrimary = quorumSize - 1;
    if (endpoints.size() < quorumExcludingPrimary) {
      logger.warn(
          "Can't extend the primary lease because there are no enough " 
              + "members to do so. membership:{} available endpoint: {}",
          currentMembership, endpoints);
      return getSuccesslResultWithFixedDelay(context, SegmentUnitStatus.Primary,
          defaultFixedDelay);
    }

    logger.trace("broadcast volumeMetadata {} to other members in the group", volumeMetadataJson);

    try {
      BroadcastRequest updateSegmentUnitRequest = DataNodeRequestResponseHelper
          .buildUpdateSegmentUnitVolumeMetadataJsonBroadcastRequest(segmentUnit.getSegId(),
              currentMembership,
              volumeMetadataJson);

      DataNodeServiceAsyncClientWrapper asyncDataNodeClient = new DataNodeServiceAsyncClientWrapper(
          dataNodeAsyncClientFactory);
      asyncDataNodeClient.broadcast(segmentUnit,
          cfg.getSecondaryLeaseInPrimaryMs(), updateSegmentUnitRequest,
          cfg.getDataNodeRequestTimeoutMs(),
          quorumExcludingPrimary, true, endpoints);
      logger.debug("We have received a quorum of responses for updating volume metadata");
    } catch (Exception e) {
      String errString = "Can't update the volume metadata in the segment unit metadata "
          + segmentUnit.getSegId();
      logger.error(errString, e);
    }
    return getSuccesslResultWithFixedDelay(context, SegmentUnitStatus.Primary, defaultFixedDelay);
  }

}

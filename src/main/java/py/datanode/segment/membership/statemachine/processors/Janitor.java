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

package py.datanode.segment.membership.statemachine.processors;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.archive.RawArchive;
import py.datanode.checksecondaryinactive.CheckSecondaryInactive;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.datanode.segment.membership.statemachine.checksecondaryinactive.NewSegmentUnitExpirationThreshold;
import py.exception.GenericThriftClientFactoryException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.DataNodeService;

public class Janitor extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(Janitor.class);

  private long defaultFixedDelay;

  public Janitor(StateProcessingContext context) {
    super(context);
   
   
  }

  public StateProcessingResult process(StateProcessingContext context) {
    defaultFixedDelay = cfg.getRateOfJanitorExecutionMs();
    SegmentUnit segmentUnit = context.getSegmentUnit();

    if (segmentUnit == null || segmentUnit.isDataNodeServiceShutdown()) {
      logger.warn(" segment unit {} not exist or dataNode service shutdown {}", context.getSegId(),
          segmentUnit);
      return getFailureResultWithRandomizedDelay(context, context.getStatus(), defaultFixedDelay);
    }

    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    SegmentUnitStatus currentStatus = metadata.getStatus();

    if (!SegmentUnitStatus.Primary.equals(currentStatus)) {
      logger.info(
          "context: {} corresponding segment unit is not in Primary "
              + "status. Abandon the current context ",
          context);
     
      return getAbandonedResult(context);
    }

    SegId segId = context.getSegId();
    SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();

    InstanceId primaryInstanceId = currentMembership.getPrimary();

    if (!primaryInstanceId.equals(appContext.getInstanceId())) {
      logger.warn("the segment unit {} is a primary but its membership indicates "
              + "that the primary is a different instance: {} abandon the current context", segId,
          primaryInstanceId);
     
      return getAbandonedResult(context);
    }

    if (currentMembership.getInactiveSecondaries().size() == 0) {
      return getSuccesslResultWithFixedDelay(context, SegmentUnitStatus.Primary,
          defaultFixedDelay);
    }

    InstanceId rollBackInstanceId = segmentUnit.getSegmentUnitMetadata()
        .getInstanceIdOfRollBackJs();
    if (rollBackInstanceId != null) {
      try {
        DataNodeService.Iface dataNodeClient = dataNodeSyncClientFactory
            .generateSyncClient(instanceStore.get(rollBackInstanceId).getEndPoint());
        dataNodeClient.ping();
        return getSuccesslResultWithFixedDelay(context, SegmentUnitStatus.Primary,
            defaultFixedDelay);
      } catch (GenericThriftClientFactoryException | TException e) {
        segmentUnit.getSegmentUnitMetadata().clearRollBackingJs();
        logger.error("rollBackInstanceId {} can not connect , i will request new one",
            rollBackInstanceId);
      }
    }
    if (informationCenterClientFactory != null) {
      RawArchive archive = segmentUnit.getArchive();
      CheckSecondaryInactive checkSecondaryInactive = NewSegmentUnitExpirationThreshold
          .getExpirationThreshold(archive);
      if (checkSecondaryInactive != null) {
        segmentUnit.requestNewSegmentUnit(informationCenterClientFactory,
            cfg.getRequestNewSegmentUnitExpirationThresholdMs(), checkSecondaryInactive);
      } else {
        logger.error("the checkSecondaryInactive is null for {} ,can not request new SegmentUnit ",
            archive);
      }
    } else {
      logger.warn(
          "ControlCenter client factory is null, janitor won't request a new segment for segId={}",
          segId);
    }

    return getSuccesslResultWithFixedDelay(context, SegmentUnitStatus.Primary, defaultFixedDelay);
  }

}

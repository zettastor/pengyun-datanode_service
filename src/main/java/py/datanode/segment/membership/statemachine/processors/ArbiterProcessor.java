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
import py.common.struct.Pair;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.exception.GenericThriftClientFactoryException;
import py.exception.LeaseExtensionFrozenException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.membership.SegmentMembershipHelper;
import py.thrift.datanode.service.ArbiterPokePrimaryRequest;
import py.thrift.datanode.service.ArbiterPokePrimaryResponse;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.datanode.service.WrongPrePrimarySessionExceptionThrift;
import py.thrift.share.InvalidMembershipExceptionThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.StaleMembershipExceptionThrift;
import py.thrift.share.YouAreNotInMembershipExceptionThrift;

public class ArbiterProcessor extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(ArbiterProcessor.class);
  private static final int DEFAULT_FIXED_DELAY = 2000;
  private static final int MAX_RANDOM_DELAY = 2000;
  private static final int DELAY_DUE_TO_NO_POTENTIAL_PRIMARY_ID_SET = 2000;

  public ArbiterProcessor(StateProcessingContext context) {
    super(context);
  }

  /**
   *true if processing has succeeded.
   */
  public StateProcessingResult process(StateProcessingContext context) {
    try {
      return internalProcess(context);
    } catch (Exception e) {
      logger.error("caught an exception when voting for seg {} ", context.getSegId(), e);
     
     
      SegmentUnitMetadata metadata = context.getSegmentUnit().getSegmentUnitMetadata();
      return getFailureResultWithRandomizedDelay(context, metadata.getStatus(), MAX_RANDOM_DELAY);
    }
  }

  private StateProcessingResult internalProcess(StateProcessingContext context) throws Exception {
    SegmentUnit segmentUnit = context.getSegmentUnit();

    if (segmentUnit == null) {
      logger.warn(" segment unit {} not exist ", context.getSegId());
      return getFailureResultWithRandomizedDelay(context, context.getStatus(), MAX_RANDOM_DELAY);
    }

    SegmentUnitStatus currentStatus = segmentUnit.getSegmentUnitMetadata().getStatus();
    logger.debug("arbiter processer: {} ", context);

    SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
    InstanceId primaryInstanceId;
    if (SegmentUnitStatus.PreArbiter.equals(currentStatus)) {
      if (!segmentUnit.potentialPrimaryIdSet()) {
        logger.warn("Can't get potential primary id because it has not been set. Sleep {}  ms",
            DELAY_DUE_TO_NO_POTENTIAL_PRIMARY_ID_SET);
        return getFailureResultWithFixedDelay(context, SegmentUnitStatus.PreArbiter,
            DELAY_DUE_TO_NO_POTENTIAL_PRIMARY_ID_SET);
      }

      primaryInstanceId = new InstanceId(segmentUnit.getPotentialPrimaryId());
    } else if (SegmentUnitStatus.Arbiter.equals(currentStatus)) {
     
      primaryInstanceId = currentMembership.getPrimary();
    } else {
      logger.debug("context: {} corresponding segment unit is at {} status. Do nothing ", context,
          currentStatus);
     
      return getFailureResultWithZeroDelay(context, currentStatus);
    }

    Instance primaryInstance = instanceStore.get(primaryInstanceId);
    if (primaryInstance == null) {
      logger.warn("Can't get primary instance {} from instance store.The current status is {}",
          primaryInstanceId, currentStatus);
     
      return getFailureResultWithRandomizedDelay(context, currentStatus, MAX_RANDOM_DELAY);
    }

    if (segmentUnit.myLeaseExpired(currentStatus)) {
      archiveManager.updateSegmentUnitMetadata(context.getSegId(), null, SegmentUnitStatus.Start);
      return getSuccesslResultWithZeroDelay(context, SegmentUnitStatus.Start);
    }

    InstanceId myself = appContext.getInstanceId();
    return pokePrimary(context, segmentUnit, currentStatus, currentMembership, primaryInstance,
        myself);
  }

  private StateProcessingResult pokePrimary(StateProcessingContext context, SegmentUnit segUnit,
      SegmentUnitStatus currentStatus,
      SegmentMembership currentMembership, Instance primaryInstance,
      InstanceId myself) {
    ArbiterPokePrimaryRequest request = DataNodeRequestResponseHelper
        .buildArbiterPokePrimaryRequest(segUnit.getSegId(),
            currentMembership, myself.getId(), currentStatus);

    SegmentMembershipThrift returnedMembershipThrift = null;
    SegmentUnitStatus newStatus = null;

    DataNodeService.Iface client = null;
    ArbiterPokePrimaryResponse response = null;
    try {
      client = dataNodeSyncClientFactory.generateSyncClient(primaryInstance.getEndPoint(),
          cfg.getDataNodeRequestTimeoutMs());
      response = client.arbiterPokePrimary(request);
     
     
      if (currentStatus != SegmentUnitStatus.PreArbiter) {
        returnedMembershipThrift = response.getLatestMembership();
      }
      segUnit.extendMyLease();
    } catch (GenericThriftClientFactoryException e) {
      logger.warn("Caught an GenerincThriftClientFactoryException. {} "
              + "Can't get a client to the primary {} request: {}. Change to Start",
          segUnit.getSegId(), primaryInstance, request);
      newStatus = SegmentUnitStatus.Start;
    } catch (ServiceHavingBeenShutdownThrift | SegmentNotFoundExceptionThrift e) {
      logger.warn(
          "{} can't find the segment unit or service " 
              + "has been shutdown. {} Changing my status to start {}",
          primaryInstance, segUnit.getSegId(), request);
      newStatus = SegmentUnitStatus.Start;
    } catch (StaleMembershipExceptionThrift e) {
      logger.warn("{} has lower (epoch) than the primary {} "
          + ". Change status Start to catch up logs {} ", myself, primaryInstance, request);
     
      if (currentStatus == SegmentUnitStatus.Arbiter) {
        returnedMembershipThrift = e.getLatestMembership();
      }
      newStatus = SegmentUnitStatus.Start;
    } catch (NotPrimaryExceptionThrift e) {
      logger.warn(
          "primary {} doesn't believe it is the primary" 
              + " any more. Changing {} status from Secondary to Start {}",
          primaryInstance, myself, request, e);
      returnedMembershipThrift = e.getMembership();
     
     
      newStatus = SegmentUnitStatus.Start;
    } catch (WrongPrePrimarySessionExceptionThrift e) {
      logger.warn("{} The preprimary {} does not believe I {} is at the same session with it {}."
              + "Change the status to Start so as to restart the primary election process",
          segUnit.getSegId(), primaryInstance, myself, e.getDetail());
      newStatus = SegmentUnitStatus.Start;
    } catch (YouAreNotInMembershipExceptionThrift e) {
      logger.error("I am not at the primary's membership "
              + "PreArbiter/Arbiter: {} "
              + " primary: {} "
              + " my membership: {} "
              + " the primary membership:{} and request {} ",
          myself, primaryInstance, currentMembership, e.getMembership(), request);
      newStatus = SegmentUnitStatus.Start;
    } catch (InvalidMembershipExceptionThrift e) {
      logger.error(
          "**************** It is impossible that" 
              + " InvalidMembershipException can be received at a secondary side *******");
      logger.error("I am not at the primary's membership "
              + "PreArbiter/Arbiter: {} "
              + " primary: {} "
              + " my membership: {} "
              + " the primary membership:{} and request {} ",
          myself, primaryInstance, currentMembership, e.getLatestMembership(), request);
      logger.error("Really don't know how to do, change my status to Start");
      newStatus = SegmentUnitStatus.Start;
    } catch (LeaseExtensionFrozenException e) {
      logger.trace(
          "Myself {} can't extend lease but still go " 
              + "ahead to process the lease. the primary is {} and segId is {} ",
          myself, primaryInstance, segUnit.getSegId());
     
    } catch (TException e) {
      logger.warn(
          "Caught an mystery exception when syncing log from {} do nothing and retry later={}",
          primaryInstance, request, e);
      return getFailureResultWithRandomizedDelay(context, currentStatus, MAX_RANDOM_DELAY);
    } finally {
      dataNodeSyncClientFactory.releaseSyncClient(client);
    }

    SegmentMembership returnedMembership = null;
    if (returnedMembershipThrift != null) {
      Pair<SegId, SegmentMembership> pair = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(
              returnedMembershipThrift);
      returnedMembership = pair.getSecond();
      if (!pair.getFirst().equals(segUnit.getSegId())) {
        logger.error("what is happening here ?? {} {} {}", request, response, segUnit);
      }
      if (returnedMembership.compareVersion(currentMembership) <= 0) {
        returnedMembership = null;
      } else if (SegmentMembershipHelper.okToUpdateToHigherMembership(
          returnedMembership, currentMembership, myself,
          segUnit.getSegmentUnitMetadata().getVolumeType().getNumMembers())) {
        logger.info("the primary has sent a membership with higher generation. ");
      } else {
        logger.warn(
            "My newStatus is supposed to be {} but since it is not " 
                + "ok to update to membership {}  my membership is {}, " 
                + "change my status to Deleting",
            newStatus, returnedMembership, currentMembership);
       
       
        returnedMembership = null;
        newStatus = SegmentUnitStatus.Deleting;
      }
    }

    if (returnedMembership != null || newStatus != null) {
      try {
        archiveManager.updateSegmentUnitMetadata(segUnit.getSegId(), returnedMembership, newStatus);
      } catch (Exception e) {
        logger.error("For some reason, we can't update segment unit metadata", e);
        return getFailureResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
      }
    }

    if (newStatus != null) {
     
      return getFailureResultWithZeroDelay(context, newStatus);
    } else {
      return getSuccesslResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
    }
  }
}

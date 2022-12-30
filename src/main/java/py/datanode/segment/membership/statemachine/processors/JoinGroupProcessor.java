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

import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.membership.SegmentMembershipHelper;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.JoiningGroupRequest;
import py.thrift.datanode.service.JoiningGroupRequestDeniedExceptionThrift;
import py.thrift.datanode.service.JoiningGroupResponse;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.share.InstanceNotExistsExceptionThrift;
import py.thrift.share.InvalidMembershipExceptionThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;

public class JoinGroupProcessor extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(JoinGroupProcessor.class);
  // 1000 ms;
  private static final int DEFAULT_FIXED_DELAY = 1000;
  // 2000 ms;
  private static final int MAX_RANDOM_DELAY = 2000;

  public JoinGroupProcessor(StateProcessingContext context) {
    super(context);
  }

  public StateProcessingResult process(StateProcessingContext context) {
    SegmentUnit segmentUnit = context.getSegmentUnit();

    if (segmentUnit == null) {
      logger.warn(" segment unit {} not exist ", context.getSegId());
      return getFailureResultWithRandomizedDelay(context, context.getStatus(), MAX_RANDOM_DELAY);
    }

    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    SegmentUnitStatus currentStatus = metadata.getStatus();
    final SegId segId = segmentUnit.getSegId();
    final InstanceId myself = appContext.getInstanceId();

    if (!SegmentUnitStatus.SecondaryApplicant.equals(currentStatus)) {
      logger.warn(
          "context: {} corresponding segment unit is at {} instead of"
              + " SecondaryApplicant status. Do nothing ",
          context, currentStatus);

      return getFailureResultWithZeroDelay(context, currentStatus);
    }

    SegmentMembership currentMembership = metadata.getMembership();
    InstanceId primaryInstanceId = currentMembership.getPrimary();
    Validate.notNull(primaryInstanceId);

    long plId = 0;
    long pclId = 0;
    if (!segmentUnit.isArbiter()) {
      SegmentLogMetadata segmentLogMetadata = context.getSegmentLogMetadata();
      LogImage logImage = segmentLogMetadata.getLogImage(0);
      plId = logImage.getPlId();
      pclId = logImage.getClId();
    }

    boolean isSecondaryCandidate = segmentUnit.getSegmentUnitMetadata().isSecondaryCandidate();
    JoiningGroupRequest request = DataNodeRequestResponseHelper.buildJoiningGroupRequest(segId,
        currentMembership,
        myself, plId, pclId, segmentUnit.isArbiter());
    request.setSecondaryCandidate(isSecondaryCandidate);

    SegmentUnitStatus newSegmentUnitStatus = null;
    SegmentMembership higherMembership = null;
    JoinGroupContext myContext = (JoinGroupContext) context;
    boolean needToStartLease = false;
    DataNodeService.Iface dataNodeClient = null;
    Instance primaryInstance = null;
    try {
      primaryInstance = instanceStore.get(primaryInstanceId);
      if (primaryInstance == null) {
        logger.warn("Can't get primary {}  from instance store.", primaryInstanceId);

        throw new InstanceNotExistsExceptionThrift();
      }

      dataNodeClient = getClient(primaryInstance.getEndPoint(), cfg.getDataNodeRequestTimeoutMs());

      JoiningGroupResponse response = dataNodeClient.canJoinGroup(request);
      logger.info(
          "{} has allowed me to join the group. Ready to transit to PreSecondary"
              + " state and its cl id is {} ",
          primaryInstanceId, response.getSfal());

      higherMembership = getHigherMembership(currentMembership, response.getMembership());
      if (segmentUnit.isArbiter()) {
        newSegmentUnitStatus = SegmentUnitStatus.Arbiter;
      } else {
        segmentUnit.setCopyPageFinished(false);
        segmentUnit.setPotentialPrimaryId(primaryInstanceId.getId());
        segmentUnit.setParticipateVotingProcess(false);
        newSegmentUnitStatus = SegmentUnitStatus.PreSecondary;
        if (isSecondaryCandidate) {
          SegmentMembership highestMembership =
              higherMembership == null ? currentMembership : higherMembership;

          if (!highestMembership.isSecondaryCandidate(myself) && highestMembership.contain(
              myself)) {
            segmentUnit.forgetAboutBeingOneSecondaryCandidate();
          }
        }
      }

      needToStartLease = true;
      return getSuccesslResultWithZeroDelay(context, newSegmentUnitStatus);
    } catch (JoiningGroupRequestDeniedExceptionThrift e) {
      myContext.incrementDeniedJoiningGroupTimes();
      logger.warn("{} has denied {} {} to join the group. This is {} times.", primaryInstanceId,
          myself, segId,
          myContext.deniedJoiningGroupTimes);
      if (myContext.reachMaxTimesOfDeniedJoiningGroupTimes()) {
        logger.error(
            "A secondary of a segment unit {} has been denied for joining the group. "
                + "Changing its status to deleting",
            segId);
        newSegmentUnitStatus = SegmentUnitStatus.Deleting;
        return getFailureResultWithZeroDelay(myContext, SegmentUnitStatus.Deleting);
      } else {
        return getFailureResultWithBackoffDelay(myContext, SegmentUnitStatus.SecondaryApplicant);
      }
    } catch (Exception e) {
      logger.warn("{} has thrown an exception", primaryInstanceId, e);
      higherMembership = getHigherMembership(currentMembership,
          DataNodeRequestResponseHelper.getSegmentMembershipFromThrowable(e));

      if (higherMembership != null) {
        logger.info("since the primary has the higher membership, let's move to Start status to "
            + "either electing a new primary or rejoining a group.");
        newSegmentUnitStatus = SegmentUnitStatus.Start;
        return getFailureResultWithZeroDelay(myContext, SegmentUnitStatus.Start);
      } else {
        logger.info("Can't get the higher membership. let's check the exception it throws ");
        if (e instanceof SegmentNotFoundExceptionThrift || e instanceof NotPrimaryExceptionThrift
            || e instanceof InvalidMembershipExceptionThrift
            || e instanceof ServiceHavingBeenShutdownThrift

            || e instanceof GenericThriftClientFactoryException
            || e instanceof InstanceNotExistsExceptionThrift) {
          logger.warn("I ({}) was told that {} is the primary but it gave a different answer. "
                  + "Let's broadcast to all members to get the latest membership",
              segmentUnit.getSegmentUnitMetadata(), primaryInstance);

          DataNodeServiceAsyncClientWrapper dataNodeAsyncClient =
              new DataNodeServiceAsyncClientWrapper(dataNodeAsyncClientFactory);

          SegmentMembership hightestMembershipFromBroadcast =
              broadcastToAllMembersToGetLatestMembership(
              segId, currentMembership, myself, dataNodeAsyncClient, 1, cfg);

          if (currentMembership.compareTo(hightestMembershipFromBroadcast) < 0) {
            logger.debug(
                "The returned membership  in {}. is higher than my current membership {}. "
                    + "Let's retry joing the group again",
                hightestMembershipFromBroadcast, currentMembership);

            higherMembership = hightestMembershipFromBroadcast;
            return getFailureResultWithFixedDelay(myContext, SegmentUnitStatus.SecondaryApplicant,
                DEFAULT_FIXED_DELAY);
          } else {
            logger.warn(
                "can't get the latest membership from the members in {}. The hightest membership"
                    + " from members is {} "
                    + "Delay a while and move it to Start status.  ", currentMembership,
                hightestMembershipFromBroadcast);

            newSegmentUnitStatus = SegmentUnitStatus.Start;
            return getFailureResultWithFixedDelay(myContext, SegmentUnitStatus.Start,
                DEFAULT_FIXED_DELAY);
          }

        } else {
          logger.warn("Caught some exceptions that need to be retried");
          if (myContext.reachMaxTimesOfFailures()) {
            logger.error(
                "Joining {} group has been through {} failures. Give up and give its"
                    + " status to Start",
                segId, myContext.getFailureTimes());
            newSegmentUnitStatus = SegmentUnitStatus.Start;
            return getFailureResultWithZeroDelay(myContext, SegmentUnitStatus.Start);
          } else {
            logger.warn("retry the joining request");
            return getFailureResultWithBackoffDelay(myContext,
                SegmentUnitStatus.SecondaryApplicant);
          }
        }
      }
    } finally {
      dataNodeSyncClientFactory.releaseSyncClient(dataNodeClient);
      if (higherMembership != null) {
        if (!SegmentMembershipHelper.okToUpdateToHigherMembership(higherMembership,
            currentMembership, myself,
            segmentUnit.getSegmentUnitMetadata().getVolumeType().getNumMembers())) {
          higherMembership = null;
          newSegmentUnitStatus = SegmentUnitStatus.Deleting;
          logger.warn("Move {} to Deleting", segmentUnit.getSegmentUnitMetadata());
        } else {
          logger.info("Updating {}  membershiph to {} ", segmentUnit.getSegmentUnitMetadata(),
              higherMembership);
        }
      }

      if (higherMembership != null || newSegmentUnitStatus != null) {
        try {
          archiveManager.updateSegmentUnitMetadata(segId, higherMembership, newSegmentUnitStatus);
          if (needToStartLease) {
            segmentUnit.startMyLease(cfg.getSecondaryLeaseMs());
          }
        } catch (Exception e) {
          logger.warn(
              "caught an exception when updating segment membership from " + currentMembership
                  + " to "
                  + higherMembership + " and update the current status from " + currentStatus
                  + " to the new status :" + newSegmentUnitStatus, e);
          return getFailureResultWithFixedDelay(context, SegmentUnitStatus.SecondaryApplicant,
              DEFAULT_FIXED_DELAY);
        }
      }
    }
  }

  private SegmentMembership broadcastToAllMembersToGetLatestMembership(SegId segId,
      SegmentMembership membership,
      InstanceId myInstanceId, DataNodeServiceAsyncClientWrapper dataNodeAsyncClient,
      int quorumSize,
      DataNodeConfiguration cfg) {
    List<EndPoint> endpoints = RequestResponseHelper.buildEndPoints(instanceStore, membership,
        true);
    try {
      BroadcastRequest request = RequestResponseHelper
          .buildGiveMeYourMembershipRequest(RequestIdBuilder.get(), segId, membership,
              myInstanceId.getId());

      BroadcastResult response = dataNodeAsyncClient
          .broadcast(request, cfg.getDataNodeRequestTimeoutMs(), quorumSize, false, endpoints);

      logger.info("We have received a quorum to get membership");
      return response.getHighestMembership();
    } catch (Exception e) {
      logger.warn("caught an exception when trying to get the latest membership", e);
      return null;
    }
  }

}

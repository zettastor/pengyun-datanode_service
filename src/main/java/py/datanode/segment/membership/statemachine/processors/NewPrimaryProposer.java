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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
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
import py.datanode.exception.FailedToSelectPrimaryException;
import py.datanode.exception.NoAvailableCandidatesForNewPrimaryException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.datanode.service.DataNodeServiceImpl.BecomePrimaryPriorityBonus;
import py.exception.FailedToSendBroadcastRequestsException;
import py.exception.QuorumNotFoundException;
import py.exception.SnapshotVersionMissMatchForMergeLogsException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.membership.SegmentMembershipHelper;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastResponse;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.FailedToKickOffPrimaryExceptionThrift;
import py.thrift.datanode.service.InvalidSegmentUnitStatusExceptionThrift;
import py.thrift.datanode.service.KickOffPotentialPrimaryRequest;
import py.thrift.datanode.service.KickOffPotentialPrimaryResponse;
import py.thrift.datanode.service.MakePrimaryDecisionRequest;
import py.thrift.datanode.service.MakePrimaryDecisionResponse;
import py.thrift.datanode.service.NoAvailableCandidatesForNewPrimaryExceptionThrift;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.volume.VolumeType;

public class NewPrimaryProposer extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(NewPrimaryProposer.class);
  private static final int DEFAULT_FIXED_DELAY = 3000;
  private static final int MAX_RANDOM_DELAY = 5000;
  private static final Random randomForIncreasingMaxN = new Random(System.currentTimeMillis());
  private static final long MAX_SOCKET_TIMEOUT = 2000;

  private enum RunningStatus {
    Start, PromisedModerator, PrimaryExists
  }

  private class Proposal {
    int proposalN;
    long value;

    Proposal(int proposalN, long value) {
      super();
      this.proposalN = proposalN;
      this.value = value;
    }
  }

  public NewPrimaryProposer(StateProcessingContext context) {
    super(context);
    Validate.isTrue(
        context.getStatus() == SegmentUnitStatus.Start
            && context.getKey().getStateProcessingType() == null);
  }

  public StateProcessingResult process(StateProcessingContext context) {
    try {
      return internalProcess(context);
    } catch (Exception e) {
      logger.error("caught an exception when voting for seg {}", context.getSegId(), e);

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

    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    SegmentUnitStatus currentStatus = metadata.getStatus();
    if (!SegmentUnitStatus.Start.equals(currentStatus)) {
      logger.info(
          "context: {} corresponding segment unit is in {} instead of Start status. Do nothing ",
          context,
          currentStatus);
      if (MigrationUtils.stopAndRemoveResourceAfterMigrationFail(context.getSegId(), segmentUnit,
          mutationLogManager,
          logPersister, catchupLogEngine)) {
        return getFailureResultWithZeroDelay(context, currentStatus);
      } else {
        return getFailureResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
      }
    }

    if (segmentUnit.isPauseVotingProcess()) {
      logger.info("voting process paused, Do nothing.");
      return getFailureResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
    }

    if (!MigrationUtils.stopAndRemoveResourceAfterMigrationFail(context.getSegId(), segmentUnit,
        mutationLogManager, logPersister, catchupLogEngine)) {
      logger.info(
          "context: {} corresponding segment unit is in {} failed to clean up migration mess",
          context,
          currentStatus);

      return getFailureResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
    }

    SegmentMembership currentMembership = metadata.getMembership();
    InstanceId myself = appContext.getInstanceId();
    if (!currentMembership.contain(myself)

        || currentMembership.isSecondaryCandidate(myself)) {
      logger.info(
          " {} is not in the group's membership {} . Changing its status from "
              + "Start to SecondaryApplicant",
          appContext.getInstanceId(), currentMembership);
      archiveManager.updateSegmentUnitMetadata(context.getSegId(), null,
          SegmentUnitStatus.SecondaryApplicant);
      return getFailureResultWithZeroDelay(context, SegmentUnitStatus.SecondaryApplicant);
    }

    if (currentMembership.getSegmentVersion().getEpoch() == 0 && (
        currentMembership.isSecondary(myself)
            || currentMembership.isArbiter(myself)) && segmentUnit.isJustCreated(
        cfg.getSecondaryFirstVotingTimeoutMs())) {
      logger.info("this is the first to create a segment unit {} "
              + "which is a secondary in the membership. Let's sleep 3000 ms to all the"
              + " primary to vote ",
          segmentUnit.getSegId());
      return getSuccesslResultWithFixedDelay(context, SegmentUnitStatus.Start, 3000);
    }

    List<EndPoint> endpoints = DataNodeRequestResponseHelper
        .buildEndPoints(instanceStore, currentMembership, false);
    int quorumSize = metadata.getVolumeType().getVotingQuorumSize();
    int totalMembers = metadata.getVolumeType().getNumMembers();
    if (endpoints.size() < quorumSize) {
      logger.info("Can't move {} the membership forward because there are "
              + "no enough members to elect a leader. membership: {} available endpoint: {}",
          metadata.getSegId(),
          currentMembership, endpoints);
      logger.info("Putting the membership back to the working queue");
      return getFailureResultWithFixedDelay(context, SegmentUnitStatus.Start, DEFAULT_FIXED_DELAY);
    }

    NewPrimaryProposerContext myContext = (NewPrimaryProposerContext) context;

    int highestN = segmentUnit.getAcceptor().getMaxN();

    Integer lastN = segmentUnit.getAcceptor().getLastN();

    highestN = generateNextN(highestN);
    BroadcastRequest prepareRequest = DataNodeRequestResponseHelper
        .buildNewPrimaryPhase1Request(segmentUnit.getSegId(), currentMembership, myself.getId(),
            highestN, lastN);

    BroadcastResult broadcastResult;
    RunningStatus runningStatus = null;
    DataNodeServiceAsyncClientWrapper asyncDataNodeClient = new DataNodeServiceAsyncClientWrapper(
        dataNodeAsyncClientFactory);
    try {
      broadcastResult = asyncDataNodeClient
          .broadcast(prepareRequest, MAX_SOCKET_TIMEOUT, quorumSize, false, endpoints);
      logger.info("We have received a quorum to promise to the prepare request.");
      runningStatus = RunningStatus.PromisedModerator;
    } catch (QuorumNotFoundException e) {
      logger.info("Can't get a quorum from endpoints:{} for {} ", endpoints, metadata.getSegId());
      broadcastResult = e.getBroadcastResult();
      SegmentMembership highestMembershipFromReturn = broadcastResult.getHighestMembership();
      logger.info("The highest membership from broadcast result is {}",
          highestMembershipFromReturn);

      if (broadcastResult.getNumSegmentNotFoundExceptions() >= quorumSize) {
        logger.info("The majority of members don't know this segment. SegId:{} ",
            segmentUnit.getSegId());
        if (currentMembership.compareTo(highestMembershipFromReturn) >= 0) {
          logger.warn("since the highest membership collected from other members are same as mine, "
              + "most like the segment has been deleted. Let's retry again");

          return getFailureResultWithBackoffDelay(myContext, SegmentUnitStatus.Start);
        } else {
          logger.info("the highest membership collected is higher than mine: {}. "
              + "Although most members think the segment is gone, there is still a chance that"
              + " my membership is too stale. "
              + "Check the membership back and redo it again", highestMembershipFromReturn);
        }

      } else if ((int) broadcastResult.getMaxLogId() >= highestN) {
        int receivedMaxN = (int) broadcastResult.getMaxLogId();
        logger.info("N value we use {} is not larger than other members: {}. retry again", highestN,
            receivedMaxN);
        myContext.setHighestN(receivedMaxN);

      }

      if (runningStatus == null) {
        if (currentMembership.compareTo(highestMembershipFromReturn) < 0) {
          logger.info(
              "current membership: {} is less than those from acceptors:{} let's wait "
                  + "and retry again",
              currentMembership, highestMembershipFromReturn);

          Validate.notNull(highestMembershipFromReturn.getPrimary());
          updateSegmentUnit(segmentUnit.getSegId(), highestMembershipFromReturn, currentMembership,
              null, myself, totalMembers);

          return getFailureResultWithZeroDelay(myContext, SegmentUnitStatus.Start);
        } else if (currentMembership.compareTo(highestMembershipFromReturn) == 0) {
          logger.info("My current membership equals all acceptors'");
          if (broadcastResult.getNumPrimaryExistsExceptions() > 0) {
            if (segmentUnit.isSecondaryZombie() && currentMembership.getAliveSecondaries()
                .contains(myself)) {
              logger.info(
                  "Although there is a PrimaryExistsException, but I am a zombie and alive in"
                      + " the current membership, I would not join this group");
              return getFailureResultWithRandomizedDelay(myContext, SegmentUnitStatus.Start,
                  DEFAULT_FIXED_DELAY);
            }
            logger.info("It seems that the primary has returned PrimaryExistsException. "
                    + "Will send a JoinGroup request to the primary {} ",
                highestMembershipFromReturn.getPrimary());
            runningStatus = RunningStatus.PrimaryExists;
          } else {
            logger.info(
                "Don't know why the voting request was denied. (might be because two parties "
                    + "having the same logId. Let's wait random time and retry again");
            return getFailureResultWithRandomizedDelay(myContext, SegmentUnitStatus.Start,
                DEFAULT_FIXED_DELAY);
          }
        } else {
          logger.info(
              "My current membership is larger than all acceptors'. let's wait other units"
                  + " to catch up and retry again");
          return getFailureResultWithRandomizedDelay(myContext, SegmentUnitStatus.Start,
              MAX_RANDOM_DELAY);
        }
      }
    } catch (FailedToSendBroadcastRequestsException e) {
      logger.warn("failed to send requests to majority of endpoints: {} ", endpoints, e);
      logger.info("Putting the membership back to the working queue");

      return getFailureResultWithRandomizedDelay(myContext, SegmentUnitStatus.Start,
          MAX_RANDOM_DELAY);
    }

    switch (runningStatus) {
      case Start:
        logger.error("can't be here");
        return getFailureResultWithBackoffDelay(myContext, SegmentUnitStatus.Start);
      case PromisedModerator:
        SegmentMembership proposal = null;
        SegmentMembership highestMembershipFromBroadcastResult =
            broadcastResult.getHighestMembership();
        if (currentMembership.compareTo(highestMembershipFromBroadcastResult) < 0) {
          logger.warn("Although {} have received quorum of good responses for NewPrimaryProposer, "
                  + "the acceptor has returned a higher membership: {} "
                  + " than the highest membership I have : {}", myself,
              broadcastResult.getHighestMembership(),
              currentMembership);

          updateSegmentUnit(segmentUnit.getSegId(), highestMembershipFromBroadcastResult,
              currentMembership,
              SegmentUnitStatus.Start, myself, totalMembers);

          return getFailureResultWithZeroDelay(myContext, SegmentUnitStatus.Start);
        }

        proposal = buildProposalFromResponse(currentMembership, segmentUnit,
            broadcastResult, quorumSize, asyncDataNodeClient);
        logger.warn("The next membership {} is selected", proposal);

        if (broadcastAcceptedMsg(asyncDataNodeClient, endpoints, prepareRequest.getRequestId(),
            highestN, proposal,
            segmentUnit.getSegId(), quorumSize, currentMembership)) {
          logger.warn("{}, successfully broadcast accepted messages to a quorum of acceptors {}",
              segmentUnit.getSegId(), proposal);

          archiveManager.updateSegmentUnitMetadata(segmentUnit.getSegId(), proposal, null);
          if (!kickOffPrimary(segmentUnit.getSegId(), proposal, instanceStore, myself)) {
            logger.error(
                "Failed to make primary decision. We will increase the propose N, change the "
                    + "status to Start and retry again");
            myContext.setHighestN(highestN);

            archiveManager.updateSegmentUnitMetadata(segmentUnit.getSegId(), null,
                SegmentUnitStatus.Start);
            return getFailureResultWithRandomizedDelay(myContext, SegmentUnitStatus.Start,
                MAX_RANDOM_DELAY);

          } else {
            logger.warn(
                "{} have successfully kicked off the potential primary. Now we just wait the"
                    + " potential primary to do its job",
                segmentUnit.getSegId());

            return getSuccesslResultWithZeroDelay(myContext, SegmentUnitStatus.ModeratorSelected);
          }
        } else {
          logger.warn("can't receive quorum responses for accepted messages");
          return getFailureResultWithRandomizedDelay(myContext, SegmentUnitStatus.Start,
              MAX_RANDOM_DELAY);
        }
      case PrimaryExists:

        archiveManager
            .updateSegmentUnitMetadata(segmentUnit.getSegId(), null,
                SegmentUnitStatus.SecondaryApplicant);
        return getSuccesslResultWithZeroDelay(myContext, SegmentUnitStatus.SecondaryApplicant);
      default:
        logger.error("can't be here");
        return getFailureResultWithBackoffDelay(myContext, SegmentUnitStatus.Start);
    }
  }

  private int generateNextN(int highestN) {
    return highestN + 1 + randomForIncreasingMaxN.nextInt(10);
  }

  private boolean kickOffPrimary(SegId segId, SegmentMembership newMembership,
      InstanceStore instanceStore, InstanceId myself) {
    Instance primary = instanceStore.get(newMembership.getPrimary());
    if (primary == null) {
      logger.warn("Can't get primary of {} from instance store", newMembership);
      return false;
    }

    KickOffPotentialPrimaryRequest request = DataNodeRequestResponseHelper
        .buildKickOffPrimaryRequest(RequestIdBuilder.get(), segId, myself.getId(), newMembership,
            newMembership.getPrimary().getId(), 1);
    logger.debug("kick off primary request: {}", request);
    DataNodeService.Iface dataNodeClient = null;
    try {
      dataNodeClient = dataNodeSyncClientFactory
          .generateSyncClient(primary.getEndPoint(), cfg.getDataNodeRequestTimeoutMs());
      KickOffPotentialPrimaryResponse response = dataNodeClient.kickoffPrimary(request);
      logger.info("{} as a potential primary has been kicked off: {}", primary, response);
      return true;
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.warn(
          "the primary doesn't have the segment any more: {}. put the ticket back to the queue, "
              + "and hopefully we can converge",
          primary, e);
      return false;
    } catch (NotPrimaryExceptionThrift e) {
      logger.warn(
          "{} think the request was sent to the wrong one. Put the ticket back to the queue. {}",
          primary,
          segId, e);
      return false;
    } catch (InvalidSegmentUnitStatusExceptionThrift e) {
      logger.warn(
          "{} is not in right status and can not be kicked off but I believe he will update "
              + "his status soon {}",
          primary, segId, e);
      return false;
    } catch (Exception e) {
      logger.warn("{} has thrown an unknown exception when kicking the primary for {} ", primary,
          segId, e);
      return false;
    } finally {
      if (dataNodeClient != null) {
        dataNodeSyncClientFactory.releaseSyncClient(dataNodeClient);
      }
    }
  }

  private SegmentMembership selectPrimary(SegmentUnit segUnit, InstanceId myself,
      DataNodeServiceAsyncClientWrapper dataNodeAsyncClient)
      throws TException, FailedToSelectPrimaryException {
    SegId segId = segUnit.getSegId();
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    BroadcastRequest collectLogInfoRequest = DataNodeRequestResponseHelper
        .buildCollectLogInfoRequest(segId, currentMembership);

    List<EndPoint> endPoints = DataNodeRequestResponseHelper
        .buildEndPoints(instanceStore, currentMembership, false);
    int quorumSize = segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize();
    try {
      DataNodeServiceAsyncClientWrapper.BroadcastResult broadcastResult = dataNodeAsyncClient
          .broadcast(collectLogInfoRequest, cfg.getDataNodeRequestTimeoutMs() / 2, quorumSize,
              false,
              endPoints);
      if (broadcastResult.getNumPrimaryExistsExceptions() > 0) {
        logger.error(
            "I got a PrimaryExistsException, which means there is already a pre primary {}",
            broadcastResult);
        throw new InternalErrorThrift().setDetail("there is already a pre-primary");
      }

      Set<BecomePrimaryPriorityBonus> becomePrimaryPriorityBonuses = new HashSet<>();
      int noArbiterCount = 0;
      int goodResponsesCount = broadcastResult.getGoodResponses().size();
      int availableCandidatesCount = 0;

      int secondaryZombiesCount = 0;
      long tempPrimary = 0;
      boolean tempPrimarySelected = false;
      boolean tempPrimaryParticipated = false;

      long oldPrimary = 0;
      boolean oldPrimaryParticipated = false;

      long maxPcl = LogImage.INVALID_LOG_ID;
      Set<Long> instancesWithMaxPcl = new HashSet<>();

      for (BroadcastResponse response : broadcastResult.getGoodResponses().values()) {
        final InstanceId instanceId = new InstanceId(response.getMyInstanceId());
        final long myPcl = response.getPcl();

        if (!response.isArbiter()) {
          noArbiterCount++;
        }

        if (response.isSecondaryZombie) {
          secondaryZombiesCount++;
        }

        if (response.isSetTempPrimary()) {
          Validate.isTrue(tempPrimary == 0 || tempPrimary == response.getTempPrimary(),
              "temp primary reported by all members should be the same");
          tempPrimary = response.getTempPrimary();
        }

        if (tempPrimary == instanceId.getId()) {
          tempPrimaryParticipated = true;
        }

        if (!currentMembership.isPrimary(instanceId) && !currentMembership.isSecondary(
            instanceId)) {
          continue;
        }

        if (myPcl > maxPcl) {
          maxPcl = myPcl;
          instancesWithMaxPcl.clear();
          instancesWithMaxPcl.add(response.getMyInstanceId());
        } else if (myPcl == maxPcl) {
          instancesWithMaxPcl.add(response.getMyInstanceId());
        }

        if (response.isMigrating()) {
          instancesWithMaxPcl.remove(response.getMyInstanceId());
          continue;
        }

        availableCandidatesCount++;

        if (currentMembership.isPrimary(instanceId)) {
          oldPrimaryParticipated = true;
          oldPrimary = instanceId.getId();
        }

      }

      if (noArbiterCount == 1) {
        becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.DATA_BACKUP_COUNT_1);
      } else if (noArbiterCount == 2) {
        becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.DATA_BACKUP_COUNT_2);
      }

      if (goodResponsesCount == quorumSize) {
        becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.JUST_ENOUGH_MEMBERS);
      } else if (availableCandidatesCount == 1) {
        becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.ONLY_ONE_CANDIDATE);
      }

      long finalSelection = 0;

      if (secondaryZombiesCount >= quorumSize && tempPrimary != 0) {
        logger.warn(
            "the secondary zombies count {} is larger than quorum size {}, we need to kick off"
                + " the temp primary {}",
            secondaryZombiesCount, quorumSize, tempPrimary);
        if (!tempPrimaryParticipated) {
          Validate.isTrue(segUnit.getSegmentUnitMetadata().getVolumeType() == VolumeType.LARGE);
          logger.warn("{} temp primary isn't here, try to select someone else", segId);
        } else {
          if (!instancesWithMaxPcl.contains(tempPrimary) && maxPcl != LogImage.INVALID_LOG_ID) {
            logger.warn("the temp primary doesn't have the max pcl");

            Validate.isTrue(instancesWithMaxPcl.contains(currentMembership.getPrimary().getId()));
          }

          final long tempPrimaryId = tempPrimary;
          Validate.isTrue(broadcastResult.getGoodResponses().values().stream().anyMatch(
              broadcastResponse -> broadcastResponse.getMyInstanceId() == tempPrimaryId));

          finalSelection = tempPrimary;
          tempPrimarySelected = true;
          becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.TEMP_PRIMARY);
        }
      }

      if (!tempPrimarySelected) {
        long myInstanceId = myself.getId();
        if (instancesWithMaxPcl.contains(myInstanceId)) {
          finalSelection = myself.getId();
        } else if (instancesWithMaxPcl.isEmpty()) {
          throw new NoAvailableCandidatesForNewPrimaryExceptionThrift();
        } else {
          finalSelection = instancesWithMaxPcl.iterator().next();
        }
      }

      if (!tempPrimarySelected && oldPrimaryParticipated && instancesWithMaxPcl.contains(
          oldPrimary)) {
        finalSelection = oldPrimary;
      }

      int becomePrimaryPriority = 0;
      for (BecomePrimaryPriorityBonus priorityBonus : becomePrimaryPriorityBonuses) {
        becomePrimaryPriority += priorityBonus.getValue();
      }

      logger.warn("{} potential primary selected {}, priority {}, bonuses are {}", segId,
          finalSelection,
          becomePrimaryPriority, becomePrimaryPriorityBonuses);
      SegmentMembership newMembership = currentMembership
          .newPrimaryChosen(new InstanceId(finalSelection));

      if (newMembership == null) {
        String errorMsg = String.format(
            "new primary %s selected, but can't produce a new membership, current one : %s",
            finalSelection, currentMembership);
        logger.error(errorMsg);
        throw new FailedToSelectPrimaryException(errorMsg);
      }

      for (BroadcastResponse response : broadcastResult.getGoodResponses().values()) {
        InstanceId instanceId = new InstanceId(response.getMyInstanceId());
        if (currentMembership.isInactiveSecondary(instanceId)) {
          if (response.isArbiter()) {
            newMembership = newMembership.inactiveSecondaryBecomeArbiter(instanceId);
          } else {
            newMembership = newMembership.inactiveSecondaryBecomeJoining(instanceId);
          }
        }
      }
      return newMembership;

    } catch (QuorumNotFoundException | FailedToSendBroadcastRequestsException
        | SnapshotVersionMissMatchForMergeLogsException e) {
      logger.warn("I cannot collect the datalog info to decide who will become primary", e);
      Validate.isTrue(segUnit.getPrimaryDecisionMade().compareAndSet(true, false));
      throw new InternalErrorThrift().setDetail(e.getMessage());
    }
  }

  private boolean makePrimaryDecision(SegId segId, long prepareRequestId,
      SegmentMembership currentMembership,
      InstanceStore instanceStore, Proposal proposal) {
    long deciderId = proposal.value;
    long proposalN = proposal.proposalN;

    Instance decider = instanceStore.get(new InstanceId(deciderId));
    if (decider == null) {
      logger.warn("Can't get the decider {} from instance store", decider);
      return false;
    }

    MakePrimaryDecisionRequest request = new MakePrimaryDecisionRequest(prepareRequestId,
        segId.getVolumeId().getId(), segId.getIndex(),
        RequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership), deciderId,
        proposalN);
    logger.debug("make primary decision request {} ", request);
    DataNodeService.Iface dataNodeClient = null;
    try {
      dataNodeClient = getClient(decider.getEndPoint(), cfg.getDataNodeRequestTimeoutMs());
      MakePrimaryDecisionResponse response = dataNodeClient.makePrimaryDecision(request);
      logger.info("got response of make primary decision {}", response);
      return true;
    } catch (SegmentNotFoundExceptionThrift segmentNotFoundExceptionThrift) {
      logger.warn("the primary decider {} does not have the segment unit anymore", decider,
          segmentNotFoundExceptionThrift);
      return false;
    } catch (InvalidSegmentUnitStatusExceptionThrift invalidSegmentUnitStatusExceptionThrift) {
      logger.warn("the primary decider {} is in a wrong status", decider,
          invalidSegmentUnitStatusExceptionThrift);
      return false;
    } catch (NotPrimaryExceptionThrift notPrimaryExceptionThrift) {
      logger.warn("the primary decider {} thought the request was sent to the wrong one", decider,
          notPrimaryExceptionThrift);
      return false;
    } catch (FailedToKickOffPrimaryExceptionThrift failedToKickOffPrimaryExceptionThrift) {
      logger.warn("the primary decider {} failed to kick off primary", decider,
          failedToKickOffPrimaryExceptionThrift);
      return false;
    } catch (Exception e) {
      logger.warn("the primary decider {} has thrown a strange exception", decider, e);
      return false;
    } finally {
      dataNodeSyncClientFactory.releaseSyncClient(dataNodeClient);
    }
  }

  private void updateSegmentUnit(SegId segId, SegmentMembership newMembership,
      SegmentMembership currentMembership,
      SegmentUnitStatus newStatus, InstanceId myself, int totalMembers) throws Exception {
    if (!SegmentMembershipHelper
        .okToUpdateToHigherMembership(newMembership, currentMembership, myself, totalMembers)) {
      archiveManager.updateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Deleting);
      throw new Exception("Changing the status of the segment unit" + segId + " to Deleting");
    } else {
      archiveManager.updateSegmentUnitMetadata(segId, newMembership, newStatus);
    }
  }

  private boolean broadcastAcceptedMsg(DataNodeServiceAsyncClientWrapper asyncDataNodeClient,
      List<EndPoint> endpoints, long prepareRequestId, long n, SegmentMembership proposal,
      SegId segId, int quorumSize,
      SegmentMembership highestMembership) {
    BroadcastRequest request = DataNodeRequestResponseHelper
        .buildNewPrimaryPhase2Request(prepareRequestId, segId, appContext.getInstanceId().getId(),
            0, proposal, highestMembership);
    request.setProposalNum((int) n);

    try {
      asyncDataNodeClient.broadcast(request, MAX_SOCKET_TIMEOUT, quorumSize, false, endpoints);
      logger.info("We have received a quorum to promise to the proposal.");
      return true;
    } catch (Exception e) {
      logger.warn("Can't get a quorum from endpoints: {}", endpoints, e);
      logger.info("Putting the membership back to the working queue");
      return false;
    }
  }

  private SegmentMembership buildProposalFromResponse(SegmentMembership currentMembership,
      SegmentUnit segUnit, final BroadcastResult broadcastResult, final int quorumSize,
      DataNodeServiceAsyncClientWrapper asyncDataNodeClient)
      throws NoAvailableCandidatesForNewPrimaryException, FailedToSelectPrimaryException {
    Validate.isTrue(broadcastResult.isBroadcastSucceeded());

    canNewPrimaryBeSelected(currentMembership);
    SegId segId = segUnit.getSegId();
    Map<EndPoint, BroadcastResponse> goodResponses = broadcastResult.getGoodResponses();
    int numOpen = 0;
    long largestCl = Long.MIN_VALUE;
    SegmentMembership mySelection = null;
    SegmentMembership acceptedValue = null;
    long maxAcceptedN = -1;

    InstanceId primaryId = currentMembership.getPrimary();
    boolean goodResponseContainsCurrentPrimary = false;
    boolean goodResponseContainsSecondary = false;
    boolean isPrimary;
    boolean isSecondary;

    long currentPrimaryCl = 0;
    for (BroadcastResponse response : goodResponses.values()) {
      isPrimary = isSecondary = false;
      Validate.notNull(response.getMembership());
      InstanceId myInstanceIdInResponse = new InstanceId(response.getMyInstanceId());
      if (myInstanceIdInResponse.compareTo(primaryId) == 0) {
        isPrimary = true;
        goodResponseContainsCurrentPrimary = true;
        currentPrimaryCl = response.getPcl();
      } else if (currentMembership.isSecondary(myInstanceIdInResponse)) {
        goodResponseContainsSecondary = true;
        isSecondary = true;
      }

      if (response.isSetAcceptedValue()) {
        if (response.getAcceptedN() > maxAcceptedN) {
          maxAcceptedN = response.getAcceptedN();
          acceptedValue = RequestResponseHelper.buildSegmentMembershipFrom(
              response.getAcceptedValue()).getSecond();
          logger.info("the acceptor has already accepted {} ", acceptedValue);
        }
      } else {
        numOpen++;
        Validate.isTrue(response.isSetPcl());
        if (largestCl < response.getPcl() && (isSecondary || isPrimary)) {
          mySelection = currentMembership
              .newPrimaryChosen(new InstanceId(response.getMyInstanceId()));
          largestCl = response.getPcl();
        }
      }
    }

    if (!goodResponseContainsCurrentPrimary && !goodResponseContainsSecondary) {
      throw new NoAvailableCandidatesForNewPrimaryException();
    }

    if (goodResponseContainsCurrentPrimary) {
      if (largestCl > currentPrimaryCl) {
        logger.warn(
            "The largest datalog id generator {} from secondaries is larger than that of the "
                + "primary {}. keep the original selection {} ",
            largestCl, currentPrimaryCl, mySelection);
      } else {
        mySelection = currentMembership.newPrimaryChosen(primaryId);
      }
    }

    if (numOpen >= quorumSize) {
      logger.warn("there are {} acceptors that have not accepted any proposal yet. "
              + "Choose my own value: {}",
          numOpen, mySelection);
      SegmentMembership selectedMembership;
      try {
        selectedMembership = selectPrimary(segUnit, appContext.getInstanceId(),
            asyncDataNodeClient);
      } catch (TException e) {
        logger.warn("{} select primary failed", segId, e);
        throw new FailedToSelectPrimaryException(e);
      }
      if (!selectedMembership.equals(mySelection)) {
        logger.warn("Two algorithms producing different result, which is normal, "
            + "printing them just for curiosity! {} {}", selectedMembership, mySelection);
      }
      return selectedMembership;
    } else {
      Validate.isTrue(acceptedValue != null);
      logger.info("there are {} acceptors that have accepted proposal. "
              + "The proposal with the highest N {} is {} ", (goodResponses.size() - numOpen),
          maxAcceptedN,
          acceptedValue);
      return acceptedValue;
    }
  }

  private void canNewPrimaryBeSelected(SegmentMembership currentMembership) {
  }
}

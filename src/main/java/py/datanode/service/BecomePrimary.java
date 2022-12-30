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

package py.datanode.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.CloneStatus;
import py.archive.segment.CloneType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitBitmap;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentVersion;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.datanode.archive.RawArchiveManager;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.EliminateFullCopySecondaryAtBecomePrimaryException;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.exception.LogNotFoundException;
import py.datanode.exception.SegmentUnitNotReadyToBecomePrimaryException;
import py.datanode.exception.StaleMembershipException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.datanode.segment.datalog.algorithm.DataLogHelper;
import py.datanode.segment.datalog.algorithm.merger.BecomePrimaryLogsMerge;
import py.datanode.segment.datalog.algorithm.merger.MergeLogCommunicator;
import py.datanode.segment.datalog.algorithm.merger.MergeLogCommunicatorImpl;
import py.datanode.segment.datalog.algorithm.merger.MergeLogIterator;
import py.datanode.segment.datalog.persist.LogStorageReader;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.datalogbak.catchup.PlalDriver;
import py.datanode.service.io.read.merge.MergeFailedException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastResponse;
import py.thrift.datanode.service.LogThrift;
import py.thrift.datanode.service.ProgressThrift;
import py.thrift.share.BroadcastLogThrift;
import py.thrift.share.CommitLogsRequestThrift;
import py.volume.VolumeType;

public class BecomePrimary implements Runnable, Comparable<BecomePrimary> {
  protected static final Set<SegId> segmentsBecomingPrimary = new HashSet<>();
  private static final Logger logger = LoggerFactory.getLogger(BecomePrimary.class);
  protected final LogStorageReader logStorageReader;
  protected final int priority;
  private final SegmentMembership membershipWhenBecomePrimaryStart;
  protected SegmentUnit segUnit;
  protected boolean membershipUpdated;
  protected InstanceId myself;
  protected long requestTimeout;
  protected long requestId;
  protected String threadName;
  protected MutationLogManager mutationLogManager;
  protected InstanceStore instanceStore;
  protected RawArchiveManager archiveManager;
  protected DataNodeServiceAsyncClientWrapper dataNodeAsyncClient;
  protected DataNodeConfiguration cfg;
  protected PlalEngine plalEngine;
  protected SegmentUnitTaskExecutor catchupLogEngine;
  private UnitedBroadcastForLargeVolume unitedBroadcast;

  public BecomePrimary(SegmentUnit segUnit, SegmentMembership membership, DataNodeConfiguration cfg,
      MutationLogManager mutationLogManager, InstanceStore instanceStore,
      RawArchiveManager archiveManager,
      SegmentUnitTaskExecutor catchupLogEngine, PlalEngine plalEngine,
      DataNodeServiceAsyncClientWrapper dataNodeAsyncClient, InstanceId myself, long requestId,
      int priority,
      LogStorageReader logStorageReader) {
    this.segUnit = segUnit;
    this.membershipWhenBecomePrimaryStart = membership;
    this.myself = myself;

    this.requestTimeout = cfg.getDataNodeRequestTimeoutMs() * 2;
    this.requestId = requestId;
    this.threadName = "BecomePrimary-" + requestId;
    this.mutationLogManager = mutationLogManager;
    this.instanceStore = instanceStore;
    this.archiveManager = archiveManager;
    this.dataNodeAsyncClient = dataNodeAsyncClient;
    this.cfg = cfg;
    this.plalEngine = plalEngine;
    this.catchupLogEngine = catchupLogEngine;
    this.priority = priority;
    this.logStorageReader = logStorageReader;
  }

  @Deprecated
  public void start() {
    Thread thread = new Thread(this, threadName);
    thread.start();
  }

  @Override
  public void run() {
    boolean everythingOk = true;
    SegId segId = segUnit.getSegId();

    if (segUnit.havingKickedOffProcessOfBecomingPrimary(myself.getId())) {
      return;
    }

    synchronized (segmentsBecomingPrimary) {
      boolean exists = segmentsBecomingPrimary.contains(segId);
      if (exists) {
        logger.error(
            "System exit: {} segment unit have two become primary driver. " 
                + "My current membership is {}, primary={}",
            segId, segUnit, segmentsBecomingPrimary);
        System.exit(-1);
      }
      segmentsBecomingPrimary.add(segId);
    }

    try {
      if (!okToBecomePrimary(getAndValidateMembership(),
          segUnit.getSegmentUnitMetadata().getStatus())) {
        everythingOk = false;
        return;
      }

      this.unitedBroadcast = new UnitedBroadcastForLargeVolume(
          plalEngine.getService().getIoClientFactory(),
          cfg, dataNodeAsyncClient, instanceStore, segUnit,
          getAndValidateMembership(), myself);

      segUnit.setPotentialPrimaryId(myself.getId());
      waitTillSegmentUnitCreationDone(segUnit);

      VolumeType volumeType = segUnit.getSegmentUnitMetadata().getVolumeType();
      int votingQuorumSize = volumeType.getVotingQuorumSize();

      List<EndPoint> availableSecondaryEndPoints = DataNodeRequestResponseHelper
          .buildEndPoints(instanceStore, getAndValidateMembership(), false, myself);

      logger.info("get available secondary end points {}", availableSecondaryEndPoints);
      if (availableSecondaryEndPoints.size() < votingQuorumSize - 1) {
        logger.warn("Can't move the membership forward because there are "
                + "no enough members to elect a leader. membership: {} available endpoint {} ",
            getAndValidateMembership(), availableSecondaryEndPoints);
        everythingOk = false;
        return;
      }

      availableSecondaryEndPoints = broadcastStartBecomePrimary(requestId, segUnit,
          getAndValidateMembership(), votingQuorumSize - 1,
          availableSecondaryEndPoints, myself, requestTimeout);

      boolean allSecondariesHaveDoneWithGiveMeYourLogs;
      Collection<BroadcastResponse> goodResponses;
      String newVolumeMetadataJson = null;
      SegmentLogMetadata segmentLogMetadata = mutationLogManager.getSegment(segId);
      LogImage logImage = segmentLogMetadata.getLogImageIncludeLogsAfterPcl();
      logger.warn("my log image is {}", logImage);
      Map<Long, Long> maxLogIdReceivedInMembers = initializeMaxLogIdReceivedFromPreSecondaries(
          getAndValidateMembership(), logImage.getClId());
      MergeLogCommunicator mergeLogCommunicator = new MergeLogCommunicatorImpl(
          availableSecondaryEndPoints, instanceStore.get(myself).getEndPoint(),
          dataNodeAsyncClient, segUnit, cfg, instanceStore, unitedBroadcast);

      BecomePrimaryLogsMerge becomePrimaryLogsMerge = new BecomePrimaryLogsMerge(
          getAndValidateMembership(), mergeLogCommunicator, myself, votingQuorumSize);
      long mergeToLogId = -1;
      int logsCountEachTime = cfg.getGiveMeYourLogsCountEachTime();
      while (true) {
        updateMaxLogIdReceivedFromPreSecondaries(maxLogIdReceivedInMembers,
            segmentLogMetadata.getClId());

        List<Long> logIdsPrimaryAlreadyExisted = new ArrayList<>(logsCountEachTime);
        for (MutationLogEntry logEntry : segmentLogMetadata
            .getLogsAfter(segmentLogMetadata.getClId(), logsCountEachTime)) {
          logIdsPrimaryAlreadyExisted.add(logEntry.getLogId());
          becomePrimaryLogsMerge
              .addLog(myself, DataLogHelper.buildDataLog(logEntry));
          if (maxLogIdReceivedInMembers.get(myself.getId()) < logEntry.getLogId()) {
            maxLogIdReceivedInMembers.put(myself.getId(), logEntry.getLogId());
          }
        }
        logImage = segmentLogMetadata.getLogImageIncludeLogsAfterPcl();
        logger.debug("my log image is {}", logImage);
        logger.warn("logIds {}, I already have after pcl {}, count {}", logIdsPrimaryAlreadyExisted,
            segmentLogMetadata.getClId(), logsCountEachTime);
        if (mergeToLogId == -1) {
          goodResponses = broadcastGiveMeYourLogs(requestId, segUnit,
              getAndValidateMembership(), votingQuorumSize - 1,
              availableSecondaryEndPoints, myself, requestTimeout, 0, maxLogIdReceivedInMembers,
              Long.MAX_VALUE, logIdsPrimaryAlreadyExisted);
          mergeToLogId = segmentLogMetadata.getMaxLogId();
          for (BroadcastResponse response : goodResponses) {
            mergeToLogId = Math.max(mergeToLogId, response.getMaxLogId());
          }
          logger.warn("mergeToLogId:{}, goodResponses {}", mergeToLogId, goodResponses);
        } else {
          goodResponses = broadcastGiveMeYourLogs(requestId, segUnit, getAndValidateMembership(),
              votingQuorumSize - 1,
              availableSecondaryEndPoints, myself, requestTimeout, 0, maxLogIdReceivedInMembers,
              mergeToLogId, logIdsPrimaryAlreadyExisted);
        }

        if (newVolumeMetadataJson == null && segUnit.isFirstUnitInVolume()) {
          newVolumeMetadataJson = mergeVolumeMetadataJson(segUnit, goodResponses);
        }

        addSecondaryLogsToLogMerger(becomePrimaryLogsMerge, goodResponses,
            maxLogIdReceivedInMembers);

        availableSecondaryEndPoints = getGoodSecondaryEndPointsInNewMembership(instanceStore,
            goodResponses).getFirst();

        allSecondariesHaveDoneWithGiveMeYourLogs = secondariesDoneWithGiveMeYourLogs(goodResponses);

        mergeLogCommunicator.updateInstance(availableSecondaryEndPoints);

        if (allSecondariesHaveDoneWithGiveMeYourLogs) {
          break;
        }

        long logMergeId = getMinMergeLogId(maxLogIdReceivedInMembers, segmentLogMetadata);
        availableSecondaryEndPoints = mergeLogs(
            becomePrimaryLogsMerge, segmentLogMetadata,
            instanceStore, logMergeId, segUnit, availableSecondaryEndPoints,
            votingQuorumSize);

        Thread.sleep(20);
      }

      while (maxLogIdReceivedInMembers.get(myself.getId()) < mergeToLogId) {
        List<MutationLogEntry> logs = segmentLogMetadata
            .getLogsAfter(maxLogIdReceivedInMembers.get(myself.getId()), logsCountEachTime);
        if (logs.size() == 0) {
          break;
        }

        boolean continueLog = true;
        for (MutationLogEntry logEntry : logs) {
          if (logEntry.getLogId() > mergeToLogId) {
            continueLog = false;
            break;
          }
          becomePrimaryLogsMerge
              .addLog(myself, DataLogHelper.buildDataLog(logEntry));
          if (maxLogIdReceivedInMembers.get(myself.getId()) < logEntry.getLogId()) {
            maxLogIdReceivedInMembers.put(myself.getId(), logEntry.getLogId());
          }
        }
        if (!continueLog) {
          break;
        }
      }
      availableSecondaryEndPoints = mergeLogs(
          becomePrimaryLogsMerge, segmentLogMetadata,
          instanceStore, mergeToLogId, segUnit, availableSecondaryEndPoints,
          votingQuorumSize);

      logger.info("broadcast check secondary full copy is required requests. {}, {}", segId,
          availableSecondaryEndPoints);
      List<EndPoint> availableSecondaryEndPointsAfterCheckFullCopy = 
          checkSecondaryFullCopyIsRequired(
          requestId, instanceStore, segUnit, myself.getId(), getAndValidateMembership(),
          votingQuorumSize - 1, availableSecondaryEndPoints);
      logger.info(
          "after broadcast check secondary full copy is " 
              + "required requests. available end point {}, quorum size {}",
          availableSecondaryEndPointsAfterCheckFullCopy, votingQuorumSize);
      if (availableSecondaryEndPointsAfterCheckFullCopy.size() >= votingQuorumSize - 1) {
        availableSecondaryEndPoints = availableSecondaryEndPointsAfterCheckFullCopy;
      }
      availableSecondaryEndPoints = broadcastStepIntoSecondaryEnrolled(requestId, instanceStore,
          segUnit,
          myself.getId(), getAndValidateMembership(), votingQuorumSize - 1,
          availableSecondaryEndPoints);

      logger.info("broadcast CatchUpMyLogs requests. {}", segId);

      long preprimarySessionId = requestId;
      segUnit.setPreprimaryDrivingSessionId(preprimarySessionId);
      broadcastCatchUpMyLogs(requestId, segUnit, getAndValidateMembership(), votingQuorumSize - 1,
          availableSecondaryEndPoints, segmentLogMetadata, myself, preprimarySessionId);

      logger.info("restarting log engines {}, {}", segId, availableSecondaryEndPoints);

      segUnit.updateAvailableSecondary(availableSecondaryEndPoints);
      restartCatchupLog(segId);

      long currentTimeMillis = System.currentTimeMillis();
      boolean secondariesInNewMembershipAllDone = false;

      while (!secondariesInNewMembershipAllDone) {
        goodResponses = broadcastCatchUpMyLogs(requestId, segUnit, getAndValidateMembership(),
            votingQuorumSize - 1,
            availableSecondaryEndPoints, segmentLogMetadata, myself, preprimarySessionId);

        availableSecondaryEndPoints = getGoodSecondaryEndPointsInNewMembership(instanceStore,
            goodResponses).getFirst();
        segUnit.updateAvailableSecondary(availableSecondaryEndPoints);
        logger.debug("{} availableSecondaryEndPoints {}", segId, availableSecondaryEndPoints);

        secondariesInNewMembershipAllDone = secondariesDoneWithSyncLogs(goodResponses,
            goodResponses.size());
        Thread.sleep(200);
      }

      long currentTimeMillis1 = System.currentTimeMillis();
      long waitTime = currentTimeMillis1 - currentTimeMillis;
      logger.info("Now swcl is same as cl {} It took {} ms", segmentLogMetadata.getClId(), waitTime,
          segId);

      segUnit.lockStatus();
      boolean persistForce = false;
      SegmentMembership newMembership;

      try {
        SegmentMembership currentMembership = getAndValidateMembership();
        newMembership = buildNewMembership(currentMembership, goodResponses,
            votingQuorumSize, segUnit.getSegmentUnitMetadata().getVolumeType());

        logger.warn("@@@@@@@@@@ new membership: {}, current membership: {}", newMembership,
            currentMembership);

        if (newMembership.getWriteSecondaries().isEmpty()) {
          for (MutationLogEntry log : segmentLogMetadata.getLogsAfterPcl()) {
            if (log.getStatus() == LogStatus.Created) {
              log.commit();
            } else if (log.getStatus() == LogStatus.Aborted) {
              log.confirmAbortAndApply();
            } else if (!log.isFinalStatus()) {
              logger.error("FATAL !! log not in a right status {}", log);
            }
          }
        }

        logger.info(
            "all secondary candidates have responded" 
                + " sycLog requests. Update my membership to the higher one");
        membershipUpdated = true;
        persistForce = archiveManager
            .asyncUpdateSegmentUnitMetadata(segId, newMembership, null, newVolumeMetadataJson);
      } finally {
        segUnit.unlockStatus();
      }
      if (persistForce) {
        segUnit.getArchive().persistSegmentUnitMeta(segUnit.getSegmentUnitMetadata(), true);
      }

      BroadcastRequest joinMeRequest = DataNodeRequestResponseHelper
          .buildJoinMeRequest(requestId, segId, getAndValidateMembership(),
              segmentLogMetadata.getSwplId(),
              segmentLogMetadata.getSwclId());

      if (segUnit.getSegmentUnitMetadata().getVolumeMetadataJson() != null) {
        joinMeRequest
            .setVolumeMetadataJson(segUnit.getSegmentUnitMetadata().getVolumeMetadataJson());
      }

      dataNodeAsyncClient
          .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), joinMeRequest, requestTimeout,
              votingQuorumSize - 1, false, availableSecondaryEndPoints);
      logger.info("majority of secondary candidates have responded joiningGroup requests. "
          + "Let's change the status to Primary to accept the traffic");

      if (segUnit.isSecondaryZombie()) {
        logger
            .info("the new membership has a new primary, clear my zoobie status {}",
                segId);
        segUnit.clearSecondaryZombie();
      }

      logger.info(
          "all secondary candidates have responded syc " 
              + "log requests. Update my status to Primary to begin to process traffic");
      archiveManager.updateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Primary);
      restartCatchupLog(segId);

      segUnit.getSegmentLogMetadata().clearNotInsertedLogs();
      MutationLogEntry dummyLog = segUnit.getSegmentLogMetadata()
          .generateOneDummyLogWhileBecomingPrimary();
      segmentLogMetadata.appendLog(dummyLog);
      movePclAndPlal(dummyLog.getLogId(), segmentLogMetadata, segUnit);

      segUnit
          .startPeerLeases(cfg.getSecondaryLeaseInPrimaryMs(), newMembership.getAliveSecondaries());
      segUnit.clearExpiredMembers();

      for (InstanceId inactiveSecondary : newMembership.getInactiveSecondaries()) {
        segUnit.startPeerLease(0, inactiveSecondary);
      }

      segUnit.setWhenNewSegmentUnitWasRequested(System.currentTimeMillis());
    } catch (Throwable e) {
      logger.warn(
          "caught an exception while becoming a primary." 
              + " change segment status back to \"Start\" for segId={}",
          segId, e);
      everythingOk = false;
    } finally {
      synchronized (segmentsBecomingPrimary) {
        segmentsBecomingPrimary.remove(segId);
      }

      segUnit.resetPreprimaryDrivingSessionId();
      segUnit.resetPotentialPrimaryId();
      segUnit.resetProcessOfBecomingPrimaryBeenKickedoff();
      segUnit.clearAvailableSecondary();

      if (!everythingOk) {
        try {
          logger.warn(
              "due to the failure of becoming a primary {} is changing its status back to Start",
              segId);
          archiveManager.updateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Start);

        } catch (Exception e) {
          logger.warn("Failed to change segment " + segId + " status back to \"Start\".", e);
        }
      }
    }
  }

  protected boolean okToBecomePrimary(SegmentMembership currentMembership,
      SegmentUnitStatus status) {
    if (!(currentMembership.isPrimary(myself))) {
      logger.warn("Myself {} in the membership {} is not a primary, so can't become a primary",
          myself.getId(), currentMembership);
      return false;
    } else if (status != SegmentUnitStatus.PrePrimary) {
      logger.warn("I am not in PrePrimary status anymore, can't become a primary {}", segUnit);
      return false;
    } else {
      return true;
    }

  }

  protected List<Pair<InstanceId, Boolean>> getGoodSecondariesInNewMembership(
      Collection<BroadcastResponse> goodResponses) {
    List<Pair<InstanceId, Boolean>> secondariesInNewMembership = new ArrayList<>();
    for (BroadcastResponse response : goodResponses) {
      InstanceId secondaryCandidate = new InstanceId(response.getMyInstanceId());
      secondariesInNewMembership.add(new Pair<>(secondaryCandidate, response.isArbiter()));
    }
    return secondariesInNewMembership;
  }

  private List<EndPoint> checkSecondaryFullCopyIsRequired(long requestId,
      InstanceStore instanceStore,
      SegmentUnit segUnit, long myInstanceId, SegmentMembership currentMembership, int subMajority,
      List<EndPoint> availableSecondaryEndPoints) throws Exception {
    Validate.isTrue(subMajority > 0);
    BroadcastRequest broadcastCheckRequest = DataNodeRequestResponseHelper
        .buildCheckSecondaryIfFullCopyIsRequired(requestId, segUnit.getSegId(), myInstanceId,
            currentMembership);

    BroadcastResult result = dataNodeAsyncClient
        .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), broadcastCheckRequest,
            cfg.getDataNodeRequestTimeoutMs(), subMajority, false, availableSecondaryEndPoints);
    logger.debug(
        "We have received a quorum to check secondary full copy is required, good response {}",
        result.getGoodResponses());

    List<EndPoint> eliminateMembers = new ArrayList<>();
    for (Map.Entry<EndPoint, BroadcastResponse> responseEntry : result.getGoodResponses()
        .entrySet()) {
      BroadcastResponse broadcastResponse = responseEntry.getValue();
      InstanceId secondaryId = new InstanceId(broadcastResponse.getMyInstanceId());

      if (currentMembership.getArbiters().contains(secondaryId)) {
        continue;
      }
      long clIdFromSecondary = broadcastResponse.getPcl();
      long plIdFromSecondary = broadcastResponse.getPpl();
      boolean isForceFullCopy = broadcastResponse.isForceFullCopy();

      boolean secondaryClIdExistInPrimary = segUnit
          .secondaryClIdExistInPrimary(secondaryId, clIdFromSecondary, plIdFromSecondary,
              logStorageReader, isForceFullCopy);

      if (!secondaryClIdExistInPrimary) {
        logger.warn(
            "a secondary need full copy at become primary. " 
                + "so remove it first, and join it again later. {} {}.",
            secondaryId, segUnit.getSegId());
        eliminateMembers.add(responseEntry.getKey());
      }
    }

    List<EndPoint> endPoints = new ArrayList<>(availableSecondaryEndPoints);
    Throwable throwable = new EliminateFullCopySecondaryAtBecomePrimaryException();
    eliminateMembers.forEach(endPoint -> {
      result.eliminateOneGoodResponseMember(endPoint, throwable);
      endPoints.remove(endPoint);
    });

    Collection<BroadcastResponse> goodResponses = result.getGoodResponses().values();
    if (subMajority > 1) {
      goodResponses = unitedBroadcast
          .broadcastToTheMissingSecondaryUtilDone(endPoints, broadcastCheckRequest, result);
    }
    return getGoodSecondaryEndPointsInNewMembership(instanceStore, goodResponses).getFirst();
  }

  private List<EndPoint> broadcastStepIntoSecondaryEnrolled(long requestId,
      InstanceStore instanceStore,
      SegmentUnit segUnit, long myInstanceId, SegmentMembership currentMembership, int subMajority,
      List<EndPoint> availableSecondaryEndPoints) throws Exception {
    Validate.isTrue(subMajority > 0);
    BroadcastRequest broadcastWriteRequest = DataNodeRequestResponseHelper
        .buildStepIntoSecondaryEnrolledRequest(requestId, segUnit.getSegId(), myInstanceId,
            currentMembership);

    BroadcastResult result = dataNodeAsyncClient
        .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), broadcastWriteRequest,
            cfg.getDataNodeRequestTimeoutMs(), subMajority, false, availableSecondaryEndPoints);
    logger.debug("We have received a quorum to step into SecondaryEnrolled");

    Collection<BroadcastResponse> goodResponses = result.getGoodResponses().values();
    if (subMajority > 1) {
      goodResponses = unitedBroadcast
          .broadcastToTheMissingSecondaryUtilDone(availableSecondaryEndPoints,
              broadcastWriteRequest, result);
    }
    return getGoodSecondaryEndPointsInNewMembership(instanceStore, goodResponses).getFirst();
  }

  protected Map<Long, Long> initializeMaxLogIdReceivedFromPreSecondaries(
      SegmentMembership currentMembership, long pclId) {
    Set<InstanceId> members = currentMembership.getMembers();
    Map<Long, Long> initialMaxLogIdForPreSecondaries = new HashMap<>(members.size());

    for (InstanceId instanceId : members) {
      initialMaxLogIdForPreSecondaries.put(instanceId.getId(), pclId);
    }
    return initialMaxLogIdForPreSecondaries;
  }

  protected void updateMaxLogIdReceivedFromPreSecondaries(
      Map<Long, Long> maxLogIdReceivedInMembers, long pclId) {
    for (Map.Entry<Long, Long> entry : maxLogIdReceivedInMembers.entrySet()) {
      if (entry.getValue() < pclId) {
        maxLogIdReceivedInMembers.put(entry.getKey(), pclId);
      }
    }
  }

  protected void addSecondaryLogsToLogMerger(BecomePrimaryLogsMerge becomePrimaryLogsMerge,
      Collection<BroadcastResponse> goodResponses,
      Map<Long, Long> maxLogIdReceivedInMembers) throws Exception {
    for (BroadcastResponse response : goodResponses) {
      InstanceId instanceId = new InstanceId(response.getMyInstanceId());
      for (LogThrift logThrift : response.getLogs()) {
        becomePrimaryLogsMerge.addLog(instanceId,
            DataLogHelper.buildDataLog(logThrift));
        Long existingMaxLogId = maxLogIdReceivedInMembers.get(response.getMyInstanceId());
        if (existingMaxLogId < logThrift.getLogId()) {
          maxLogIdReceivedInMembers
              .put(response.getMyInstanceId(), logThrift.getLogId());
        }
      }
    }
  }

  protected void addSecondaryLogsToLogMerger(LogMerger merger,
      Collection<BroadcastResponse> goodResponses,
      Map<Long, Long> maxLogIdReceivedFromSecondaries) throws Exception {
    maxLogIdReceivedFromSecondaries.clear();
    for (BroadcastResponse response : goodResponses) {
      Long maxLogIdReceived = merger.addLogsFromResponse(response.getLogs());
      Long existingMaxLogId = maxLogIdReceivedFromSecondaries.get(response.getMyInstanceId());
      if (maxLogIdReceived != null && (existingMaxLogId == null
          || existingMaxLogId < maxLogIdReceived)) {
        maxLogIdReceivedFromSecondaries.put(response.getMyInstanceId(), maxLogIdReceived);
      }

    }
  }

  protected boolean secondariesDoneWithGiveMeYourLogs(Collection<BroadcastResponse> goodResponses) {
    for (BroadcastResponse response : goodResponses) {
      if (response.getProgress() == ProgressThrift.InProgress) {
        return false;
      }
    }
    return true;
  }

  protected String mergeVolumeMetadataJson(SegmentUnit segUnit,
      Collection<BroadcastResponse> goodResponses) {
    String newVolumeMetadataJson;
    VolumeMetadataJsonMerger vmMerger = new VolumeMetadataJsonMerger(
        segUnit.getSegmentUnitMetadata().getVolumeMetadataJson());
    for (BroadcastResponse response : goodResponses) {
      vmMerger.add(response.getVolumeMetadataJson());
    }
    newVolumeMetadataJson = vmMerger.merge();
    if (newVolumeMetadataJson != null) {
      logger.debug("segment unit {} 's volume metadata will be updated to {} ", segUnit.getSegId(),
          newVolumeMetadataJson);
    }

    return newVolumeMetadataJson;
  }

  private long getMinMergeLogId(Map<Long, Long> maxLogIdReceivedInMembers,
      SegmentLogMetadata segmentLogMetadata) {
    long maxMergeLogId = Long.MAX_VALUE;
    for (Long logId : maxLogIdReceivedInMembers.values()) {
      if (logId <= segmentLogMetadata.getClId()) {
        continue;
      }
      maxMergeLogId = Long.min(maxMergeLogId, logId);
    }

    return maxMergeLogId;
  }

  protected List<EndPoint> mergeLogs(BecomePrimaryLogsMerge merger,
      SegmentLogMetadata segmentLogMetadata, InstanceStore instanceStore,
      long maxMergeLogId, SegmentUnit segUnit, List<EndPoint> availableSecondaryEndPoints,
      int votingQuorumSize) throws Exception {
    long lastLogId = LogImage.INVALID_LOG_ID;

    long currentMaxLogId = Long.MIN_VALUE;

    List<MutationLogEntry> mergeSuccessLog = new ArrayList<>();
    MergeLogIterator iterator = merger.iterator(maxMergeLogId);
    int maxRetryTimes = 4;
    while (iterator.hasNext()) {
      Pair<DataLog, Set<InstanceId>> mergeResult = null;
      try {
        mergeResult = iterator.next();
      } catch (MergeFailedException e) {
        logger.info("no resource to create a merged log, sleep 500ms");
        Thread.sleep(500);
        if (maxRetryTimes == 0) {
          logger.info(
              "have tried 3 times and still can't allocate a data buffer." 
                  + " Quit the BecomePrimary process");
          throw e;
        }
        maxRetryTimes--;
        iterator = merger.iterator(maxMergeLogId);
        continue;
      }
      DataLog dataLog = mergeResult.getFirst();
      MutationLogEntry logEntry = segmentLogMetadata
          .getLog(dataLog.getLogId());
      Validate.notNull(logEntry);
      LogStatus mergedLogStatus = dataLog.getLogStatus();
      if (mergedLogStatus == LogStatus.Committed
          || mergedLogStatus == LogStatus.AbortedConfirmed) {
        if (mergedLogStatus == LogStatus.Committed) {
          logEntry.commit();

          int pageIndex = (int) (logEntry.getOffset() / cfg.getPageSize());
          segUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
        } else {
          logEntry.confirmAbort();
        }
        if (logEntry.canBeApplied()) {
          plalEngine.putLog(segUnit.getSegId(), logEntry);
        }
      } else {
        logger.error("merge the create logs={},{} for segId={}", logEntry,
            dataLog.getLogStatus(), segUnit.getSegId());
        Validate.isTrue(false);
      }

      lastLogId = logEntry.getLogId();
      Validate.isTrue(lastLogId > currentMaxLogId);
      currentMaxLogId = lastLogId;
      Set<EndPoint> successEndPoint = new HashSet<>();
      for (InstanceId instanceId : mergeResult.getSecond()) {
        if (instanceId.equals(myself)) {
          continue;
        }
        EndPoint endPoint = instanceStore.get(instanceId).getEndPoint();
        successEndPoint.add(endPoint);
      }
      availableSecondaryEndPoints.clear();
      availableSecondaryEndPoints.addAll(successEndPoint);
      mergeSuccessLog.add(logEntry);
    }

    if (mergeSuccessLog.size() > 0) {
      movePclAndPlal(lastLogId, segmentLogMetadata, segUnit);
      logger
          .debug("After merging logs, the PCL of PrePrimary at {} is {} and the new max id is {}",
              segUnit.getSegId(), segmentLogMetadata.getClId(), lastLogId);
      try {
        broadcastFinalStatusLog(mergeSuccessLog, segUnit.getSegId(), getAndValidateMembership(),
            votingQuorumSize - 1, availableSecondaryEndPoints);
      } catch (Exception e) {
        logger.info("broadcastFinalStatusLog error", e);
      }
    }
    return availableSecondaryEndPoints;
  }

  private Collection<BroadcastResponse> broadcastFinalStatusLog(
      List<MutationLogEntry> finalStausLogs, SegId segId, SegmentMembership membership,
      int subMajority, List<EndPoint> availableSecondaryEndPoints)
      throws Exception {
    CommitLogsRequestThrift commitLogRequest = new CommitLogsRequestThrift();
    commitLogRequest.setRequestId(RequestIdBuilder.get());
    commitLogRequest.setMembership(
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    Map<Long, List<BroadcastLogThrift>> broadcastLogMap = 
        new HashMap<Long, List<BroadcastLogThrift>>();
    List<BroadcastLogThrift> logThriftList = new ArrayList<BroadcastLogThrift>();

    for (MutationLogEntry log : finalStausLogs) {
      Validate.isTrue(log.isFinalStatus());
      BroadcastLogThrift logThrift = new BroadcastLogThrift();
      logThrift.setLogId(log.getLogId());
      logThrift.setLogUuid(log.getUuid());
      logThrift
          .setLogStatus(MutationLogEntry.convertStatusToThriftStatus(log.getStatus()));
      logThriftList.add(logThrift);
    }

    broadcastLogMap.put(RequestIdBuilder.get(), logThriftList);
    commitLogRequest.setMapRequestIdToLogs(broadcastLogMap);

    BroadcastRequest broadcastRequest = RequestResponseHelper
        .buildBroadcastLogResultsRequest(segId, membership, myself.getId(), commitLogRequest);

    BroadcastResult broadcastResult = dataNodeAsyncClient
        .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), broadcastRequest,
            requestTimeout,
            subMajority, false, availableSecondaryEndPoints);
    if (subMajority == 1) {
      return broadcastResult.getGoodResponses().values();
    } else {
      return unitedBroadcast
          .broadcastToTheMissingSecondaryUtilDone(availableSecondaryEndPoints,
              broadcastRequest, broadcastResult);
    }
  }

  protected void movePclAndPlal(long newCommittedLogId, SegmentLogMetadata segmentLogMetadata,
      SegmentUnit segmentUnit)
      throws LogNotFoundException, LogIdTooSmall {
    if (newCommittedLogId > segmentLogMetadata.getClId()) {
      Pair<Long, List<MutationLogEntry>> result = segmentLogMetadata
          .moveClTo(newCommittedLogId);
      plalEngine.putLogs(segmentUnit.getSegId(), result.getSecond());
      PlalDriver.drive(segmentLogMetadata, segmentUnit, plalEngine);
    }
  }

  private Collection<BroadcastResponse> broadcastGiveMeYourLogs(long requestId, SegmentUnit segUnit,
      SegmentMembership currentMembership, int subMajority,
      List<EndPoint> availableSecondaryEndPoints,
      InstanceId myself, long requestTimeout, int lease,
      Map<Long, Long> maxLogIdReceivedFromSecondaries,
      long mergeToLogId, List<Long> logIdsPrimaryAlreadyExisted)
      throws Exception {
    BroadcastRequest giveMeYourLogsRequest = DataNodeRequestResponseHelper
        .buildGiveMeYourLogsRequest(requestId, mergeToLogId, segUnit.getSegId(), currentMembership,
            lease,
            myself.getId(), maxLogIdReceivedFromSecondaries, logIdsPrimaryAlreadyExisted,
            segUnit.getSegmentLogMetadata().getClId());

    BroadcastResult broadcastResult = dataNodeAsyncClient
        .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), giveMeYourLogsRequest,
            requestTimeout,
            subMajority, false, availableSecondaryEndPoints);
    if (subMajority == 1) {
      return broadcastResult.getGoodResponses().values();
    } else {
      return unitedBroadcast.broadcastToTheMissingSecondaryUtilDone(availableSecondaryEndPoints,
          giveMeYourLogsRequest, broadcastResult);
    }
  }

  private List<EndPoint> broadcastStartBecomePrimary(long requestId,
      SegmentUnit segUnit, SegmentMembership currentMembership, int subMajority,
      List<EndPoint> availableSecondaryEndPoints, InstanceId myself, long requestTimeout)
      throws Exception {
    BroadcastRequest startBecomePrimaryRequest = DataNodeRequestResponseHelper
        .buildStartBecomePrimaryRequest(requestId, segUnit.getSegId(), myself.getId(),
            currentMembership);

    BroadcastResult broadcastResult = dataNodeAsyncClient
        .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), startBecomePrimaryRequest,
            requestTimeout, subMajority, false, availableSecondaryEndPoints);

    Collection<BroadcastResponse> goodResponses = broadcastResult.getGoodResponses().values();
    if (subMajority > 1) {
      goodResponses = unitedBroadcast
          .broadcastToTheMissingSecondaryUtilDone(availableSecondaryEndPoints,
              startBecomePrimaryRequest, broadcastResult);
    }
    return getGoodSecondaryEndPointsInNewMembership(instanceStore, goodResponses).getFirst();
  }

  private Collection<BroadcastResponse> broadcastCatchUpMyLogs(long requestId, SegmentUnit segUnit,
      SegmentMembership currentMembership, int subMajority, List<EndPoint> endPointsToBroadcast,
      SegmentLogMetadata segmentLogMetadata, InstanceId myself, long preprimaryDrivingSessionId)
      throws Exception {
    try {
      BroadcastRequest broadcastCatchUpMyLogsRequest = DataNodeRequestResponseHelper
          .buildCatchUpMyLogsRequest(requestId, 1L, segUnit.getSegId(), currentMembership,
              segmentLogMetadata.getClId(), myself.getId(), preprimaryDrivingSessionId);

      BroadcastResult result = dataNodeAsyncClient
          .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), broadcastCatchUpMyLogsRequest,
              cfg.getDataNodeRequestTimeoutMs(), subMajority, false, endPointsToBroadcast);
      if (subMajority == 1) {
        return result.getGoodResponses().values();
      } else {
        return unitedBroadcast.broadcastToTheMissingSecondaryUtilDone(endPointsToBroadcast,
            broadcastCatchUpMyLogsRequest, result);
      }

    } catch (Exception e) {
      logger.warn("Caught an exception when broadcasting CatchUpMyLogs request", e);
      throw e;
    }
  }

  protected boolean secondariesDoneWithSyncLogs(Collection<BroadcastResponse> goodResponses,
      int numSecondaries) {
    int numDone = 0;
    for (BroadcastResponse response : goodResponses) {
      if (response.getProgress() == ProgressThrift.Done) {
        numDone++;
      }
    }
    return numDone >= numSecondaries;
  }

  private boolean checkIfSwclSameAsCl(SegmentLogMetadata logMetadata) {
    long clId = logMetadata.getClId();
    long swclId = logMetadata.getSwclId();
    logger.info("check whether swcl {} catches up cl {} ", swclId, clId);
    if (swclId != clId) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        logger.info("got interrupted while waiting for swcl catches up CL.", e);
      }
      return false;
    } else {
      return true;
    }
  }

  protected void waitTillSegmentUnitCreationDone(SegmentUnit unit)
      throws SegmentUnitNotReadyToBecomePrimaryException {
    long timeout = 10000;
    long sleepUnit = 200;
    while (timeout > 0) {
      if (unit.isSegmentUnitCreateDone()) {
        return;
      }

      try {
        Thread.sleep(sleepUnit);
      } catch (InterruptedException e) {
        logger.warn("interrupted while waiting for segment unit {} creation is done",
            unit.getSegId());
      }
      timeout -= sleepUnit;
    }
    logger.warn("segment {} has been in creating status for {} ms. It can't be a preprimary",
        unit.getSegId(),
        timeout);
    throw new SegmentUnitNotReadyToBecomePrimaryException();
  }

  private Pair<List<EndPoint>, Set<Instance>> getGoodSecondaryEndPointsInNewMembership(
      InstanceStore instanceStore,
      Collection<BroadcastResponse> goodResponses) {
    List<EndPoint> endPoints = new ArrayList<>();
    Set<Instance> instances = new HashSet<>();
    for (BroadcastResponse response : goodResponses) {
      InstanceId secondaryCandidate = new InstanceId(response.getMyInstanceId());
      Instance instance = instanceStore.get(secondaryCandidate);
      if (instance != null) {
        endPoints.add(instance.getEndPoint());
        instances.add(instance);
      } else {
        logger.warn("Can't get instance {} from instance store", secondaryCandidate);
      }
    }
    return new Pair(endPoints, instances);
  }
  
  
  protected void restartCatchupLog(SegId segId) {
    logger.warn("Restart catch up engines: {}, catchupLogEngine: {}", segId, catchupLogEngine);
    segUnit.setDisallowPrePrimaryPclDriving(false);
    if (catchupLogEngine != null) {
      catchupLogEngine.revive(segId);
    }
    segUnit.setPauseCatchUpLogForSecondary(false);
  }
  
  private SegmentMembership buildNewMembership(SegmentMembership currentMembership,
      Collection<BroadcastResponse> goodResponses, int votingQuorumSize, VolumeType volumeType) {
    Validate.isTrue(currentMembership.isPrimary(myself));

    List<Pair<InstanceId, Boolean>> goodSecondariesInNewMembership = 
        getGoodSecondariesInNewMembership(
        goodResponses);

    List<InstanceId> availableSecondariesInNewMembership = new ArrayList<>();
    List<InstanceId> availableArbitersInNewMembership = new ArrayList<>();
    List<InstanceId> inactiveSecondariesInNewMembership = new ArrayList<>();

    for (Pair<InstanceId, Boolean> memberAndIsArbiter : goodSecondariesInNewMembership) {
      if (memberAndIsArbiter.getSecond()) {
        if (availableArbitersInNewMembership.size() >= volumeType.getNumArbiters()) {
          inactiveSecondariesInNewMembership.add(memberAndIsArbiter.getFirst());
        } else {
          availableArbitersInNewMembership.add(memberAndIsArbiter.getFirst());
        }

      } else {
        if (availableSecondariesInNewMembership.size() >= volumeType.getNumSecondaries()) {
          inactiveSecondariesInNewMembership.add(memberAndIsArbiter.getFirst());
        } else {
          availableSecondariesInNewMembership.add(memberAndIsArbiter.getFirst());
        }
      }
    }

    for (InstanceId instanceId : currentMembership.getMembers()) {
      if (goodSecondariesInNewMembership.stream()
          .noneMatch(instanceIdBooleanPair -> instanceId.equals(instanceIdBooleanPair.getFirst()))
          && !myself.equals(instanceId)) {
        inactiveSecondariesInNewMembership.add(instanceId);
      }
    }

    Validate.isTrue(goodSecondariesInNewMembership.size() + 1 >= votingQuorumSize);

    SegmentVersion newVersion = currentMembership.getSegmentVersion();
    for (int i = 0; i < votingQuorumSize * 2; i++) {
      newVersion = newVersion.incGeneration();
    }

    return new SegmentMembership(newVersion, myself, availableSecondariesInNewMembership,
        availableArbitersInNewMembership, inactiveSecondariesInNewMembership, null);
  }

  private SegmentMembership getAndValidateMembership() throws StaleMembershipException {
    SegmentMembership latestMembership = segUnit.getSegmentUnitMetadata().getMembership();
    if (latestMembership.compareEpoch(membershipWhenBecomePrimaryStart) != 0) {
      logger.warn("membership epoch has been changed while becoming primary {} {}",
          latestMembership, membershipWhenBecomePrimaryStart);
      throw new StaleMembershipException();
    } else if (latestMembership.getTempPrimary() != null
        && latestMembership.getTempPrimary() != myself) {
      logger.warn("has new membership {} and new Tp is not myself ",
          latestMembership);
      throw new StaleMembershipException();
    } else {
      return latestMembership;
    }
  }

  @Override
  public int compareTo(BecomePrimary other) {
    return Integer.compare(priority, other.priority);
  }
}

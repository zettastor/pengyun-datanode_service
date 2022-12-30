/*
 * Copyright (c) 2022-2022. PengYunNetWork
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

package py.datanode.service.io;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.struct.BogusEndPoint;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.datanode.client.ResponseCollector;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.service.Validator;
import py.datanode.service.worker.MembershipPusher;
import py.exception.NoAvailableBufferException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.AbstractMethodCallback;
import py.netty.core.MethodCallback;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.NettyExceptionHelper;
import py.netty.datanode.PyWriteRequest;
import py.netty.exception.DisconnectionException;
import py.netty.exception.HasNewPrimaryException;
import py.netty.exception.IncompleteGroupException;
import py.netty.exception.MembershipVersionLowerException;
import py.proto.Broadcastlog.GiveYouLogIdRequest;
import py.proto.Broadcastlog.GiveYouLogIdResponse;
import py.proto.Broadcastlog.LogUuidAndLogId;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbMembership;
import py.proto.Broadcastlog.PbWriteRequest;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Broadcastlog.PbWriteResponse.Builder;

/**
 * What primary does during a writing. <br>
 * Create logs, generate a log Id for each log and broadcast them to secondaries.
 *
 */
public class PrimaryWriteTask extends WriteTask {
  private static final Logger logger = LoggerFactory.getLogger(PrimaryWriteTask.class);

  private static String I_AM_WTS = "i am wts ";
  private static String I_AM_BROADCAST = "i am broacast log id ";
  private static AtomicLong broadcastLogRequestIdBuilder = new AtomicLong(0);

  private final InstanceStore instanceStore;
  private final InstanceId myself;
  private final boolean isTempPrimary;
  private final long[] logIds;
  private final SortedMap<MutationLogEntry, Boolean> mapLogsToNewlyCreated;
  private final Map<MutationLogEntry, Integer> mapLogToIndex;
  private GenericAsyncClientFactory<AsyncDataNode.AsyncIface> ioClientFactory;
  private BroadcastResult broadcastResult;
  private AtomicInteger countBothWtsAndBroadCast = new AtomicInteger();
  private String className;

  public PrimaryWriteTask(PyWriteRequest request, MethodCallback<PbWriteResponse> callback,
      Builder responseBuilder,
      PbIoUnitResult[] unitResults, SegmentUnit segmentUnit, PlalEngine plalEngine,
      GenericAsyncClientFactory<AsyncDataNode.AsyncIface> ioClientFactory,
      InstanceStore instanceStore,
      DataNodeConfiguration cfg, boolean isTempPrimary, InstanceId myself,
      boolean infiniteTimeoutAllocatingFastBuffer) {
    super(request, callback, responseBuilder, unitResults, segmentUnit, plalEngine,
        infiniteTimeoutAllocatingFastBuffer, cfg);
    Validate.notNull(cfg);
    this.logIds = new long[unitResults.length];
    this.ioClientFactory = ioClientFactory;
    this.instanceStore = instanceStore;
    this.logTimeoutMs = cfg.getIoTimeoutMs();
    this.mapLogToIndex = new HashMap<>();
    this.mapLogsToNewlyCreated = new TreeMap<>();
    this.isTempPrimary = isTempPrimary;
    this.myself = myself;

    this.className = "PrimaryWriteTask";

  }

  @Override
  protected void process() throws Exception {
    logger.debug("{} process write request: {} for segId: {}", myself, request.getMetadata(),
        segmentUnit.getSegId());

    boolean nofree = false;
    PbWriteRequest metadata = request.getMetadata();

    Long requestId = metadata.getRequestId();

    sendWriteRequestToSecondaryCandidateIfNecessary();

    SegmentLogMetadata segLogMetadata = segmentUnit.getSegmentLogMetadata();
    for (int i = 0; i < unitResults.length; i++) {
      if (unitResults[i] != PbIoUnitResult.SKIP) {
        continue;
      }

      PbWriteRequestUnit writeUnit = metadata.getRequestUnits(i);
      logger.debug("uuid {} offset {} seg {}", writeUnit.getLogUuid(), writeUnit.getOffset(),
          segmentUnit.getSegId());

      Pair<Long, Boolean> logIdAndAlreadyExists = segLogMetadata
          .getNewOrOldLogId(writeUnit.getLogUuid(), true);
      if (nofree) {
        segLogMetadata.discardLogIdForLog(writeUnit.getLogUuid(), logIdAndAlreadyExists.getFirst());
        unitResults[i] = PbIoUnitResult.EXHAUSTED;
        continue;
      }

      if (logIdAndAlreadyExists.getSecond()) {
        MutationLogEntry logEntry = segLogMetadata.getLog(logIdAndAlreadyExists.getFirst());
        if (logEntry == null) {
          throw new RuntimeException(
              "primary should absolutely have the log if log id already exists " + writeUnit);
        }

        logIds[i] = logEntry.getLogId();
        mapLogsToNewlyCreated.put(logEntry, false);
        mapLogToIndex.put(logEntry, i);
        continue;
      }

      try {
        MutationLogEntry entry = createLog(writeUnit, request.getRequestUnitData(i), true);
        Validate.notNull(entry);
        entry.setLogId(logIdAndAlreadyExists.getFirst());
        mapLogsToNewlyCreated.put(entry, true);
        logIds[i] = entry.getLogId();
        mapLogToIndex.put(entry, i);
      } catch (Exception e) {
        if (e instanceof NoAvailableBufferException) {
          logger.info("there is no fast buffer for request unit: {}, {}", writeUnit.getLogUuid(),
              request.getMetadata().getFailTimes());
        } else {
          logger.error("", e);
        }
        segLogMetadata.discardLogIdForLog(writeUnit.getLogUuid(), logIdAndAlreadyExists.getFirst());
        unitResults[i] = PbIoUnitResult.EXHAUSTED;
        nofree = true;
      }
    }
    Validate.notNull(mapLogsToNewlyCreated);
    LinkedList<MutationLogEntry> logsToBeInserted = new LinkedList<>();

    long logExpirationTime = System.currentTimeMillis() + logTimeoutMs;
    for (Entry<MutationLogEntry, Boolean> logAndNewlyCreated : mapLogsToNewlyCreated.entrySet()) {
      if (logAndNewlyCreated.getValue()) {
        MutationLogEntry log = logAndNewlyCreated.getKey();
        log.setExpirationTimeMs(logExpirationTime);
        logsToBeInserted.addLast(log);
      }
    }

    logger.debug("insert logs {}", logsToBeInserted);
    try {
      insertLogs(logsToBeInserted);
    } catch (LogIdTooSmall logIdTooSmall) {
      logger.error("log id too small, this is NOT A ERROR near the end of becoming primary",
          logIdTooSmall);
      for (MutationLogEntry logEntry : logsToBeInserted) {
        if (segLogMetadata.getLog(logEntry.getLogId()) == null) {
          segLogMetadata.discardLogIdForLog(logEntry.getUuid(), logEntry.getLogId());
          MutationLogEntryFactory.releaseLogData(logEntry);
        }
      }
      callback.fail(NettyExceptionHelper.buildServerProcessException("log id too small"));
      return;
    }

    tryToCommitLogs(I_AM_WTS);

    broadcastGiveYouLogIdIfNecessary(segmentUnit.getSegmentUnitMetadata()
        .getMembership());
    logger.debug("dealt with {} logs, newly created {}", mapLogsToNewlyCreated.size(),
        logsToBeInserted.size());
  }

  private Set<InstanceId> getMembersToBroadcast(SegmentMembership membership, int quorumSize) {
    Set<InstanceId> membersToBroadcast = new HashSet<>();

    if (!cfg.isBroadcastLogId()) {
      return membersToBroadcast;
    }

    membersToBroadcast.addAll(membership.getWriteSecondaries());
    if (isTempPrimary) {
      membersToBroadcast.remove(myself);
    }

    int left = (quorumSize - 1) - membersToBroadcast.size();

    if (left > 0) {
      membersToBroadcast.addAll(membership.getArbiters());
    }

    InstanceId secondaryCandidate = membership.getSecondaryCandidate();
    if (!isTempPrimary && secondaryCandidate != null) {
      membersToBroadcast.add(secondaryCandidate);
    }

    return membersToBroadcast;
  }

  private void broadcastGiveYouLogIdIfNecessary(SegmentMembership segmentMembership) {
    if (mapLogsToNewlyCreated.isEmpty()) {
      logger.debug("no need to broadcast log ids {}", segmentMembership);
      continueAfterBroadcast(BroadcastResult.SUCCESS);
      return;
    }

    int quorumSize = segmentUnit.getSegmentUnitMetadata().getVolumeType().getWriteQuorumSize();
    Set<InstanceId> membersToBroadcast = getMembersToBroadcast(segmentMembership, quorumSize);
    if (membersToBroadcast.size() < quorumSize - 1) {
      logger.debug("notEnoughMembers{}", segmentMembership);
      continueAfterBroadcast(BroadcastResult.FAILED);
      return;
    }

    logger.debug("broadcast to {}", membersToBroadcast);

    List<LogUuidAndLogId> uuidAndLogIds = new ArrayList<>(mapLogsToNewlyCreated.size());
    for (MutationLogEntry log : mapLogsToNewlyCreated.keySet()) {
      uuidAndLogIds
          .add(LogUuidAndLogId.newBuilder().setLogUuid(log.getUuid()).setLogId(log.getLogId())
              .build());
    }

    SegId segId = segmentUnit.getSegId();
    GiveYouLogIdRequest.Builder requestBuilder = GiveYouLogIdRequest.newBuilder();

    requestBuilder.setRequestId(broadcastLogRequestIdBuilder.incrementAndGet());

    requestBuilder.setMyInstanceId(myself.getId());
    requestBuilder.addAllLogUuidAndLogIds(uuidAndLogIds);
    requestBuilder
        .setPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(segmentMembership));
    requestBuilder.setVolumeId(segId.getVolumeId().getId());
    requestBuilder.setSegIndex(segId.getIndex());
    GiveYouLogIdRequest request = requestBuilder.build();

    List<EndPoint> eps = new LinkedList<>();
    Map<EndPoint, InstanceId> mapEndPointToInstanceId = new HashMap<>();
    ResponseCollector<EndPoint, GiveYouLogIdResponse> responseCollector = new ResponseCollector<>();
    for (InstanceId member : membersToBroadcast) {
      Instance instance = instanceStore.get(member);
      if (instance != null) {
        EndPoint ep = instance.getEndPointByServiceName(PortType.IO);
        eps.add(ep);
        mapEndPointToInstanceId.put(ep, member);
      } else {
        logger.warn("can't get end point from instance store for {} {}", segId, member);
        EndPoint bogusEp = new BogusEndPoint();
        eps.add(bogusEp);
        mapEndPointToInstanceId.put(bogusEp, member);
      }
    }

    AtomicBoolean finished = new AtomicBoolean(false);
    for (EndPoint endPoint : eps) {
      try {
        if (endPoint instanceof BogusEndPoint) {
          throw new Exception("bogus end point");
        }
        AsyncDataNode.AsyncIface client = ioClientFactory.generate(endPoint);
        GiveYouLogIdCallBack callback = new GiveYouLogIdCallBack(endPoint, quorumSize,
            segmentMembership,
            mapEndPointToInstanceId, finished, responseCollector, request);
        logger.debug("sent {} a request {}", endPoint, request.getRequestId());
        client.giveYouLogId(request, callback);
      } catch (Exception e) {
        logger.warn("can't send give you log id request to {} {} {} in membership {}", segId,
            endPoint,
            mapEndPointToInstanceId.get(endPoint), segmentMembership, e);
        responseCollector.addClientSideThrowable(endPoint, e);
      }
    }

    if (tooManyBadAccordingToMembership(responseCollector.getBadOnes(), mapEndPointToInstanceId,
        segmentMembership,
        quorumSize)) {
      if (finished.compareAndSet(false, true)) {
        continueAfterBroadcast(getBadBroadcastResult(responseCollector, mapEndPointToInstanceId));
      }
    } else if (!cfg.isWaitForBroadcastLogId()) {
      continueAfterBroadcast(BroadcastResult.SUCCESS);
    }
  }

  private BroadcastResult getBadBroadcastResult(
      ResponseCollector<EndPoint, GiveYouLogIdResponse> responseCollector,
      Map<EndPoint, InstanceId> mapEndPointToInstanceId) {
    Set<Entry<EndPoint, Throwable>> exceptions = new HashSet<>();
    exceptions.addAll(responseCollector.getClientSideThrowables().entrySet());
    exceptions.addAll(responseCollector.getServerSideThrowables().entrySet());
    if (isTempPrimary) {
      return exceptions.stream()
          .allMatch(throwable -> throwable.getValue() instanceof DisconnectionException) 
          ? BroadcastResult.FATAL :
          BroadcastResult.FAILED;
    } else {
      SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
      for (Entry<EndPoint, Throwable> epAndException : exceptions) {
        if (epAndException.getValue() instanceof DisconnectionException) {
          if (membership
              .isSecondaryCandidate(mapEndPointToInstanceId.get(epAndException.getKey()))) {
            return BroadcastResult.CANDIDATE_GONE;
          }
        } else if (epAndException.getValue() instanceof HasNewPrimaryException) {
          return BroadcastResult.HAS_NEW_PRIMARY;
        }
      }
      return BroadcastResult.FAILED;
    }
  }

  private void insertLogs(Collection<MutationLogEntry> logsToBeInserted) throws LogIdTooSmall {
    List<MutationLogEntry> logsThatCanBeApplied = Lists.newLinkedList();
    SegmentLogMetadata segLogMetadata = segmentUnit.getSegmentLogMetadata();
    segLogMetadata.insertOrUpdateLogs(logsToBeInserted, false, logsThatCanBeApplied);

    for (MutationLogEntry logToApply : logsThatCanBeApplied) {
      plalEngine.putLog(segLogMetadata.getSegId(), logToApply);
    }
  }

  private void continueAfterBroadcast(final BroadcastResult result) {
    if (result == BroadcastResult.FATAL) {
      logger.warn("refuse the write request {}", segmentUnit);
      callback.fail(new IncompleteGroupException());
    } else if (result == BroadcastResult.FAILED) {
      broadcastResult = result;
      tryToCommitLogs(I_AM_BROADCAST);
    } else if (result == BroadcastResult.CANDIDATE_GONE) {
      SegId segId = segmentUnit.getSegId();
      SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
      SegmentMembership newMembership = membership
          .removeSecondaryCandidate(membership.getSecondaryCandidate());
      try {
        segmentUnit.getArchive().asyncUpdateSegmentUnitMetadata(segId, newMembership, null);
        MembershipPusher.getInstance(myself).submit(segmentUnit.getSegId());
      } catch (Exception e) {
        logger.warn("{} cannot update membership to remove secondary candidate {}", segId,
            membership);
      }
      callback
          .fail(NettyExceptionHelper.buildMembershipVersionLowerException(segId, newMembership));
    } else if (result == BroadcastResult.HAS_NEW_PRIMARY) {
      logger.warn("secondary has new primary");
      callback.fail(new HasNewPrimaryException());
    } else {
      broadcastResult = result;
      tryToCommitLogs(I_AM_BROADCAST);
    }
  }

  private void checkMembershipAgain() {
    SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getMetadata().getMembership());
    if (Validator.requestHavingLowerGeneration(segmentUnit.getSegId(), currentMembership,
        requestMembership)) {
      PbMembership membershipToBeUpdated = PbRequestResponseHelper
          .buildPbMembershipFrom(currentMembership);
      responseBuilder.setMembership(membershipToBeUpdated);
    }
  }

  @Override
  public void failToProcess(Exception e) {
    Long requestId = request.getMetadata().getRequestId();

    callback.fail(e);
  }

  private void tryToCommitLogs(String who) {
    int okCount = countBothWtsAndBroadCast.incrementAndGet();
    logger.debug("{}, finish my job and try commit log, okCount = {}", who, okCount);

    if (okCount != 2) {
      return;
    }
    logger.debug("now commit logs");
    List<MutationLogEntry> committedLogs = new ArrayList<>();
    try {
      long totalSize = 0;
      for (MutationLogEntry log : mapLogsToNewlyCreated.keySet()) {
        totalSize += log.getLength();
        if (broadcastResult == BroadcastResult.SUCCESS) {
          if (!cfg.isPrimaryCommitLog() || segmentUnit.getSegmentLogMetadata()
              .commitLog(log.getLogId())) {
            unitResults[mapLogToIndex.get(log)] = PbIoUnitResult.PRIMARY_COMMITTED;
            if (log.canBeApplied()) {
              committedLogs.add(log);
            }
          } else {
            unitResults[mapLogToIndex.get(log)] = PbIoUnitResult.BROADCAST_FAILED;
          }
        } else if (broadcastResult == BroadcastResult.FAILED) {
          unitResults[mapLogToIndex.get(log)] = PbIoUnitResult.BROADCAST_FAILED;
        } else {
          throw new IllegalArgumentException();
        }
      }
      checkMembershipAgain();
    } catch (Exception e) {
      logger.warn("fail to broadcast caught an exception {}", segmentUnit.getSegId(), e);
    }

    PbWriteRequest metadata = request.getMetadata();
    for (int i = 0; i < unitResults.length; i++) {
      responseBuilder.addResponseUnits(PbRequestResponseHelper
          .buildPbWriteResponseUnitFrom(metadata.getRequestUnits(i), unitResults[i], logIds[i]));
    }

    for (MutationLogEntry committedLog : committedLogs) {
      plalEngine.putLog(segmentUnit.getSegId(), committedLog);
    }
    callback.complete(responseBuilder.build());
  }

  private void sendWriteRequestToSecondaryCandidateIfNecessary() throws Exception {
    SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
    InstanceId secondaryCandidate = membership.getSecondaryCandidate();
    if (secondaryCandidate != null && !isTempPrimary) {
      Instance secondaryCandidateInstance = instanceStore.get(secondaryCandidate);
      Validate.notNull(secondaryCandidateInstance);
      AsyncDataNode.AsyncIface client = ioClientFactory
          .generate(secondaryCandidateInstance.getEndPointByServiceName(PortType.IO));
      client.write(request.clone(), new AbstractMethodCallback<PbWriteResponse>() {
        @Override
        public void complete(PbWriteResponse object) {
        }

        @Override
        public void fail(Exception e) {
        }
      });
    }
  }

  private boolean checkResultAccordingToMembership(Collection<EndPoint> goodEndPoints,
      Map<EndPoint, InstanceId> mapEndPointToInstanceId, SegmentMembership membership,
      int quorumSize) {
    int numSecondaries = 0;
    int numArbiters = 0;
    int numJoiningSecondaries = 0;
    int numSecondaryCandidate = 0;
    int numPrimaryCandidate = 0;
    for (EndPoint ep : goodEndPoints) {
      InstanceId instanceId = mapEndPointToInstanceId.get(ep);
      if (membership.isSecondary(instanceId)) {
        numSecondaries++;
        if (membership.isPrimaryCandidate(instanceId)) {
          numPrimaryCandidate++;
        }
      } else if (membership.isArbiter(instanceId)) {
        numArbiters++;
      } else if (membership.isJoiningSecondary(instanceId)) {
        numJoiningSecondaries++;
      } else if (membership.isSecondaryCandidate(instanceId)) {
        numSecondaryCandidate++;
      } else {
        throw new IllegalArgumentException(instanceId + " @@ " + membership.toString());
      }
    }
    if (membership.getSecondaryCandidate() != null && numSecondaryCandidate == 0) {
      return false;
    } else if (membership.getPrimaryCandidate() != null && numPrimaryCandidate == 0) {
      return false;
    }

    return membership
        .checkWriteResultOfSecondariesAndArbiters(quorumSize, numSecondaries, numJoiningSecondaries,
            numArbiters);
  }

  private boolean tooManyBadAccordingToMembership(Collection<EndPoint> badEndPoints,
      Map<EndPoint, InstanceId> mapEndPointToInstanceId, SegmentMembership membership,
      int quorumSize) {
    int numBadSecondaries = 0;
    int numBadArbiters = 0;
    int numBadJoiningSecondaries = 0;
    for (EndPoint ep : badEndPoints) {
      InstanceId instanceId = mapEndPointToInstanceId.get(ep);
      if (membership.isSecondary(instanceId)) {
        numBadSecondaries++;
        if (membership.isPrimaryCandidate(instanceId)) {
          return true;
        }
      } else if (membership.isArbiter(instanceId)) {
        numBadArbiters++;
      } else if (membership.isJoiningSecondary(instanceId)) {
        numBadJoiningSecondaries++;
      } else if (membership.isSecondaryCandidate(instanceId)) {
        return true;
      }
    }
    logger.debug("not enough members ? {} {} {}", quorumSize, numBadSecondaries,
        numBadJoiningSecondaries);
    return membership
        .checkBadWriteResultOfSecondariesAndArbiters(quorumSize, numBadSecondaries,
            numBadJoiningSecondaries,
            numBadArbiters) || badEndPoints.size() == mapEndPointToInstanceId.size();
  }

  enum BroadcastResult {
    SUCCESS,
    FAILED,
    FATAL,
    CANDIDATE_GONE,

    HAS_NEW_PRIMARY,
  }

  class GiveYouLogIdCallBack extends AbstractMethodCallback<GiveYouLogIdResponse> {
    private final EndPoint endPoint;
    private final int quorumSize;
    private final SegmentMembership membership;
    private final Map<EndPoint, InstanceId> mapEndPointToInstanceId;
    private final AtomicBoolean finished;
    private final ResponseCollector<EndPoint, GiveYouLogIdResponse> responseCollector;
    private final GiveYouLogIdRequest giveYouLogIdRequest;

    GiveYouLogIdCallBack(EndPoint endPoint, int quorumSize, SegmentMembership membership,
        Map<EndPoint, InstanceId> mapEndPointToInstanceId, AtomicBoolean finished,
        ResponseCollector<EndPoint, GiveYouLogIdResponse> responseCollector,
        GiveYouLogIdRequest request) {
      this.endPoint = endPoint;
      this.quorumSize = quorumSize;
      this.membership = membership;
      this.mapEndPointToInstanceId = mapEndPointToInstanceId;
      this.finished = finished;
      this.responseCollector = responseCollector;
      this.giveYouLogIdRequest = request;
    }

    @Override
    public void complete(GiveYouLogIdResponse response) {
      try {
        logger.debug("{} responded {}", endPoint, giveYouLogIdRequest.getRequestId());
        responseCollector.addGoodResponse(endPoint, response);
        checkDone();
      } catch (Exception e) {
        logger.error("fail to broadcast to commit log", e);
        if (finished.compareAndSet(false, true)) {
          continueAfterBroadcast(BroadcastResult.FAILED);
        }
      }
    }

    @Override
    public void fail(Exception e) {
      responseCollector.addServerSideThrowable(endPoint, e);
      try {
        logger.info("{} got a failed response at {}, request {}", segmentUnit.getSegId(), endPoint,
            giveYouLogIdRequest.getRequestId(), e);
        if (e instanceof MembershipVersionLowerException) {
          SegmentMembership membership = NettyExceptionHelper
              .getMembershipFromBuffer(
                  ((MembershipVersionLowerException) e).getMembership());
          segmentUnit.lockStatus();
          try {
            SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata()
                .getMembership();
            if (currentMembership.compareTo(membership) < 0) {
              segmentUnit.getArchive()
                  .updateSegmentUnitMetadata(segmentUnit.getSegId(), membership,
                      null);
            }
          } finally {
            segmentUnit.unlockStatus();
          }
        }

        checkDone();
      } catch (Exception t) {
        logger.error("caught an exception", t);
        if (finished.compareAndSet(false, true)) {
          continueAfterBroadcast(BroadcastResult.FAILED);
        }
      }
    }

    private void checkDone() {
      Collection<EndPoint> goodOnes = responseCollector.getGoodOnes();
      Collection<EndPoint> badOnes = responseCollector.getBadOnes();
      if (checkResultAccordingToMembership(goodOnes, mapEndPointToInstanceId, membership,
          quorumSize)) {
        logger.debug("Got a good quorum. Bait out");
        if (finished.compareAndSet(false, true)) {
          continueAfterBroadcast(BroadcastResult.SUCCESS);
        }
      } else if (tooManyBadAccordingToMembership(badOnes, mapEndPointToInstanceId, membership,
          quorumSize)) {
        logger
            .info("it is impossible to get a good quorum any more. Bait out {}", responseCollector);
        if (finished.compareAndSet(false, true)) {
          continueAfterBroadcast(getBadBroadcastResult(responseCollector, mapEndPointToInstanceId));
        }
      } else {
        if (goodOnes.size() + badOnes.size() >= mapEndPointToInstanceId.size()) {
          logger.error(
              "what's wrong ? response collector :" 
                  + " {}, good ones : {}, bad ones : {}, segment unit : {}",
              responseCollector, goodOnes, badOnes, segmentUnit);
          throw new IllegalStateException("there must be something wrong !!");
        }
      }
    }

  }
}
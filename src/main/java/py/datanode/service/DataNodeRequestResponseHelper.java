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
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.exception.NoAvailableBufferException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.thrift.datanode.service.AbortedLogThrift;
import py.thrift.datanode.service.ArbiterPokePrimaryRequest;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastTypeThrift;
import py.thrift.datanode.service.ExistingLogsThrift;
import py.thrift.datanode.service.ImReadyToBeSecondaryRequest;
import py.thrift.datanode.service.InitiateCopyPageRequestThrift;
import py.thrift.datanode.service.JoiningGroupRequest;
import py.thrift.datanode.service.KickOffPotentialPrimaryRequest;
import py.thrift.datanode.service.LogInfoThrift;
import py.thrift.datanode.service.LogStatusThrift;
import py.thrift.datanode.service.LogSyncActionThrift;
import py.thrift.datanode.service.LogThrift;
import py.thrift.datanode.service.SecondaryCopyPagesRequest;
import py.thrift.datanode.service.SyncLogsRequest;
import py.thrift.datanode.service.SyncShadowPagesRequest;
import py.thrift.share.SegmentMembershipThrift;

/**
 * helper class that converts internal classes to and from thrift classes.
 *
 */
public class DataNodeRequestResponseHelper extends RequestResponseHelper {
  public static BroadcastRequest buildNewPrimaryPhase1Request(SegId segId,
      SegmentMembership membership, long myInstanceId, int n, int lastN) {
    BroadcastRequest request = new BroadcastRequest(RequestIdBuilder.get(), 0,
        segId.getVolumeId().getId(), segId.getIndex(), BroadcastTypeThrift.VotePrimaryPhase1,
        buildThriftMembershipFrom(segId, membership));
    request.setMyself(myInstanceId);
    request.setProposalNum(n);
    request.setMinProposalNum(lastN);
    return request;
  }

  public static BroadcastRequest buildNewPrimaryPhase2Request(long requestId, SegId segId,
      long myInstanceId, long n,
      SegmentMembership chosenMembership, SegmentMembership newMembership) {
    BroadcastRequest request = new BroadcastRequest(requestId, n, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.VotePrimaryPhase2,
        RequestResponseHelper.buildThriftMembershipFrom(segId, newMembership));
    request.setProposalValue(buildThriftMembershipFrom(segId, chosenMembership));
    request.setMyself(myInstanceId);
    return request;
  }

  public static BroadcastRequest buildUpdateSegmentUnitVolumeMetadataJsonBroadcastRequest(
      SegId segId,
      SegmentMembership currentMembership, String volumeMetadataJson) {
    BroadcastRequest request = new BroadcastRequest(RequestIdBuilder.get(), 1,
        segId.getVolumeId().getId(),
        segId.getIndex(), BroadcastTypeThrift.UpdateSegmentUnitVolumeMetadataJson,
        RequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    request.setVolumeMetadataJson(volumeMetadataJson);
    return request;
  }

  public static KickOffPotentialPrimaryRequest buildKickOffPrimaryRequest(long requestId,
      SegId segId,
      long myInstanceId, SegmentMembership membership, long potentialPrimary,
      int becomePrimaryPriority) {
    return new KickOffPotentialPrimaryRequest(requestId, segId.getVolumeId().getId(),
        segId.getIndex(),
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership), potentialPrimary,
        becomePrimaryPriority).setMyself(myInstanceId);

  }

  public static KickOffPotentialPrimaryRequest buildKickOffPrimaryRequest(SegId segId,
      long myInstanceId,
      SegmentMembership membership, long potentialPrimary) {
    return buildKickOffPrimaryRequest(RequestIdBuilder.get(), segId, myInstanceId, membership,
        potentialPrimary, 0);
  }

  public static BroadcastRequest buildCollectLogInfoRequest(SegId segId,
      SegmentMembership membership) {
    return new BroadcastRequest(RequestIdBuilder.get(), 0L, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.CollectLogsInfo, buildThriftMembershipFrom(segId, membership));
  }

  public static JoiningGroupRequest buildJoiningGroupRequest(SegId segId,
      SegmentMembership membership,
      InstanceId myInstanceId, long pplId, long pclId, boolean isArbiter) {
    return new JoiningGroupRequest(RequestIdBuilder.get(), segId.getVolumeId().getId(),
        segId.getIndex(),
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership), myInstanceId.getId(),
        pplId, pclId,
        isArbiter);
  }

  public static BroadcastRequest buildWriteDataRequest(long requestId, SegId segId,
      SegmentMembership membership,
      DataLog entry, Collection<DataLog> unconfirmedAbortedLogs) {
    BroadcastRequest request = new BroadcastRequest();

    request.setRequestId(requestId);
    request.setLogUuid(entry.getLogUuid());
    request.setLogId(entry.getLogId());
    request.setOffset(entry.getOffset());
    request.setData(entry.getData());
    request.setChecksum(entry.getCheckSum());
    request.setStatus(convertToThriftStatus(entry.getLogStatus()));

    request.setSegIndex(segId.getIndex());
    request.setVolumeId(segId.getVolumeId().getId());
    request.setLogType(BroadcastTypeThrift.WriteMutationLog);
    request.setMembership(RequestResponseHelper.buildThriftMembershipFrom(segId, membership));

    if (unconfirmedAbortedLogs != null && unconfirmedAbortedLogs.size() > 0) {
      List<AbortedLogThrift> listAbortedLogThrift = new ArrayList<>(unconfirmedAbortedLogs.size());
      for (DataLog abortedLog : unconfirmedAbortedLogs) {
        listAbortedLogThrift.add(buildAbortedLogThriftFrom(abortedLog));
      }
      request.setAbortedLogs(listAbortedLogThrift);
    }
    return request;
  }

  public static BroadcastRequest buildWriteDataRequest(long requestId, SegId segId,
      SegmentMembership membership,
      MutationLogEntry entry, Collection<MutationLogEntry> unconfirmedAbortedLogs) {
    BroadcastRequest request = new BroadcastRequest();

    request.setRequestId(requestId);
    request.setLogUuid(entry.getUuid());
    request.setLogId(entry.getLogId());
    request.setOffset(entry.getOffset());
    request.setData(entry.getData());
    request.setChecksum(entry.getChecksum());
    request.setStatus(convertToThriftStatus(entry.getStatus()));

    request.setSegIndex(segId.getIndex());
    request.setVolumeId(segId.getVolumeId().getId());
    request.setLogType(BroadcastTypeThrift.WriteMutationLog);
    request.setMembership(RequestResponseHelper.buildThriftMembershipFrom(segId, membership));

    if (unconfirmedAbortedLogs != null && unconfirmedAbortedLogs.size() > 0) {
      List<AbortedLogThrift> listAbortedLogThrift = new ArrayList<>(unconfirmedAbortedLogs.size());
      for (MutationLogEntry abortedLog : unconfirmedAbortedLogs) {
        listAbortedLogThrift.add(buildAbortedLogThriftFrom(abortedLog));
      }
      request.setAbortedLogs(listAbortedLogThrift);
    }
    return request;
  }

  public static BroadcastRequest buildWriteLogsRequest(long requestId, long logId, SegId segId,
      SegmentMembership membership, List<MutationLogEntry> logs,
      Collection<MutationLogEntry> unconfirmedAbortedLogs) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.WriteMutationLogs,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));

    List<LogThrift> listLogThrift = new ArrayList<>(logs.size());
    for (MutationLogEntry log : logs) {
      listLogThrift.add(buildLogThriftFrom(segId, log, LogSyncActionThrift.Add));
    }
    request.setLogs(listLogThrift);

    if (unconfirmedAbortedLogs != null && unconfirmedAbortedLogs.size() > 0) {
      List<AbortedLogThrift> listAbortedLogThrift = new ArrayList<>(unconfirmedAbortedLogs.size());
      for (MutationLogEntry abortedLog : unconfirmedAbortedLogs) {
        listAbortedLogThrift.add(buildAbortedLogThriftFrom(abortedLog));
      }
      request.setAbortedLogs(listAbortedLogThrift);
    }
    return request;
  }

  public static BroadcastRequest buildCheckSecondaryIfFullCopyIsRequired(long requestId,
      SegId segId, long myInstanceId,
      SegmentMembership membership) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.CheckSecondaryIfFullCopyIsRequired,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    request.setMyself(myInstanceId);
    return request;
  }

  public static BroadcastRequest buildStepIntoSecondaryEnrolledRequest(long requestId, SegId segId,
      long myInstanceId,
      SegmentMembership membership) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.StepIntoSecondaryEnrolled,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    request.setMyself(myInstanceId);
    return request;
  }

  public static BroadcastRequest buildBroadcastAbortedLogRequestByDataLog(long requestId,
      SegId segId,
      SegmentMembership membership, Collection<DataLog> unconfirmedAbortedLogs) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1L, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.BroadcastAbortedLogs,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));

    if (unconfirmedAbortedLogs != null && unconfirmedAbortedLogs.size() > 0) {
      List<AbortedLogThrift> listAbortedLogThrift = new ArrayList<>(unconfirmedAbortedLogs.size());
      for (DataLog abortedLog : unconfirmedAbortedLogs) {
        listAbortedLogThrift.add(buildAbortedLogThriftFrom(abortedLog));
      }
      request.setAbortedLogs(listAbortedLogThrift);
    }
    return request;
  }

  public static BroadcastRequest buildBroadcastAbortedLogRequest(long requestId, SegId segId,
      SegmentMembership membership, Collection<MutationLogEntry> unconfirmedAbortedLogs) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1L, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.BroadcastAbortedLogs,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));

    if (unconfirmedAbortedLogs != null && unconfirmedAbortedLogs.size() > 0) {
      List<AbortedLogThrift> listAbortedLogThrift = new ArrayList<>(unconfirmedAbortedLogs.size());
      for (MutationLogEntry abortedLog : unconfirmedAbortedLogs) {
        listAbortedLogThrift.add(buildAbortedLogThriftFrom(abortedLog));
      }
      request.setAbortedLogs(listAbortedLogThrift);
    }
    return request;
  }

  public static MutationLogEntry buildAbortedLogFromThrift(AbortedLogThrift logThrift) {
    MutationLogEntry abortedLog = MutationLogEntryFactory
        .createEmptyLog(logThrift.getLogUuid(), logThrift.getLogId(), logThrift.getOffset(),
            logThrift.getChecksum(), logThrift.getLength());
    abortedLog.setStatus(LogStatus.Aborted);
    return abortedLog;
  }

  public static AbortedLogThrift buildAbortedLogThriftFrom(DataLog abortedLog) {
    return new AbortedLogThrift(abortedLog.getLogUuid(), abortedLog.getLogId(),
        abortedLog.getOffset(),
        abortedLog.getLength(), abortedLog.getCheckSum());
  }

  public static AbortedLogThrift buildAbortedLogThriftFrom(MutationLogEntry abortedLog) {
    return new AbortedLogThrift(abortedLog.getUuid(), abortedLog.getLogId(),
        abortedLog.getOffset(),
        abortedLog.getLength(), abortedLog.getChecksum());
  }

  public static BroadcastRequest buildAddOrRemoveMemberRequest(SegId segId,
      SegmentMembership membership) {
    BroadcastRequest request = new BroadcastRequest(RequestIdBuilder.get(), 0L,
        segId.getVolumeId().getId(),
        segId.getIndex(), BroadcastTypeThrift.AddOrRemoveMember,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    return request;
  }

  public static BroadcastRequest buildAddOrRemoveMemberRequest(long requestId, SegId segId,
      SegmentMembership membership, long swplId, long swclId) {
    BroadcastRequest request = new BroadcastRequest(requestId, 0L, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.AddOrRemoveMember,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    request.setPswpl(swplId);
    request.setPswcl(swclId);
    return request;
  }

  public static BroadcastRequest buildJoinMeRequest(long requestId, SegId segId,
      SegmentMembership membership,
      long swplId, long swclId) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1L, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.JoinMe,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    request.setPswpl(swplId);
    request.setPswcl(swclId);
    return request;
  }

  public static BroadcastRequest buildPrimaryChangedRequest(long requestId, SegId segId,
      SegmentMembership membership) {
    return new BroadcastRequest(requestId, 1L, segId.getVolumeId().getId(), segId.getIndex(),
        BroadcastTypeThrift.PrimaryChanged,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
  }

  public static SyncLogsRequest buildSyncLogRequest(SegId segId, long plId, long clId,
      List<Long> uncommittedLogIds,
      List<Long> uuidsOfLogsWithoutLogId, SegmentMembership membership, long myself,
      long preprimarySessionId,
      SegmentUnitStatus requestSegmentUnitStatus) {
    return buildSyncLogRequest(segId, plId, clId, uncommittedLogIds, uuidsOfLogsWithoutLogId,
        membership, myself,
        preprimarySessionId, requestSegmentUnitStatus);
  }

  public static ArbiterPokePrimaryRequest buildArbiterPokePrimaryRequest(SegId segId,
      SegmentMembership membership,
      long myself, SegmentUnitStatus requestSegmentUnitStatus) {
    SegmentMembershipThrift membershipThrift = buildThriftMembershipFrom(segId, membership);

    ArbiterPokePrimaryRequest request = new ArbiterPokePrimaryRequest(RequestIdBuilder.get(),
        segId.getVolumeId().getId(), segId.getIndex(), membershipThrift,
        requestSegmentUnitStatus.getSegmentUnitStatusThrift(), myself);
    return request;
  }

  public static ImReadyToBeSecondaryRequest buildImReadyToBeSecondaryRequest(SegId segId,
      SegmentMembership membership, long myself, SegmentUnitStatus requestSegmentUnitStatus) {
    SegmentMembershipThrift membershipThrift = buildThriftMembershipFrom(segId, membership);

    ImReadyToBeSecondaryRequest request = new ImReadyToBeSecondaryRequest(RequestIdBuilder.get(),
        segId.getVolumeId().getId(), segId.getIndex(), membershipThrift, myself);
    return request;
  }

  public static SyncShadowPagesRequest buildSyncShadowPagesRequest(SegId segId,
      SegmentMembership membership,
      long myself, long preprimarySessionId, long startLogId, long endLogId, int snapshotVersion,
      boolean inRollbackProgress) {
    SyncShadowPagesRequest request = new SyncShadowPagesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setMembership(buildThriftMembershipFrom(segId, membership));
    request.setMyself(myself);
    request.setPreprimarySid(preprimarySessionId);
    request.setVolumeId(segId.getVolumeId().getId());
    request.setSegIndex(segId.getIndex());
    request.setStartLogId(startLogId);
    request.setEndLogId(endLogId);
    request.setInRollbackProgress(inRollbackProgress);
    request.setSnapshotVersion(snapshotVersion);
    return request;
  }

  public static LogStatusThrift convertToThriftStatus(LogStatus status) {
    switch (status) {
      case Aborted:
        return LogStatusThrift.Aborted;
      case Committed:
        return LogStatusThrift.Committed;
      case Created:
        return LogStatusThrift.Created;
      case AbortedConfirmed:
        return LogStatusThrift.AbortedConfirmed;
      default:
        Validate.isTrue(false, " Log status is invalid " + status);
        return null;
    }
  }

  public static LogStatus convertFromThriftStatus(LogStatusThrift status) {
    switch (status) {
      case Aborted:
        return LogStatus.Aborted;
      case Committed:
        return LogStatus.Committed;
      case Created:
        return LogStatus.Created;
      case AbortedConfirmed:
        return LogStatus.AbortedConfirmed;
      default:
        Validate.isTrue(false, " Log status is invalid " + status);
        return null;
    }
  }

  public static LogThrift buildLogThriftFrom(SegId segId, MutationLogEntry logEntry, byte[] data,
      LogSyncActionThrift action) {
    return buildLogThriftFrom(segId, logEntry.getUuid(), logEntry.getLogId(), logEntry.getStatus(),
        logEntry.getOffset(), logEntry.getChecksum(), logEntry.getLength(), data,
        action, true);
  }

  public static LogThrift buildLogThriftFrom(SegId segId, MutationLogEntry logEntry,
      LogSyncActionThrift action) {
    return buildLogThriftFrom(segId, logEntry.getUuid(), logEntry.getLogId(), logEntry.getStatus(),
        logEntry.getOffset(), logEntry.getChecksum(), logEntry.getLength(), null,
        action, false);
  }

  public static LogThrift buildLogThriftFrom(SegId segId, long uuid, long logId,
      LogStatus logStatus, long offset,
      long checksum, int length, byte[] data, LogSyncActionThrift action,
      boolean checkDataExists) {
    LogThrift logThrift = new LogThrift(uuid, logId, action, convertToThriftStatus(logStatus));
    if (action == LogSyncActionThrift.Add || action == LogSyncActionThrift.AddMetadata) {
      LogInfoThrift logInfo = new LogInfoThrift(offset, checksum, length);
      if (logStatus == LogStatus.Created || logStatus == LogStatus.Committed) {
        if (data == null && checkDataExists) {
          StringBuilder sb = new StringBuilder();
          sb.append("segId:").append(segId).append(", logEntry:[id:").append(logId)
              .append("status:")
              .append(logStatus).append("offset:").append(offset).append("checksum:")
              .append(checksum)
              .append("length:").append(length).append("]action:").append(action);
          Validate.isTrue(false, sb.toString());
        }

        if (data != null) {
          if (data.length != length) {
            Validate.isTrue(false, "data=" + data.length + ", length=" + length);
          }
        }

        logInfo.setData(data);
      }
      logThrift.setLogInfo(logInfo);
    }

    return logThrift;
  }

  public static LogThrift buildLogThriftFrom(SegId segId, long uuid, long logId,
      LogStatus logStatus, long offset,
      long checksum, int length, byte[] data, int snapshotVersion, LogSyncActionThrift action,
      boolean checkDataExists, long cloneVolumeDataOffset, int cloneVolumeDataLength) {
    LogThrift logThrift = buildLogThriftFrom(segId, uuid, logId, logStatus, offset, checksum,
        length, data,
        action, checkDataExists);
    return logThrift;
  }

  public static LogStatus buildLogStatusFrom(LogStatusThrift logStatusThrift) {
    switch (logStatusThrift) {
      case Aborted:
        return LogStatus.Aborted;
      case AbortedConfirmed:
        return LogStatus.AbortedConfirmed;
      case Committed:
        return LogStatus.Committed;
      case Created:
        return LogStatus.Created;
      default:
        return null;
    }
  }

  public static ExistingLogsThrift buildExistingLogsThriftFrom(
      ExistingLogsForSyncLogResponse existingLogs) {
    return new ExistingLogsThrift(existingLogs.sizeOfConfirmedLogs(),
        existingLogs.getBitMapForLogIds(),
        existingLogs.getBitMapForCommitted(), existingLogs.getBitMapForAbortedConfirmed());
  }

  public static ExistingLogsForSyncLogResponse buildExistingLogsFrom(
      ExistingLogsThrift existingLogsThrift) {
    if (existingLogsThrift == null) {
      return new ExistingLogsForSyncLogResponse(0, true);
    }

    ExistingLogsForSyncLogResponse existingLogs = new ExistingLogsForSyncLogResponse(
        existingLogsThrift.getConfirmedSize(), true);
    existingLogs.setBitMapForLogIds(existingLogsThrift.getBitMapForLogIds());
    existingLogs.setBitMapForCommitted(existingLogsThrift.getBitMapForCommitted());
    existingLogs.setBitMapForAbortedConfirmed(existingLogsThrift.getBitMapForAbortedConfirmed());

    return existingLogs;
  }

  public static MutationLogEntry buildMutationLogEntryFrom(long uuid, long logId, long archiveId,
      LogInfoThrift logInfo)
      throws NoAvailableBufferException {
    MutationLogEntry logEntry = MutationLogEntryFactory
        .createLogForPrimary(uuid, logId, archiveId, logInfo.getOffset(),
            logInfo.data == null ? null : logInfo.data.array(), logInfo.getChecksum());
    logEntry.setLength(logInfo.getLength());
    return logEntry;
  }

  public static MutationLogEntry buildMutationLogEntryForSecondary(long uuid, long logId,
      long archiveId, LogInfoThrift logInfo)
      throws NoAvailableBufferException {
    MutationLogEntry logEntry = MutationLogEntryFactory
        .createLogForSecondary(uuid, logId, archiveId, logInfo.getOffset(),
            logInfo.data == null ? null : logInfo.data.array(), logInfo.getChecksum());
    logEntry.setLength(logInfo.getLength());
    return logEntry;
  }

  public static MutationLogEntry buildMutationLogEntryForSyncLog(long uuid, long logId,
      LogInfoThrift logInfo)
      throws NoAvailableBufferException {
    MutationLogEntry logEntry = MutationLogEntryFactory
        .createLogForSyncLog(uuid, logId, logInfo.getOffset(),
            logInfo.data == null ? null : logInfo.data.array(), logInfo.getChecksum());
    logEntry.setLength(logInfo.getLength());
    return logEntry;
  }

  public static MutationLogEntry buildMutationLogEntryForMovingPclLogInfoMayNull(long uuid,
      long logId,
      LogInfoThrift logInfo) throws NoAvailableBufferException {
    MutationLogEntry logEntry = null;
    if (logInfo == null) {
      logEntry = MutationLogEntryFactory.createLogForSyncLog(0L, logId, 0, null, 0);
      logEntry.setDummy(true);
    } else {
      logEntry = buildMutationLogEntryForSyncLog(uuid, logId, logInfo);
    }
    return logEntry;
  }

  public static BroadcastRequest buildGiveMeYourLogsRequest(long requestId, long logId, SegId segId,
      SegmentMembership membership, int lease, long myInstanceId, Map<Long, Long> receivedLogIdMap,
      List<Long> logIdsPrimaryAlreadyExisted, long pcl) {
    BroadcastRequest request = new BroadcastRequest(requestId, logId, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.GiveMeYourLogs,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership, lease));
    request.setMyself(myInstanceId);
    request.setLogIdsAlreadyHave(logIdsPrimaryAlreadyExisted);
    if (receivedLogIdMap != null && !receivedLogIdMap.isEmpty()) {
      request.setReceivedLogIdMap(receivedLogIdMap);
    }
    request.setPcl(pcl);
    return request;
  }

  public static BroadcastRequest buildCatchUpMyLogsRequest(long requestId, long logId, SegId segId,
      SegmentMembership membership, long latestPrePrimaryClId, long myInstanceId,
      long preprimarySessionId) {
    BroadcastRequest request = new BroadcastRequest(requestId, logId, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.CatchUpMyLogs,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    request.setMyself(myInstanceId);
    request.setPcl(latestPrePrimaryClId);
    request.setPreprimarySid(preprimarySessionId);
    return request;
  }

  public static SecondaryCopyPagesRequest buildSecondaryCopyPagesRequest(SegId segId,
      SegmentMembership membership,
      long myself, long plId, long pclId, long preprimarySessionId, boolean forceCopyPage) {
    SecondaryCopyPagesRequest request = new SecondaryCopyPagesRequest(RequestIdBuilder.get(),
        segId.getVolumeId().getId(), segId.getIndex(), plId, pclId,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership), myself,
        preprimarySessionId, forceCopyPage);
    return request;
  }

  public static InitiateCopyPageRequestThrift buildInitiateCopyPageRequestThrift(
      long preprimarySessionId, SegId segId,
      SegmentMembership membership, long myself, long sessionId, long catchUpLog,
      int maxSnapshotId) {
    InitiateCopyPageRequestThrift request = new InitiateCopyPageRequestThrift(
        RequestIdBuilder.get(),
        preprimarySessionId, segId.getVolumeId().getId(), segId.getIndex(), sessionId,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership), myself, catchUpLog,
        maxSnapshotId);
    return request;
  }

  public static BroadcastRequest buildGetMaxLogIdRequest(long requestId, SegId segId,
      SegmentMembership membership) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1L, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.GetPrimaryMaxLogId,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    return request;
  }

  public static BroadcastRequest buildStartBecomePrimaryRequest(long requestId, SegId segId,
      long instanceId, SegmentMembership membership) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1L, segId.getVolumeId().getId(),
        segId.getIndex(), BroadcastTypeThrift.StartBecomePrimary,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    request.setMyself(instanceId);
    return request;
  }

  public static LogStatus convertFromPbStatus(PbBroadcastLogStatus status) {
    switch (status) {
      case ABORT:
        return LogStatus.Aborted;
      case COMMITTED:
        return LogStatus.Committed;
      case CREATED:
        return LogStatus.Created;
      case ABORT_CONFIRMED:
        return LogStatus.AbortedConfirmed;
      default:
        Validate.isTrue(false, " Log status is invalid " + status);
        return null;
    }
  }
}

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.RequestResponseHelper;
import py.app.context.AppContext;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.datanode.archive.RawArchiveManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.InvalidSegmentStatusException;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.exception.LogNotFoundException;
import py.datanode.exception.StaleMembershipException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.copy.CopyPageStatus;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.broadcast.CompleteLog;
import py.datanode.segment.datalog.broadcast.CompletingLog;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.datalog.sync.log.driver.PclDriverStatus;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogReduceCollector;
import py.datanode.segment.datalogbak.catchup.PclDrivingType;
import py.datanode.segment.datalogbak.catchup.PlalDriver;
import py.exception.GenericThriftClientFactoryException;
import py.exception.LeaseExtensionFrozenException;
import py.exception.NoAvailableBufferException;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.membership.SegmentMembershipHelper;
import py.proto.Broadcastlog.PbBackwardSyncLogMetadata;
import py.proto.Broadcastlog.PbBackwardSyncLogRequestUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogResponseUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbErrorCode;
import py.proto.Broadcastlog.PbMembership;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.Iface;
import py.thrift.datanode.service.ImReadyToBePrimaryRequest;
import py.thrift.datanode.service.ImReadyToBePrimaryResponse;

public class SyncLogReceiver implements SyncLogTask {

  private static final Logger logger = LoggerFactory.getLogger(SyncLogReceiver.class);
  private final AppContext context;
  private final InstanceStore instanceStore;
  private final SegmentUnitManager segmentUnitManager;
  private final DataNodeConfiguration configuration;
  private final RawArchiveManager archiveManager;
  private final GenericThriftClientFactory<Iface> clientFactory;
  private final long requestId;
  private final PbBackwardSyncLogRequestUnit requestUnit;
  private final PlalEngine plalEngine;
  private final List<PbBackwardSyncLogMetadata> missLogsAtSecondary = new ArrayList<>();
  private final SyncLogReduceCollector<PbBackwardSyncLogResponseUnit,
      PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest> reduceCollector;
  long logIdOfCatchUpLog = LogImage.INVALID_LOG_ID;

  private PbErrorCode errorCode = PbErrorCode.Ok;

  public SyncLogReceiver(long requestId, AppContext context, InstanceStore instanceStore,
      PlalEngine plalEngine, DataNodeConfiguration configuration, RawArchiveManager archiveManager,
      GenericThriftClientFactory<DataNodeService.Iface> clientFactory,
      SegmentUnitManager segmentUnitManager, PbBackwardSyncLogRequestUnit requestUnit,
      SyncLogReduceCollector<PbBackwardSyncLogResponseUnit,
          PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest> reduceCollector) {
    this.context = context;
    this.instanceStore = instanceStore;
    this.configuration = configuration;
    this.archiveManager = archiveManager;
    this.clientFactory = clientFactory;
    this.segmentUnitManager = segmentUnitManager;
    this.requestId = requestId;
    this.requestUnit = requestUnit;
    this.plalEngine = plalEngine;
    this.reduceCollector = reduceCollector;
  }

  private static void logsCountWarning(int logsCountWarningLevel, int logsCountWarningThreshold,
      SegId segId,
      LogImage logImage) {
    if ((logsCountWarningLevel & 1) != 0
        && logImage.getNumLogsAfterPcl() > logsCountWarningThreshold) {
      logger.warn("a lot of not committed logs! seg id {} log image : {}", segId, logImage);
    } else if ((logsCountWarningLevel & 1 << 1) != 0
        && logImage.getNumLogsFromPlalToPcl() > logsCountWarningThreshold) {
      logger.warn("a lot of not applied logs! seg id {} log image : {}", segId, logImage);
    } else if ((logsCountWarningLevel & 1 << 2) != 0
        && logImage.getNumLogsFromPplToPlal() > logsCountWarningThreshold) {
      logger.warn("a lot of not persisted logs! seg id {} log image : {}", segId, logImage);
    }
  }

  @Override
  public SyncLogTaskType type() {
    return SyncLogTaskType.RECEIVER;
  }

  @Override
  public SegId getSegId() {
    return new SegId(requestUnit.getVolumeId(), requestUnit.getSegIndex());
  }

  /**
   * process requests from primary.
   */
  @Override
  public boolean process() {
    SegId segId = new SegId(requestUnit.getVolumeId(), requestUnit.getSegIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (null == segmentUnit) {
      logger.warn("got a backward sync log request {}, but segment unit not found", requestUnit);
      errorCode = PbErrorCode.SEGMENT_NOT_FOUND;
      return true;
    }

    if (segmentUnit.beginProcessPclDriverForSecondary()) {
      try {
        return processInternal(segmentUnit);
      } finally {
        segmentUnit.finishProcessPclDriverForSecondary();
      }
    } else {
      logger.warn("what happen? more than one pcl driver processing at the same time {}", segId);
      errorCode = PbErrorCode.SERVICE_IS_BUSY;
      return true;
    }
  }

  private boolean processInternal(SegmentUnit segmentUnit) {
    SegId segId = segmentUnit.getSegId();
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    if (segmentUnit.isPausedCatchUpLogForSecondary()) {
      logger.info("got a backward sync log request {}, but segment unit has paused catch up log",
          requestUnit);
      errorCode = PbErrorCode.NOT_SECONDARY_PCL_DRIVE_TYPE;
      return true;
    }

    try {
      LogImage logImage = segmentLogMetadata
          .getLogImage(configuration.getMaxNumberLogsForSecondaryToSyncUpOnSecondarySide());

      logsCountWarning(configuration.getLogsCountWarningLevel(),
          configuration.getLogsCountWarningThreshold(), segId, logImage);
      PclDrivingType drivingType = segmentUnit.getPclDrivingType(context.getInstanceId());
      if (!drivingType.isSecondary()) {
        logger.info(
            "got a backward sync log request {}, "
                + "but segment unit pcl driver type is not secondary type",
            requestUnit);
        errorCode = PbErrorCode.NOT_SECONDARY_PCL_DRIVE_TYPE;
        return true;
      }

      if (segmentUnit.isHoldToExtendLease()) {
        logger.info(
            "got a backward sync log request {}, but segment unit has hold to extend lease",
            requestUnit);
        errorCode = PbErrorCode.SECONDARY_HAS_HOLD_EXTEND_LEASE;
        return true;
      }

      SegmentUnitStatus newStatus = null;
      SegmentUnitStatus currentStatus = segmentUnit.getSegmentUnitMetadata().getStatus();
      PbMembership returnedMembership = null;
      switch (requestUnit.getCode()) {
        case SEGMENT_NOT_FOUND:
          logger.warn("Caught SegmentNotFoundExceptionThrift exception. "
                  + "{} can't find the segment unit {} Changing my status from secondary to start."
                  + "Request:{}",
              requestUnit.getMyself(), segmentUnit.getSegId(), requestUnit);

          SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata()
              .getMembership();
          if (currentMembership.getTempPrimary() != null
              && currentMembership.getTempPrimary().getId() != requestUnit.getMyself()) {
            return true;
          }

          if (currentMembership.getTempPrimary() == null
              && currentMembership.getPrimary().getId() != requestUnit.getMyself()) {
            return true;
          }
          newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
              ? SegmentUnitStatus.Start :
              SegmentUnitStatus.OFFLINED;
          break;
        case STALE_MEMBERSHIP:
          logger
              .warn("Caught an StaleMembershipException. {} has lower (epoch) than the primary {} "
                      + ". Changing status from Secondary to Start to catch up logs {} ",
                  context.getInstanceId(), requestUnit.getMyself(), requestUnit);
          returnedMembership = requestUnit.getMembership();
          Validate.notNull(returnedMembership);

          newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
              ? SegmentUnitStatus.Start :
              SegmentUnitStatus.OFFLINED;

          if (currentStatus == SegmentUnitStatus.OFFLINING) {
            returnedMembership = null;
          }
          break;
        case CL_TOO_SMALL:

          MutationLogEntry pclLog = segmentLogMetadata.getLog(requestUnit.getPswcl());
          logger.warn(
              "Caught an LogIdTooSmallException The log id of PCLDriver {} requests doesn't "
                  + "exist any more"
                  + "in the primary {}. Hold its lease and don't extend it until it gets "
                  + "expired and "
                  + "changed to Start status {}, PCL log {}, log image {}",
              context.getInstanceId(), requestUnit.getMyself(), requestUnit, pclLog,
              segmentLogMetadata.getLogImage(0));
          if (drivingType == PclDrivingType.Secondary && !segmentUnit
              .isNotAcceptWriteReadRequest()) {
            logger.warn("current status: {}, we should hold the extend lease for segId={}.",
                currentStatus,
                segId);
            segmentUnit.setHoldToExtendLease(true);
            return true;
          } else if (drivingType == PclDrivingType.JoiningSecondary
              || drivingType == PclDrivingType.VotingSecondary) {
            newStatus = SegmentUnitStatus.Start;
          } else {
            newStatus = SegmentUnitStatus.OFFLINED;
          }
          returnedMembership = requestUnit.getMembership();
          Validate.notNull(returnedMembership);
          break;
        case STILL_AT_PRE_PRIMARY:
          logger
              .info(" ***** {} still at the PrePrimary status, let's {} wait and retry later. {} ",
                  requestUnit.getMyself(), context.getInstanceId(), segmentUnit.getSegId());

          return true;
        case NOT_PRIMARY:
          logger.warn(
              "Caught a NotPrimaryExceptionThrift. {} primary doesn't "
                  + "believe it is the primary any more. Changing {} "
                  + " status from Secondary to Start. {}. Exception: {} ",
              requestUnit.getMyself(), context.getInstanceId(), requestUnit);
          returnedMembership = requestUnit.getMembership();
          Validate.notNull(returnedMembership);

          newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
              ? SegmentUnitStatus.Start :
              SegmentUnitStatus.OFFLINED;
          break;
        case YOU_ARE_NOT_IN_MEMBERSHIP:
          logger.error(
              "Caught a YouAreNotInMembershipExceptionThrift Exception. "
                  + "I am not at the primary's membership "
                  + "Secondary: {} primary: {} my membership: {} the primary membership:{} {} ",
              context.getInstanceId(), requestUnit.getMyself(),
              segmentUnit.getSegmentUnitMetadata().getMembership(), requestUnit.getMembership(),
              requestUnit);

          returnedMembership = requestUnit.getMembership();
          Validate.notNull(returnedMembership);
          newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
              ? SegmentUnitStatus.Start :
              SegmentUnitStatus.OFFLINED;
          break;
        case INVALID_MEMBERSHIP:
          logger.error(
              "**************** It is impossible that InvalidMembershipException can be "
                  + "received at a secondary side *************. "
                  + "Secondary: {} primary:{}  my membership: {} the primary membership:{} {} ",
              context.getInstanceId(), requestUnit.getMyself(),
              segmentUnit.getSegmentUnitMetadata().getMembership(), requestUnit.getMembership(),
              requestUnit);
          logger.error("Really don't know how to do, change my status to Start");

          returnedMembership = requestUnit.getMembership();
          Validate.notNull(returnedMembership);
          newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
              ? SegmentUnitStatus.Start :
              SegmentUnitStatus.OFFLINED;
          break;
        case WRONG_PRE_PRIMARY_SESSION:
          logger
              .warn("session has changed when voting, there is no need retry for segId={}", segId);
          newStatus = SegmentUnitStatus.Start;
          break;
        case UNKNOWN_ERROR:
          logger.warn(
              "Caught an mystery exception when syncing log from {}, request={}, accept io={}　",
              requestUnit.getMyself(), requestUnit, segmentUnit.isNotAcceptWriteReadRequest());
          if (!segmentUnit.isNotAcceptWriteReadRequest()) {
            return true;
          }
          break;
        default:
          break;
      }

      if (requestUnit.getCode() != PbErrorCode.Ok && segmentUnit.isNotAcceptWriteReadRequest()) {
        logger.warn(
            "can't sync logs from primary for an offlining segment unit {},"
                + " going to remove all logs after pcl",
            segId);
        return true;
      }

      if (requestUnit.getCode() == PbErrorCode.Ok) {
        try {
          logger.debug("going to extend my:[{}] lease", segmentUnit.getSegId());
          segmentUnit.extendMyLease();

        } catch (LeaseExtensionFrozenException e) {
          logger.trace(
              "Myself {} can't extend lease but still go ahead to process it lease. "
                  + "the primary is {} and segId is {} ",
              context.getInstanceId(), requestUnit.getMyself(), segmentUnit.getSegId());
        }
      }

      {
        if (segmentUnit.getSegmentLogMetadata().gapOfPclLogIds() > configuration
            .getMaxGapOfPclLoggingForSyncLogs()) {
          logger.warn(
              "seg id {} gap of primary cl too much {}, last packager time {} "
                  + "last receiver time {} current time {}",
              segId, segmentUnit.getSegmentLogMetadata().gapOfPclLogIds(),
              segmentUnit.getSecondaryPclDriver().getLastProcessPackagerTimestamp(),
              segmentUnit.getSecondaryPclDriver().getLastProcessReceiverTimestamp(),
              System.currentTimeMillis());
        }
        segmentUnit.getSecondaryPclDriver()
            .setLastProcessReceiverTimestamp(System.currentTimeMillis());
      }

      SegmentMembership newMembership = null;
      if (returnedMembership != null) {
        newMembership = PbRequestResponseHelper.buildMembershipFrom(returnedMembership);
        if (newMembership.compareVersion(segmentUnit.getSegmentUnitMetadata().getMembership())
            <= 0) {
          if (requestUnit.getCode() != PbErrorCode.Ok
              && requestUnit.getCode() != PbErrorCode.CL_TOO_SMALL) {
            logger.error("there is an error {},but membership has change {} to {} ",
                requestUnit.getCode(), newMembership,
                segmentUnit.getSegmentUnitMetadata().getMembership());
            return true;
          }
        } else if (SegmentMembershipHelper
            .okToUpdateToHigherMembership(newMembership,
                segmentUnit.getSegmentUnitMetadata().getMembership(), context.getInstanceId(),
                segmentUnit.getSegmentUnitMetadata().getVolumeType().getNumMembers())) {
          logger.info("the primary has sent a membership with higher generation. ");
        } else {
          logger.warn(
              "My newStatus is supposed to be {} but since it is not ok update the membership {}, "
                  + "change my status to Deleting",
              newStatus, newMembership);

          newMembership = null;
          newStatus = SegmentUnitStatus.Deleting;
        }
      } else if (requestUnit.getCode() == PbErrorCode.Ok) {
        SegmentMembership membershipFromPrimary = PbRequestResponseHelper
            .buildMembershipFrom(requestUnit.getMembership());
        if (membershipFromPrimary.compareTo(segmentUnit.getSegmentUnitMetadata().getMembership())
            > 0) {
          newMembership = membershipFromPrimary;
        }
      }

      if (newMembership != null || newStatus != null) {
        if (currentStatus != SegmentUnitStatus.OFFLINING
            || (currentStatus == SegmentUnitStatus.OFFLINING) && (newStatus
            == SegmentUnitStatus.OFFLINED)) {
          try {
            archiveManager
                .updateSegmentUnitMetadata(segmentUnit.getSegId(), newMembership, newStatus);
          } catch (Exception e) {
            logger.error("For some reason, we can't update segment unit metadata", e);
            return false;
          }
        }
      }

      if (newStatus != null) {
        return true;
      }

      segmentLogMetadata.setSwclIdTo(requestUnit.getPswcl());
      segmentLogMetadata.setSwplIdTo(requestUnit.getPswpl());
      segmentLogMetadata.updateSwplAndremoveLogsPriorToSwpl();

      if (requestUnit.getMetadatOfLogsCount() <= 0) {
        logger.info("primary return any logs for sync log {}", requestUnit);
        return true;
      }

      long maxLogId = requestUnit.getMetadatOfLogs(requestUnit.getMetadatOfLogsCount() - 1)
          .getLogId();
      if (segmentUnit.isNotAcceptWriteReadRequest()) {
        logger.warn(
            "we just need load our max log id because this segment unit "
                + "doesn't accept write read request anymore {}",
            segmentLogMetadata.getMaxLogId());
        maxLogId = segmentLogMetadata.getMaxLogId();
      }

      logger.debug("begin process all logs from primary {}, max logs {}", requestUnit, maxLogId);
      int count = processAllLogsFromPrimary(segmentLogMetadata, segmentUnit, requestUnit,
          plalEngine, maxLogId);
      logger.info("process {} logs in this sync log request", count);

      long primaryMaxLogIdWhenPsi = requestUnit.getPrimaryMaxLogIdWhenPsi();
      AtomicBoolean missLogWhenPsi = segmentUnit.getSegmentUnitMetadata().getMissLogWhenPsi();
      if (segmentLogMetadata.getClId() < primaryMaxLogIdWhenPsi
          && segmentLogMetadata.getPrimaryMaxLogIdWhenPsi() < primaryMaxLogIdWhenPsi) {
        segmentLogMetadata.setPrimaryMaxLogIdWhenPsi(primaryMaxLogIdWhenPsi);
        if (missLogWhenPsi.compareAndSet(false, true)) {
          logger.warn("segment unit {} miss log when psi", segmentUnit);
        }
      }

      if (missLogWhenPsi.get()) {
        if (segmentLogMetadata.getClId() >= primaryMaxLogIdWhenPsi) {
          if (missLogWhenPsi.compareAndSet(true, false)) {
            logger.warn("segment unit {} not miss log when psi", segmentUnit);
          }
        }
      }
      return true;
    } finally {
      if (segmentUnit.getSegmentLogMetadata().getClId() > segmentUnit.getSegmentLogMetadata()
          .getLalId()) {
        try {
          PlalDriver.drive(segmentUnit.getSegmentLogMetadata(), segmentUnit, plalEngine);
        } catch (Throwable t) {
          logger.warn("fail to drive plal for segment {} ", segmentUnit.getSegId(), t);
        } finally {
          segmentUnit.updateDrivePlalLease();
        }
      }

      SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
      InstanceId myself = context.getInstanceId();
      if (membership.isPrimaryCandidate(myself)) {
        long newCl = segmentLogMetadata.getClId();
        if (newCl == requestUnit.getPrimaryClId()) {
          logger.warn("I think I am ready to be primary {}", segId);
          DataNodeService.Iface client = null;
          try {
            EndPoint primaryEndPoint = instanceStore.get(new InstanceId(requestUnit.getMyself()))
                .getEndPoint();
            client = clientFactory
                .generateSyncClient(primaryEndPoint, configuration.getDataNodeRequestTimeoutMs());
            ImReadyToBePrimaryResponse notifyResponse = client.iAmReadyToBePrimary(
                new ImReadyToBePrimaryRequest(RequestIdBuilder.get(),
                    RequestResponseHelper.buildThriftMembershipFrom(segId, membership), newCl,
                    myself.getId(), RequestResponseHelper.buildThriftSegIdFrom(segId)));
            SegmentMembership newMembership = RequestResponseHelper
                .buildSegmentMembershipFrom(notifyResponse.getNewMembership()).getSecond();
            if (newMembership.compareTo(segmentUnit.getSegmentUnitMetadata().getMembership()) > 0) {
              logger.warn(
                  "{} receive a new membership(which I should already have), but just update it {}",
                  segId, newMembership);
              segmentUnit.getArchive().updateSegmentUnitMetadata(segId, newMembership, null);
            }
          } catch (TException e) {
            logger.warn("fail to notify current primary that I am ready to be primary", e);
          } catch (InvalidSegmentStatusException | StaleMembershipException e) {
            logger.error("can't update membership ");
          } catch (GenericThriftClientFactoryException e) {
            logger.error("", e);
          } finally {
            clientFactory.releaseSyncClient(client);
            client = null;
          }
        }
      }

      if (missLogsAtSecondary.size() == 0) {
        if (segmentUnit.getSecondaryPclDriver()
            .setStatusWithCheck(PclDriverStatus.Processing, PclDriverStatus.Free)) {
          segmentUnit.getSecondaryPclDriver().updateLease();

          segmentUnit.setPrimaryClIdAndCheckNeedPclDriverOrNot(requestUnit.getPrimaryClId());
        }
      }
    }
  }

  /**
   * generate responses and respond (might be batched).
   */
  @Override
  public boolean reduce() {
    SegId segId = new SegId(requestUnit.getVolumeId(), requestUnit.getSegIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    PbBackwardSyncLogResponseUnit.Builder builder = PbBackwardSyncLogResponseUnit.newBuilder();
    builder.setVolumeId(requestUnit.getVolumeId());
    builder.setSegIndex(requestUnit.getSegIndex());
    builder.setPcl(segmentUnit.getSegmentLogMetadata().getClId());
    builder.setPpl(segmentUnit.getSegmentLogMetadata().getPlId());
    if (errorCode != PbErrorCode.Ok) {
      builder.setCode(errorCode);
    }
    if (logIdOfCatchUpLog != LogImage.INVALID_LOG_ID) {
      builder.setCatchUpLogId(logIdOfCatchUpLog);
    }

    for (PbBackwardSyncLogMetadata logMetadata : missLogsAtSecondary) {
      builder.addMissDataLogs(logMetadata);
    }

    PbBackwardSyncLogResponseUnit responseUnit = builder.build();

    if (!reduceCollector.collect(requestId, responseUnit)) {
      if (missLogsAtSecondary.size() != 0) {
        if (segmentUnit.getSecondaryPclDriver()
            .setStatusWithCheck(PclDriverStatus.Processing, PclDriverStatus.Free)) {
          segmentUnit.getSecondaryPclDriver().updateLease();

          segmentUnit.setPrimaryClIdAndCheckNeedPclDriverOrNot(requestUnit.getPrimaryClId());
        }
      }
    }
    return true;
  }

  public int processAllLogsFromPrimary(SegmentLogMetadata segLogMetadata, SegmentUnit segUnit,
      PbBackwardSyncLogRequestUnit request, PlalEngine plalEngine, long maxLogId) {
    logger.debug("segLogMetadata {}", segLogMetadata);
    if (request == null) {
      logger.debug("the response is null");
      return 0;
    }

    logIdOfCatchUpLog = LogImage.INVALID_LOG_ID;
    if (isCatchingUpLog(segUnit)) {
      logIdOfCatchUpLog = segUnit.getSecondaryCopyPageManager().getCatchUpLog().getLogId();
    }

    segLogMetadata.setSwclIdTo(request.getPswcl());
    segLogMetadata.setSwplIdTo(request.getPswpl());
    segLogMetadata.updateSwplAndremoveLogsPriorToSwpl();

    segLogMetadata.setPrimaryClId(request.getPrimaryClId());

    long clId = segLogMetadata.getClId();
    List<PbBackwardSyncLogMetadata> missingLogs = new ArrayList<>();
    Map<MutationLogEntry, LogStatus> beCompletedLogs = new HashMap<>(
        request.getMetadatOfLogsCount());
    List<CompletingLog> beInsertLogs = new ArrayList<>();
    List<PbBackwardSyncLogMetadata> beInstertCompleteLogs = new ArrayList<>();
    List<CompleteLog> logsAlreadyInsertedButNotCompleted = new ArrayList<>();

    try {
      segLogMetadata
          .giveYouLogId(clId, maxLogId, request.getMetadatOfLogsList(), plalEngine, missingLogs,
              logIdOfCatchUpLog, beInsertLogs, beCompletedLogs, logsAlreadyInsertedButNotCompleted,
              beInstertCompleteLogs);
      logger.debug("after give you log id, found {} logs missing at secondary", missingLogs.size());

      missLogsAtSecondary.addAll(missingLogs);
      processAllBeInsertLogs(segUnit, maxLogId, beInsertLogs, beCompletedLogs,
          logsAlreadyInsertedButNotCompleted, beInstertCompleteLogs);
    } catch (LogIdTooSmall e) {
      try {
        logger.warn("i have a commit log {} but primary do not have for segId={}",
            e.getCurrentSmallestId(),
            segLogMetadata.getSegId());
        segUnit.getArchive()
            .updateSegmentUnitMetadata(segUnit.getSegId(), null, SegmentUnitStatus.Start);
        segUnit.getSegmentUnitMetadata().setMigrationStatus(MigrationStatus.FROMFAILMIGRATION);
      } catch (InvalidSegmentStatusException | StaleMembershipException sm) {
        logger.error("archive {} can not update segment unit to start", segUnit.getArchive(), sm);
      }
      missLogsAtSecondary.clear();
      return 0;
    }

    return request.getMetadatOfLogsCount() - missingLogs.size();
  }

  private void processAllBeInsertLogs(SegmentUnit segUnit, long maxLogId,
      List<CompletingLog> beInsertLogs, Map<MutationLogEntry, LogStatus> beCompletedLogs,
      List<CompleteLog> logsAlreadyInsertedButNotCompleted,
      List<PbBackwardSyncLogMetadata> beInstertCompleteLogs) {
    logger.debug("begin process all be insert logs {}, be completed logs {}, "
            + "be insert completed logs {}, expect new cl id {}",
        beInsertLogs, beCompletedLogs, beInstertCompleteLogs, maxLogId);

    List<CompletableFuture<Boolean>> resultsOfInsertedLogs = new LinkedList<>();
    if (!beInsertLogs.isEmpty()) {
      for (int i = 0; i < beInsertLogs.size(); i++) {
        resultsOfInsertedLogs.add(beInsertLogs.get(i).getFuture());
      }
    }

    if (!logsAlreadyInsertedButNotCompleted.isEmpty()) {
      for (int i = 0; i < logsAlreadyInsertedButNotCompleted.size(); i++) {
        resultsOfInsertedLogs.add(logsAlreadyInsertedButNotCompleted.get(i).getFuture());
      }
    }

    for (Map.Entry<MutationLogEntry, LogStatus> entry : beCompletedLogs.entrySet()) {
      try {
        SegmentLogMetadata
            .finalizeLogStatus(entry.getKey(), entry.getValue(), segUnit.getSegmentLogMetadata());
      } catch (Exception e) {
        logger.warn(
            "System exit, caught an exception,"
                + " backward sync log status={}, local log status ={}",
            segUnit.getSegmentLogMetadata(),
            entry.getKey(), e);

        System.exit(0);
      }
      if (entry.getKey().isCommitted()) {
        int pageIndex = (int) (entry.getKey().getOffset() / configuration.getPageSize());
        segUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
      }
      plalEngine.putLog(segUnit.getSegId(), entry.getKey());
    }

    List<MutationLogEntry> mutationLogEntries = new ArrayList<>();
    int pageIndex = 0;
    try {
      for (PbBackwardSyncLogMetadata logMetadata : beInstertCompleteLogs) {
        LogStatus newStatus = LogStatus.convertPbStatusToLogStatus(
            logMetadata.hasStatus() ? logMetadata.getStatus() : PbBroadcastLogStatus.COMMITTED);
        MutationLogEntry entry = null;
        try {
          if (logMetadata.hasLogData()) {
            byte[] data = logMetadata.getLogData().getData().toByteArray();
            entry = MutationLogEntryFactory
                .createLogForSyncLog(logMetadata.getUuid(), logMetadata.getLogId(),
                    logMetadata.getLogData().getOffset(),
                    data,
                    logMetadata.getLogData().getCheckSum());
            entry.setLength(data.length);
            entry.setStatus(newStatus);
          } else {
            entry = MutationLogEntryFactory
                .createLogForSyncLog(logMetadata.getUuid(), logMetadata.getLogId(), 0,
                    null, 0);
            entry.setStatus(newStatus);
            entry.setDummy(true);
          }
        } catch (NoAvailableBufferException e) {
          logger
              .info("create log for sync log failed, no available buffer. log id {}, log status {}",
                  logMetadata.getLogId(), logMetadata.getStatus(), e);
          missLogsAtSecondary.add(logMetadata);
          continue;
        }

        int pageSize = configuration.getPageSize();
        if (!entry.hasData()) {
          entry.apply();
          entry.setPersisted();
          pageIndex = (int) (entry.getOffset() / pageSize);
          if (entry.isCommitted()) {
            segUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
          }
          if (segUnit.getSecondaryCopyPageManager() != null) {
            CopyPageStatus status = segUnit.getSecondaryCopyPageManager().getCopyPageStatus();
            if (status == CopyPageStatus.CatchupLog
                || status == CopyPageStatus.InitiateCatchupLog) {
              if (!entry.getStatus().equals(LogStatus.AbortedConfirmed)) {
                segUnit.getSecondaryCopyPageManager().getCopyPageBitmap().clear(pageIndex);
              } else {
                logger.warn("copy page status {} entry {}", status, entry);
              }
            } else {
              logger.warn("copy page status {} entry {}", status, entry);
            }
          }
          logger.info("a log need do copy page {}", pageIndex);
        }

        if (entry.getStatus() == LogStatus.Committed) {
          pageIndex = (int) (entry.getOffset() / pageSize);
          segUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);

        }
        mutationLogEntries.add(entry);
      }
    } catch (Exception e) {
      logger.warn("got some exception when process clone page index {}", pageIndex, e);
      logger.warn("{} logs will not be process go on",
          beInstertCompleteLogs.size() - mutationLogEntries.size());
    }

    int timeoutMs = 10000;
    boolean insertResult = segUnit.getSegmentLogMetadata()
        .insertLogsForSecondary(plalEngine, mutationLogEntries, timeoutMs);

    boolean waitCompletingLogs = true;
    for (CompletableFuture<Boolean> future : resultsOfInsertedLogs) {
      long startTime = System.currentTimeMillis();
      try {
        waitCompletingLogs &= future.get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        logger.error("wait insert log failed {}, {}", beInsertLogs,
            logsAlreadyInsertedButNotCompleted, e);
        waitCompletingLogs = false;
        break;
      } catch (Exception e) {
        logger.error("wait insert log failed {}, {}", beInsertLogs,
            logsAlreadyInsertedButNotCompleted, e);
        waitCompletingLogs = false;
        break;
      }
      timeoutMs -= System.currentTimeMillis() - startTime;
    }

    if (insertResult && waitCompletingLogs) {
      try {
        if (missLogsAtSecondary.size() != 0) {
          PbBackwardSyncLogMetadata minLog = Collections
              .min(missLogsAtSecondary, (o1, o2) -> Long.compare(o1.getLogId(), o2.getLogId()));
          int indexOfNewClId = -1;
          int indexOfLower = 0;
          int indexOfHigher = requestUnit.getMetadatOfLogsCount() - 1;
          int indexOfMiddler = (indexOfHigher + indexOfLower) / 2;
          while (indexOfLower <= indexOfHigher) {
            indexOfMiddler = (indexOfHigher + indexOfLower) / 2;
            PbBackwardSyncLogMetadata logOfMiddler = requestUnit.getMetadatOfLogs(indexOfMiddler);
            int com = Long.compare(minLog.getLogId(), logOfMiddler.getLogId());
            if (com < 0) {
              indexOfHigher = indexOfMiddler - 1;
            } else if (com > 0) {
              indexOfLower = indexOfMiddler + 1;
            } else {
              indexOfNewClId = indexOfMiddler;
              break;
            }

          }
          if (indexOfNewClId <= 0) {
            logger.info("the first log metadata from sync log request unit missing at secondary, "
                    + "we can not move cl, seg id {}, missing log id {}, request unit first log {}",
                minLog.getLogId(), requestUnit.getMetadatOfLogs(0).getLogId(), segUnit.getSegId());
            return;
          }
          maxLogId = requestUnit.getMetadatOfLogs(indexOfMiddler - 1).getLogId();
          logger.info("after calculate missing log, try to set new cl id {}", maxLogId);
        }

        if (maxLogId > segUnit.getSegmentLogMetadata().getClId()) {
          logger.info("begin move cl to {}, max log id {}, primary cl id {}, seg id {}", maxLogId,
              segUnit.getSegmentLogMetadata().getMaxLogId(),
              segUnit.getSegmentLogMetadata().getPrimaryClId(), segUnit.getSegId());
          Pair<Long, List<MutationLogEntry>> result = segUnit.getSegmentLogMetadata()
              .moveClTo(maxLogId);
          plalEngine.putLogs(segUnit.getSegId(), result.getSecond());
        } else {
          logger.info("no need move cl to {}, secondary cl has moved at id {}", maxLogId,
              segUnit.getSegmentLogMetadata().getClId());
        }
      } catch (LogIdTooSmall | LogNotFoundException e) {
        logger.error(
            "found a exception, i traversal all logs from log double queue and primary log metadata"
                + ", calculate the new cl {} and try to move cl to the new cl failed {}",
            maxLogId, segUnit.getSegmentLogMetadata().getLogImageIncludeLogsAfterPcl(), e);
      }
    } else {
      logger.info(
          "we can not move cl, because insert completed logs (result = {}) "
              + "or insert completing (result = {})",
          insertResult, waitCompletingLogs);
    }
  }

  private boolean isCatchingUpLog(SegmentUnit segUnit) {
    SecondaryCopyPageManager manager = segUnit.getSecondaryCopyPageManager();
    if (manager != null && manager.getCopyPageStatus() == CopyPageStatus.CatchupLog) {
      return true;
    }
    return false;
  }

  public void fail(PbErrorCode errorCode) {
    this.errorCode = errorCode;
    if (!reduce()) {
      logger.warn("reduce sync log receiver failed");
    }
  }

  @Override
  public InstanceId homeInstanceId() {
    return context.getInstanceId();
  }

}

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

package py.datanode.segment.datalogbak.catchup;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.datanode.archive.RawArchiveManager;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.InappropriateLogStatusException;
import py.datanode.exception.InvalidSegmentStatusException;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.exception.StaleMembershipException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPageStatus;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.datanode.service.ExistingLogsForSyncLogResponse;
import py.exception.FailedToSendBroadcastRequestsException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.LeaseExtensionFrozenException;
import py.exception.NoAvailableBufferException;
import py.exception.QuorumNotFoundException;
import py.exception.SnapshotVersionMissMatchForMergeLogsException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.membership.SegmentMembershipHelper;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.Iface;
import py.thrift.datanode.service.ImReadyToBePrimaryRequest;
import py.thrift.datanode.service.ImReadyToBePrimaryResponse;
import py.thrift.datanode.service.LogSyncActionThrift;
import py.thrift.datanode.service.LogThrift;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.datanode.service.StillAtPrePrimaryExceptionThrift;
import py.thrift.datanode.service.SyncLogsRequest;
import py.thrift.datanode.service.SyncLogsResponse;
import py.thrift.datanode.service.WrongPrePrimarySessionExceptionThrift;
import py.thrift.share.InvalidMembershipExceptionThrift;
import py.thrift.share.LogIdTooSmallExceptionThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.StaleMembershipExceptionThrift;
import py.thrift.share.YouAreNotInMembershipExceptionThrift;

public class PclDriver implements ChainedLogDriver {
  private static final Logger logger = LoggerFactory.getLogger(PclDriver.class);
  private static final int MAX_DEFAULT_NUM_LOG_AFTER_PCL = 200;
  private static final int THRESHOLD_NUM_LOGS_HAVING_BEEN_PROCESSED = 10;
  private static Random random = new Random(System.currentTimeMillis());
  private final DataNodeServiceAsyncClientWrapper dataNodeAsyncClient;
  int numlogsprocessedbypcldriving = 0;
  int numLogsWillBeDriven = 0;
  private SegmentUnit segUnit;
  private RawArchiveManager archiveManager;
  private SegmentLogMetadata segLogMetadata;
  private EndPoint primaryEndPoint;
  private SegmentMembership membership;
  private GenericThriftClientFactory<DataNodeService.Iface> clientFactory;
  private DataNodeConfiguration cfg;
  private InstanceId myself;
  private PlalEngine plalEngine;
  private InstanceStore instanceStore;
  
  
  public PclDriver(RawArchiveManager archiveManager, SegmentUnit segmentUnit,
      SegmentLogMetadata segLogMetadata,
      DataNodeConfiguration cfg, GenericThriftClientFactory<Iface> clientFactory,
      EndPoint primaryEndPoint, SegmentMembership membership,
      DataNodeServiceAsyncClientWrapper dataNodeAsyncClient, InstanceId myself,
      PlalEngine plalEngine,
      InstanceStore instanceStore) {
    this.archiveManager = archiveManager;
    this.segUnit = segmentUnit;
    this.segLogMetadata = segLogMetadata;
    this.clientFactory = clientFactory;
    this.primaryEndPoint = primaryEndPoint;
    this.membership = membership;
    this.cfg = cfg;
    this.dataNodeAsyncClient = dataNodeAsyncClient;
    this.myself = myself;
    this.plalEngine = plalEngine;
    this.instanceStore = instanceStore;
  }

  public static int processAllLogsFromPrimary(SegmentLogMetadata segLogMetadata,
      SegmentUnit segUnit,
      SyncLogsRequest request, SyncLogsResponse response, boolean notAcceptRequest,
      PlalEngine plalEngine,
      DataNodeConfiguration cfg, InstanceId myself) {
    try {
      logger.debug("segLogMetadata {}", segLogMetadata);
      if (response == null) {
        logger.debug("the response is null");
        return 0;
      }

      logger.debug("the response is {}", response);

      List<Long> missingLogsAtPrimary = new ArrayList<>();
      for (Long uuid : request.getUuidsOfLogsWithoutLogId()) {
        if (!response.getMapUuidToLogId().containsKey(uuid)) {
          missingLogsAtPrimary.add(uuid);
        }
      }
      segLogMetadata.checkPrimaryMissingLogs(missingLogsAtPrimary, cfg.getIoTimeoutMs());

      segLogMetadata.giveYouLogId(response.getMapUuidToLogId(), plalEngine, true, false);

      segLogMetadata.setSwclIdTo(response.getPswcl());
      segLogMetadata.setSwplIdTo(response.getPswpl());
      segLogMetadata.updateSwplAndremoveLogsPriorToSwpl();

      if (response.getMissingLogsAtSecondary() == null && response.getExistingLogs() == null) {
        return 0;
      }

      if (request.getLogsAfterCl() != null && request.getLogsAfterClSize() > 0) {
        Validate.notNull(response.getExistingLogs());
        if (request.getLogsAfterClSize() < response.getExistingLogs().getConfirmedSize()) {
          logger.error(
              "size of logs in sync-log-request {} is " 
                  + "less than size of logs confirmed by primary {}",
              request.getLogsAfterClSize(), response.getExistingLogs().getConfirmedSize());
          Validate.isTrue(false);
        }
      }

      List<Long> uncommittedLogIds = request.getLogsAfterCl();
      List<LogThrift> missingLogsAtSecondaryToldByPrimary = response.getMissingLogsAtSecondary();
      if (uncommittedLogIds == null) {
        uncommittedLogIds = new ArrayList<Long>();
      }
      logger.debug("missingLogsAtSecondaryToldByPrimary {}", missingLogsAtSecondaryToldByPrimary);
      if (missingLogsAtSecondaryToldByPrimary == null) {
        missingLogsAtSecondaryToldByPrimary = new ArrayList<LogThrift>();
      }

      ExistingLogsForSyncLogResponse existingLogsAtSecondaryToldByPrimary =
          DataNodeRequestResponseHelper
          .buildExistingLogsFrom(response.getExistingLogs());

      List<LogThrift> missingLogsAtSecondary = new ArrayList<LogThrift>();
      long maxLogId = LogImage.INVALID_LOG_ID;
      int numLogs = 0;
      int positionForMissing = 0;
      long primaryClId = response.getPrimaryClId();
      int lastLogVersion = -1;
      LogImage oldLogImage = segLogMetadata.getLogImage(0);
      for (int positionForUncommitted = 0;
          positionForUncommitted < existingLogsAtSecondaryToldByPrimary
              .sizeOfConfirmedLogs(); positionForUncommitted++) {
        numLogs++;
       
       
       
        long uncommittedLogId = uncommittedLogIds.get(positionForUncommitted);
        MutationLogEntry logAtSecondary = segLogMetadata.getLog(uncommittedLogId);

        while (positionForMissing < missingLogsAtSecondaryToldByPrimary.size()) {
          LogThrift missingLogAtSecondary = missingLogsAtSecondaryToldByPrimary
              .get(positionForMissing);
          if (missingLogAtSecondary.getLogId() > uncommittedLogId) {
           
            break;
          }

          missingLogsAtSecondary.add(missingLogAtSecondary);
          maxLogId = Math.max(maxLogId, missingLogAtSecondary.getLogId());
          positionForMissing++;
          numLogs++;
        }

        if (existingLogsAtSecondaryToldByPrimary.exist(positionForUncommitted)) {
         
         
         

          if (logAtSecondary == null) {
            Validate.isTrue(false, "after sync log, the log " + uncommittedLogId + " disappears");
          } else {
            LogStatus logStatusAtPrimary = existingLogsAtSecondaryToldByPrimary
                .getLogStatus(positionForUncommitted);
            boolean canBeAppliedBefore = logAtSecondary.canBeApplied();
            try {
              finalizeLogStatus(logAtSecondary, logStatusAtPrimary, segLogMetadata);
              if (logAtSecondary.isCommitted()) {
                int pageIndex = (int) (logAtSecondary.getOffset() / cfg.getPageSize());
                segUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
              }
            } catch (Exception e) {
              logger.warn("System exit, caught an exception, request={}, response={}", request,
                  response,
                  e);
              System.exit(0);
            }
            if (!canBeAppliedBefore) {
             
              if (logAtSecondary.canBeApplied()) {
                plalEngine.putLog(segUnit.getSegId(), logAtSecondary);
              }
            }
          }

          maxLogId = Math.max(maxLogId, uncommittedLogId);
        } else {
         
          
          if (uncommittedLogId < response.getPrimaryClId()) {
            logger
                .warn("remove a log which is missing at the primary: {}, segId={}", logAtSecondary,
                    segUnit.getSegId());
            try {
              MutationLogEntry logToRemove = segLogMetadata.removeLog(uncommittedLogId);

              if (logToRemove.getStatus() == LogStatus.Committed) {
                logger.warn("i have a commit log {} but primary do not have for segId={}",
                    logToRemove,
                    segLogMetadata.getSegId());
                segUnit.getArchive()
                    .updateSegmentUnitMetadata(segUnit.getSegId(), null, SegmentUnitStatus.Start);
                segUnit.getSegmentUnitMetadata()
                    .setMigrationStatus(MigrationStatus.FROMFAILMIGRATION);
              }
            } catch (InappropriateLogStatusException | LogIdTooSmall e) {
              logger.warn("Can't remove the log {} which is missing at the primary. "
                      + "primary CLID: {}, my CLID: {}", logAtSecondary, primaryClId,
                  oldLogImage.getClId());
            } catch (InvalidSegmentStatusException | StaleMembershipException sm) {
              logger.error("archive {} can not update segment unit to start", segUnit.getArchive(),
                  sm);
            }
          }
        }
      }

      for (; positionForMissing < missingLogsAtSecondaryToldByPrimary.size();
          positionForMissing++) {
        LogThrift logThrift = missingLogsAtSecondaryToldByPrimary.get(positionForMissing);
        missingLogsAtSecondary.add(logThrift);
        maxLogId = Math.max(maxLogId, logThrift.getLogId());
        numLogs++;
      }

      if (notAcceptRequest) {
        missingLogsAtSecondary = truncateLogWhoseIdLargerThanValidId(missingLogsAtSecondary,
            oldLogImage.getMaxLogId());
        maxLogId = oldLogImage.getMaxLogId();
      }

      boolean meetErrorWhenCreateLogs = false;
      List<MutationLogEntry> mutationLogs = new ArrayList<>();
      for (LogThrift logThrift : missingLogsAtSecondary) {
        try {
          LogStatus logStatus = DataNodeRequestResponseHelper
              .buildLogStatusFrom(logThrift.getStatus());
         
          if (!MutationLogEntry.isFinalStatus(logStatus)) {
            logger.warn(
                "the secondary misses a log at the primary, " 
                    + "but this log at the primary is not at the final status: {}",
                logThrift);
            Validate.isTrue(false);
          }

          MutationLogEntry entry = segLogMetadata.getLog(logThrift.getLogId());
          if (entry == null) {
            entry = DataNodeRequestResponseHelper
                .buildMutationLogEntryForMovingPclLogInfoMayNull(logThrift.getLogUuid(),
                    logThrift.getLogId(), logThrift.getLogInfo());
            entry.setStatus(logStatus);
            int pageSize = cfg.getPageSize();
           
           
           
            if (logThrift.getAction() == LogSyncActionThrift.AddMetadata) {
              entry.apply();
              entry.setPersisted();
              int pageIndex = (int) (entry.getOffset() / pageSize);
              segUnit.getSecondaryCopyPageManager().getCopyPageBitmap().clear(pageIndex);
              logger.info("a log need do copy page {}", pageIndex);
            }

            if (logStatus == LogStatus.Committed) {
              int pageIndex = (int) (entry.getOffset() / pageSize);
              segUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
            }
            mutationLogs.add(entry);
          } else {
            if (entry.isFinalStatus()) {
              if (entry.getStatus() != logStatus) {
                logger.warn("entry: {}, primary status: {}", entry, logStatus);
              }
            } else {
              addSetStatus(entry, logStatus, segLogMetadata);
              if (entry.isCommitted()) {
                int pageIndex = (int) (entry.getOffset() / cfg.getPageSize());
                segUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
              }
            }
          }
        } catch (NoAvailableBufferException e) {
          logger
              .info("there is no enough buffer secondary, message: {}, logImage {}", e.getMessage(),
                  oldLogImage);
          meetErrorWhenCreateLogs = true;
          break;
        } catch (Exception e) {
          meetErrorWhenCreateLogs = true;
          logger.warn("catch an exception when build mutation log from log thrift", e);
          break;
        }
      }

      if (!mutationLogs.isEmpty()) {
       
        try {
          maxLogId = Math.min(maxLogId, mutationLogs.get(mutationLogs.size() - 1).getLogId());
          boolean success = segLogMetadata.insertLogsForSecondary(plalEngine, mutationLogs, 10000);

          if (!success) {
            logger.error("fail to wait insertLogsForSecondary seg {} image {} ", segUnit.getSegId(),
                segLogMetadata.getLogImageIncludeLogsAfterPcl());
          } else {
            movePclAfterInsertLogsForSecondary(maxLogId, oldLogImage, segLogMetadata, response,
                request,
                segUnit, myself, plalEngine);
          }
        } catch (Exception e) {
          logger.error("", e);
        }
      } else {
        
        if (meetErrorWhenCreateLogs) {
          maxLogId = Math.min(missingLogsAtSecondary.get(0).getLogId() - 1, maxLogId);
          logger.warn("create secondary log occur error, maxLogId:{} for segId={}", maxLogId,
              segUnit.getSegId());
        }
        movePclAfterInsertLogsForSecondary(maxLogId, oldLogImage, segLogMetadata, response,
            request, segUnit,
            myself, plalEngine);
      }

      return numLogs;
    } finally {
     
      if (segLogMetadata.getClId() > segLogMetadata.getLalId()) {
        try {
          PlalDriver.drive(segLogMetadata, segUnit, plalEngine);
        } catch (Throwable t) {
          logger.warn("fail to drive plal for segment {} ", segUnit.getSegId());
        }
      }
    }
  }

  private static void movePclAfterInsertLogsForSecondary(long maxLogId, LogImage oldLogImage,
      SegmentLogMetadata segLogMetadata, SyncLogsResponse response, SyncLogsRequest request,
      SegmentUnit segUnit,
      InstanceId myself, PlalEngine plalEngine) {
    segUnit.lockStatus();
    try {
     
      if (segUnit.getPclDrivingType(myself).isSecondary() && !segUnit
          .isDisallowZombieSecondaryPclDriving()) {
       
       
       
        long primaryClId = response.getPrimaryClId();
        long newClId = LogImage.INVALID_LOG_ID;
        if (maxLogId == LogImage.INVALID_LOG_ID) {
          newClId = oldLogImage.getClId();
        } else {
          try {
            synchronized (segUnit.getLockForZombiePcl()) {
              if (segUnit.isDisallowZombieSecondaryPclDriving()) {
                logger.warn("can't move cl, due to zombie marked {} {}", segUnit.getSegId(),
                    segLogMetadata);
                return;
              }
              maxLogId = Math.min(primaryClId, maxLogId);
              Pair<Long, List<MutationLogEntry>> result = segLogMetadata.moveClTo(maxLogId);
              newClId = result.getFirst();
              plalEngine.putLogs(segUnit.getSegId(), result.getSecond());
            }
          } catch (Exception e) {
            logger.error("can't move to {}, log image {} , response {}, request {}", maxLogId,
                oldLogImage,
                response, request, e);
            newClId = segLogMetadata.getClId();
          }
        }

        logger.debug("move cl to {}. The old logImage {}, maxLogId={}", newClId, oldLogImage,
            maxLogId);
      } else {
        logger.debug("can not move cl, due to pcl driving type {} and disallow zombie {}.",
            segUnit.getPclDrivingType(myself), segUnit.isDisallowZombieSecondaryPclDriving());
      }
    } finally {
      segUnit.unlockStatus();
    }
  }

  private static void finalizeLogStatus(MutationLogEntry logAtSecondary,
      LogStatus logStatusAtPrimary, SegmentLogMetadata segmentLogMetadata) {
    
    if (MutationLogEntry.isFinalStatus(logStatusAtPrimary)) {
      if (logAtSecondary.isFinalStatus()) {
        if (logAtSecondary.getStatus() != logStatusAtPrimary) {
          Validate.isTrue(false,
              "existing=" + logAtSecondary + ", primary status=" + logStatusAtPrimary);
        }
      } else {
        addSetStatus(logAtSecondary, logStatusAtPrimary,
            segmentLogMetadata);
      }
    }
  }

  private static void addSetStatus(MutationLogEntry logAtSecondary,
      LogStatus logStatusAtPrimary, SegmentLogMetadata segmentLogMetadata) {
    logAtSecondary.setStatus(logStatusAtPrimary);
  }

  private static List<LogThrift> truncateLogWhoseIdLargerThanValidId(List<LogThrift> logs,
      long validId) {
    List<LogThrift> truncatedList = new ArrayList<>();
    for (LogThrift log : logs) {
      if (log.getLogId() > validId) {
        return truncatedList;
      } else {
        truncatedList.add(log);
      }
    }
    return truncatedList;
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
  public ExecuteLevel drive() throws Exception {
    try {
      numlogsprocessedbypcldriving = 0;
      drivingPcl();
    } catch (Throwable t) {
      logger.error("PCL self driving failed at the segment unit {}, myself={}", segUnit.getSegId(),
          myself, t);
    }

    logger.debug("PCL driver have processed {} logs for myself={}", numlogsprocessedbypcldriving,
        myself);
    if (numlogsprocessedbypcldriving >= THRESHOLD_NUM_LOGS_HAVING_BEEN_PROCESSED) {
     
      return ExecuteLevel.QUICKLY;
    } else if (numLogsWillBeDriven >= MAX_DEFAULT_NUM_LOG_AFTER_PCL) {
      return ExecuteLevel.QUICKLY;
    } else {
      logger.debug("{} logs have been processed. {} logs will be driven. Go to sleep for a while",
          numlogsprocessedbypcldriving, numLogsWillBeDriven);
      return ExecuteLevel.SLOWLY;
    }
  }

  private boolean isCatchingUpLog() {
    SecondaryCopyPageManager manager = segUnit.getSecondaryCopyPageManager();
    if (manager != null && manager.getCopyPageStatus() == CopyPageStatus.CatchupLog) {
      return true;
    }
    return false;
  }

  private void drivingPcl() {
    SegmentUnitMetadata metadata = segUnit.getSegmentUnitMetadata();
    this.membership = metadata.getMembership();
    SegId segId = segLogMetadata.getSegId();
    boolean encountException = false;

    SegmentUnitStatus currentStatus = metadata.getStatus();
    PclDrivingType pclDrivingType = segUnit.getPclDrivingType(myself);
    if (pclDrivingType.isPrimary()) {
      segUnit.lockStatus();
      try {
        pclDrivingType = segUnit.getPclDrivingType(myself);
        if (!pclDrivingType.isPrimary()) {
          return;
        }
        Set<EndPoint> endPoints = new HashSet<>();
        if (segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.PrePrimary) {
          if (segUnit.getAvailableSecondary().size() == 0) {
            logger.error("pre primary S count 0");
            return;
          }
          endPoints.addAll(segUnit.getAvailableSecondary());
        } else {
          Set<InstanceId> aliveSecondary = membership
              .getAliveSecondariesWithoutArbitersAndCandidate();
          for (InstanceId instanceId : aliveSecondary) {
            EndPoint secondaryEndPoint = instanceStore.get(instanceId).getEndPoint();
            endPoints.add(secondaryEndPoint);
          }
        }
        logger.debug("driving pcl for primary {}", segUnit);
        Predicate<List<MutationLogEntry>> mutationLogEntryPredicate = mutationLogEntry -> {
         
          if (!membership.isQuorumUpdated()
              && segUnit.getSegmentUnitMetadata().getStatus()
              == SegmentUnitStatus.Primary) {
            logger.error("membership not quorum update");
            return false;
          }

          BroadcastRequest broadcastRequest = DataNodeRequestResponseHelper
              .buildBroadcastAbortedLogRequest(RequestIdBuilder.get(), segId,
                  membership,
                  mutationLogEntry);
          try {
            dataNodeAsyncClient
                .broadcast(broadcastRequest,
                    cfg.getDataNodeRequestTimeoutMs(), endPoints.size(), true,
                    endPoints);
          } catch (QuorumNotFoundException | FailedToSendBroadcastRequestsException 
              | SnapshotVersionMissMatchForMergeLogsException e) {
            logger.error("broadcast abort log {} error ", mutationLogEntry, e);
            return false;
          }
          return true;
        };
        plalEngine
            .putLogs(segId, segLogMetadata.moveClToInPrimary(mutationLogEntryPredicate));

        if (pclDrivingType == PclDrivingType.OrphanPrimary) {
          LogImage logImage = segLogMetadata.getLogImageIncludeLogsAfterPcl();
          segLogMetadata.setSwclIdTo(logImage.getClId());
          segLogMetadata.setSwplIdTo(logImage.getPlId());
        }
      } finally {
        segUnit.unlockStatus();
      }

      if (cfg.getLogsCountWarningLevel() != 0) {
        logsCountWarning(cfg.getLogsCountWarningLevel(), cfg.getLogsCountWarningThreshold(), segId,
            segLogMetadata.getLogImageIncludeLogsAfterPcl());
      }

      long clId = segLogMetadata.getClId();
      logger.debug("segment unit={}, status={}, clId={}", metadata, currentStatus, clId);
      try {
        if (clId > segLogMetadata.getLalId()) {
          try {
            PlalDriver.drive(segLogMetadata, segUnit, plalEngine);
          } catch (Throwable t) {
            logger.warn("fail to drive plal for segment {} ", segId);
          }
        }
      } catch (Exception e) {
        logger.warn("caught an exception when driving cl:{} for the primary", currentStatus, e);
      }
    } else if (pclDrivingType.isSecondary()) {
     
     
      if (true) {
        return;
      }

      if (segUnit.isHoldToExtendLease()) {
        return;
      }

      if (pclDrivingType == PclDrivingType.VotingSecondary) {
        if (!segUnit.potentialPrimaryIdSet()) {
          logger.warn("Can't get potential primary id because it has not been set. {}", segId);
          return;
        }

        InstanceId potentialPrimaryInstanceId = new InstanceId(segUnit.getPotentialPrimaryId());
        Instance potentialPrimaryInstance = instanceStore.get(potentialPrimaryInstanceId);
        if (potentialPrimaryInstance == null) {
          logger.warn("Can't get potential primary {} from instance store",
              potentialPrimaryInstanceId);
          return;
        }
        primaryEndPoint = potentialPrimaryInstance.getEndPoint();
      }

      LogImage logImage = segLogMetadata
          .getLogImage(cfg.getMaxNumberLogsForSecondaryToSyncUpOnSecondarySide());

      logsCountWarning(cfg.getLogsCountWarningLevel(), cfg.getLogsCountWarningThreshold(), segId,
          logImage);

      logger.debug("PCL driver is driving cl for a secondary ({}). Metadata:{} and logImage {} ",
          myself,
          metadata, logImage);

      if (segUnit.isNotAcceptWriteReadRequest() && logImage.getClId() >= logImage.getMaxLogId()
          && logImage
          .getUuidsOfLogsWithoutLogId().isEmpty()) {
        logger.warn(
            "no need to sync logs because this segment"
                + " unit doesn't accept write read request anymore "
                + "and has all logs already, segment unit : {}", segUnit);
        return;
      }

      numLogsWillBeDriven = logImage.getNumLogsAfterPcl();
      List<Long> uncommittedLogIds = logImage.getUncommittedLogIds();
      if (!okToSyncLogs(uncommittedLogIds == null ? 0 : uncommittedLogIds.size(),
          cfg.getMaxNumberLogsForSecondaryToSyncUp(), numLogsWillBeDriven,
          MAX_DEFAULT_NUM_LOG_AFTER_PCL,
          cfg.getProbabilityToSyncLogsWhenNoEnoughLogs())) {
        return;
      }

      SyncLogsRequest request = DataNodeRequestResponseHelper
          .buildSyncLogRequest(segId, logImage.getPlId(), logImage.getClId(), uncommittedLogIds,
              logImage.getUuidsOfLogsWithoutLogId(), membership, myself.getId(),
              segUnit.getPreprimaryDrivingSessionId(), currentStatus);

      if (isCatchingUpLog()) {
        request.setCatchUpLogId(segUnit.getSecondaryCopyPageManager().getCatchUpLog().getLogId());
      }

      DataNodeService.Iface client = null;
      SyncLogsResponse response = null;
      SegmentMembershipThrift returnedMembershipThrift = null;
      SegmentUnitStatus newStatus = null;

      logger.info("sync request {} ", request);
      try {
        client = clientFactory
            .generateSyncClient(primaryEndPoint, cfg.getDataNodeRequestTimeoutMs());
        response = client.syncLogs(request);
        logger.info("{} Got syncLog response {} from {} and let's expend {} lease ", myself,
            response,
            primaryEndPoint, metadata.getSegId());
        if (response != null) {
          returnedMembershipThrift = response.getLatestMembership();
        }

        logger.debug("going to extend my:[{}] lease", segUnit.getSegId());
        segUnit.extendMyLease();
      } catch (GenericThriftClientFactoryException e) {
        if (!segUnit.isDataNodeServiceShutdown()) {
          logger.warn(
              "Can't get a client {}  to the primary {} , "
                  + "Exception:GenericThriftClientFactoryException:{}",
              myself, primaryEndPoint, e.getMessage());
          return;
        } else {
          encountException = true;
        }
      } catch (SegmentNotFoundExceptionThrift e) {
        logger.warn("Caught SegmentNotFoundExceptionThrift exception. "
                + "{} can't find the segment unit {} "
                + "Changing my status from secondary to start. Request:{}",
            primaryEndPoint, segUnit.getSegId(), request);

        newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
            ? SegmentUnitStatus.Start :
            SegmentUnitStatus.OFFLINED;
        encountException = true;
      } catch (StaleMembershipExceptionThrift e) {
        logger
            .warn("Caught an StaleMembershipException. {} has lower (epoch) than the primary {} "
                    + ". Changing status from Secondary to Start to catch up logs {} ", myself,
                primaryEndPoint,
                request);
        returnedMembershipThrift = e.getLatestMembership();
        Validate.notNull(returnedMembershipThrift);

        newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
            ? SegmentUnitStatus.Start :
            SegmentUnitStatus.OFFLINED;

        encountException = true;
        if (currentStatus == SegmentUnitStatus.OFFLINING) {
          encountException = false;
          returnedMembershipThrift = null;
        }
      } catch (LogIdTooSmallExceptionThrift e) {
        MutationLogEntry pclLog = segLogMetadata.getLog(logImage.getClId());
        logger.warn(
            "Caught an LogIdTooSmallException The log "
                + "id of PCLDriver {} requests doesn't exist any more"
                + " in the primary {}. Hold its lease "
                + "and don't extend it until it gets expired and "
                + "changed to Start status {}, PCL log {}, log image {}", myself, primaryEndPoint,
            request,
            pclLog, logImage);
        if (pclDrivingType == PclDrivingType.Secondary && !segUnit
            .isNotAcceptWriteReadRequest()) {
          logger.warn("current status: {}, we should hold the extend lease for segId={}.",
              currentStatus,
              segId);
          segUnit.setHoldToExtendLease(true);
          return;
        } else if (pclDrivingType == PclDrivingType.JoiningSecondary
            || pclDrivingType == PclDrivingType.VotingSecondary) {
          newStatus = SegmentUnitStatus.Start;
        } else {
          newStatus = SegmentUnitStatus.OFFLINED;
        }
        encountException = true;
      } catch (LeaseExtensionFrozenException e) {
        logger.trace(
            "Myself {} can't extend lease but still "
                + "go ahead to process it lease. the primary is {} and segId is {} ",
            myself, primaryEndPoint, segUnit.getSegId());

      } catch (StillAtPrePrimaryExceptionThrift e) {
        logger
            .info(" ***** {} still at the PrePrimary status, let's {} wait and retry later. {} ",
                primaryEndPoint, myself, segUnit.getSegId());

        return;
      } catch (NotPrimaryExceptionThrift e) {
        logger.warn(
            "Caught a NotPrimaryExceptionThrift. "
                + "{} primary doesn't believe it is the primary any more. Changing {} "
                + " status from Secondary to Start. {}. Exception: {} ", primaryEndPoint, myself,
            request, e);
        returnedMembershipThrift = e.getMembership();
        Validate.notNull(returnedMembershipThrift);

        newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
            ? SegmentUnitStatus.Start :
            SegmentUnitStatus.OFFLINED;
        encountException = true;
      } catch (YouAreNotInMembershipExceptionThrift e) {
        logger.error(
            "Caught a YouAreNotInMembershipExceptionThrift "
                + "Exception. I am not at the primary's membership "
                + "Secondary: {} primary: {} my membership: {} the primary membership:{} {} ",
            myself, primaryEndPoint, membership, e.getMembership(), request);

        newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
            ? SegmentUnitStatus.Start :
            SegmentUnitStatus.OFFLINED;
        encountException = true;
      } catch (InvalidMembershipExceptionThrift e) {
        logger.error(
            "**************** It is impossible that "
                + "InvalidMembershipException can be received at a secondary side *************. "
                + "Secondary: {} primary:{}  my membership: {} the primary membership:{} {} ",
            myself, primaryEndPoint, membership, e.getLatestMembership(), request);
        logger.error("Really don't know how to do, change my status to Start");

        newStatus = (currentStatus != SegmentUnitStatus.OFFLINING)
            ? SegmentUnitStatus.Start :
            SegmentUnitStatus.OFFLINED;
        encountException = true;
      } catch (WrongPrePrimarySessionExceptionThrift e) {
        logger
            .warn("session has changed when voting, there is no need retry for segId={}", segId);
        newStatus = SegmentUnitStatus.Start;
      } catch (Exception e) {
        logger.warn(
            "Caught an mystery exception when syncing log from {}, request={}, accept io={}　",
            primaryEndPoint, request, segUnit.isNotAcceptWriteReadRequest(), e);
        if (!segUnit.isNotAcceptWriteReadRequest()) {
          return;
        } else {
          encountException = true;
        }
      } finally {
        clientFactory.releaseSyncClient(client);
        client = null;
      }

      if (encountException && segUnit.isNotAcceptWriteReadRequest() && logImage
          .getUuidsOfLogsWithoutLogId()
          .isEmpty()) {
        logger.warn(
            "can't sync logs from primary for an"
                + " offlining segment unit {}, going to remove all logs after pcl",
            segId);
        numLogsWillBeDriven = 0;
        return;
      }

      SegmentMembership returnedMembership = null;
      if (returnedMembershipThrift != null) {
        returnedMembership = DataNodeRequestResponseHelper
            .buildSegmentMembershipFrom(returnedMembershipThrift).getSecond();
        if (returnedMembership.compareVersion(this.membership) <= 0) {
          returnedMembership = null;
        } else if (SegmentMembershipHelper
            .okToUpdateToHigherMembership(returnedMembership, this.membership, myself,
                segUnit.getSegmentUnitMetadata().getVolumeType().getNumMembers())) {
          logger.info("the primary has sent a membership with higher generation. ");
        } else {
          logger.warn(
              "My newStatus is supposed to be {} but"
                  + " since it is not ok update the membership, {}, change my status to Deleting",
              newStatus, returnedMembership);

          returnedMembership = null;
          newStatus = SegmentUnitStatus.Deleting;
        }
      }

      if (returnedMembership != null || newStatus != null) {
        if (currentStatus != SegmentUnitStatus.OFFLINING
            || (currentStatus == SegmentUnitStatus.OFFLINING) && (newStatus
            == SegmentUnitStatus.OFFLINED)) {
          try {
            archiveManager
                .updateSegmentUnitMetadata(segUnit.getSegId(), returnedMembership, newStatus);
          } catch (Exception e) {
            logger.error("For some reason, we can't update segment unit metadata", e);
            numLogsWillBeDriven = 0;
            return;
          }
        }
      }

      if (newStatus != null) {
        numLogsWillBeDriven = 0;
        return;
      }

      numlogsprocessedbypcldriving = processAllLogsFromPrimary(segLogMetadata, segUnit, request,
          response,
          segUnit.isNotAcceptWriteReadRequest(), plalEngine, cfg, myself);

      if (membership.isPrimaryCandidate(myself)) {
        long newCl = segLogMetadata.getClId();
        if (response != null && newCl == response.getPrimaryClId()) {
          logger.warn("I think I am ready to be primary {}", segId);
          try {
            client = clientFactory
                .generateSyncClient(primaryEndPoint, cfg.getDataNodeRequestTimeoutMs());
            ImReadyToBePrimaryResponse notifyResponse = client.iAmReadyToBePrimary(
                new ImReadyToBePrimaryRequest(RequestIdBuilder.get(),
                    RequestResponseHelper
                        .buildThriftMembershipFrom(segId, membership), newCl,
                    myself.getId(), RequestResponseHelper.buildThriftSegIdFrom(segId)));
            SegmentMembership newMembership = RequestResponseHelper
                .buildSegmentMembershipFrom(notifyResponse.getNewMembership()).getSecond();
            if (newMembership.compareTo(segUnit.getSegmentUnitMetadata().getMembership()) > 0) {
              logger.warn(
                  "{} receive a new membership(which"
                      + " I should already have), but just update it {}",
                  segId, newMembership);
              segUnit.getArchive().updateSegmentUnitMetadata(segId, newMembership, null);
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
    } else {
      logger.debug("segment unit {} is type={}, do nothing", metadata, pclDrivingType);
    }
  }

  public boolean okToSyncLogs(int numUncommittedLogIds, int maxNumberLogsForSecondaryToSyncUp,
      int numlogsAfterPcl,
      int maxDefaultLogsAfterPcl, int probabilityOfSyncLogsWhenNoEnoughLogs) {
    if (numUncommittedLogIds >= maxNumberLogsForSecondaryToSyncUp
        || numlogsAfterPcl >= maxDefaultLogsAfterPcl) {
     
     
      return true;
    }

    if (isCatchingUpLog()) {
      return true;
    }

    if (probabilityOfSyncLogsWhenNoEnoughLogs < 0 || probabilityOfSyncLogsWhenNoEnoughLogs > 100) {
      logger.warn("the probability of sync logs when there "
              + "is no enough logs is {}, which is out of range, set it to 30%",
          probabilityOfSyncLogsWhenNoEnoughLogs);
      probabilityOfSyncLogsWhenNoEnoughLogs = 30;
    }

    int randomNumber = random.nextInt(100) + 1;
    if (randomNumber >= 1 && randomNumber <= probabilityOfSyncLogsWhenNoEnoughLogs) {
     
      logger.debug(
          "the random number {}. Hit {}% probability " 
              + "of no syncing log when there are no enough logs",
          randomNumber, probabilityOfSyncLogsWhenNoEnoughLogs);
      return true;
    } else {
      logger.debug(
          "the random number {}. Not Hit {}% probability " 
              + "of no syncing log when there are no enough logs",
          randomNumber, probabilityOfSyncLogsWhenNoEnoughLogs);
      return false;
    }
  }

  @Override
  public ChainedLogDriver getNextDriver() {
    throw new NotImplementedException("this is a PCLDriver");
  }

  @Override
  public void setNextDriver(ChainedLogDriver driver) {
    throw new NotImplementedException("this is a PCLDriver");
  }
}

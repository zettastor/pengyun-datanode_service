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

import static py.datanode.segment.copy.CopyPageStatus.CatchupLog;
import static py.datanode.segment.copy.CopyPageStatus.CopyPage;
import static py.datanode.segment.copy.CopyPageStatus.Done;
import static py.datanode.segment.copy.CopyPageStatus.InitiateCatchupLog;
import static py.datanode.segment.copy.CopyPageStatus.InitiateCopyPage;
import static py.datanode.segment.copy.CopyPageStatus.None;

import java.util.BitSet;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.datanode.exception.CopyPageAbortException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPageStatus;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.copy.SecondaryCopyPageManagerImpl;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.membership.Lease;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.datanode.service.ReadyToBecomeSecondary;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.membership.SegmentMembershipHelper;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.InitiateCopyPageRequestThrift;
import py.thrift.datanode.service.InitiateCopyPageResponseThrift;
import py.thrift.datanode.service.LogStatusThrift;
import py.thrift.datanode.service.LogThrift;
import py.thrift.datanode.service.PrimaryNeedRollBackFirstExceptionThrift;
import py.thrift.datanode.service.SecondaryCopyPagesRequest;
import py.thrift.datanode.service.SecondaryCopyPagesResponse;
import py.thrift.datanode.service.SecondarySnapshotVersionTooHighExceptionThrift;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.datanode.service.WrongPrePrimarySessionExceptionThrift;
import py.thrift.share.InvalidMembershipExceptionThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.StaleMembershipExceptionThrift;
import py.thrift.share.YouAreNotInMembershipExceptionThrift;
import py.volume.VolumeType;

public class CopyPagesFromPrimaryProcessor extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(CopyPagesFromPrimaryProcessor.class);
  private static final int MAX_RANDOM_DELAY = 3000;
  private static final int DELAY_DUE_TO_NO_POTENTIAL_PRIMARY_ID_SET = 1000;

  private IoThrottleManager ioThrottleManager;
  private SecondaryCopyPageManager copyPageManager;
  private SegmentUnit segmentUnit;
  private CopyPagesContext copyPagesContext;
  private Instance primary;

  public CopyPagesFromPrimaryProcessor(StateProcessingContext context,
      IoThrottleManager ioThrottleManager) {
    super(context);
    this.ioThrottleManager = ioThrottleManager;
  }

  public IoThrottleManager getIoThrottleManager() {
    return ioThrottleManager;
  }

  public void setIoThrottleManager(IoThrottleManager ioThrottleManager) {
    this.ioThrottleManager = ioThrottleManager;
  }

  @Override
  public StateProcessingResult process(StateProcessingContext context) {
    copyPagesContext = (CopyPagesContext) context;
    segmentUnit = copyPagesContext.getSegmentUnit();
    copyPageManager = segmentUnit.getSecondaryCopyPageManager();
    if (copyPageManager == null) {
      InstanceId potentialPrimaryInstanceId = new InstanceId(segmentUnit.getPotentialPrimaryId());

      primary = instanceStore.get(potentialPrimaryInstanceId);
      if (primary == null) {
        if (segmentUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Secondary) {
          return getSuccesslResultWithRandomeDelay(copyPagesContext, SegmentUnitStatus.Secondary,
              MAX_RANDOM_DELAY);
        } else if (segmentUnit.getSegmentUnitMetadata().getStatus().isFinalStatus()) {
          // because context execute delay, when it is PreSecondary, and delay execute,
          // before execute next time, segmentUnit become secondary,
          // and become Deleting or Broken
          // later, we should return currentStatus to generate context
          logger.warn("segmentUnit:{} become final status between PreSecondary and Secondary, "
              + "potential primary={}", segmentUnit, potentialPrimaryInstanceId);
          return getSuccesslResultWithZeroDelay(context,
              segmentUnit.getSegmentUnitMetadata().getStatus());
        } else {
          logger.warn("Can't get potential primary={} of segment={} from instance store",
              potentialPrimaryInstanceId,
              segmentUnit);
          return dealWithCopyPageResult(SegmentUnitStatus.Start);
        }
      }
      long sessionId = segmentUnit.getSegmentUnitMetadata().isSecondaryCandidate() 
          ? IoThrottleManager.IoType.Rebalance.newSessionId()
          : IoThrottleManager.IoType.CopyPage.newSessionId();
      copyPageManager = new SecondaryCopyPageManagerImpl(cfg, segmentUnit, primary, sessionId);
      copyPageManager.setIoThrottleManager(ioThrottleManager);
      segmentUnit.setSecondaryCopyPageManager(copyPageManager);
    }
    primary = copyPageManager.getPeer();
    if (primary.getId().getId() != segmentUnit.getPotentialPrimaryId()) {
      logger.error("potential primary={} not equal copy page manager peer {}, segment unit {}",
          segmentUnit.getPotentialPrimaryId(), primary, segmentUnit);
      return dealWithCopyPageResult(SegmentUnitStatus.Start);
    }

    try {
      StateProcessingResult result = checkContextValidate();
      if (result != null) {
        return result;
      }

      CopyPageStatus copyPageStatus = copyPageManager.getCopyPageStatus();
      if (copyPageStatus == None || copyPageStatus == InitiateCatchupLog) {
        return initiateCatchUpLog();
      } else if (copyPageStatus == CopyPageStatus.CatchupLog) {
        return catchUpLog();
      } else if (copyPageStatus == CopyPageStatus.InitiateCopyPage) {
        return initiateCopyPages();
      } else {
        return checkProcess();
      }
    } catch (CopyPageAbortException e) {
      logger.error("caught an exception for seg {}, copyPagesContext {} manager {}",
          context.getSegId(), copyPagesContext, copyPageManager, e);
      ioThrottleManager.finish(segmentUnit.getArchive(), segmentUnit.getSegId());
      return dealWithCopyPageResult(SegmentUnitStatus.Start);
    } catch (Exception e) {
      logger.error("caught an exception for seg {}", context.getSegId(), e);

      SegmentUnitMetadata metadata = context.getSegmentUnit().getSegmentUnitMetadata();
      return getFailureResultWithRandomizedDelay(context, metadata.getStatus(), MAX_RANDOM_DELAY);
    }
  }

  private StateProcessingResult initiateCatchUpLog()
      throws Exception {
    segmentUnit.setCopyPageFinished(false);
    copyPageManager.setCopyPageStatus(InitiateCatchupLog);
    SegId segId = copyPagesContext.getSegId();

    if (preSecondaryAlreadyCatchupLogs()) {
      logger.warn("no need go to sync log or copy page for large volume, just become secondary");
      return becomeSecondary();
    }

    SegmentUnitMetadata segmentUnitMetadata = segmentUnit.getSegmentUnitMetadata();
    SegmentLogMetadata segmentLogMetadata = copyPagesContext.getSegmentLogMetadata();
    LogImage logImage = segmentLogMetadata.getLogImage(0);
    logger.warn(
        "send the copy logs request from the segment unit :{} to primary directly {}, logImage: {}",
        segmentUnit, primary, logImage);
    SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
    InstanceId myself = appContext.getInstanceId();

    DataNodeService.Iface dataNodeClient;
    try {
      dataNodeClient = getClient(primary.getEndPoint(),
          cfg.getDataNodeRequestTimeoutMs());
    } catch (GenericThriftClientFactoryException e1) {
      logger.error("I: {} can't build a client with primary: {}, membership: {}", myself,
          primary, currentMembership);
      archiveManager.updateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Start);
      return dealWithCopyPageResult(SegmentUnitStatus.Start);
    }

    boolean forceFullCopy = segmentUnit.isForceFullCopy(cfg, currentMembership, myself);

    SecondaryCopyPagesRequest request = DataNodeRequestResponseHelper
        .buildSecondaryCopyPagesRequest(segId, currentMembership, myself.getId(),
            logImage.getPlId(),
            logImage.getClId(), segmentUnit.getPreprimaryDrivingSessionId(), forceFullCopy);
    SecondaryCopyPagesResponse response;
    SegmentUnitStatus newStatus = SegmentUnitStatus.PreSecondary;
    SegmentMembership newMembership = currentMembership;

    try {
      response = dataNodeClient.secondaryCopyPages(request);
      newMembership = getHigherMembership(currentMembership, response.getMembership());
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.warn("The primary: {} of segId: {} has disappeared, Change my status to Start.",
          primary, segId);
      newStatus = SegmentUnitStatus.Start;
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    } catch (NotPrimaryExceptionThrift e) {
      logger.warn("The primary: {} of segId: {} is not primary any more, Change my status to Start",
          primary, segId);
      newMembership = getHigherMembership(currentMembership, e.getMembership());
      newStatus = SegmentUnitStatus.Start;
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    } catch (StaleMembershipExceptionThrift e) {
      newMembership = getHigherMembership(currentMembership, e.getLatestMembership());
      logger.error(
          "my membership: {} of segId: {} is a stale membership. " 
              + "Change my status to Start {}, new membership: {}",
          primary, segId, newMembership, currentMembership);
      newStatus = SegmentUnitStatus.Start;
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    } catch (InvalidMembershipExceptionThrift e) {
      newMembership = getHigherMembership(currentMembership, e.getLatestMembership());
      logger.error(
          "my membership: {} of segId: {} is an invalid membership." 
              + " Change my status to Start {}, new membership: {}",
          primary, segId, newMembership, currentMembership);
      newStatus = SegmentUnitStatus.Start;
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    } catch (ServiceHavingBeenShutdownThrift e) {
      logger.warn("The primary: {} of segId: {} is being shutdown, Change my status to Start.",
          primary, segId);
      newStatus = SegmentUnitStatus.Start;
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    } catch (YouAreNotInMembershipExceptionThrift e) {
      newMembership = getHigherMembership(currentMembership, e.getMembership());
      logger.error(
          "myself: {} of segId: {} is not in priamry's membership: " 
              + "{}, and my membership: {}, primary: {} ",
          myself, segId, newMembership, currentMembership, primary);
      newStatus = SegmentUnitStatus.Start;
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    } catch (WrongPrePrimarySessionExceptionThrift e) {
      logger.warn(
          "The preprimary: {} of segId: {} does not believe I " 
              + "{} is at the same session with detail: {} Change the status to Start",
          segId, primary, myself, e.getDetail());
      newStatus = SegmentUnitStatus.Start;
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    } catch (PrimaryNeedRollBackFirstExceptionThrift e) {
      logger.info("primary need roll back first");
      return getFailureResultWithFixedDelay(copyPagesContext, SegmentUnitStatus.Start,
          MAX_RANDOM_DELAY);
    } catch (SecondarySnapshotVersionTooHighExceptionThrift e) {
      logger.warn("sendary snapshot version too high");
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    } catch (Exception e) {
      logger.warn("fail to copy page from potential primary: {} of segId: {} and myself: {}",
          primary, segId, myself, e);
      return getFailureResultWithBackoffDelay(copyPagesContext, SegmentUnitStatus.PreSecondary);
    } finally {
      dataNodeSyncClientFactory.releaseSyncClient(dataNodeClient);

      if (newMembership != null) {
        if (!SegmentMembershipHelper
            .okToUpdateToHigherMembership(newMembership, currentMembership, myself,
                segmentUnit.getSegmentUnitMetadata().getVolumeType().getNumMembers())) {
          logger.warn(
              "update my={} status to Deleting, which is " 
                  + "exclude of the segment={}, new membership {}",
              myself, segId, newMembership);

          newMembership = null;
          newStatus = SegmentUnitStatus.Deleting;
        }
      }

      if (newStatus != SegmentUnitStatus.PreSecondary) {
        segmentUnit.getArchive().updateSegmentUnitMetadata(segId, newMembership, newStatus);
      }
    }

    logger.warn(
        "{} response me to about log catch up. response:" 
            + " {} , request: {}, copy page context={}, syncLogUpTo={}",
        primary, response, request, copyPagesContext);

    boolean isFullCopy = !response.isSecondaryClIdExistInPrimary();
    copyPageManager.setFullCopy(isFullCopy);
    copyPageManager.setPrimaryMaxLogId(response.getMaxLogId());
    segmentLogMetadata.setSwplIdTo(response.getPswpl());
    segmentLogMetadata.setSwclIdTo(response.getPswcl());

    LogThrift thriftLog = response.getCatchUpLogFromPrimary();
    MutationLogEntry catchUpLogFromPrimary = DataNodeRequestResponseHelper
        .buildMutationLogEntryFrom(thriftLog.getLogUuid(), thriftLog.getLogId(),
            segmentUnit.getArchive().getArchiveId(), thriftLog.getLogInfo());
    if (thriftLog.getStatus().equals(LogStatusThrift.AbortedConfirmed)) {
      catchUpLogFromPrimary.confirmAbortAndApply();
    } else if (thriftLog.getStatus().equals(LogStatusThrift.Committed)) {
      catchUpLogFromPrimary.commitAndApply();
    } else {
      logger.error("wrong {}", thriftLog);
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    }
    catchUpLogFromPrimary.setPersisted();

    if (newMembership == null) {
      newMembership = currentMembership;
    }

    if (segmentUnit.getSegmentUnitMetadata().getMigrationStatus().isMigrating()) {
      return getFailureResultWithZeroDelay(copyPagesContext, SegmentUnitStatus.Start);
    }

    if (isFullCopy) {
      final long startTime = System.currentTimeMillis();

      BitSet bitSet = BitSet.valueOf(response.getSemgentUnitFreePageBitmap());
      for (int i = 0; i < segmentUnitMetadata.getPageCount(); i++) {
        if (bitSet.get(i)) {
          copyPageManager.getCopyPageBitmap().clear(i);
          logger.debug("set the page need copy {}", i);
        }
      }

      updateMigrationStatus(segmentUnit, segmentUnitMetadata, newMembership);

      segmentUnit.getArchive()
          .updateSegmentUnitMetadata(segId, newMembership, SegmentUnitStatus.PreSecondary, true);

      updateCatchUpLogToSecondaryMaxLogId(catchUpLogFromPrimary);

      if (!plalEngine.checkSegmentUnitStopped(segId)) {
        logger.warn("check plal engine stop for segId={}", segId);
        return dealWithCopyPageResult(SegmentUnitStatus.Start);
      }

      long catchUpLogIdFromPrimary = catchUpLogFromPrimary.getLogId();
      if (catchUpLogIdFromPrimary != LogImage.INVALID_LOG_ID) {
        MutationLogEntry existing = segmentLogMetadata.getLog(catchUpLogIdFromPrimary);
        if (existing == null) {
          segmentLogMetadata.appendLog(catchUpLogFromPrimary);
        } else if (!existing.isFinalStatus()) {
          existing.setStatus(catchUpLogFromPrimary.getStatus());
          logger
              .warn("set status for existing {} catchup log  {}", existing, catchUpLogFromPrimary);
        } else {
          catchUpLogFromPrimary = existing;
        }

        long newClId = segmentLogMetadata.forceMovePclAndRemoveBefore(catchUpLogIdFromPrimary);
        logger.warn("new clId={}, catchup logId={}", newClId, catchUpLogFromPrimary);
        segmentUnit.getSegmentUnitMetadata().setLogIdOfPersistBitmap(catchUpLogIdFromPrimary);

        logPersister.removeLogMetaData(segId);
      } else {
        logger.warn("it is a strange catch up log from primary, log={}", catchUpLogFromPrimary);
      }

      segmentLogMetadata.setSwplIdTo(catchUpLogFromPrimary.getLogId());

      catchupLogEngine.revive(segId);
      segmentUnit.setPauseCatchUpLogForSecondary(false);

      logger.warn("cost time={} ms for initializing the full-copy for segId={}",
          System.currentTimeMillis() - startTime, segId);

      segmentUnit.extendMyLease();
      return catchUpLog();
    } else {
      logger.warn("there is no need copying full pages for segId={}, myself={}", segId,
          appContext.getInstanceId());

      updateCatchUpLogToSecondaryMaxLogId(catchUpLogFromPrimary);

      updateMigrationStatus(segmentUnit, segmentUnit.getSegmentUnitMetadata(), newMembership);

      segmentUnit.getArchive()
          .updateSegmentUnitMetadata(segId, newMembership, SegmentUnitStatus.PreSecondary, true);

      catchupLogEngine.revive(segId);
      segmentUnit.setPauseCatchUpLogForSecondary(false);

      segmentUnit.extendMyLease();

      return catchUpLog();
    }
  }

  private void updateCatchUpLogToSecondaryMaxLogId(MutationLogEntry catchUpLogFromPrimary) {
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    MutationLogEntry maxLogFromSecondary = segmentLogMetadata
        .getLog(segmentLogMetadata.getMaxLogId());
    if (maxLogFromSecondary != null && maxLogFromSecondary.getLogId() > catchUpLogFromPrimary
        .getLogId()) {
      logger.warn("update catch up log from {} to max log id {}", catchUpLogFromPrimary,
          maxLogFromSecondary);
      copyPageManager.setCatchUpLog(maxLogFromSecondary);
    } else {
      copyPageManager.setCatchUpLog(catchUpLogFromPrimary);
    }
  }

  private StateProcessingResult catchUpLog() {
    copyPageManager.setCopyPageStatus(CatchupLog);
    if (segmentUnit.isSecondaryZombie() && segmentUnit.isDisallowZombieSecondaryPclDriving()) {
      logger.warn("allow the zombie secondary moving pcl {}", segmentUnit.getSegId());
      segmentUnit.setDisallowZombieSecondaryPclDriving(false);
    }
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    SegId segId = segmentUnit.getSegId();
    copyPageManager.setLastPushTime(System.currentTimeMillis());
    Validate.isTrue(!catchupLogEngine.isPaused(segId));

    long lalId = segmentLogMetadata.getLalId();
    long catchUpLogId = copyPageManager.getCatchUpLog().getLogId();
    if (lalId >= catchUpLogId) {
      logger.warn("finish to wait catch up logs from primary={}, segId={}, full={}, catch up to {}",
          segmentUnit.getPotentialPrimaryId(), segId, copyPageManager.isFullCopy(),
          copyPageManager.getCatchUpLog());
      return initiateCopyPages();
    } else {
      if (lalId % 1 == 0) {
        logger.warn("catchUpLog, lalId={}, expected {} for segId={}", lalId,
            copyPageManager.getCatchUpLog(), segId);
      }
      SegmentUnitStatus status = segmentUnit.getSegmentUnitMetadata().getStatus();
      return getSuccesslResultWithFixedDelay(copyPagesContext, status, MAX_RANDOM_DELAY);
    }
  }

  private StateProcessingResult initiateCopyPages() {
    copyPageManager.setCopyPageStatus(InitiateCopyPage);
    final SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    SegmentUnitMetadata segmentUnitMetadata = segmentUnit.getSegmentUnitMetadata();
    SegId segId = segmentUnit.getSegId();
    copyPageManager.setLastPushTime(System.currentTimeMillis());

    if (copyPageManager.isDone()
        && segmentUnit.getSegmentLogMetadata().getClId() >= copyPageManager.getPrimaryMaxLogId()) {
      logger.warn("there is no need copying for segId={}, context={}", segId, copyPagesContext);
      ioThrottleManager.finish(segmentUnit.getArchive(), segId);
      return becomeSecondary();
    }

    int countOfPageToCopy = copyPageManager.getTotalCountOfPageToCopy();
    try {
      Pair<Boolean, Integer> resultPair = ioThrottleManager.register(segmentUnit.getArchive(),
          segId, countOfPageToCopy, copyPageManager.getSessionId());
      if (!resultPair.getFirst()) {
        logger.info("currently not allow the secondary {} register copy page delay to retry {}",
            segId, resultPair.getSecond());
        return getSuccesslResultWithRandomeDelay(copyPagesContext, segmentUnitMetadata.getStatus(),
            resultPair.getSecond());
      }
      segmentUnit.extendMyLease();
    } catch (Exception e) {
      logger.error("ignore the exception ", e);
      return getSuccesslResultWithRandomeDelay(copyPagesContext, segmentUnitMetadata.getStatus(),
          MAX_RANDOM_DELAY);
    }

    long myself = appContext.getInstanceId().getId();

    int maxSnapshotId = segmentLogMetadata.getMaxAppliedSnapId();
    SegmentMembership segmentMembership = segmentUnitMetadata.getMembership();
    InitiateCopyPageRequestThrift pushRequest = DataNodeRequestResponseHelper
        .buildInitiateCopyPageRequestThrift(segmentUnit.getPreprimaryDrivingSessionId(), segId,
            segmentMembership, myself, copyPageManager.getSessionId(),
            copyPageManager.getCatchUpLog().getLogId(), maxSnapshotId);
    InitiateCopyPageResponseThrift response;
    DataNodeService.Iface dataNodeClient = null;
    try {
      EndPoint endPoint = primary.getEndPoint();
      dataNodeClient = dataNodeSyncClientFactory
          .generateSyncClient(endPoint, cfg.getDataNodeRequestTimeoutMs());
      response = dataNodeClient.initiateCopyPage(pushRequest);
    } catch (Exception e) {
      logger.warn("can not let primary push data for segId={}", segId, e);
      ioThrottleManager.finish(segmentUnit.getArchive(), segId);
      return dealWithCopyPageResult(SegmentUnitStatus.Start);
    } finally {
      dataNodeSyncClientFactory.releaseSyncClient(dataNodeClient);
    }

    if (!response.canStart) {
      logger
          .warn("primary says I need wait {} ms to retry {}", response.getWaitTimeToRetry(), segId);
      ioThrottleManager.unregister(segmentUnit.getArchive(), segId);
      return getSuccesslResultWithRandomeDelay(copyPagesContext, segmentUnitMetadata.getStatus(),
          response.getWaitTimeToRetry());
    }

    logger.warn(
        "register copypage, receive an response: {} from primary segId: {}, " 
            + "archive {} totalPage {}, full copy={}",
        response, segmentUnit.getSegId(),
        segmentUnit.getArchive().getArchiveId(), countOfPageToCopy, copyPageManager.isFullCopy());

    copyPageManager.setCopyPageStatus(CopyPageStatus.CopyPage);
    return dealWithCopyPageResult(SegmentUnitStatus.PreSecondary);
  }

  private StateProcessingResult checkProcess() {
    copyPageManager.setCopyPageStatus(CopyPage);
    if (segmentUnit.getSegmentLogMetadata().getClId() < copyPageManager.getPrimaryMaxLogId()) {
      return dealWithCopyPageResult(SegmentUnitStatus.PreSecondary);
    }

    if ((copyPageManager == null || copyPageManager.isDone())) {
      logger.warn("copy page has done for context: {}", copyPagesContext);
      return becomeSecondary();
    }

    if (System.currentTimeMillis() - copyPageManager.getLastPushTime() > cfg
        .getMaxPushIntervalTimeMs()) {
      logger.warn("i haven't received primary pages about {} for segId={}, so change to Start",
          cfg.getMaxPushIntervalTimeMs(), segmentUnit.getSegId());
      ioThrottleManager.finish(segmentUnit.getArchive(), segmentUnit.getSegId());
      return dealWithCopyPageResult(SegmentUnitStatus.Start);
    }
    return dealWithCopyPageResult(SegmentUnitStatus.PreSecondary);
  }

  private boolean preSecondaryAlreadyCatchupLogs() {
    VolumeType volumeType = segmentUnit.getSegmentUnitMetadata().getVolumeType();
    if (volumeType == VolumeType.LARGE) {
      SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
      boolean imAlreadySecondary = membership.getSecondaries()
          .contains(appContext.getInstanceId());
      logger.warn("large volume, pre secondary is S in membership {}", membership);
      return imAlreadySecondary;
    }
    return false;
  }

  private void updateMetadataAfterCopyDone(SegmentUnitStatus newStatus) throws Exception {
    SegId segId = segmentUnit.getSegId();
    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    MigrationStatus oldStatus = metadata.getMigrationStatus();
    logger.warn(
        "update segment={} status from {} to {}, migration progress={}, " 
            + "status in context ={}, in migration={} after copying page done",
        segId, metadata.getStatus(), newStatus, metadata.getRatioMigration(),
        copyPagesContext.status, oldStatus);
    segmentUnit.getArchive().asyncUpdateSegmentUnitMetadata(segId, null, newStatus);
    metadata.setMigrationStatus(oldStatus.getNextStatusFromResult(true));

    segmentUnit.getArchive().persistSegmentUnitMetaAndBitmap(metadata);
  }

  private StateProcessingResult dealWithCopyPageResult(SegmentUnitStatus newStatus) {
    return dealWithCopyPageResult(newStatus, 10);
  }

  private StateProcessingResult dealWithCopyPageResult(SegmentUnitStatus newStatus, int delay) {
    SegId segId = segmentUnit.getSegId();
    MigrationStatus oldStatus = segmentUnit.getSegmentUnitMetadata().getMigrationStatus();
    logger.info("update segment={} status to {}, migration progress={}, status={}, in migration={}",
        segId,
        newStatus, segmentUnit.getSegmentUnitMetadata().getRatioMigration(),
        copyPagesContext.status, oldStatus);
    if (newStatus == SegmentUnitStatus.PreSecondary) {
      return getSuccesslResultWithRandomeDelay(copyPagesContext, newStatus, MAX_RANDOM_DELAY);
    } else if (newStatus == SegmentUnitStatus.Secondary) {
      segmentUnit.setSecondaryCopyPageManager(null);
      return getSuccesslResultWithRandomeDelay(copyPagesContext, newStatus, MAX_RANDOM_DELAY);
    } else {
      if (copyPageManager != null) {
        logger.warn("reset copy page manager to null, new status {}", newStatus);
        segmentUnit.clearFirstWritePageIndex();
        segmentUnit.setSecondaryCopyPageManager(null);
      }

      if (newStatus == SegmentUnitStatus.Start) {
        segmentUnit.lockStatus();
        try {
          SegmentUnitStatus currentStatus = segmentUnit.getSegmentUnitMetadata().getStatus();
          if (currentStatus == SegmentUnitStatus.PrePrimary
              || currentStatus == SegmentUnitStatus.Primary) {
            logger.warn("i have become primary, not need copy page {}", segId);
            return getSuccesslResultWithRandomeDelay(copyPagesContext,
                currentStatus, MAX_RANDOM_DELAY);
          }
        } finally {
          segmentUnit.unlockStatus();
        }

      }
      try {
        archiveManager.updateSegmentUnitMetadata(segId, null, newStatus);
      } catch (Exception e) {
        logger.warn("can't update the segment unit: ", segId);
      }

      return getFailureResultWithFixedDelay(copyPagesContext, newStatus, delay);
    }
  }

  private void updateMigrationStatus(SegmentUnit segmentUnit, SegmentUnitMetadata unitMetadata,
      SegmentMembership membership) {
    MigrationStatus newStatus;
    MigrationStatus oldStatus = unitMetadata.getMigrationStatus();
    if (segmentUnit.hasParticipatedVotingProcess() || membership
        .isJoiningSecondary(appContext.getInstanceId())) {
      newStatus = oldStatus.getNextStatusFromSecondary(false);
      logger.warn("voting={}, membership={} for segId={} not deleted, old status={}, new status={}",
          segmentUnit.hasParticipatedVotingProcess(), membership, segmentUnit.getSegId(), oldStatus,
          newStatus);
    } else {
      newStatus = oldStatus.getNextStatusFromSecondary(true);
      logger.warn("voting={}, membership={} for segId={} not deleted, old status={}, new status={}",
          segmentUnit.hasParticipatedVotingProcess(), membership, segmentUnit.getSegId(), oldStatus,
          newStatus);
    }

    unitMetadata.setMigrationStatus(newStatus);
  }

  private StateProcessingResult checkContextValidate() throws Exception {
    SegId segId = segmentUnit.getSegId();
    SegmentUnitStatus currentStatus = segmentUnit.getSegmentUnitMetadata().getStatus();
    logger.debug("CopyPage {} current status is {} and context is {}", segId, currentStatus,
        copyPagesContext);

    if (!SegmentUnitStatus.PreSecondary.equals(currentStatus)) {
      logger.warn(
          "context: {} corresponding segment unit: {} is not in " 
              + "PreSecondary status={}. Do nothing ",
          copyPagesContext, segId, currentStatus);
      ioThrottleManager.finish(segmentUnit.getArchive(), segmentUnit.getSegId());

      return dealWithCopyPageResult(currentStatus);
    }

    if (segmentUnit.myLeaseExpired(currentStatus)) {
      logger.warn("The lease of Segment unit ({} ) at {} has expired. Changing my status from {} "
              + " to Start so that I can start the primary election process",
          appContext.getInstanceId(),
          segmentUnit.getSegmentUnitMetadata(), currentStatus);
      return dealWithCopyPageResult(SegmentUnitStatus.Start);
    }

    if (copyPagesContext.reachMaxEndurance()) {
      logger.error("{} catching up from the primary has been through {} failures. "
          + "Give up and give its status to Start", segId, copyPagesContext.getFailureTimes());

      return dealWithCopyPageResult(SegmentUnitStatus.Start);
    }

    if (!segmentUnit.potentialPrimaryIdSet()) {
      logger.warn(
          "Can't get potential primary id because it has not been set for segId: {}. Sleep {} ms",
          segId,
          DELAY_DUE_TO_NO_POTENTIAL_PRIMARY_ID_SET);
      return getFailureResultWithFixedDelay(copyPagesContext, SegmentUnitStatus.PreSecondary,
          DELAY_DUE_TO_NO_POTENTIAL_PRIMARY_ID_SET);
    }

    return null;
  }

  public StateProcessingResult becomeSecondary() {
    SegId segId = segmentUnit.getSegId();
    SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
    if (Done != copyPageManager.getCopyPageStatus()) {
      logger.warn("set the status from {} to Done for context={}",
          copyPageManager.getCopyPageStatus(), copyPagesContext);
      try {
        updateMetadataAfterCopyDone(SegmentUnitStatus.PreSecondary);
        copyPageManager.setCopyPageStatus(Done);
        segmentUnit.setCopyPageFinished(true);
      } catch (Exception e) {
        logger.error("caught an exception", e);
        return dealWithCopyPageResult(SegmentUnitStatus.Start);
      }
    }

    if (segmentUnit.hasParticipatedVotingProcess()) {
      if (!segmentUnit.getCopyPageFinished()) {
        logger.warn("maybe a new epoch has come {} {}", segId, copyPagesContext);
        return dealWithCopyPageResult(SegmentUnitStatus.Start);
      }

      logger.warn("just wait primary to join me for segId={}, membership={}", segId,
          currentMembership);
      return dealWithCopyPageResult(segmentUnit.getSegmentUnitMetadata().getStatus());
    }

    SegmentUnitStatus currentStatus = segmentUnit.getSegmentUnitMetadata().getStatus();

    ReadyToBecomeSecondary.NotifyPrimaryResult notifyResult = ReadyToBecomeSecondary
        .notifyPrimary(dataNodeSyncClientFactory, archiveManager, cfg, segmentUnit,
            currentMembership, this,
            copyPageManager.getPeer(), appContext.getInstanceId(), currentStatus);

    if (notifyResult.success()) {
      notifyResult.setNewStatus(SegmentUnitStatus.Secondary);
    }

    if (notifyResult.getNewMembership() != null || notifyResult.getNewStatus() != null) {
      try {
        archiveManager
            .updateSegmentUnitMetadata(segmentUnit.getSegId(), notifyResult.getNewMembership(),
                notifyResult.getNewStatus());
        logger.warn("{} sync logs done, old and new membership {} {}, zombie={}, success={}", segId,
            currentMembership, notifyResult.getNewMembership(), segmentUnit.isSecondaryZombie(),
            notifyResult.success());
        boolean membershipUpdated = notifyResult.getNewMembership() != null && !currentMembership
            .equals(notifyResult.getNewMembership());
        if (membershipUpdated && segmentUnit.isSecondaryZombie()) {
          segmentUnit.clearSecondaryZombie();
        }
      } catch (Exception e) {
        logger.error("For some reason, we can't update segment unit metadata", e);
        return dealWithCopyPageResult(currentStatus);
      }
    }

    if (notifyResult.success()) {
      segmentUnit.resetPreprimaryDrivingSessionId();
      segmentUnit.resetPotentialPrimaryId();
      segmentUnit.forgetAboutBeingOneSecondaryCandidate();
      logger.warn("Moved to Secondary status {}", segId);

      Lease lease = segmentUnit.getMyLease();
      if (lease.expire()) {
        logger.warn("SegId {} lease expired {}. The secondary might become Start right away", segId,
            lease);
      }

      catchupLogEngine.revive(segId);
      segmentUnit.setPauseCatchUpLogForSecondary(false);

      return dealWithCopyPageResult(SegmentUnitStatus.Secondary);
    } else {
      return dealWithCopyPageResult(currentStatus);
    }
  }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.app.context.AppContext;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.client.ByteStringUtils;
import py.common.struct.Pair;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.LogIdNotFoundException;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.impl.BogusPageAddress;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.SegmentLogMetadata.PeerStatus;
import py.datanode.segment.datalog.persist.LogStorageReader;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogReduceCollector;
import py.datanode.service.DataNodeServiceImpl;
import py.datanode.service.PageErrorCorrector;
import py.datanode.service.Validator;
import py.exception.ChecksumMismatchedException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogRequestUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbErrorCode;
import py.proto.Broadcastlog.PbSyncLogData;
import py.third.rocksdb.KvStoreException;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.datanode.service.StillAtPrePrimaryExceptionThrift;
import py.thrift.datanode.service.WrongPrePrimarySessionExceptionThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.InvalidMembershipExceptionThrift;
import py.thrift.share.LogIdTooSmallExceptionThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.thrift.share.StaleMembershipExceptionThrift;
import py.thrift.share.YouAreNotInMembershipExceptionThrift;

public class SyncLogPusher implements SyncLogTask {
  private static final Logger logger = LoggerFactory.getLogger(SyncLogPusher.class);
  private final SegmentUnitManager segmentUnitManager;
  private final AppContext context;
  private final DataNodeConfiguration dataNodeConfiguration;
  private final LogStorageReader logStorageReader;
  private final PbAsyncSyncLogBatchUnit batchUnit;
  private final SyncLogReduceCollector<PbBackwardSyncLogRequestUnit,
      PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest> reduceCollector;
  private final long createdTimeMs;
  private DataNodeServiceImpl dataNodeService;
  private List<MutationLogEntry> secondaryWaitingSyncLogs = null;
  private PbErrorCode errorCode = PbErrorCode.Ok;
  private Set<Long> missingLogDataAtSecondary = null;

  public SyncLogPusher(SegmentUnitManager segmentUnitManager, PbAsyncSyncLogBatchUnit batchUnit,
      AppContext context, DataNodeServiceImpl dataNodeService,
      SyncLogReduceCollector<PbBackwardSyncLogRequestUnit,
          PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest> reduceCollector,
      LogStorageReader logStorageReader, DataNodeConfiguration dataNodeConfiguration) {
    this.createdTimeMs = System.currentTimeMillis();
    this.segmentUnitManager = segmentUnitManager;
    this.batchUnit = batchUnit;
    this.context = context;
    this.reduceCollector = reduceCollector;
    this.logStorageReader = logStorageReader;
    this.dataNodeConfiguration = dataNodeConfiguration;
    this.dataNodeService = dataNodeService;
  }

  @Override
  public SyncLogTaskType type() {
    return SyncLogTaskType.PUSHER;
  }

  @Override
  public SegId getSegId() {
    return new SegId(batchUnit.getVolumeId(), batchUnit.getSegIndex());
  }

  public void setMissingLogDataAtSecondary(Set<Long> missingLogDataAtSecondary) {
    this.missingLogDataAtSecondary = missingLogDataAtSecondary;
  }

  @Override
  public boolean process() {
    logger.debug("begin to process sync log pusher, {}", this);
   
    SegId segId = new SegId(batchUnit.getVolumeId(), batchUnit.getSegIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    SegmentLogMetadata segmentLogMetadata = null;
    segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    try {
      try {
       
       
       
       
       
        Validator
            .validateSyncLogInput(batchUnit, segmentUnit, context, batchUnit.getSegmentStatus());

      } catch (TException e) {
        logger.info("sync log validator, seg id {} ", segId, e);
        errorCode = convertThriftExceptionToPbErrorCode(e);
        return true;
      }

      if (System.currentTimeMillis() - createdTimeMs > dataNodeConfiguration
          .getSyncLogMaxWaitProcessTimeMs()) {
        logger
            .warn(
                "sync log pusher got a driver task {} who has expired {}",
                batchUnit, System.currentTimeMillis() - createdTimeMs);
        return true;

      }
     
     
      long minClId = batchUnit.getPcl();

      if (minClId == segmentUnit.getSegmentLogMetadata().getClId()) {
        logger
            .info(
                "sync log pusher got a driver task {} who " 
                    + "secondary min cl id equal primary cl id {}",
                batchUnit, segmentLogMetadata.getClId());
        return true;
      }
      if (segmentLogMetadata.getPeerClId(new InstanceId(batchUnit.getMyself())) > minClId) {
        logger
            .warn(
                "sync log pusher got a driver task {} who" 
                    + " secondary min cl id letter than peer cl id {}",
                batchUnit, segmentLogMetadata.getPeerClId(new InstanceId(batchUnit.getMyself())));
        return true;
      }

      try {
        secondaryWaitingSyncLogs = getAllLogsBetweenPcl(segmentUnit, minClId,
            segmentUnit.getSegmentLogMetadata().getClId());
        logger.debug("segId {}, got all logs {}for sync to secondary", segId,
            secondaryWaitingSyncLogs);

        InstanceId requestSender = new InstanceId(batchUnit.getMyself());

        PeerStatus peerStatus = PeerStatus.getPeerStatusInMembership(
            segmentUnit.getSegmentUnitMetadata().getMembership(), requestSender);

        long newSwplId = segmentLogMetadata
            .peerUpdatePlId(batchUnit.getPpl(), requestSender,
                dataNodeConfiguration.getThresholdToClearSecondaryPlMs(), peerStatus);
        logger.info("update segment swpl id {}, segment id {}, destination {}", newSwplId, segId,
            requestSender.getId());
        segmentLogMetadata.setSwplIdTo(newSwplId);
        long secondaryCl = batchUnit.getPcl();
       
        long newSwclId = segmentLogMetadata
            .peerUpdateClId(secondaryCl, requestSender,
                dataNodeConfiguration.getThresholdToClearSecondaryClMs(), peerStatus);
        segmentLogMetadata.setSwclIdTo(newSwclId);
      } catch (LogIdTooSmallExceptionThrift logIdTooSmallExceptionThrift) {
        logger.warn("get all logs but secondary log id {} to small, log metadata {}",
            batchUnit.getPcl(), segmentLogMetadata.getFirstNode(), logIdTooSmallExceptionThrift);
        errorCode = PbErrorCode.CL_TOO_SMALL;
        return true;
      } catch (InternalErrorThrift errorThrift) {
        logger.error("get all logs between pcl failed", errorThrift);
        return false;
      }
    } finally {
      logger.info("nothing need to do here");
    }

    return true;
  }

  @Override
  public boolean reduce() {
    SegId segId = new SegId(batchUnit.getVolumeId(), batchUnit.getSegIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    SegmentLogMetadata logMetadata = segmentUnit.getSegmentLogMetadata();

    Broadcastlog.PbBackwardSyncLogRequestUnit.Builder builder = PbBackwardSyncLogRequestUnit
        .newBuilder();
    builder.setVolumeId(segmentUnit.getSegId().getVolumeId().getId());
    builder.setSegIndex(segmentUnit.getSegId().getIndex());
    builder.setPswcl(logMetadata.getSwclId());
    builder.setPswpl(logMetadata.getSwplId());
    builder.setPrimaryClId(logMetadata.getClId());
    builder.setPrimaryMaxLogIdWhenPsi(logMetadata.getPrimaryMaxLogIdWhenPsi());
    builder.setMyself(context.getInstanceId().getId());
    builder.setMembership(PbRequestResponseHelper
        .buildPbMembershipFrom(segmentUnit.getSegmentUnitMetadata().getMembership()));
    builder.setCode(errorCode);
    if (errorCode == PbErrorCode.Ok) {
      if (secondaryWaitingSyncLogs != null) {
        AtomicInteger atomicMaxSyncLogDataOnceTime = new AtomicInteger(
            dataNodeConfiguration.getBackwardSyncLogPackageFrameSize() / dataNodeConfiguration
                .getPageSize()
        );

        try {
          secondaryWaitingSyncLogs.stream().forEach(mutationLogEntry -> {
            Broadcastlog.PbBackwardSyncLogMetadata.Builder metadataBuilder =
                Broadcastlog.PbBackwardSyncLogMetadata.newBuilder();

            metadataBuilder.setLogId(mutationLogEntry.getLogId());
            metadataBuilder.setUuid(mutationLogEntry.getUuid());
            Validate.isTrue(mutationLogEntry.isFinalStatus());
            if (mutationLogEntry.getStatus() != LogStatus.Committed) {
              metadataBuilder.setStatus(mutationLogEntry.getStatus().getPbLogStatus());
            }

            if (atomicMaxSyncLogDataOnceTime.getAndDecrement() > 0
                && null != missingLogDataAtSecondary && missingLogDataAtSecondary
                .contains(metadataBuilder.getLogId())) {
              logger.debug("found a secondary miss log (log id = {})in sync log at primary",
                  metadataBuilder.getLogId());
              PbSyncLogData.Builder dataBuilder = PbSyncLogData.newBuilder();
              dataBuilder.setOffset(mutationLogEntry.getOffset());
              if (batchUnit.hasCatchUpLogId() && mutationLogEntry.getLogId() <= batchUnit
                  .getCatchUpLogId()) {
                logger.debug("found a secondary miss log (log id = {})in sync log at primary"
                        + ", but log id small than catch up log id {}",
                    metadataBuilder.getLogId(), batchUnit.getCatchUpLogId());
                dataBuilder.setCheckSum(0);
              } else {
                try {
                  byte[] data = fillLogData(segmentUnit, mutationLogEntry,
                      dataNodeConfiguration.getPageSize());
                  dataBuilder.setCheckSum(0);
                  dataBuilder.setData(ByteStringUtils.newInstance(data, 0, data.length));
                } catch (Exception e) {
                  logger.error("fill log data failed, segment unit {}, log Id {}, log Image {}",
                      segmentUnit, mutationLogEntry.getLogId(), logMetadata.getLogImage(0), e);
                  return;
                }
              }
              metadataBuilder.setLogData(dataBuilder);
            } else if (batchUnit.hasCatchUpLogId() && mutationLogEntry.getLogId() <= batchUnit
                .getCatchUpLogId()) {
              PbSyncLogData.Builder dataBuilder = PbSyncLogData.newBuilder();
              dataBuilder.setOffset(mutationLogEntry.getOffset());

              logger.debug("found a secondary log (log id = {})in sync log at primary"
                      + ", but log id small than catch up log id {}, " 
                      + "so send it log metadata together",
                  metadataBuilder.getLogId(), batchUnit.getCatchUpLogId());
              dataBuilder.setCheckSum(0);
              metadataBuilder.setLogData(dataBuilder);
            }

            builder.addMetadatOfLogs(metadataBuilder);
          });
          secondaryWaitingSyncLogs.clear();
        } finally {
          logger.info("nothing need to do here");
        }
      }
    } else {
      switch (errorCode) {
        case NOT_PRIMARY:
        case STALE_MEMBERSHIP:
        case INVALID_MEMBERSHIP:
        case YOU_ARE_NOT_IN_MEMBERSHIP:
          builder.setMembership(PbRequestResponseHelper
              .buildPbMembershipFrom(segmentUnit.getSegmentUnitMetadata().getMembership()));
          break;
        case CL_TOO_SMALL:
          builder.setTooSmallClId(batchUnit.getPcl());
          break;
        default:
      }
    }

    PbBackwardSyncLogRequestUnit message = builder.build();
    InstanceId requestSender = new InstanceId(batchUnit.getMyself());
    reduceCollector.submit(requestSender, message);

    segmentUnit
        .extendPeerLease(dataNodeConfiguration.getSecondaryLeaseInPrimaryMs(), requestSender);
    return true;
  }

  private List<MutationLogEntry> getAllLogsBetweenPcl(SegmentUnit segmentUnit, long secondaryCl,
      long primaryCl)
      throws InternalErrorThrift, LogIdTooSmallExceptionThrift {
    Pair<Boolean, List<MutationLogEntry>> logsAtMemory = segmentUnit.getSegmentLogMetadata()
        .getLogsBetweenClAndCheckCompletion(secondaryCl, primaryCl,
            dataNodeConfiguration.getMaxNumberLogsForSecondaryToSyncUp());
    List<MutationLogEntry> logsAtStorage = null;
    if (!logsAtMemory.getFirst()) {
      try {
        long firstLogId = Long.MAX_VALUE;
        if (logsAtMemory.getSecond().size() > 0) {
          firstLogId = logsAtMemory.getSecond().get(0).getLogId();
        }
        logger.info("we got some logs from log storage, is very slow. seg id {}, "
                + "min log id in memory {}, secondary cl id {}, destination {}",
            segmentUnit.getSegId(),
            segmentUnit.getSegmentLogMetadata().getFirstNode(), secondaryCl,
            this.batchUnit.getMyself());
        List<MutationLogEntry> logsReadFromStorage = logStorageReader
            .readLogsAfter(segmentUnit.getSegId(), secondaryCl,
                dataNodeConfiguration.getMaxNumberLogsForSecondaryToSyncUp(), true);
        logsAtStorage = new ArrayList<>();
        for (MutationLogEntry logEntry : logsReadFromStorage) {
          if (logEntry.getLogId() < firstLogId) {
            logsAtStorage.add(logEntry);
          }
        }
        if (logsAtStorage.size() < dataNodeConfiguration.getMaxNumberLogsForSecondaryToSyncUp()) {
          logsAtStorage.addAll(logsAtMemory.getSecond());
        }
      } catch (IOException | KvStoreException e) {
        String errMsg =
            "Fail to read logs after " + secondaryCl + " for segId " + segmentUnit.getSegId();
        logger.error(errMsg, e);
        throw new InternalErrorThrift().setDetail(errMsg);
      } catch (LogIdNotFoundException e) {
        LogImage image = segmentUnit.getSegmentLogMetadata().getLogImage(0);
        String errMsg =
            "Logs (segId : " + segmentUnit.getSegId() + " after log id: " + secondaryCl
                + " requested by the secondary "
                + " doesn't exist in the log storage system, my self logImage "
                + image;
        logger.error(errMsg);
        throw new LogIdTooSmallExceptionThrift().setLatestLogId(secondaryCl);
      }
    }

    if (null != logsAtStorage && logsAtStorage.size() != 0) {
      return logsAtStorage;
    } else {
      return logsAtMemory.getSecond();
    }
  }

  protected byte[] fillLogData(SegmentUnit segmentUnit, MutationLogEntry firstLogAtPrimary,
      int pageSize)
      throws Exception {
    byte[] data = null;
    if (firstLogAtPrimary.getStatus() == LogStatus.Created
        || firstLogAtPrimary.getStatus() == LogStatus.Committed) {
      
      if ((data = firstLogAtPrimary.getData()) == null) {
       
        PageContext<Page> pageContext = null;
        PageAddress pageAddress = segmentUnit.getAddressManager()
            .getPhysicalPageAddress(firstLogAtPrimary.getOffset());
        Validate.isTrue(!GarbagePageAddress.isGarbagePageAddress(pageAddress));
        Validate.isTrue(!BogusPageAddress.isAddressBogus(pageAddress));

        try {
          pageContext = dataNodeService.getPageManager().checkoutForRead(pageAddress);
          if (!pageContext.isSuccess()) {
            throw pageContext.getCause();
          }

          int offsetWithinPage = (int) (firstLogAtPrimary.getOffset() % pageSize);
          data = new byte[firstLogAtPrimary.getLength()];
         
          pageContext.getPage().getData(offsetWithinPage, data, 0, firstLogAtPrimary.getLength());
        } catch (ChecksumMismatchedException e) {
          logger.error(
              "Caught a checksum mismatched exception. " 
                  + "PageAddress={} submitting a request to correct the page",
              pageAddress);
          SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
          PageErrorCorrector corrector = buildPageErrorCorrector(pageAddress, membership,
              segmentUnit);
          corrector.correctPage();
          throw e;
        } catch (Exception e) {
          logger.error("Can't get the page for read. PageAddress: {}, firstLogAtPrimary: {}",
              pageAddress,
              firstLogAtPrimary, e);
          throw e;
        } finally {
          pageContext.updateSegId(segmentUnit.getSegId());
          dataNodeService.getPageManager().checkin(pageContext);
        }
      } else {
        return data;
      }
    }
    return data;
  }

  public PageErrorCorrector buildPageErrorCorrector(PageAddress pageAddress,
      SegmentMembership membership, SegmentUnit segmentUnit) {
    Validate.isTrue(pageAddress != null);
    Validate.isTrue(membership != null);
    return new PageErrorCorrector(dataNodeService, pageAddress, membership,
        segmentUnit);
  }

  private PbErrorCode convertThriftExceptionToPbErrorCode(TException e) {
    if (e instanceof SegmentNotFoundExceptionThrift) {
      return PbErrorCode.SEGMENT_NOT_FOUND;
    } else if (e instanceof NotPrimaryExceptionThrift) {
      return PbErrorCode.NOT_PRIMARY;
    } else if (e instanceof WrongPrePrimarySessionExceptionThrift) {
      return PbErrorCode.WRONG_PRE_PRIMARY_SESSION;
    } else if (e instanceof StillAtPrePrimaryExceptionThrift) {
      return PbErrorCode.STILL_AT_PRE_PRIMARY;
    } else if (e instanceof InvalidMembershipExceptionThrift) {
      return PbErrorCode.YOU_ARE_NOT_IN_MEMBERSHIP;
    } else if (e instanceof StaleMembershipExceptionThrift) {
      return PbErrorCode.STALE_MEMBERSHIP;
    } else if (e instanceof YouAreNotInMembershipExceptionThrift) {
      return PbErrorCode.YOU_ARE_NOT_IN_MEMBERSHIP;
    }
    return PbErrorCode.Ok;
  }

  @Override
  public InstanceId homeInstanceId() {
    return new InstanceId(batchUnit.getMyself());
  }

  @Override
  public String toString() {
    return "SyncLogPusher{"
        + ", context=" + context
        + ", logStorageReader=" + logStorageReader
        + ", batchUnit=" + batchUnit
        + ", reduceCollector=" + reduceCollector
        + ", secondaryWaitingSyncLogs=" + secondaryWaitingSyncLogs
        + ", errorCode=" + errorCode
        + ", missingLogDataAtSecondary=" + missingLogDataAtSecondary
        + '}';
  }
}

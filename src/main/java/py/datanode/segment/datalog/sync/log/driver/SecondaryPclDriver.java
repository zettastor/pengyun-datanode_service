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

package py.datanode.segment.datalog.sync.log.driver;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPageStatus;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.datanode.segment.datalog.sync.log.task.SyncLogRequestPackager;
import py.datanode.segment.datalogbak.catchup.PclDrivingType;
import py.instance.InstanceId;

public class SecondaryPclDriver extends PclDriver {
  private static final Logger logger = LoggerFactory.getLogger(SecondaryPclDriver.class);
  private static Random random = new Random(System.currentTimeMillis());
  private final SegmentUnit segmentUnit;
  private final DataNodeConfiguration dataNodeConfiguration;
  private final SyncLogTaskExecutor taskExecutor;
  private final InstanceId myself;
  private final AppContext context;

  private long lastProcessPackagerTimestamp;
  private long lastProcessReceiverTimestamp;

  public SecondaryPclDriver(SegmentUnit segmentUnit,
      DataNodeConfiguration dataNodeConfiguration, SyncLogTaskExecutor taskExecutor) {
    super(dataNodeConfiguration.getSyncLogMaxWaitProcessTimeMs());
    this.segmentUnit = segmentUnit;
    this.dataNodeConfiguration = dataNodeConfiguration;
    this.taskExecutor = taskExecutor;
    this.myself = taskExecutor.getContext().getInstanceId();
    this.context = taskExecutor.getContext();
  }

  @Override
  public Boolean call() throws Exception {
    logger.debug("begin check is need to sync log");
    if (this.expire()) {
      logger.info("a pcl driver task has been expired, change status to expired");
      this.setStatus(PclDriverStatus.Expired);
    }

    if (!this.getStatus().isFreeStatus()) {
      return false;
    }
   
    if (segmentUnit.isHoldToExtendLease()) {
      return false;
    }

    InstanceId potentialPrimaryInstanceId = null;
    PclDrivingType pclDrivingType = segmentUnit.getPclDrivingType(myself);
    if (!pclDrivingType.isSecondary()) {
      logger.info("segment {} not secondary driving type, no need secondary driver",
          segmentUnit.getSegId());
      return false;
    }

    if (pclDrivingType == PclDrivingType.VotingSecondary) {
      if (!segmentUnit.potentialPrimaryIdSet()) {
        logger.warn("Can't get potential primary id because it has not been set. {}",
            segmentUnit.getSegId());
        return false;
      }

      potentialPrimaryInstanceId = new InstanceId(segmentUnit.getPotentialPrimaryId());
    }

    SegmentLogMetadata logMetadata = segmentUnit.getSegmentLogMetadata();
   
   
   
    if (segmentUnit.isNotAcceptWriteReadRequest() && logMetadata.getClId() >= logMetadata
        .getMaxLogId() 
        && logMetadata.getNotInsertedLogsCount() != 0) {
      logger.warn(
          "no need to sync logs because this segment unit doesn't accept write read request anymore"
              + "and has all logs already, segment unit : {}", segmentUnit);
      return false;
    }

    if (!okToSyncLogs(segmentUnit.getSegmentLogMetadata().gapOfPclLogIds(),
        dataNodeConfiguration.getMaxNumberLogsForSecondaryToSyncUp(),
        dataNodeConfiguration.getProbabilityToSyncLogsWhenNoEnoughLogs())) {
      return false;
    }

    SyncLogRequestPackager packager = new SyncLogRequestPackager(
        taskExecutor.getSyncLogBatchRequestReduceCollector(), segmentUnit,
        potentialPrimaryInstanceId == null ? segmentUnit.getSegmentUnitMetadata().getMembership()
            .getPrimary() : potentialPrimaryInstanceId, context);
    if (!taskExecutor.submit(packager)) {
      logger.warn("submit package message failed {}, set status is processing for delay a period",
          segmentUnit.getSegId());

      this.setStatus(PclDriverStatus.Processing);
      this.updateLease();
      return false;
    }

    logger.info("create a new sync log task {}", packager);

    this.setStatus(PclDriverStatus.Processing);
    this.updateLease();

    if (segmentUnit.getSegmentLogMetadata().gapOfPclLogIds() > dataNodeConfiguration
        .getMaxGapOfPclLoggingForSyncLogs()) {
      logger.warn(
          "seg id {} gap of primary cl too much {},last packager time {} last receiver time {}"
              + "current time {}",
          segmentUnit.getSegId(), segmentUnit.getSegmentLogMetadata().gapOfPclLogIds(),
          segmentUnit.getSecondaryPclDriver().getLastProcessPackagerTimestamp(),
          segmentUnit.getSecondaryPclDriver().getLastProcessReceiverTimestamp(),
          System.currentTimeMillis());
    }
    this.setLastProcessPackagerTimestamp(System.currentTimeMillis());
    return true;
  }

  public boolean okToSyncLogs(int gapOfPclLogIds, int maxNumberLogsForSecondaryToSyncUp,
      int probabilityOfSyncLogsWhenNoEnoughLogs) {
    if (gapOfPclLogIds >= maxNumberLogsForSecondaryToSyncUp) {
     
     
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
          "the random number {}. " 
              + "Hit {}% probability of no syncing log when there are no enough logs",
          randomNumber, probabilityOfSyncLogsWhenNoEnoughLogs);
      return true;
    } else {
      logger.debug(
          "the random number {}. " 
              + "Not Hit {}% probability of no syncing log when there are no enough logs",
          randomNumber, probabilityOfSyncLogsWhenNoEnoughLogs);
      return false;
    }
  }

  private boolean isCatchingUpLog() {
    SecondaryCopyPageManager manager = segmentUnit.getSecondaryCopyPageManager();
    if (manager != null && manager.getCopyPageStatus() == CopyPageStatus.CatchupLog) {
      return true;
    }
    return false;
  }

  public long getLastProcessPackagerTimestamp() {
    return lastProcessPackagerTimestamp;
  }

  public void setLastProcessPackagerTimestamp(long lastProcessPackagerTimestamp) {
    this.lastProcessPackagerTimestamp = lastProcessPackagerTimestamp;
  }

  public long getLastProcessReceiverTimestamp() {
    return lastProcessReceiverTimestamp;
  }

  public void setLastProcessReceiverTimestamp(long lastProcessReceiverTimestamp) {
    this.lastProcessReceiverTimestamp = lastProcessReceiverTimestamp;
  }
}

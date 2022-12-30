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

package py.datanode.segment.membership.statemachine.processors;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.Utils;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.GlobalDeletingSegmentUnitCollection;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.infocenter.client.InformationCenterClientWrapper;

public class SegmentUnitDeleter extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitDeleter.class);
  private long defaultFixedDelay = 0;
  private long deletedReportFixedDelay = 0;

  public SegmentUnitDeleter(StateProcessingContext context) {
    super(context);
    Validate.isTrue(
        (context.getStatus() == SegmentUnitStatus.Deleting
            || context.getStatus() == SegmentUnitStatus.Broken));
  }

  public StateProcessingResult process(StateProcessingContext context) {
    defaultFixedDelay = cfg.getRateOfSegmentUnitDeleterExecutionMs();
    deletedReportFixedDelay = cfg.getRateOfReportingSegmentsMs() + 100;
    SegmentUnit segmentUnit = context.getSegmentUnit();

    if (segmentUnit == null) {
      logger.warn(" segment unit {} not exist ", context.getSegId());
      return getFailureResultWithRandomizedDelay(context, context.getStatus(), defaultFixedDelay);
    }

    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    SegmentUnitStatus currentStatus = metadata.getStatus();

    if (!SegmentUnitStatus.Deleting.equals(currentStatus) && !SegmentUnitStatus.Broken
        .equals(currentStatus)) {
      logger.warn("context: {} corresponding segment unit is not in Deleting or Broken status"
          + " and the current status is {}. Do nothing ", context, currentStatus);

      return getFailureResultWithZeroDelay(context, currentStatus);
    }

    SegId segId = context.getSegId();

    long waitTimeToDeleted = 0L;
    long timeToChange = metadata.getLastUpdated() + waitTimeToDeleted;
    logger.debug("run segment unit deleter for {}, timeToChange {}, waitTimeMsToMoveToDeleted {}",
        metadata,
        timeToChange, waitTimeToDeleted);
    long currentTime = System.currentTimeMillis();
    if (timeToChange > currentTime) {
      logger.info(
          "The last time of updating segment unit was {}  and time to change to deleted is {} "
              + ", and current time is {} So wait {} ms. "
              + "Let's wait {} ms until the next deleter execution, timeToDeleted {}, segUnit {}",
          Utils.millsecondToString(metadata.getLastUpdated()),
          Utils.millsecondToString(timeToChange),
          Utils.millsecondToString(currentTime), (timeToChange - currentTime), defaultFixedDelay,
          waitTimeToDeleted, segmentUnit);

      return getSuccesslResultWithFixedDelay(context, currentStatus, defaultFixedDelay);
    }

    InformationCenterClientWrapper informationClient = null;
    try {
      Collection<SegmentUnitMetadata> unitMetadatas = new ArrayList<>(1);
      SegmentUnitMetadata newMetaData = new SegmentUnitMetadata(metadata);
      unitMetadatas.add(newMetaData);
      newMetaData.setStatus(SegmentUnitStatus.Deleted);

      GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitCollection 
          deletingSegmentUnitCollection = GlobalDeletingSegmentUnitCollection
          .getDeletingSegmentUnitCollection(segmentUnit.getSegId());
      if (null == deletingSegmentUnitCollection) {
        GlobalDeletingSegmentUnitCollection.newDeletingSegmentUnit(newMetaData);
        logger.debug("new report segment units {} deleted status to info center ",
            segmentUnit.getSegId());
        return getFailureResultWithFixedDelay(context, SegmentUnitStatus.Deleting,
            deletedReportFixedDelay);
      } else if (GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitStatus.Begin
          == deletingSegmentUnitCollection.status) {
        logger.info(
            "report segment units {} deleted status to info " 
                + "center has not been execute, retry it again",
            segmentUnit.getSegId());
        return getFailureResultWithFixedDelay(context, SegmentUnitStatus.Deleting,
            deletedReportFixedDelay);
      } else if (GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitStatus.Success
          == deletingSegmentUnitCollection.status) {
        logger.debug(
            "report segment units deleted status to info center success," 
                + " release segment unit {} resource",
            segmentUnit.getSegId());
        GlobalDeletingSegmentUnitCollection
            .getAndRemoveDeletingSegmentUnitCollection(segmentUnit.getSegId());
      } else if (GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitStatus.Failed
          == deletingSegmentUnitCollection.status) {
        logger.warn("report segment units {} deleted status to info center failed , retry it again",
            segmentUnit.getSegId());
        GlobalDeletingSegmentUnitCollection
            .getAndRemoveDeletingSegmentUnitCollection(segmentUnit.getSegId());
        return getFailureResultWithFixedDelay(context, SegmentUnitStatus.Deleting,
            deletedReportFixedDelay);
      }

      logger.debug("start to remove segment log data and metadata for {}", segId);
      if (!segmentUnit.isArbiter()) {
        mutationLogManager.removeSegmentLogMetadata(segId);
      }

      archiveManager.removeSegmentUnit(segId, false);

      if (!segmentUnit.isArbiter()) {
        logPersister.removeLogMetaData(segId);
      }

      logger.info("Done with removing {}", segId);

      return getSuccesslResultWithZeroDelay(context, SegmentUnitStatus.Deleted);
    } catch (Exception e) {
      logger.warn("Can't remove {} and its other structures in the memory", metadata, e);
      logger.info("Putting the membership back to the working queue");

      return getFailureResultWithFixedDelay(context, SegmentUnitStatus.Deleting,
          defaultFixedDelay);
    }
  }
}

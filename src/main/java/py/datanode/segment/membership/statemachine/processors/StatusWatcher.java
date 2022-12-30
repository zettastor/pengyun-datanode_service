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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;

public class StatusWatcher extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(StatusWatcher.class);
  private static final long DEFAULT_FIXED_DELAY = 2000;

  public StatusWatcher(StateProcessingContext context) {
    super(context);
  }

  public StateProcessingResult process(StateProcessingContext context) {
    logger.debug("watching the context: {} ", context);

    SegmentUnit segmentUnit = context.getSegmentUnit();
    if (segmentUnit == null) {
      logger.warn(" segment unit {} not exist ", context.getSegId());
      return getFailureResultWithRandomizedDelay(context, context.getStatus(), DEFAULT_FIXED_DELAY);
    }

    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    SegmentUnitStatus currentStatus = metadata.getStatus();

    if (currentStatus != SegmentUnitStatus.PrePrimary && currentStatus != SegmentUnitStatus.Broken
        && currentStatus != SegmentUnitStatus.Deleted && currentStatus != SegmentUnitStatus.Unknown
        && currentStatus != SegmentUnitStatus.OFFLINED) {
      logger.debug("move to the new status {} ASAP", currentStatus);
      if (currentStatus == SegmentUnitStatus.Start) {
        return getSuccesslResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
      } else {
        return getSuccesslResultWithZeroDelay(context, currentStatus);
      }
    } else {
      logger.debug("stay in the current status {}", currentStatus);
      return getSuccesslResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
    }
  }
}

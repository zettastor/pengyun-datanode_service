/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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

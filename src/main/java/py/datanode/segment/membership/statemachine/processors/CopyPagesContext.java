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

import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.StateProcessingContextKey;

/**
 * This class is used to record CatchupLogsFromPrimaryProcess process information.
 *
 */
public class CopyPagesContext extends StateProcessingContext {
  // max times of failures
  private static final int MAX_FAILURE_TIMES = 10;

  public CopyPagesContext(SegId segId, SegmentUnit segmentUnit) {
    super(new StateProcessingContextKey(segId, "CopyPage"), SegmentUnitStatus.PreSecondary,
        segmentUnit);
  }

  public boolean reachMaxEndurance() {
    return getFailureTimes() >= MAX_FAILURE_TIMES;
  }

}
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
 * This class is used to record process information for JoinGroupProcessor.
 *
 */
public class JoinGroupContext extends StateProcessingContext {
  // max times of denied joining a group.
  public static final int MAX_DENIED_JOINING_GROUP_TIMES = 6;
  private static final int MAX_FAILURE_TIMES = 5;

  protected int deniedJoiningGroupTimes;

  public JoinGroupContext(SegId segId, SegmentUnit segmentUnit) {
    super(new StateProcessingContextKey(segId), SegmentUnitStatus.SecondaryApplicant, segmentUnit);
    deniedJoiningGroupTimes = 0;
  }

  public void incrementDeniedJoiningGroupTimes() {
    deniedJoiningGroupTimes++;
  }

  public boolean reachMaxTimesOfDeniedJoiningGroupTimes() {
    return deniedJoiningGroupTimes >= MAX_DENIED_JOINING_GROUP_TIMES;
  }

  public boolean reachMaxTimesOfFailures() {
    return getFailureTimes() >= MAX_FAILURE_TIMES;
  }
}
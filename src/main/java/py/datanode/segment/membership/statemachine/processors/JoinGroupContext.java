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
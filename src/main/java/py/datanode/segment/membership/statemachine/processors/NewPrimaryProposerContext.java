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

public class NewPrimaryProposerContext extends StateProcessingContext {
  // the highest N that been seen in the last round of proposing.
  private int highestN;

  public NewPrimaryProposerContext(SegId segId, SegmentUnit segmentUnit) {
    // the newly created context has expiring delay,
    // which means NewPrimaryProposer will be executed very soon
    super(new StateProcessingContextKey(segId), SegmentUnitStatus.Start, segmentUnit);
    highestN = -1;
  }

  public int getHighestN() {
    return highestN;
  }

  public void setHighestN(int newHighestN) {
    if (newHighestN > this.highestN) {
      this.highestN = newHighestN;
    }
  }
}
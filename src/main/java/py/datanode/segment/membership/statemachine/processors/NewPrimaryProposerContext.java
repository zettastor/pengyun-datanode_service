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
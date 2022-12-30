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

package py.datanode.segment.membership.statemachine;

import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.recurring.SegmentUnitProcessResult;
import py.datanode.segment.membership.statemachine.processors.StateProcessingContext;

public class StateProcessingResult extends SegmentUnitProcessResult {
  private final SegmentUnitStatus newStatus;

  public StateProcessingResult(StateProcessingContext context, SegmentUnitStatus newStatus) {
    super(context);
    this.newStatus = newStatus;
  }

  public SegmentUnitStatus getNewStatus() {
    return newStatus;
  }

  public StateProcessingContext getContext() {
    return (StateProcessingContext) super.getContext();
  }

  @Override
  public String toString() {
    return "StateProcessingResult [newStatus=" + newStatus + ", toString()=" + super.toString()
        + "]";
  }
}

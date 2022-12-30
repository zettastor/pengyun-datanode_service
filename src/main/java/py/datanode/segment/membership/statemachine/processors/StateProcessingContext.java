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

import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.recurring.SegmentUnitTaskContext;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.membership.statemachine.StateProcessingContextKey;

/**
 * It contains all information Updater needs to move membership state.
 *
 */
public class StateProcessingContext extends SegmentUnitTaskContext {
  protected final SegmentUnitStatus status;
  protected final SegmentUnit segmentUnit;

  public StateProcessingContext(StateProcessingContextKey k,
      SegmentUnitStatus status,
      SegmentUnit segmentUnit
  ) {
    super(k);
    this.status = status;
    this.segmentUnit = segmentUnit;
  }

  public StateProcessingContext(StateProcessingContext otherContext) {
    super(otherContext);
    this.status = otherContext.status;
    this.segmentUnit = otherContext.segmentUnit;
  }

  public StateProcessingContextKey getKey() {
    return (StateProcessingContextKey) (super.getKey());
  }

  public SegmentUnitStatus getStatus() {
    return status;
  }

  public SegmentUnit getSegmentUnit() {
    return segmentUnit;
  }

  @Override
  public String toString() {
    return "StateProcessingContext [status=" + status + ", " + super.toString() + "]";
  }

  public SegmentLogMetadata getSegmentLogMetadata() {
    return segmentUnit.getSegmentLogMetadata();
  }
}
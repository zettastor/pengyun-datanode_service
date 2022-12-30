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

package py.datanode.segment.datalogbak.catchup;

import py.archive.segment.recurring.SegmentUnitTaskContext;
import py.datanode.segment.SegmentUnit;

/**
 * It contains all information Updater needs to move membership state.
 *
 */
public class CatchupLogContext extends SegmentUnitTaskContext {
  private final SegmentUnit segmentUnit;
  private boolean firstTime = true;

  public CatchupLogContext(CatchupLogContextKey k, SegmentUnit segmentUnit) {
    super(k);
    this.segmentUnit = segmentUnit;
  }

  public CatchupLogContext(CatchupLogContext otherContext) {
    super(otherContext);
    this.segmentUnit = otherContext.segmentUnit;
  }

  public CatchupLogContextKey getKey() {
    return (CatchupLogContextKey) (super.getKey());
  }

  public SegmentUnit getSegmentUnit() {
    return segmentUnit;
  }

  public boolean isFirstTime() {
    return firstTime;
  }

  public void setFirstTime(boolean firstTime) {
    this.firstTime = firstTime;
  }

  @Override
  public String toString() {
    return "CatchupLogContext [ " + super.toString() + " , firstTime=" + this.firstTime + "]";
  }
}
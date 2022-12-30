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

package py.datanode.service.io;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import py.datanode.segment.SegmentUnit;

public abstract class DelayedIoTask extends IoTask implements Delayed {
  private long deadline;

  public DelayedIoTask(int delayMs, SegmentUnit segmentUnit) {
    super(segmentUnit);
    this.deadline = System.currentTimeMillis() + delayMs;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    return (int) (getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
  }

  public void updateDelay(int delayMs) {
    deadline = System.currentTimeMillis() + delayMs;
  }
}

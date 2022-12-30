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

package py.datanode.segment.copy.unused;

import py.engine.ResultImpl;

public class CopyPageResult extends ResultImpl {
  private volatile CopyPageTask task;
  private volatile boolean done;

  public CopyPageResult(CopyPageTask task) {
    this(task, null);
  }

  public CopyPageResult(CopyPageTask task, Exception e) {
    super(e);
    this.task = task;
    this.done = false;
  }

  public boolean isDone() {
    return done;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  public CopyPageTask getCopyPageTask() {
    return task;
  }

  @Override
  public String toString() {
    return "CopyPageResult [super=" + super.toString() + ", task=" + task + "]";
  }
}

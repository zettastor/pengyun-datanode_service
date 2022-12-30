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

package py.datanode.segment.datalog.sync.log.task;

import java.util.concurrent.atomic.AtomicInteger;
import py.archive.segment.SegId;
import py.instance.InstanceId;

public interface SyncLogTask {
  AtomicInteger syncLogTaskIdGenerator = new AtomicInteger(0);
  int syncLogTaskId = syncLogTaskIdGenerator.getAndIncrement();

  /**
   * process sync log task unit.
   *
   * @return true if success
   */
  public boolean process();

  /**
   * after process sync log task unit, using this function to collect all result info.
   *
   * @return true if success
   */
  public boolean reduce();

  public InstanceId homeInstanceId();

  public SyncLogTaskType type();

  public SegId getSegId();
}

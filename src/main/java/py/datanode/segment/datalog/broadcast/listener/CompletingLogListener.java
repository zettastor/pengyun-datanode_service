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

package py.datanode.segment.datalog.broadcast.listener;

public interface CompletingLogListener {
  /**
   * when the completing log get completed.
   *
   * @param logUuid the uuid of the completed log
   */
  public void complete(long logUuid);

  /**
   * the completing log ends up failed.
   *
   * @param logUuid the uuid of the incomplete log
   */
  public void fail(long logUuid, Exception e);
}

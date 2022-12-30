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

package py.datanode.segment.datalog.sync.log.driver;

import py.datanode.segment.datalogbak.catchup.LogDriver;

public class PrimaryPclDriver extends PclDriver implements LogDriver {
  public PrimaryPclDriver(int timeout) {
    super(timeout);
  }

  @Override
  public Boolean call() throws Exception {
    return drive() == ExecuteLevel.SLOWLY;
  }

  @Override
  public ExecuteLevel drive() throws Exception {
    return ExecuteLevel.SLOWLY;
  }

}

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

package py.datanode.segment.datalog.broadcast;

import py.datanode.segment.datalog.broadcast.exception.HeIsBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.ImBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.WeDontHaveLogEntry;
import py.datanode.segment.datalog.broadcast.listener.CompletingLogListener;

public class LogWithoutLogEntry extends CompletingLog {
  private final long logId;

  public LogWithoutLogEntry(long logUuid, long logId, CompletingLogListener listener) {
    super(logUuid, listener);
    this.logId = logId;
  }

  @Override
  public CompleteLog tryComplete(LogWithoutLogId another) throws ImBrokenLogException {
    return mergeLog(this, another);
  }

  @Override
  public CompleteLog tryComplete(BrokenLogWithoutLogId another) throws HeIsBrokenLogException {
    throw new HeIsBrokenLogException();
  }

  @Override
  public CompleteLog tryComplete(LogWithoutLogEntry another) throws WeDontHaveLogEntry {
    throw new WeDontHaveLogEntry();
  }

  public long getLogId() {
    return logId;
  }

  @Override
  public String toString() {
    return "LogWithoutLogEntry{" + "super=" + super.toString() + ", logId=" + logId + '}';
  }
}

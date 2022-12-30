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

import org.apache.commons.lang3.NotImplementedException;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.broadcast.exception.ImBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.WeDontHaveLogEntry;
import py.datanode.segment.datalog.broadcast.exception.WeDontHaveLogId;
import py.datanode.segment.datalog.broadcast.listener.CompletingLogListener;

/**
 * complete log with both log entry and log ID.
 */
public class CompleteLog extends CompletingLog {
  private final MutationLogEntry logEntry;

  public CompleteLog(MutationLogEntry logEntry, CompletingLogListener listener) {
    super(logEntry.getUuid(), listener);
    this.logEntry = logEntry;
  }

  @Override
  public CompleteLog tryComplete(LogWithoutLogId another)
      throws ImBrokenLogException, WeDontHaveLogId {
    throw new NotImplementedException("I am completed");
  }

  @Override
  public CompleteLog tryComplete(LogWithoutLogEntry another)
      throws ImBrokenLogException, WeDontHaveLogEntry {
    throw new NotImplementedException("I am completed");
  }

  public MutationLogEntry getLogEntry() {
    return logEntry;
  }

  @Override
  public String toString() {
    return "CompleteLog{" + "super=" + super.toString() + ", logEntry=" + logEntry + '}';
  }
}

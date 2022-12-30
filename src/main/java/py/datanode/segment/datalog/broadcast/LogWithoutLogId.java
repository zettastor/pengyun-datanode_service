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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.broadcast.exception.HeIsBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.ImBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.WeDontHaveLogId;
import py.datanode.segment.datalog.broadcast.listener.CompletingLogListener;

public class LogWithoutLogId extends CompletingLog {
  private static final Logger logger = LoggerFactory.getLogger(LogWithoutLogId.class);
  private final MutationLogEntry logEntry;

  public LogWithoutLogId(MutationLogEntry logEntry, CompletingLogListener listener) {
    super(logEntry.getUuid(), listener);
    this.logEntry = logEntry;
  }

  public LogWithoutLogId(MutationLogEntry logEntry) {
    super(logEntry.getUuid(), null);
    this.logEntry = logEntry;
  }

  public MutationLogEntry getLogEntry() {
    return logEntry;
  }

  @Override
  public CompleteLog tryComplete(LogWithoutLogId another)
      throws ImBrokenLogException, WeDontHaveLogId {
    throw new WeDontHaveLogId();
  }

  @Override
  public CompleteLog tryComplete(BrokenLogWithoutLogId another)
      throws ImBrokenLogException, HeIsBrokenLogException {
    throw new HeIsBrokenLogException();
  }

  @Override
  public CompleteLog tryComplete(LogWithoutLogEntry another) throws ImBrokenLogException {
    return mergeLog(another, this);

  }

  @Override
  public String toString() {
    return "LogWithoutLogId{" + "super=" + super.toString() + ", logEntry=" + logEntry + '}';
  }
}

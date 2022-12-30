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

import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.broadcast.exception.HeIsBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.ImBrokenLogException;

/**
 * broken log without log id, the log entry did come but for some reason we couldn't create a real
 * mutation log entry .
 */
public class BrokenLogWithoutLogId extends LogWithoutLogId {
  private BrokenReason reason;

  public BrokenLogWithoutLogId(long logUuid, BrokenReason reason) {
    super(MutationLogEntryFactory.createEmptyLog(logUuid, LogImage.INVALID_LOG_ID, -1, -1));
    this.reason = reason;
  }

  @Override
  public CompleteLog tryComplete(LogWithoutLogId another) throws ImBrokenLogException {
    throw new ImBrokenLogException();
  }

  @Override
  public CompleteLog tryComplete(BrokenLogWithoutLogId another)
      throws ImBrokenLogException, HeIsBrokenLogException {
    throw new ImBrokenLogException();
  }

  @Override
  public CompleteLog tryComplete(LogWithoutLogEntry another) throws ImBrokenLogException {
    throw new ImBrokenLogException();
  }

  @Override
  public MutationLogEntry getLogEntry() {
    return null;
  }

  public boolean isSuccess() {
    switch (reason) {
      case Discarded:
        return true;
      default:
        return false;
    }
  }

  @Override
  public String toString() {
    return "BrokenLogWithoutLogId{" + "super=" + super.toString() + ", reason=" + reason + '}';
  }

  public enum BrokenReason {
    ResourceExhausted, Discarded, BadRequest
  }
}

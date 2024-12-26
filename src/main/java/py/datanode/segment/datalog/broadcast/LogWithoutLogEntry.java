/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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

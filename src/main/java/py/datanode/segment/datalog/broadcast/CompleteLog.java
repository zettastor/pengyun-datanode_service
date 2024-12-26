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

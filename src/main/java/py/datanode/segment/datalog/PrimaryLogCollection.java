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

package py.datanode.segment.datalog;

import java.util.List;

public class PrimaryLogCollection {
  private List<MutationLogEntry> logsAtMemory;
  private List<MutationLogEntry> logsAtStorage;

  private List<MutationLogEntry> finalLogs;

  public PrimaryLogCollection(List<MutationLogEntry> logsAtMemory,
      List<MutationLogEntry> logsAtStorage,
      List<MutationLogEntry> finalLogs) {
    this.logsAtMemory = logsAtMemory;
    this.logsAtStorage = logsAtStorage;
    this.finalLogs = finalLogs;
  }

  public List<MutationLogEntry> getLogsAtMemory() {
    return logsAtMemory;
  }

  public void setLogsAtMemory(List<MutationLogEntry> logsAtMemory) {
    this.logsAtMemory = logsAtMemory;
  }

  public List<MutationLogEntry> getLogsAtStorage() {
    return logsAtStorage;
  }

  public void setLogsAtStorage(List<MutationLogEntry> logsAtStorage) {
    this.logsAtStorage = logsAtStorage;
  }

  public List<MutationLogEntry> getFinalLogs() {
    return finalLogs;
  }

  public void setFinalLogs(List<MutationLogEntry> finalLogs) {
    this.finalLogs = finalLogs;
  }

}

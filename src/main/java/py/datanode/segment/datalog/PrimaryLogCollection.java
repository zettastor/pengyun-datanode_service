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

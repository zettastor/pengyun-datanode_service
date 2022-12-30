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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import py.common.struct.Pair;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.plal.engine.PlalEngine;

public interface NotInsertedLogsInterface {
  Pair<List<CompletableFuture<Boolean>>, List<MutationLogEntry>> addCompleteLogs(
      PlalEngine plalEngine, List<MutationLogEntry> logs);

  List<MutationLogEntry> addCompletingLog(PlalEngine plalEngine, boolean replaceOldValue,
      CompletingLog... completingLogs);

  Map<Long, MutationLogEntry> getAllNotInsertedLogs();

  int getAllNotInsertedLogsCount();

  MutationLogEntry getLogEntry(long logUuid);

  void removeLog(MutationLogEntry log, boolean completedOnly);

  void clearAllLogs(boolean force);

  void removeCompletingLogsAndCompletedLogsPriorTo(long pclId);

  void checkPrimaryMissingLogs(List<Long> uuids, long timeOut);

  CompletingLog getCompletingLog(long logUuid);

  void cleanUpTimeOutCompletingLog(long cleanCompletingLogPeriod);
}

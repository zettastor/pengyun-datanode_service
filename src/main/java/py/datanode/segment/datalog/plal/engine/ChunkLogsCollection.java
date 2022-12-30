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

package py.datanode.segment.datalog.plal.engine;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;

public class ChunkLogsCollection {
  private static final Logger logger = LoggerFactory.getLogger(ChunkLogsCollection.class);

  private final AtomicInteger refCounter;
  private final Set<MutationLogEntry> logsToBePersisted = Collections
      .newSetFromMap(new ConcurrentHashMap<>());
  private final Set<MutationLogEntry> logsToBeApplied = Collections
      .newSetFromMap(new ConcurrentHashMap<>());
  private boolean applied = false;
  private long maxLogIdInChunkPlans;
  private AtomicBoolean allSuccess = new AtomicBoolean(true);

  ChunkLogsCollection(int multiCount) {
    this.refCounter = new AtomicInteger(multiCount);
  }

  synchronized void logPersisted(MutationLogEntry log) {
    if (applied) {
      log.setPersisted();
    } else {
      logsToBePersisted.add(log);
    }

  }

  public long getMaxLogIdInChunkPlans() {
    return maxLogIdInChunkPlans;
  }

  public void setMaxLogIdInChunkPlans(long maxLogIdInChunkPlans) {
    this.maxLogIdInChunkPlans = maxLogIdInChunkPlans;
  }

  void saveJustAppliedLogs(Collection<MutationLogEntry> justAppliedLogs) {
    logsToBeApplied.addAll(justAppliedLogs);
  }

  void removeAppliedLogs(Collection<MutationLogEntry> removeAppliedLogs) {
    logsToBeApplied.removeAll(removeAppliedLogs);
  }

  synchronized void applyAllLogs(boolean tracing) {
    applied = true;

    for (MutationLogEntry log : logsToBeApplied) {
      log.apply();
      if (tracing) {
        logger.warn("log applied uuid={} id={}", log.getUuid(), log.getLogId());
      }
    }

    for (MutationLogEntry log : logsToBePersisted) {
      log.setPersisted();
      MutationLogEntryFactory.releaseLogData(log);
    }

  }

  public boolean isAllSuccess() {
    return allSuccess.get();
  }

  public int decrementAndGet(boolean success) {
    allSuccess.compareAndSet(true, success);

    return refCounter.decrementAndGet();
  }

}

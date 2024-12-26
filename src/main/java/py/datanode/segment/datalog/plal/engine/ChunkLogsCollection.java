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

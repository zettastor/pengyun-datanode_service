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

package py.datanode.segment.datalog.sync.log;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.archive.segment.SegId;
import py.consumer.MultiThreadConsumerServiceWithBlockingQueue;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogReduceCollector;
import py.datanode.segment.datalog.sync.log.task.SyncLogTask;
import py.datanode.segment.datalog.sync.log.task.SyncLogTaskType;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogRequestUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogResponseUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;

/**
 * a global sync log task executor. all sync log task will be deal by this executor, both sync log
 * receiver and pusher.
 */
public class SyncLogTaskExecutor extends MultiThreadConsumerServiceWithBlockingQueue<SyncLogTask> {
  public static final String className = "SyncLogTaskExecutor";
  private static final Logger logger = LoggerFactory.getLogger(SyncLogTaskExecutor.class);

  private final Map<SyncLogTaskType, Set<SyncLogTaskKey>> syncLogTaskSet =
      new ConcurrentHashMap<>();

  private final AppContext context;
  private final InstanceStore instanceStore;
  private final DataNodeConfiguration configuration;

  private SyncLogReduceCollector<PbBackwardSyncLogRequestUnit,
      PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest> backwardSyncLogRequestReduceCollector;
  private SyncLogReduceCollector<PbBackwardSyncLogResponseUnit,
      PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest> backwardSyncLogResponseReduceCollector;
  private SyncLogReduceCollector<PbAsyncSyncLogBatchUnit,
      PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest> syncLogBatchRequestReduceCollector;

  public SyncLogTaskExecutor(int threadCount, String name, AppContext context,
      InstanceStore instanceStore, DataNodeConfiguration configuration) {
    super(threadCount, SyncLogTaskExecutor::consumer, new LinkedBlockingQueue<>(30000), name);
    this.context = context;
    this.instanceStore = instanceStore;
    this.configuration = configuration;
  }

  protected static void consumer(SyncLogTask task) {
    logger.debug("process sync log task process {}, type {}", task.getSegId(), task.type());
    try {
      if (task.process()) {
        if (!task.reduce()) {
          if (!task.reduce()) {
            logger.error("sync log task reduce failed {}", task);
          }
        }
      } else {
        logger.error("sync log task process failed {}, type {}", task.getSegId(), task.type());
      }
    } catch (Throwable throwable) {
      logger.error("found a exception when task process {}", task, throwable);
    }
  }

  @Override
  public boolean submit(SyncLogTask element) {
    Set<SyncLogTaskKey> syncLogTasks = syncLogTaskSet
        .computeIfAbsent(element.type(), v -> Collections.newSetFromMap(new ConcurrentHashMap<>()));

    if (syncLogTasks.add(new SyncLogTaskKey(element.homeInstanceId(), element.getSegId()))) {
      boolean result = super.submit(element);
      if (!result) {
        syncLogTasks.remove(new SyncLogTaskKey(element.homeInstanceId(), element.getSegId()));
      }

      return result;
    }

    return true;
  }

  @Override
  protected SyncLogTask pollElement() {
    SyncLogTask task = super.pollElement();
    if (task != null) {
      Set<SyncLogTaskKey> syncLogTasks = syncLogTaskSet
          .computeIfAbsent(task.type(), v -> Collections.newSetFromMap(new ConcurrentHashMap<>()));

      syncLogTasks.remove(new SyncLogTaskKey(task.homeInstanceId(), task.getSegId()));
    }

    return task;
  }

  @Override
  protected SyncLogTask pollElement(int time, TimeUnit timeUnit) throws InterruptedException {
    SyncLogTask task = super.pollElement(time, timeUnit);
    if (task != null) {
      Set<SyncLogTaskKey> syncLogTasks = syncLogTaskSet
          .computeIfAbsent(task.type(), v -> Collections.newSetFromMap(new ConcurrentHashMap<>()));

      syncLogTasks.remove(new SyncLogTaskKey(task.homeInstanceId(), task.getSegId()));
      logger.debug("poll one task {}, {}", task.syncLogTaskId, task);
    }

    return task;
  }

  public SyncLogReduceCollector<PbBackwardSyncLogRequestUnit, PbBackwardSyncLogsRequest, 
      PbBackwardSyncLogsRequest> getBackwardSyncLogRequestReduceCollector() {
    return backwardSyncLogRequestReduceCollector;
  }

  public void setBackwardSyncLogRequestReduceCollector(
      SyncLogReduceCollector
          <PbBackwardSyncLogRequestUnit, PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest>
          backwardSyncLogRequestReduceCollector) {
    this.backwardSyncLogRequestReduceCollector = backwardSyncLogRequestReduceCollector;
  }

  public SyncLogReduceCollector<PbBackwardSyncLogResponseUnit, PbBackwardSyncLogsResponse, 
      PbBackwardSyncLogsRequest> getBackwardSyncLogResponseReduceCollector() {
    return backwardSyncLogResponseReduceCollector;
  }

  public void setBackwardSyncLogResponseReduceCollector(
      SyncLogReduceCollector<PbBackwardSyncLogResponseUnit, PbBackwardSyncLogsResponse, 
          PbBackwardSyncLogsRequest> backwardSyncLogResponseReduceCollector) {
    this.backwardSyncLogResponseReduceCollector = backwardSyncLogResponseReduceCollector;
  }

  public SyncLogReduceCollector
      <PbAsyncSyncLogBatchUnit, PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest>
      getSyncLogBatchRequestReduceCollector() {
    return syncLogBatchRequestReduceCollector;
  }

  public void setSyncLogBatchRequestReduceCollector(
      SyncLogReduceCollector
          <PbAsyncSyncLogBatchUnit, PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest>
          syncLogBatchRequestReduceCollector) {
    this.syncLogBatchRequestReduceCollector = syncLogBatchRequestReduceCollector;
  }

  public AppContext getContext() {
    return context;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public DataNodeConfiguration getConfiguration() {
    return configuration;
  }

  private final class SyncLogTaskKey {
    private final InstanceId instanceId;
    private final SegId segId;

    public SyncLogTaskKey(InstanceId instanceId, SegId segId) {
      this.instanceId = instanceId;
      this.segId = segId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SyncLogTaskKey that = (SyncLogTaskKey) o;

      if (instanceId != null ? !instanceId.equals(that.instanceId) : that.instanceId != null) {
        return false;
      }
      return segId != null ? segId.equals(that.segId) : that.segId == null;
    }

    @Override
    public int hashCode() {
      int result = instanceId != null ? instanceId.hashCode() : 0;
      result = 31 * result + (segId != null ? segId.hashCode() : 0);
      return result;
    }
  }
}

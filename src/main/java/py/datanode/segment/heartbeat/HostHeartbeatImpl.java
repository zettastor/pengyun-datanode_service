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

package py.datanode.segment.heartbeat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.NamedThreadFactory;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.thrift.datanode.service.DataNodeService;

public class HostHeartbeatImpl implements HostHeartbeat {
  private static final Logger logger = LoggerFactory.getLogger(HostHeartbeatImpl.class);
  private static final long TIME_WAIT_THREADPOOL_TERMINATION = 10;
  private static final long DELAY_EXECUTION_OF_DISALLOW_SEGMENT_UNIT_MS = 100;
  public static PrimaryHeartGroup INVALID_GROUP;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory;
  private InstanceStore instanceStore;
  private DataNodeConfiguration dataNodeCfg;
  // Map a host instance id to a PrimaryHeartGroup
  private Map<Long, PrimaryHeartGroup> instanceMapPrimary =
      new ConcurrentHashMap<Long, PrimaryHeartGroup>();
  private DelayQueue<PrimaryHeartGroup> hostHeartbeatQueue;
  private Thread thread;
  private ThreadPoolExecutor threadPoolExecutor;
  private boolean pause = false;

  public HostHeartbeatImpl(InstanceStore instanceStore, DataNodeConfiguration dataNodeCfg,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory) {
    this.dataNodeCfg = dataNodeCfg;
    this.instanceStore = instanceStore;
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
    this.hostHeartbeatQueue = new DelayQueue<PrimaryHeartGroup>();
    threadPoolExecutor = new ThreadPoolExecutor(
        dataNodeCfg.getCorePoolSizeForStateProcessingEngineHeartbeat(),
        dataNodeCfg.getMaxPoolSizeForStateProcessingEngineHeartbeat(), 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new NamedThreadFactory("Host-heartbeat-worker"));
    INVALID_GROUP = new PrimaryHeartGroup(new InstanceId(0), instanceStore, dataNodeCfg,
        hostHeartbeatQueue,
        dataNodeSyncClientFactory);
  }

  @Override
  public void start() {
    thread = new Thread() {
      public void run() {
        List<PrimaryHeartGroup> groupList = new ArrayList<PrimaryHeartGroup>();
        while (true) {
          if (pause) {
            try {
              Thread.sleep(DELAY_EXECUTION_OF_DISALLOW_SEGMENT_UNIT_MS);
            } catch (InterruptedException e) {
              logger.error("", e);
            }
            continue;
          }

          try {
            groupList.clear();
            hostHeartbeatQueue.drainTo(groupList);
            if (groupList.size() == 0) {
              try {
                Thread.sleep(10);
                continue;
              } catch (InterruptedException e) {
                logger.warn("catch an exception wehen take something from queue", e);
              }
            }

            for (PrimaryHeartGroup group : groupList) {
              if (group == INVALID_GROUP) {
                logger.debug("somebody notice us terminate");
                return;
              }

              logger.debug("submit the heartbeat context {}", group);
              try {
                threadPoolExecutor.execute(group);
              } catch (RejectedExecutionException e) {
                group.setLastTimeRun(System.currentTimeMillis()
                    - dataNodeCfg.getRateOfExtendingLeaseMs() / 2);
                hostHeartbeatQueue.put(group);
              }
            }

          } catch (Exception e) {
            logger.error("catch exception when run heartbeat", e);
          }
        }
      }
    };

    thread.setName("heartbeat-puller");
    thread.start();
  }

  @Override
  public void stop() {
    this.hostHeartbeatQueue.put(INVALID_GROUP);
    threadPoolExecutor.shutdown();
    try {
      if (!threadPoolExecutor
          .awaitTermination(TIME_WAIT_THREADPOOL_TERMINATION, TimeUnit.SECONDS)) {
        logger.warn("heart bead thread still are running");
      }
    } catch (InterruptedException e) {
      logger.error("", e);
    }
  }

  @Override
  public void addRelationBetweenPrimaryAndAllAliveSecondaries(SegmentUnit segUnit) {
    logger.debug("construct relationship between primary and secondary {}", segUnit);

    for (InstanceId instanceId : segUnit.getSegmentUnitMetadata().getMembership()
        .getHeartBeatMembers()) {
      addSecondary(instanceId, segUnit);
    }
  }

  @Override
  public void removeRelationBetweenPrimaryAndAllAliveSecondaries(SegmentUnit segUnit) {
    logger.debug("remove relationship between primary and secondary {}", segUnit);
    for (InstanceId instance : segUnit.getSegmentUnitMetadata().getMembership()
        .getHeartBeatMembers()) {
      PrimaryHeartGroup group = instanceMapPrimary.get(instance.getId());
      if (group != null) {
        group.removePrimary(segUnit);
        if (group.getGroupCount() == 0) {
          logger.debug("delete the group from delay queue {}", group);
          instanceMapPrimary.remove(instance.getId());
          hostHeartbeatQueue.remove(group);
        }
      }
    }
    segUnit.clearHeartbeatResult();
  }

  @Override
  public void addRelationBetweenPrimaryAndAliveSecondary(SegmentUnit segUnit,
      long aliveSecondaryInstanceId) {
    SegmentUnitStatus myStatus = segUnit.getSegmentUnitMetadata().getStatus();
    if (!myStatus.isPrimary()) {
      logger.warn("segment is not primary status: {}", segUnit);
      return;
    }

    logger
        .debug("add relationship {}, secondary instance id {}", segUnit, aliveSecondaryInstanceId);
    addSecondary(new InstanceId(aliveSecondaryInstanceId), segUnit);
  }

  private void addSecondary(InstanceId instanceId, SegmentUnit segUnit) {
    while (true) {
      PrimaryHeartGroup group = instanceMapPrimary.get(instanceId.getId());
      if (group == null) {
        group = new PrimaryHeartGroup(instanceId, this.instanceStore, this.dataNodeCfg,
            this.hostHeartbeatQueue,
            this.dataNodeSyncClientFactory);
        group.addPrimary(segUnit);
        PrimaryHeartGroup existingGroup = instanceMapPrimary.putIfAbsent(instanceId.getId(), group);
        if (existingGroup != null) {
          group = existingGroup;
        } else {
          hostHeartbeatQueue.put(group);
          logger.debug("put heart group {} to delay queue", group);
          return;
        }
      }

      if (group.addPrimary(segUnit)) {
        break;
      }
    }
  }

  @Override
  public void removeRelationBetweenPrimaryAndAliveSecondary(SegmentUnit segUnit,
      long aliveSecondaryInstanceId) {
    SegmentUnitStatus myStatus = segUnit.getSegmentUnitMetadata().getStatus();
    if (!myStatus.isPrimary()) {
      logger.warn("segment is not primary status: {}", segUnit);
      return;
    }

    PrimaryHeartGroup group = this.instanceMapPrimary.get(aliveSecondaryInstanceId);
    Validate.notNull(group);

    group.removePrimary(segUnit);
    if (group.getGroupCount() == 0) {
      logger.debug("delete the group from delay queue {}", group);
      instanceMapPrimary.remove(aliveSecondaryInstanceId);
      hostHeartbeatQueue.remove(group);
    }

    segUnit.removeHeartbeatResult(new InstanceId(aliveSecondaryInstanceId));
  }

  @Override
  public void pause() {
    pause = true;
  }

  @Override
  public void restart() {
    pause = false;
  }

  public PrimaryHeartGroup getPrimaryHeartGroupByInstanceId(InstanceId instance) {
    return this.instanceMapPrimary.get(instance.getId());
  }

}

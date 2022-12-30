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

package py.datanode.service.worker;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.exception.FailedToSendBroadcastRequestsException;
import py.exception.QuorumNotFoundException;
import py.exception.SnapshotVersionMissMatchForMergeLogsException;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastTypeThrift;

/**
 * Membership pusher, when membership updated by primary, he will need to push the latest membership
 * to other members.
 *
 */
public class MembershipPusher {
  private static final Logger logger = LoggerFactory.getLogger(MembershipPusher.class);
  private static final int DEFAULT_DELAY_ON_FAIL_MS = 1000;
  private static final int DEFAULT_BROADCAST_TIME_OUT = 100;
  private static final int DEFAULT_FAIL_COUNT = 10;
  private static Map<InstanceId, MembershipPusher> instanceMap = new HashMap<>();
  private final DelayQueue<PushMembershipContext> taskQueue = new DelayQueue<>();
  private final Set<SegId> pendingSegIds = Sets.newConcurrentHashSet();
  private final DataNodeServiceAsyncClientWrapper clientWrapper;
  private final InstanceStore instanceStore;
  private final SegmentUnitManager segmentUnitManager;
  private final InstanceId myself;
  private final Thread workingThread;
  private final AtomicBoolean started = new AtomicBoolean(false);
  

  private MembershipPusher(DataNodeServiceAsyncClientWrapper clientWrapper,
      InstanceStore instanceStore,
      SegmentUnitManager segmentUnitManager, InstanceId myInstanceId) {
    this.clientWrapper = clientWrapper;
    this.instanceStore = instanceStore;
    this.segmentUnitManager = segmentUnitManager;
    this.myself = myInstanceId;
    this.workingThread = new Thread(this::doJob);
    this.workingThread.setName("membership-pusher-" + myInstanceId);
  }

  public static void init(DataNodeServiceAsyncClientWrapper clientWrapper,
      InstanceStore instanceStore,
      SegmentUnitManager segmentUnitManager, InstanceId myInstanceId) {
    MembershipPusher instance = new MembershipPusher(clientWrapper, instanceStore,
        segmentUnitManager,
        myInstanceId);
    instance.startTraffic();
    instanceMap.put(myInstanceId, instance);
  }

  public static MembershipPusher getInstance(InstanceId instanceId) {
    MembershipPusher instance = instanceMap.get(instanceId);
    if (instance == null) {
      throw new RuntimeException("not initialized !");
    }
    return instance;
  }

  public void submit(SegId segId) {
    logger.debug("submit {}", segId);
    if (pendingSegIds.add(segId)) {
      internalSubmit(segId, 0, 0);
    }
  }

  private void internalSubmit(SegId segId, long delayMs, int failCount) {
    PushMembershipContext context = new PushMembershipContext(segId, delayMs, failCount);
    taskQueue.put(context);
  }

  private void startTraffic() {
    if (started.compareAndSet(false, true)) {
      workingThread.start();
    }
  }

  public void stop() throws InterruptedException {
    taskQueue.put(new ExitSignal());
    workingThread.join();
  }

  private void doJob() {
    List<PushMembershipContext> tasks = new LinkedList<>();
    while (started.get()) {
      taskQueue.drainTo(tasks);
      if (tasks.isEmpty()) {
        PushMembershipContext polledTask = null;
        try {
          polledTask = taskQueue.take();
        } catch (InterruptedException ignored) {
          logger.error("", ignored);
        }
        if (polledTask != null) {
          tasks.add(polledTask);
        }
      }
      if (tasks.stream().anyMatch(context -> context instanceof ExitSignal)) {
        started.set(false);
      }
      tasks.forEach(this::processTask);
      tasks.clear();
    }
  }

  private void processTask(PushMembershipContext context) {
    if (context instanceof ExitSignal) {
      logger.warn("exit signal got, going to exit");
      return;
    }

    SegId segId = context.segId;
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    final SegmentUnitStatus status = segmentUnit.getSegmentUnitMetadata().getStatus();
    logger.debug("push membership processing {}", segmentUnit);
    if (segmentUnit == null) {
      logger.debug("the segment unit has been removed, abandon the task {}", segId);
      Validate.isTrue(pendingSegIds.remove(segId));
      return;
    }

    Validate.isTrue(pendingSegIds.remove(segId));
    SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
    if (!membership.isPrimary(myself) && !membership.isTempPrimary(myself)) {
      logger.debug("I {} am not primary {}, abandon the task {}", myself, membership, segId);
      return;
    }

    if (membership.isQuorumUpdated()) {
      logger.debug("quorum already updated, abandon the task {}", segId);
      return;
    }

    if (status.hasGone()) {
      logger.debug("my status is {}, abandon the task {}", status, segId);
      return;
    }

    List<EndPoint> members;
    if (context.getFailCount() < DEFAULT_FAIL_COUNT) {
      members = RequestResponseHelper.buildEndPoints(instanceStore, membership, true, myself);
    } else {
      members = RequestResponseHelper.buildEndPoints(instanceStore, membership, false,
          myself);
    }

    BroadcastRequest broadcastRequest = new BroadcastRequest(RequestIdBuilder.get(), 1L,
        segId.getVolumeId().getId(), segId.getIndex(), BroadcastTypeThrift.AddOrRemoveMember,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    try {
      int quorumSize =
          segmentUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize() - 1;
      clientWrapper
          .broadcast(broadcastRequest, DEFAULT_BROADCAST_TIME_OUT, quorumSize, true, members);
      membership.setQuorumUpdated(true);
      logger.debug("successfully broadcast to quorum {}", segId);
    } catch (QuorumNotFoundException | FailedToSendBroadcastRequestsException 
        | SnapshotVersionMissMatchForMergeLogsException e) {
      logger.debug("failed to broadcast {}", segId);
      if (pendingSegIds.add(segId)) {
        internalSubmit(segId, DEFAULT_DELAY_ON_FAIL_MS, context.getFailCount() + 1);
      }
    }
  }

  private class ExitSignal extends PushMembershipContext {
    ExitSignal() {
      super(null, 0, 0);
    }
  }

  private class PushMembershipContext implements Delayed {
    final SegId segId;
    private final int failCount;

    private long expireTime;

    PushMembershipContext(SegId segId, long delayMs, int failCount) {
      this.segId = segId;
      this.failCount = failCount;
      setDelay(delayMs);
    }

    public void setDelay(long delayMills) {
      this.expireTime = System.currentTimeMillis() + delayMills;
    }

    @Override
    public long getDelay(@Nonnull TimeUnit unit) {
      if (expireTime == 0) {
        return 0;
      }
      return unit.convert(expireTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(@Nonnull Delayed delayed) {
      if (delayed == this) {
        return 0;
      }

      long d = (getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS));
      return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
    }

    public int getFailCount() {
      return failCount;
    }
  }

}

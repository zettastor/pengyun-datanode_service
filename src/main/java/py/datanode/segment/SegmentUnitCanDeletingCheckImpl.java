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

package py.datanode.segment;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.client.thrift.GenericThriftClientFactory;
import py.common.NamedThreadFactory;
import py.datanode.exception.NoNeedToBeDeletingException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.Iface;

public class SegmentUnitCanDeletingCheckImpl implements SegmentUnitCanDeletingCheck {
  private static final Logger logger = LoggerFactory.getLogger(
      SegmentUnitCanDeletingCheckImpl.class);
  private final SegmentUnitManager segmentUnitManager;
  private final HashedWheelTimer hashedWheelTimer;
  private final ExecutorService checkingExecutor;
  private final InstanceStore instanceStore;
  private final InstanceId instanceId;
  private final GenericThriftClientFactory<Iface> dataNodeSyncClientFactory;
  private final long delayTimerWhenFail;
  private final Map<SegId, Long> deletingSegment = new ConcurrentHashMap<>();
  private final Map<SegId, Long> workingSegmentUnit = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(true);

  public SegmentUnitCanDeletingCheckImpl(SegmentUnitManager segmentUnitManager,
      InstanceStore instanceStore, InstanceId instanceId,
      GenericThriftClientFactory<Iface> dataNodeSyncClientFactory, long delayTimerWhenFail) {
    this.segmentUnitManager = segmentUnitManager;
    this.instanceStore = instanceStore;
    this.instanceId = instanceId;
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
    this.delayTimerWhenFail = delayTimerWhenFail;
    this.checkingExecutor = new ThreadPoolExecutor(
        1,
        5,
        30,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder()
            .setNameFormat("segment-unit-checking")
            .setUncaughtExceptionHandler(
                (t, e) -> logger.error("catch uncaught exception for thread:[{}]", t, e))
            .build()
    );

    this.hashedWheelTimer = new HashedWheelTimer(new NamedThreadFactory("delay-check-segment"), 1,
        TimeUnit.SECONDS);
    hashedWheelTimer.start();
  }

  @Override
  public void stop() {
    running.set(false);
    hashedWheelTimer.stop();
    checkingExecutor.shutdownNow();
  }

  @Override
  public void deleteSegmentUnitWithOutCheck(SegId segId, boolean syncPersist) throws Exception {
    if (!running.get()) {
      return;
    }
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null || segmentUnit.getSegmentUnitMetadata().getStatus().isFinalStatus()) {
      return;
    }
    logger.warn("mark {} to deleting", segId);
    Long value = deletingSegment.putIfAbsent(segId, 1L);
    if (value == null) {
      try {
        segmentUnit.getArchive().markSegmentUnitDeleting(segId, syncPersist);
      } catch (NoNeedToBeDeletingException e) {
        logger.info("no need delete");
      }
      deletingSegment.remove(segId);
    }
  }

  @Override
  public void deleteSegmentUnitWithCheck(SegId segId) {
    if (!running.get()) {
      return;
    }
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null || segmentUnit.getSegmentUnitMetadata().getStatus().isFinalStatus()) {
      return;
    }

    logger.warn("mark {} to deleting", segId);
    if (workingSegmentUnit.putIfAbsent(segId, 100L) == null) {
      checkingExecutor.submit(new CheckSegmentUnitTask(segmentUnit));
    }

  }

  @Override
  public boolean checkSegmentDeletingSync(SegId segId) {
    logger.warn("mark {} to deleting", segId);
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null) {
      return true;
    }
    if (deletingSegment.get(segId) == null && segmentUnit.getSegmentUnitMetadata().getStatus()
        .isFinalStatus()) {
      return true;
    } else {
      return false;
    }
  }

  private class CheckSegmentUnitTask implements Runnable {
    private final SegmentUnit segmentUnit;

    private CheckSegmentUnitTask(SegmentUnit segmentUnit) {
      this.segmentUnit = segmentUnit;
    }

    @Override
    public void run() {
      SegId segId = segmentUnit.getSegId();
      logger.warn("mark {} to deleting", segId);
      if (segmentUnit.getSegmentUnitMetadata().getStatus().isFinalStatus()) {
        workingSegmentUnit.remove(segId);
        return;
      }
      try {
        SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
        Instance instance = instanceStore.get(membership.getPrimary());
        DataNodeService.Iface dataNodeClient = dataNodeSyncClientFactory
            .generateSyncClient(instance.getEndPoint());

        if (dataNodeClient == null) {
          logger.error("datanodeCient is null because primary {} is not in store {}",
              membership.getPrimary(), instanceStore.getAll());
          throw new Exception();
        }
        dataNodeClient.departFromMembership(RequestResponseHelper
            .buildDepartFromMembershipRequest(segmentUnit.getSegId(), membership,
                instanceId.getId()).setSynPersist(true));
        workingSegmentUnit.remove(segId);
      } catch (Exception e) {
        logger.error("deleting segment error", e);
        hashedWheelTimer.newTimeout(new TimerTask() {
          @Override
          public void run(Timeout timeout) throws Exception {
            checkingExecutor.submit(new CheckSegmentUnitTask(segmentUnit));
          }
        }, delayTimerWhenFail, TimeUnit.MILLISECONDS);
      }

    }
  }

}

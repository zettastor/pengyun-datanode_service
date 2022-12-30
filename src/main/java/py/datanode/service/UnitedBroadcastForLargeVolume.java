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

package py.datanode.service;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.Validate;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.AbstractMethodCallback;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.AsyncDataNode.AsyncIface;
import py.netty.exception.MembershipVersionLowerException;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbCheckResponse;
import py.proto.Broadcastlog.PbMembership;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastResponse;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.share.ConnectionRefusedExceptionThrift;
import py.thrift.share.SegmentUnitBeingDeletedExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.volume.VolumeType;

public class UnitedBroadcastForLargeVolume {
  private static final Logger logger = LoggerFactory.getLogger(UnitedBroadcastForLargeVolume.class);
  private final int subMajority = 2;
  private int timeOutForMissSecondaryMs = 10000;
  private GenericAsyncClientFactory<AsyncIface> ioClientFactory;
  private DataNodeConfiguration cfg;
  private DataNodeServiceAsyncClientWrapper dataNodeAsyncClient;
  private InstanceStore instanceStore;
  private SegmentUnit segUnit;
  private SegmentMembership currentMembership;
  private InstanceId myself;

  public UnitedBroadcastForLargeVolume(
      GenericAsyncClientFactory<AsyncIface> ioClientFactory,
      DataNodeConfiguration cfg,
      DataNodeServiceAsyncClientWrapper dataNodeAsyncClient, InstanceStore instanceStore,
      SegmentUnit segUnit, SegmentMembership currentMembership, InstanceId myself) {
    this.ioClientFactory = ioClientFactory;
    this.cfg = cfg;
    this.dataNodeAsyncClient = dataNodeAsyncClient;
    this.instanceStore = instanceStore;
    this.segUnit = segUnit;
    this.currentMembership = currentMembership;
    this.myself = myself;
  }

  public Collection<BroadcastResponse> broadcastToTheMissingSecondaryUtilDone(
      List<EndPoint> broadcastEndPoints,
      BroadcastRequest broadcastRequest,
      BroadcastResult result)
      throws Exception {
    Instance missingSecondary = null;
    try {
      Collection<BroadcastResponse> goodResponses = new LinkedList<>();
      goodResponses.addAll(result.getGoodResponses().values());
      Pair<List<EndPoint>, Set<Instance>> pair = getGoodSecondaryEndPointsInNewMembership(
          instanceStore, goodResponses);
      List<EndPoint> availableSecondaryEndPoints = pair.getFirst();
      Set<Instance> availableSecondaryInstances = pair.getSecond();
      missingSecondary = findOutTheMissingSecondary(broadcastEndPoints, availableSecondaryInstances,
          result.getExceptions());

      long startTimeMillis = System.currentTimeMillis();
      while (missingSecondary != null && cfg.isNeedRetryBroadCastWhenBecomePrimary()) {
       
        if (System.currentTimeMillis() > startTimeMillis + timeOutForMissSecondaryMs) {
          logger.warn("time out , let {} gone", missingSecondary);
          throw new TimeoutException();
        }
        logger.info("{} qurom done {} miss some secondary {}", broadcastRequest.getLogType(),
            availableSecondaryEndPoints, missingSecondary);
        try {
         
          goodResponses.addAll(dataNodeAsyncClient
              .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), broadcastRequest,
                  cfg.getDataNodeRequestTimeoutMs(), 1, false,
                  Lists.newArrayList(missingSecondary.getEndPoint())).getGoodResponses().values());
          availableSecondaryEndPoints.add(missingSecondary.getEndPoint());
          missingSecondary = null;
        } catch (Exception e) {
          boolean stillReachable = askAvailableSecondariesToCheckTheSuspendedOne(
              availableSecondaryInstances, missingSecondary.getId());
          if (!stillReachable) {
            logger.warn("luckily, the suspend S died finally {}, we do not need to pull him back",
                missingSecondary);
            break;
          }
          logger.warn("{} a secondary {} still not response me, after this retry.",
              broadcastRequest.getLogType(), missingSecondary, e);
          TimeUnit.MILLISECONDS.sleep(100);
        }
      }

      return goodResponses;
    } catch (Exception e) {
      logger.warn("Caught an exception when broadcasting request {} to {}", broadcastRequest,
          missingSecondary, e);
      throw e;
    }
  }

  private Pair<List<EndPoint>, Set<Instance>> getGoodSecondaryEndPointsInNewMembership(
      InstanceStore instanceStore,
      Collection<BroadcastResponse> goodResponses) {
    List<EndPoint> endPoints = new ArrayList<>();
    Set<Instance> instances = new HashSet<>();
    for (BroadcastResponse response : goodResponses) {
      InstanceId secondaryCandidate = new InstanceId(response.getMyInstanceId());
      Instance instance = instanceStore.get(secondaryCandidate);
      if (instance != null) {
        endPoints.add(instance.getEndPoint());
        instances.add(instance);
      } else {
        logger.warn("Can't get instance {} from instance store", secondaryCandidate);
      }
    }
    return new Pair(endPoints, instances);
  }

  private Instance findOutTheMissingSecondary(List<EndPoint> broadcastEndPoints,
      Set<Instance> goodInstances, Map<EndPoint, Throwable> exceptions) throws Exception {
    VolumeType volumeType = segUnit.getSegmentUnitMetadata().getVolumeType();
   
   
    boolean justCreated = currentMembership.getSegmentVersion().getEpoch() == 0;
    boolean safeToMissTheSecondary = justCreated || (volumeType != VolumeType.LARGE);
    if (safeToMissTheSecondary) {
      return null;
    }

    Set<InstanceId> aliveSecondaries = currentMembership.getSecondaries();
    for (InstanceId secondaryId : aliveSecondaries) {
      if (secondaryId.equals(myself)) {
        continue;
      }
      Instance secondaryInstance = instanceStore.get(secondaryId);
      if (broadcastEndPoints.contains(secondaryInstance.getEndPoint()) && !goodInstances
          .contains(secondaryInstance)) {
        logger.warn(
            "current membership {} only secondaries {}" 
                + " support me {} to become primary, miss him {}",
            currentMembership, goodInstances, myself, secondaryId);
        Throwable t = exceptions.get(secondaryInstance.getEndPoint());
        if (t != null && memberHasGone(t)) {
          logger.warn("a secondary is in final status {}, safe to miss him", secondaryInstance, t);
          return null;
        }
        boolean reachable = askAvailableSecondariesToCheckTheSuspendedOne(goodInstances,
            secondaryId);
        logger.warn("after ask my supporters to check the miss secondary {} result is {}",
            secondaryInstance, reachable);
        if (reachable) {
          return secondaryInstance;
        }
      }
    }
    return null;
  }

  private boolean memberHasGone(Throwable t) {
    return t instanceof ServiceHavingBeenShutdownThrift
        || t instanceof ConnectionRefusedExceptionThrift
        || t instanceof SegmentNotFoundExceptionThrift
        || t instanceof SegmentUnitBeingDeletedExceptionThrift
        || t instanceof TTransportException;
  }

  /**
   * ask my supporters to check the missed S.
   *
   */
  private boolean askAvailableSecondariesToCheckTheSuspendedOne(Set<Instance> instances,
      InstanceId secondaryId) throws InterruptedException {
    return askAvailableSecondariesToCheckTheSuspendedOneAndCountZombie(instances, secondaryId)
        .getFirst();
  }

  private Pair<Boolean, Integer> askAvailableSecondariesToCheckTheSuspendedOneAndCountZombie(
      Set<Instance> instances, InstanceId secondaryId) throws InterruptedException {
    final SegId segId = segUnit.getSegId();

    SegmentMembership newMembership = segUnit.getSegmentUnitMetadata().getMembership();
    if (newMembership.getSegmentVersion().compareTo(currentMembership.getSegmentVersion()) > 0) {
     
      if (newMembership.isInactiveSecondary(secondaryId)) {
        return new Pair<>(false, 0);
      }
    }
    Broadcastlog.PbCheckRequest.Builder requestBuilder = Broadcastlog.PbCheckRequest.newBuilder();
    requestBuilder.setRequestId(RequestIdBuilder.get());
    requestBuilder.setCheckInstanceId(secondaryId.getId());
    requestBuilder.setRequestOption(Broadcastlog.RequestOption.SECONDARY_CHECK_SECONDARY);
    requestBuilder.setVolumeId(segId.getVolumeId().getId());
    requestBuilder.setSegIndex(segId.getIndex());
    requestBuilder.setRequestPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(
        currentMembership));

    AtomicInteger reachableCount = new AtomicInteger(0);
    AtomicInteger unreachableCount = new AtomicInteger(0);
    AtomicInteger zombieCount = new AtomicInteger(0);

    for (Instance secondaryInstance : instances) {
      EndPoint secondaryEndPoint = secondaryInstance.getEndPointByServiceName(PortType.IO);
      try {
        AsyncDataNode.AsyncIface dataNodeClient = ioClientFactory.generate(secondaryEndPoint);
        dataNodeClient.check(requestBuilder.build(), new AbstractMethodCallback<PbCheckResponse>() {
          @Override
          public void complete(Broadcastlog.PbCheckResponse object) {
            logger.debug("complete {}", object);
            if (object.getReachable()) {
              reachableCount.incrementAndGet();
            } else {
              unreachableCount.incrementAndGet();
            }
            if (object.hasZombie() && object.getZombie()) {
              zombieCount.incrementAndGet();
            }
          }

          @Override
          public void fail(Exception e) {
            logger.error("fail to ask {} check secondary {}", secondaryEndPoint, secondaryId, e);
           
            if (e instanceof MembershipVersionLowerException) {
             
              try {
                PbMembership builder = PbMembership
                    .parseFrom(((MembershipVersionLowerException) e).getMembership());
                SegmentMembership newMembership = PbRequestResponseHelper
                    .buildMembershipFrom(builder);
                logger.warn("new membership {}, current is {}", newMembership, currentMembership);
                Validate.isTrue(newMembership.getSegmentVersion()
                    .compareTo(currentMembership.getSegmentVersion()) > 0);
                if (newMembership.getInactiveSecondaries().contains(secondaryId)) {
                  unreachableCount.incrementAndGet();
                } else {
                  reachableCount.incrementAndGet();
                }
              } catch (InvalidProtocolBufferException e1) {
                reachableCount.incrementAndGet();
              }
            }

          }
        });
      } catch (Exception e) {
        logger.error("fail to ask {} check secondary {}", secondaryEndPoint, secondaryId, e);
        reachableCount.incrementAndGet();
      }
    }

    long timeout = timeOutForMissSecondaryMs;
    while (timeout > 0) {
      if (reachableCount.get() >= subMajority) {
        logger.warn("{} members say {} is reachable", reachableCount.get(), secondaryId);
        return new Pair<>(true, zombieCount.get());
      } else if (unreachableCount.get() >= subMajority) {
        logger.warn("{} members say {} is unreachable", unreachableCount.get(), secondaryId);
        return new Pair<>(false, zombieCount.get());
      } else {
        timeout -= 100;
        TimeUnit.MILLISECONDS.sleep(100);
      }
    }
    return new Pair<>(true, zombieCount.get());
  }

}

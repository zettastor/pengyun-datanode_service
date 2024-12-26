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

package py.datanode.segment.datalog.algorithm.merger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.datanode.service.UnitedBroadcastForLargeVolume;
import py.exception.FailedToSendBroadcastRequestsException;
import py.exception.QuorumNotFoundException;
import py.exception.SnapshotVersionMissMatchForMergeLogsException;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastResponse;

public class MergeLogCommunicatorImpl implements MergeLogCommunicator {
  private static final Logger logger = LoggerFactory.getLogger(MergeLogCommunicatorImpl.class);
  private final List<EndPoint> avaliblesEndPoint;
  private final EndPoint mySelfEndpoint;
  private final DataNodeServiceAsyncClientWrapper dataNodeAsyncClient;
  private final SegmentUnit segmentUnit;
  private final DataNodeConfiguration cfg;
  private final UnitedBroadcastForLargeVolume broadcastForLargeVolume;

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final InstanceStore instanceStore;

  public MergeLogCommunicatorImpl(List<EndPoint> avalibleEndPoint,
      EndPoint mySelfEndpoint, DataNodeServiceAsyncClientWrapper dataNodeAsyncClient,
      SegmentUnit segmentUnit, DataNodeConfiguration cfg, InstanceStore instanceStore,
      UnitedBroadcastForLargeVolume broadcastForLargeVolume) {
    this.avaliblesEndPoint = avalibleEndPoint;
    this.mySelfEndpoint = mySelfEndpoint;
    this.dataNodeAsyncClient = dataNodeAsyncClient;
    this.segmentUnit = segmentUnit;
    this.cfg = cfg;
    this.instanceStore = instanceStore;
    this.broadcastForLargeVolume = broadcastForLargeVolume;
  }

  @Override
  public Set<InstanceId> broadcastCommitLog(DataLog dataLog) {
    Set<InstanceId> goodResponses = new HashSet<>();
    readWriteLock.readLock().lock();
    try {
      List<EndPoint> broadcastEndpoint = new ArrayList<>();
      broadcastEndpoint.addAll(avaliblesEndPoint);
      broadcastEndpoint.add(mySelfEndpoint);
      Collection<BroadcastResponse> responses = broadcastMutationLog(RequestIdBuilder.get(),
          segmentUnit,
          segmentUnit.getSegmentUnitMetadata().getMembership(), broadcastEndpoint,
          segmentUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize(), dataLog,
          null);
      if (Objects.nonNull(responses)) {
        for (BroadcastResponse broadcastResponse : responses) {
          goodResponses.add(new InstanceId(broadcastResponse.getMyInstanceId()));
        }
      }
    } catch (SnapshotVersionMissMatchForMergeLogsException e) {
      logger.error("", e);
    }
    readWriteLock.readLock().unlock();
    return goodResponses;
  }

  @Override
  public Set<InstanceId> broadcastAbortLog(DataLog dataLog) {
    readWriteLock.readLock().lock();
    Set<InstanceId> goodResponses = new HashSet<>();
    try {
      List<EndPoint> broadcastEndpoint = new ArrayList<>();
      broadcastEndpoint.addAll(avaliblesEndPoint);
      broadcastEndpoint.add(mySelfEndpoint);
      Collection<BroadcastResponse> responses = broadcastMutationLog(RequestIdBuilder.get(),
          segmentUnit,
          segmentUnit.getSegmentUnitMetadata().getMembership(), broadcastEndpoint,
          segmentUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize(), null,
          Arrays.asList(dataLog));
      if (Objects.nonNull(responses)) {
        for (BroadcastResponse broadcastResponse : responses) {
          goodResponses.add(new InstanceId(broadcastResponse.getMyInstanceId()));
        }
      }
    } catch (SnapshotVersionMissMatchForMergeLogsException e) {
      logger.error("", e);
    }
    readWriteLock.readLock().unlock();
    return goodResponses;
  }

  @Override
  public Set<InstanceId> getOkInstance() {
    readWriteLock.readLock().lock();
    Set<InstanceId> instanceIds = new HashSet<>();
    try {
      instanceIds.add(instanceStore.get(mySelfEndpoint).getId());
      for (EndPoint endPoint : avaliblesEndPoint) {
        instanceIds.add(instanceStore.get(endPoint).getId());
      }
      return instanceIds;
    } finally {
      readWriteLock.readLock().unlock();
    }

  }

  @Override
  public void updateInstance(List<EndPoint> avalibleEndPoint) {
    readWriteLock.writeLock().lock();
    try {
      avaliblesEndPoint.clear();
      avaliblesEndPoint.addAll(avalibleEndPoint);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private Collection<BroadcastResponse> broadcastMutationLog(long requestId,
      SegmentUnit segmentUnit,
      SegmentMembership membership, List<EndPoint> endPoints, int quorumSize, DataLog dataLog,
      Collection<DataLog> unconfirmedAbortedLogs)
      throws SnapshotVersionMissMatchForMergeLogsException {
    try {
      BroadcastRequest broadcastWriteRequest;
      if (dataLog != null) {
        broadcastWriteRequest = DataNodeRequestResponseHelper
            .buildWriteDataRequest(requestId, segmentUnit.getSegId(), membership, dataLog,
                unconfirmedAbortedLogs);
      } else if (unconfirmedAbortedLogs != null && unconfirmedAbortedLogs.size() > 0) {
        broadcastWriteRequest = DataNodeRequestResponseHelper
            .buildBroadcastAbortedLogRequestByDataLog(requestId, segmentUnit.getSegId(), membership,
                unconfirmedAbortedLogs);
      } else {
        logger.warn("no logs need to be broadcast");
        return null;
      }

      broadcastWriteRequest.setMergeLogs(true);

      BroadcastResult result = dataNodeAsyncClient
          .broadcast(segmentUnit, cfg.getSecondaryLeaseInPrimaryMs(), broadcastWriteRequest,
              cfg.getDataNodeRequestTimeoutMs(), quorumSize, false, endPoints, true,
              2 * cfg.getDataNodeRequestTimeoutMs());
      logger.debug("We have received a quorum to extend my lease.");
      if (quorumSize == 2 && broadcastForLargeVolume != null) {
        return broadcastForLargeVolume
            .broadcastToTheMissingSecondaryUtilDone(avaliblesEndPoint, broadcastWriteRequest,
                result);
      } else {
        return result.getGoodResponses().values();
      }
    } catch (QuorumNotFoundException e) {
      logger.warn("Can't get a quorum from endpoints:" + endPoints, e);
      return null;
    } catch (FailedToSendBroadcastRequestsException e) {
      logger.warn("failed to send requests to majority of endpoints." + endPoints, e);
      return null;
    } catch (SnapshotVersionMissMatchForMergeLogsException e) {
      logger.error("now there is no way to here. segId:{} membership:{}", segmentUnit.getSegId(),
          membership);
      throw e;
    } catch (Exception e) {
      logger.warn("unknown exception when broadcast a log", e);
      return null;
    }
  }

}

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

package py.datanode.segment.datalog.sync.log.reduce;

import com.google.protobuf.Message;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.app.context.AppContext;
import py.archive.segment.SegId;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.SegmentLogMetadata.PeerStatus;
import py.datanode.segment.datalog.persist.LogStorageReader;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.datanode.segment.datalog.sync.log.driver.PclDriverStatus;
import py.datanode.segment.datalog.sync.log.task.SyncLogPusher;
import py.datanode.service.DataNodeServiceImpl;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.AbstractMethodCallback;
import py.netty.core.MethodCallback;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.AsyncDataNode.AsyncIface;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchResponse;
import py.proto.Broadcastlog.PbBackwardSyncLogMetadata;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.proto.Broadcastlog.PbErrorCode;

public class SyncLogReduceCollectorImpl<U extends Message, M extends Message, R extends Message> 
    implements SyncLogReduceCollector<U, M, R> {
  private static final Logger logger = LoggerFactory.getLogger(SyncLogReduceCollectorImpl.class);
  private final int maxReduceBuilderWaitTimer;
  private final int maxPackgetLength;
  private final AppContext context;
  private final InstanceStore instanceStore;
  private final LogStorageReader logStorageReader;
  private final SyncLogTaskExecutor syncLogTaskExecutor;
  private final SegmentUnitManager segmentUnitManager;
  private final DataNodeConfiguration configuration;
  private final Map<InstanceId, AsyncDataNode.AsyncIface> instanceIdAsyncIfaceMap =
      new ConcurrentHashMap<>();
  private final Timer reduceBuilderTimer;
  private final SyncLogMessageType messageType;
  private final AbstractPbSyncLogReduceBuilderFactory<U, M> builderFactory;
  private final Map<InstanceId, PbSyncLogReduceBuilder<U, M>> reduceBuilderMap = 
      new ConcurrentHashMap<>();
  private final Map<Long, PbSyncLogReduceBuilder<U, M>> requestRegistion = 
      new ConcurrentHashMap<>();

  private GenericAsyncClientFactory<AsyncIface> clientFactory;
  private DataNodeServiceImpl dataNodeService;

  public SyncLogReduceCollectorImpl(int maxPackgetLength, AppContext context,
      InstanceStore instanceStore,
      LogStorageReader logStorageReader,
      SyncLogTaskExecutor syncLogTaskExecutor,
      SegmentUnitManager segmentUnitManager,
      DataNodeConfiguration configuration,
      SyncLogMessageType messageType,
      Timer reduceBuilderTimer,
      AbstractPbSyncLogReduceBuilderFactory<U, M> builderFactory) {
    this.maxPackgetLength = maxPackgetLength;
    if (messageType.isWaitingResponseType()) {
      this.maxReduceBuilderWaitTimer = configuration.getSyncLogPackageResponseWaitTimeMs();
    } else {
      this.maxReduceBuilderWaitTimer = configuration.getSyncLogPackageWaitTimeMs();
    }
    this.context = context;
    this.instanceStore = instanceStore;
    this.logStorageReader = logStorageReader;
    this.syncLogTaskExecutor = syncLogTaskExecutor;
    this.segmentUnitManager = segmentUnitManager;
    this.configuration = configuration;
    this.messageType = messageType;
    this.builderFactory = builderFactory;

    this.reduceBuilderTimer = reduceBuilderTimer;
  }

  @Override
  public void setDataNodeAsyncClientFactory(
      GenericAsyncClientFactory<AsyncIface> clientFactory) {
    this.clientFactory = clientFactory;
  }

  @Override
  public void setDataNodeService(DataNodeServiceImpl dataNodeService) {
    this.dataNodeService = dataNodeService;
  }

  @Override
  public boolean submit(InstanceId destination, U messageUnit) {
    logger
        .debug("submit a message unit {} into sync log task executor, destination {}", messageUnit,
            destination);

    if (null == instanceStore.get(destination)) {
      logger.error("a sync log unit destination {} not exist in instance store", destination);
      return false;
    }

    Set<PbSyncLogReduceBuilder<U, M>> builderSet = getReduceBuilder(destination, messageUnit);
    for (PbSyncLogReduceBuilder<U, M> reduceBuilder : builderSet) {
      Timeout timeout = reduceBuilderTimer.newTimeout(v -> {
        packageAndSendToPeer(reduceBuilder, true);
      }, maxReduceBuilderWaitTimer, TimeUnit.MILLISECONDS);

      if (!reduceBuilder.setTimeout(timeout)) {
        timeout.cancel();
      }
    }

    return true;
  }

  /**
   * collect all response unit for the request which has register to this collector at before.
   *
   */
  public boolean collect(long requestId, U messageUnit) {
    PbSyncLogReduceBuilder<U, M> completedBuilder = null;
    synchronized (requestRegistion) {
      PbSyncLogReduceBuilder<U, M> builder = requestRegistion.get(requestId);
      if (null == builder) {
        logger
            .warn(
                "can not found any request register {} in collector. discard this message unit {}",
                requestId, messageUnit);
        return false;
      }

      if (!builder.submitUnit(messageUnit)) {
        logger
            .info(
                "reduce builder {} has fill up, can not submit any response unit. " 
                    + "discard this message unit {}",
                requestId, messageUnit);
        return false;
      }

      if (builder.overflow()) {
        completedBuilder = requestRegistion.remove(requestId);
      }
    }
    if (completedBuilder != null) {
      packageAndSendToPeer(completedBuilder, false);
    }
    return true;
  }

  @Override
  public void register(InstanceId destination, R request, MethodCallback<M> callback) {
    switch (messageType) {
      case SYNC_LOG_MESSAGE_TYPE_BACKWARD_RESPONSE:
        PbBackwardSyncLogsRequest pbBackwardAsyncLogsRequest = (PbBackwardSyncLogsRequest) request;
        Validate.notNull(instanceStore.get(destination));
        Validate.notNull(instanceStore.get(destination).getEndPoint());

        PbSyncLogReduceBuilder<U, M> builder = builderFactory
            .generate(pbBackwardAsyncLogsRequest.getRequestId(),
                pbBackwardAsyncLogsRequest.getUnitsCount(), destination, callback);
        logger.debug("a new reduce builder has been generate {}", builder);
        PbSyncLogReduceBuilder<U, M> old = requestRegistion
            .put(pbBackwardAsyncLogsRequest.getRequestId(), builder);
        if (null != old) {
          logger.warn("found a old reduce builder by request id {} {}, the new request {}",
              pbBackwardAsyncLogsRequest.getRequestId(), old, builder);
        }
        if (builder == null) {
          logger.warn("register a invalid request {}", request);
          return;
        }

        long requestId = pbBackwardAsyncLogsRequest.getRequestId();
        Timeout timeout = reduceBuilderTimer.newTimeout(v -> {
          requestRegistion.remove(requestId);
          packageAndSendToPeer(builder, true);
        }, maxReduceBuilderWaitTimer, TimeUnit.MILLISECONDS);

        if (!builder.setTimeout(timeout)) {
          timeout.cancel();
        }
        break;
      default:
        Validate.isTrue(false);
    }
  }

  private AsyncDataNode.AsyncIface getClient(InstanceId instanceId) {
    Validate.notNull(clientFactory);

    Validate.notNull(instanceStore.get(instanceId));
    Validate.notNull(instanceStore.get(instanceId).getEndPoint());
    AsyncIface asyncIface = clientFactory
        .generate(instanceStore.get(instanceId).getEndPointByServiceName(
            PortType.IO));
    Validate.notNull(asyncIface);
    return asyncIface;
  }

  private Set<PbSyncLogReduceBuilder<U, M>> getReduceBuilder(InstanceId destination, U unit) {
    Set<PbSyncLogReduceBuilder<U, M>> builderSet = new LinkedHashSet<>();
    Set<PbSyncLogReduceBuilder<U, M>> fillupBuilderSet = new LinkedHashSet<>();
    PbSyncLogReduceBuilder<U, M> reduceBuilder = reduceBuilderMap
        .computeIfAbsent(destination, v -> {
          PbSyncLogReduceBuilder<U, M> temp = builderFactory
              .generate(destination, maxPackgetLength);
          logger.debug("a new reduce builder has been generate {}", temp);
          builderSet.add(temp);
          return temp;
        });

    int times = 3;
    while (!reduceBuilder.submitUnit(unit)) {
      if (times-- <= 0 || unit.getSerializedSize() >= maxPackgetLength) {
        reduceBuilder = builderFactory.generate(destination, maxPackgetLength);
        reduceBuilder.submitUnit(unit, true);
        logger.debug("a new reduce builder has been generate {}", reduceBuilder);
        fillupBuilderSet.add(reduceBuilder);
        break;
      }
      synchronized (reduceBuilderMap) {
        if (!reduceBuilder.submitUnit(unit)) {
          fillupBuilderSet.add(reduceBuilder);
          reduceBuilder = builderFactory.generate(destination, maxPackgetLength);
          logger.debug("a new reduce builder has been generate {}", reduceBuilder);
          builderSet.add(reduceBuilder);
          reduceBuilderMap.put(destination, reduceBuilder);
        } else {
          break;
        }
      }
    }

    for (PbSyncLogReduceBuilder builder : fillupBuilderSet) {
      packageAndSendToPeer(builder, false);
    }

    return builderSet;
  }

  /**
   * build a protocol buffer message and send it to peer.
   *
   */
  private void packageAndSendToPeer(PbSyncLogReduceBuilder<U, M> reduceBuilder,
      boolean isTimeoutCall) {
    logger.debug("begin to package and send to peer {}", reduceBuilder);
    if (reduceBuilder.getTimeout() != null) {
      reduceBuilder.getTimeout().cancel();
    }

    if (clientFactory == null) {
      reduceBuilderTimer.newTimeout(v -> {
        packageAndSendToPeer(reduceBuilder, true);
      }, 1000, TimeUnit.MILLISECONDS);

      logger.warn("a reduce builder {} will package and send, but async face has not been ready, "
          + "retry it after 1 second ", reduceBuilder);
      return;
    }
    if (reduceBuilder.hasDone()) {
      logger.info("got a candidate reduce builder {} has done", reduceBuilder);
      return;
    }
    reduceBuilder.setHasDone();

    M message;
    if (isTimeoutCall) {
      message = reduceBuilder.expiredBuild();
    } else {
      message = reduceBuilder.build();
    }
    logger.debug("reduce builder message length {}", message.getSerializedSize());

    if (messageType == SyncLogMessageType.SYNC_LOG_MESSAGE_TYPE_BACKWARD_REQUEST) {
      PbBackwardSyncLogsRequest request = (PbBackwardSyncLogsRequest) message;
      if (request.getUnitsCount() <= 0) {
        logger
            .warn("found a backward sync log request, but has not any units. request {}", request);
        return;
      }
      AsyncDataNode.AsyncIface asyncIface = getClient(reduceBuilder.getDestination());

      final SyncLogReduceCollector syncLogReduceCollector = this;
      final InstanceId destinationId = reduceBuilder.getDestination();
      final int unitCount = request.getUnitsCount();
      MethodCallback<PbBackwardSyncLogsResponse> callback = 
              new AbstractMethodCallback<PbBackwardSyncLogsResponse>() {
            @Override
            public void complete(PbBackwardSyncLogsResponse response) {
              logger.debug("got a response for backward sync log {}", response);
              if (response.getUnitsCount() != unitCount) {
                logger.warn(
                    "backward sync log request units {} not equal response units {}, " 
                        + "may be some request unit has been failed or timeout",
                    unitCount, response.getUnitsCount());
              }
    
              response.getUnitsList().stream().forEach(unit -> {
                if (unit.hasCode() && !unit.getCode().equals(PbErrorCode.Ok)) {
                  return;
                }
                SegmentUnit segmentUnit = segmentUnitManager
                    .get(new SegId(unit.getVolumeId(), unit.getSegIndex()));
                if (segmentUnit != null) {
                  PeerStatus peerStatus = PeerStatus.getPeerStatusInMembership(
                      segmentUnit.getSegmentUnitMetadata().getMembership(), destinationId);
    
                  long newSwclId = segmentUnit.getSegmentLogMetadata()
                      .peerUpdateClId(unit.getPcl(), destinationId,
                          configuration.getThresholdToClearSecondaryClMs(), peerStatus);
                  segmentUnit.getSegmentLogMetadata().setSwclIdTo(newSwclId);
    
                  long newSwplId = segmentUnit.getSegmentLogMetadata()
                      .peerUpdatePlId(unit.getPpl(), destinationId,
                          configuration.getThresholdToClearSecondaryPlMs(), peerStatus);
                  segmentUnit.getSegmentLogMetadata().setSwplIdTo(newSwplId);
                  logger
                      .debug("update segment unit pswpl to {} and pswcl to {}",
                          newSwplId, newSwclId);
    
                  if (unit.getMissDataLogsOrBuilderList() != null && !unit
                      .getMissDataLogsOrBuilderList().isEmpty()) {
                    PbAsyncSyncLogBatchUnit.Builder syncLogBatchUnitBuilder = 
                        PbAsyncSyncLogBatchUnit
                        .newBuilder();
                    syncLogBatchUnitBuilder.setVolumeId(unit.getVolumeId());
                    syncLogBatchUnitBuilder.setSegIndex(unit.getSegIndex());
                    syncLogBatchUnitBuilder.setPpl(unit.getPpl());
                    syncLogBatchUnitBuilder.setPcl(unit.getPcl());
                    syncLogBatchUnitBuilder.setMyself(destinationId.getId());
                    syncLogBatchUnitBuilder.setSegmentStatus(
                        segmentUnit.getSegmentUnitMetadata().getStatus().getPbSegmentUnitStatus());
                    if (unit.hasCatchUpLogId()) {
                      syncLogBatchUnitBuilder.setCatchUpLogId(unit.getCatchUpLogId());
                    }

                    syncLogBatchUnitBuilder.setMembership(PbRequestResponseHelper
                        .buildPbMembershipFrom(segmentUnit.getSegmentUnitMetadata()
                            .getMembership()));
    
                    SyncLogPusher syncLogTask = new SyncLogPusher(segmentUnitManager,
                        syncLogBatchUnitBuilder.build(), context, dataNodeService,
                        syncLogReduceCollector, logStorageReader, configuration);
                    Set<Long> missLogs = new TreeSet<>();
                    for (PbBackwardSyncLogMetadata logMetadata : unit.getMissDataLogsList()) {
                      missLogs.add(logMetadata.getLogId());
                    }
                    syncLogTask.setMissingLogDataAtSecondary(missLogs);
    
                    logger.debug("task {}", syncLogTask);
                    syncLogTaskExecutor.submit(syncLogTask);
                  }
                } else {
                  logger.error(
                      "can not found segment unit for sync log response volumeId: {}, Index: {}}",
                      unit.getVolumeId(), unit.getSegIndex());
                }
    
              });
            }
    
            @Override
            public void fail(Exception e) {
              logger.error("backward sync log failed", e);
            }
          };

      logger.debug("begin send a backward sync log request id {}",
          ((PbBackwardSyncLogsRequest) message).getRequestId());
      asyncIface.backwardSyncLog((PbBackwardSyncLogsRequest) message, callback);
    } else if (messageType == SyncLogMessageType.SYNC_LOG_MESSAGE_TYPE_BACKWARD_RESPONSE) {
      BackwardSyncLogResponseReduceBuilder responseReduceBuilder = 
          (BackwardSyncLogResponseReduceBuilder) reduceBuilder;
      Validate.notNull(responseReduceBuilder.getCallback());
      logger.debug("call back backward sync log response {}", message);
      responseReduceBuilder.getCallback().complete(message);
    } else if (messageType == SyncLogMessageType.SYNC_LOG_MESSAGE_TYPE_SYNC_LOG_BATCH_REQUEST) {
      Validate.isTrue(message instanceof PbAsyncSyncLogsBatchRequest);
      AsyncDataNode.AsyncIface asyncIface = getClient(reduceBuilder.getDestination());

      MethodCallback<PbAsyncSyncLogsBatchResponse> callback =
          new AbstractMethodCallback<PbAsyncSyncLogsBatchResponse>() {
            @Override
            public void complete(PbAsyncSyncLogsBatchResponse response) {
              logger.info("got a PBASyncLogsBatchResponse ");
            }

            @Override
            public void fail(Exception e) {
              ((PbAsyncSyncLogsBatchRequest) message).getSegmentUnitsList().stream().forEach(
                  unit -> {
                    SegmentUnit segmentUnit = segmentUnitManager
                        .get(new SegId(unit.getVolumeId(), unit.getSegIndex()));
                    Validate.notNull(segmentUnit);
                    if (segmentUnit.getSecondaryPclDriver()
                        .setStatusWithCheck(PclDriverStatus.Processing, PclDriverStatus.Free)) {
                      segmentUnit.getSecondaryPclDriver().updateLease();
                    }
                  });
              logger
                  .error("sync log request failed, destination {}",
                      reduceBuilder.getDestination(), e);
            }
          };

      logger.debug("begin send a backward sync log request id {}",
          ((PbAsyncSyncLogsBatchRequest) message).getRequestId());
      asyncIface.syncLog((PbAsyncSyncLogsBatchRequest) message, callback);
    } else {
      logger.error("got a invalid message type in sync log {}", message);
    }
  }

  @Override
  public String toString() {
    return "SyncLogReduceCollectorImpl{"
        + "maxReduceBuilderWaitTimer=" + maxReduceBuilderWaitTimer
        + ", maxPackgetLength=" + maxPackgetLength
        + ", clientFactory=" + clientFactory
        + ", instanceIdAsyncIfaceMap=" + instanceIdAsyncIfaceMap
        + ", messageType=" + messageType
        + ", reduceBuilderMap=" + reduceBuilderMap
        + ", requestRegistion=" + requestRegistion
        + '}';
  }
}

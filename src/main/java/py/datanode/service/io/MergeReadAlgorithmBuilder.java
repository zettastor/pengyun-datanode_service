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

package py.datanode.service.io;

import com.google.protobuf.ByteStringHelper;
import io.netty.buffer.ByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.datanode.service.io.read.merge.LocalLogs;
import py.datanode.service.io.read.merge.MergeReadAlgorithm;
import py.datanode.service.io.read.merge.MergeReadCommunicator;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.AbstractMethodCallback;
import py.netty.datanode.AsyncDataNode.AsyncIface;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Commitlog.PbCommitlogRequest;
import py.proto.Commitlog.PbCommitlogRequest.Builder;
import py.proto.Commitlog.PbCommitlogResponse;
import py.proto.Commitlog.RequestType;

public class MergeReadAlgorithmBuilder {
  private static final Logger logger = LoggerFactory.getLogger(MergeReadAlgorithm.class);

  public static MergeReadAlgorithm build(InstanceStore instanceStore, InstanceId myself,
      SegmentUnit segmentUnit, SegmentMembership membership, int pageSize,
      PbReadRequest request, GenericAsyncClientFactory<AsyncIface> clientFactory,
      ByteBufAllocator allocator) {
    boolean stablePrimary =
        segmentUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Primary;
    MergeReadAlgorithm algorithm = new MergeReadAlgorithm(
        pageSize, myself, membership,
        new MergeReadCommunicator() {
          @Override
          public CompletableFuture<PyReadResponse> read(InstanceId target,
              PbReadRequest request) {
            CompletableFuture<PyReadResponse> future = new CompletableFuture<>();
            try {
              Instance instance = instanceStore.get(target);
              AsyncIface datanode =
                  clientFactory
                      .generate(instance.getEndPointByServiceName(PortType.IO));
              datanode.read(request, new AbstractMethodCallback<PyReadResponse>() {
                @Override
                public void complete(PyReadResponse object) {
                  future.complete(object);
                }

                @Override
                public void fail(Exception e) {
                  future.completeExceptionally(e);
                }
              });
            } catch (Throwable throwable) {
              logger.error("caught an exception", throwable);
              future.completeExceptionally(throwable);
            }
            return future;
          }

          @Override
          public CompletableFuture<Void> addOrUpdateLogs(InstanceId target,
              Iterable<DataLog> logs) {
            Builder builder = PbCommitlogRequest.newBuilder();
            builder.setRequestId(RequestIdBuilder.get());
            builder.setVolumeId(request.getVolumeId());
            builder.setSegIndex(request.getSegIndex());
            builder.setType(RequestType.ADD);
            builder.setMembership(request.getMembership());
            PbBroadcastLogManager.Builder logManagerBuilder = PbBroadcastLogManager.newBuilder();
            logManagerBuilder.setRequestId(1L);
            for (DataLog log : logs) {
              logManagerBuilder.addBroadcastLogs(buildPbLogFrom(log));
            }
            builder.addBroadcastManagers(logManagerBuilder);
            PbCommitlogRequest addOrUpdateLogsRequest = builder.build();
            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
              Instance instance = instanceStore.get(target);
              AsyncIface datanode =
                  clientFactory
                      .generate(instance.getEndPointByServiceName(PortType.IO));
              datanode.addOrCommitLogs(addOrUpdateLogsRequest,
                  new AbstractMethodCallback<PbCommitlogResponse>() {
                    @Override
                    public void complete(PbCommitlogResponse object) {
                      future.complete(null);
                    }

                    @Override
                    public void fail(Exception e) {
                      future.completeExceptionally(e);
                    }
                  });
            } catch (Throwable throwable) {
              logger.error("caught an exception", throwable);
              future.completeExceptionally(throwable);
            }
            return future;
          }
        }, new LocalLogs() {
            @Override
            public boolean isLogCommitted(long logId) {
              MutationLogEntry log = segmentUnit.getSegmentLogMetadata().getLog(logId);
              if (log == null) {
                return false;
              }
                return log.isCommitted();
            }
        }, request, allocator, stablePrimary);
    return algorithm;
  }

  private static PbBroadcastLog.Builder buildPbLogFrom(DataLog log) {
    PbBroadcastLog.Builder logBuilder = PbBroadcastLog.newBuilder();
    logBuilder.setLogId(log.getLogId());
    logBuilder.setLogUuid(log.getLogUuid());
    logBuilder.setOffset(log.getOffset());
    logBuilder.setLength(log.getLength());
    logBuilder.setLogStatus(
        log.getLogStatus() == LogStatus.Committed ? PbBroadcastLogStatus.COMMITTED
            : PbBroadcastLogStatus.CREATED);
    if (!log.isApplied()) {
      ByteBuffer buffer = ByteBuffer.allocate(log.getLength());
      log.getData(buffer, 0, log.getLength());
      logBuilder.setData(ByteStringHelper.wrap(buffer.array()));
      logBuilder.setChecksum(0);
    } else {
      logBuilder.setChecksum(0);
    }
    return logBuilder;
  }

}

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

package py.datanode.service.io.read.merge;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.exception.DestinationNotInSamePageException;
import py.datanode.exception.LogsNotInRightOrderException;
import py.datanode.exception.LogsNotInSamePageException;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.algorithm.AppliedLog;
import py.datanode.segment.datalog.algorithm.CommittedYetAppliedLog;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.datanode.segment.datalog.algorithm.DataLogsApplierWithByteBuf;
import py.datanode.segment.datalog.algorithm.ImmutableStatusDataLog;
import py.datanode.segment.datalog.algorithm.merger.DataLogsMerger;
import py.datanode.segment.datalog.algorithm.merger.MergeLogIterator;
import py.datanode.segment.datalog.algorithm.merger.TempPrimaryDataLogsMerger;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.proto.Broadcastlog.ReadCause;

public class MergeReadAlgorithm {

  private static final Logger logger = LoggerFactory.getLogger(MergeReadAlgorithm.class);

  private final int pageSize;
  private final InstanceId myself;
  private final SegmentMembership membership;
  private final MergeReadCommunicator communicator;
  private final LocalLogs localLogs;
  private final PbReadRequest readRequest;
  private final ByteBufAllocator allocator;
  private final SegId segId;
  private final boolean stablePrimary;

  private LongPredicate pageClonedPredicate;

  public MergeReadAlgorithm(int pageSize, InstanceId myself,
      SegmentMembership membership,
      MergeReadCommunicator communicator, LocalLogs localLogs,
      PbReadRequest readRequest, ByteBufAllocator allocator, boolean stablePrimary) {
    this.pageSize = pageSize;
    this.myself = myself;
    this.membership = membership;
    this.communicator = communicator;
    this.localLogs = localLogs;
    this.readRequest = readRequest.toBuilder().setReadCause(ReadCause.MERGE).build();
    this.allocator = allocator;
    this.stablePrimary = stablePrimary;
    this.segId = new SegId(readRequest.getVolumeId(), readRequest.getSegIndex());
  }

  public CompletableFuture<PyReadResponse> process() {
    if (membership.isPrimary(myself)) {
      if (stablePrimary) {
        logger.debug("primary merge read processing {}, request id {}, membership {}", segId,
            readRequest.getRequestId(), membership);
        return readFromMembers(MergeReadType.StablePrimary, membership.getWriteSecondaries());
      } else {
        logger.debug("unstable primary merge read processing {}, request id {}, membership {}",
            segId, readRequest.getRequestId(), membership);
        Set<InstanceId> membersToRead = new HashSet<>(membership.getWriteSecondaries());
        membersToRead.add(membership.getPrimary());
        return readFromMembers(MergeReadType.UnstablePrimary, membersToRead);
      }
    } else if (membership.isTempPrimary(myself)) {
      logger.debug("temp primary merge read processing {}, request id {}, membership {}", segId,
          readRequest.getRequestId(), membership);
      Validate.isTrue(!stablePrimary, "temp primary is not stable primary");
      Set<InstanceId> secondariesToRead = membership.getWriteSecondaries();
      return readFromMembers(MergeReadType.UnstablePrimary, secondariesToRead);
    } else {
      throw new IllegalArgumentException("i am not primary or temp primary, can't continue");
    }
  }

  public void setPageClonedPredicate(LongPredicate pageClonedPredicate) {
    this.pageClonedPredicate = pageClonedPredicate;
  }

  private CompletableFuture<PyReadResponse> primaryProcess(
      Map<InstanceId, PyReadResponse> responses) {
    try {
      logger.debug("primary sync process start");
      PyReadResponse readResponse = primarySyncProcess(responses);
      logger.debug("primary sync process done");
      return CompletableFuture.completedFuture(readResponse);
    } catch (MergeFailedException e) {
      logger.warn("merge failed", e);
      CompletableFuture<PyReadResponse> future = new CompletableFuture<>();
      future.completeExceptionally(e);
      return future;
    } catch (Throwable t) {
      logger.error("unexpected error", t);
      CompletableFuture<PyReadResponse> future = new CompletableFuture<>();
      future.completeExceptionally(t);
      return future;
    }
  }

  private List<DataLog> mergeLogs(
      Map<InstanceId, PyReadResponse> responses) throws MergeFailedException {
    if (!enoughResponse(MergeReadType.UnstablePrimary, responses)) {
      logger.warn("merge failed, not enough responses {}, membership {}", responses.keySet(),
          membership);
      throw new MergeFailedException("not enough responses");
    }

    List<DataLog> finalLogs = new ArrayList<>();
    {
      PyReadResponse firstResponse = responses.values().iterator().next();
      DataLogsMerger merger = new TempPrimaryDataLogsMerger();
      for (int index = 0; index < firstResponse.getResponseUnitsCount(); index++) {
        for (Entry<InstanceId, PyReadResponse> secondaryResponse : responses.entrySet()) {
          InstanceId secondary = secondaryResponse.getKey();
          PyReadResponse response = secondaryResponse.getValue();
          PbReadResponseUnit unit = response.getMetadata().getResponseUnits(index);
          for (PbBroadcastLog log : unit.getLogsToMergeList()) {
            final int finalIndex = index;
            logger.debug("adding log from {} : log id {}, offset {}, status {}, with data {}",
                secondary, log.getLogId(), log.getOffset(), log.getLogStatus(), log.hasData());
            switch (log.getLogStatus()) {
              case CREATED:
                merger.addLog(secondary,
                    new ImmutableStatusDataLog(log.getLogId(), log.getOffset(), log.getLength(),
                        log.getLogUuid(), LogStatus.Created, false,
                        false) {
                      @Override
                      public void getData(ByteBuffer destination, int offset, int length) {
                        ByteBuffer src = log.getData().asReadOnlyByteBuffer();
                        src.position(src.position() + offset);
                        src.limit(src.position() + length);
                        destination.put(src);
                      }

                      @Override
                      public void getData(ByteBuf destination, int offset, int length) {
                        throw new UnsupportedOperationException();
                      }
                    });
                break;
              case COMMITTED:
                merger.addLog(secondary,
                    new CommittedYetAppliedLog(log.getLogId(), log.getOffset(), log.getLength(),
                        log.getLogUuid()) {
                      @Override
                      public void getData(ByteBuffer destination, int offset, int length) {
                        ByteBuf src = response.getResponseUnitDataWithoutRetain(finalIndex);
                        int logOffsetInUnit = (int) (log.getOffset() - response.getMetadata()
                            .getResponseUnits(finalIndex).getOffset());
                        src.readerIndex(src.readerIndex() + logOffsetInUnit + offset);
                        int oldLimit = destination.limit();
                        destination.limit(destination.position() + length);
                        src.readBytes(destination);
                        destination.limit(oldLimit);
                      }

                      @Override
                      public void getData(ByteBuf destination, int offset, int length) {
                        throw new UnsupportedOperationException();
                      }
                    });
                break;
              case ABORT:
              case ABORT_CONFIRMED:
                logger.info("an abort log! ignoring it {} {} {} {}", log.getLogId(),
                    log.getOffset(), log.getLength(), log.getLogStatus());
                break;
              default:
                throw new IllegalArgumentException(String
                    .format("unsupported log status: %s, id %s, offset %s, length %s",
                        log.getLogStatus(), log.getLogId(), log.getOffset(), log.getLength()));
            }
          }
        }
      }

      MergeLogIterator it = merger.iterator(Long.MAX_VALUE);
      while (it.hasNext()) {
        DataLog log = null;
        try {
          log = it.next().getFirst();
        } catch (MergeFailedException e) {
          logger.error("merge fail");
        }
        finalLogs.add(log);
      }

    }

    return finalLogs;
  }

  private CompletableFuture<PyReadResponse> tempPrimaryProcess(
      Map<InstanceId, PyReadResponse> responses) {
    CompletableFuture<Void> commitFuture = CompletableFuture.completedFuture(null);

    try {
      List<DataLog> finalLogs;
      try {
        finalLogs = mergeLogs(responses);
      } catch (MergeFailedException e) {
        logger.warn("merge failed", e);
        CompletableFuture<PyReadResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(e);
        return failed;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("merge log result dumping {} {} {}", segId, membership,
            readRequest.getRequestId());
        StringBuilder sb = new StringBuilder("logs : ");
        for (DataLog finalLog : finalLogs) {
          sb.append(String
              .format(" [logId=%s, offset=%s, committed=%s, applied=%s] ", finalLog.getLogId(),
                  finalLog.getOffset(), finalLog.getLogStatus(), finalLog.isApplied()));
        }
        logger.debug(sb.toString());
      }

      for (InstanceId secondary : responses.keySet()) {
        CompletableFuture<Void> f = communicator.addOrUpdateLogs(secondary, finalLogs);
        commitFuture = CompletableFuture.allOf(commitFuture, f);
      }
    } catch (Throwable t) {
      logger.error("an unexpected error !!", t);
      CompletableFuture<Void> errorFuture = new CompletableFuture<>();
      errorFuture.completeExceptionally(t);
      commitFuture = CompletableFuture.allOf(commitFuture, errorFuture);
    }

    CompletableFuture<PyReadResponse> future = new CompletableFuture<>();
    commitFuture.thenRun(() -> {
      logger
          .warn("reading from myslef {} {}", readRequest.getSegIndex(),
              readRequest.getRequestId());
      communicator.read(myself, readRequest)
          .thenAccept(future::complete)
          .exceptionally(throwable -> {
            logger.warn("caught a throwable", throwable);
            future.completeExceptionally(throwable);
            return null;
          });
    }).exceptionally(throwable -> {
      logger.warn("caught a throwable", throwable);
      future.completeExceptionally(throwable);
      return null;
    });

    return future;

  }

  private PyReadResponse selectBaseResponse(Collection<PyReadResponse> responses)
      throws MergeFailedException {
    PyReadResponse result = null;
    long maxPcl = -1;
    for (PyReadResponse response : responses) {
      if (response.getMetadata().getPclId() > maxPcl) {
        maxPcl = response.getMetadata().getPclId();
        result = response;
      }
    }
    if (maxPcl < 0) {
      throw new MergeFailedException("no response with a valid pcl");
    }
    return result;
  }

  private PyReadResponse primarySyncProcess(Map<InstanceId, PyReadResponse> responses)
      throws MergeFailedException {
    if (!enoughResponse(MergeReadType.StablePrimary, responses)) {
      throw new MergeFailedException();
    }

    PyReadResponse returnResponse = null;
    ByteBuf[] data = new ByteBuf[responses.values().iterator().next().getResponseUnitsCount()];

    try {
      PyReadResponse baseResponse = selectBaseResponse(responses.values());

      List<Entry<InstanceId, PyReadResponse>> entryList = new ArrayList<>(responses.size());
      entryList.addAll(responses.entrySet());

      entryList.sort(Comparator.comparingLong(o -> -o.getValue().getMetadata().getPclId()));
      Validate.isTrue(baseResponse == entryList.get(0).getValue());

      for (int index = 0; index < baseResponse.getResponseUnitsCount(); index++) {
        int finalIndex = index;
        boolean isUnitFree = true;
        PbReadResponseUnit returnResponseUnit = baseResponse.getMetadata().getResponseUnits(index);

        DataLogsApplierWithByteBuf applier = new DataLogsApplierWithByteBuf(
            returnResponseUnit.getOffset(), returnResponseUnit.getLength(), pageSize);
        applier.setPageClonedPredicate(pageClonedPredicate);
        for (Entry<InstanceId, PyReadResponse> secondaryResponse : entryList) {
          InstanceId secondary = secondaryResponse.getKey();
          PyReadResponse response = secondaryResponse.getValue();
          PbReadResponseUnit unit = response.getMetadata().getResponseUnits(index);
          boolean baseUnit = false;
          if (isUnitFree) {
            if (unit.getResult() == PbIoUnitResult.OK) {
              logger.debug("using {}'s response as base unit", secondary);
              baseUnit = true;
              isUnitFree = false;
              data[index] = allocator.buffer(unit.getLength());
              data[index].markWriterIndex();
              data[index].writeBytes(response.getResponseUnitDataWithoutRetain(index));
              data[index].resetWriterIndex();
              applier.setDestination(data[index]);
            }
          }

          for (PbBroadcastLog log : unit.getLogsToMergeList()) {
            if (applier.containsLog(log.getLogId())) {
              continue;
            }
            if (log.getLogStatus() == PbBroadcastLogStatus.CREATED) {
              if (log.getData() == null) {
                throw new IllegalArgumentException("a created log's data is null !!");
              }
              if (localLogs.isLogCommitted(log.getLogId())) {
                if (isUnitFree) {
                  logger.debug("using {}'s response as base unit, no page data, only logs",
                      secondary);
                  Validate.isTrue(unit.getResult() != PbIoUnitResult.OK);
                  isUnitFree = false;
                  data[index] = allocator.buffer(unit.getLength());
                  applier.setDestination(data[index]);
                }
                logger.debug("{} found a created log committed in primary, apply it {}", secondary,
                    log.getLogId());

                DataLog logToApply = new CommittedYetAppliedLog(log.getLogId(), log.getOffset(),
                    log.getLength(),
                    log.getLogUuid()) {
                  @Override
                  public void getData(ByteBuffer destination, int offset, int length) {
                    throw new UnsupportedOperationException();
                  }

                  @Override
                  public void getData(ByteBuf destination, int offset, int length) {
                    ByteBuffer src = log.getData().asReadOnlyByteBuffer();
                    src.position(src.position() + offset);
                    src.limit(src.position() + length);
                    destination.writeBytes(src);
                  }
                };

                applier.addLog(logToApply);
              }
            } else if (log.getLogStatus() == PbBroadcastLogStatus.COMMITTED) {
              Validate.isTrue(unit.getResult() == PbIoUnitResult.OK);
              Validate.isTrue(!isUnitFree);

              DataLog logToApply;
              if (baseUnit) {
                logger.debug("{} a committed log {}, data already in return buffer", secondary,
                    log.getLogId());
                logToApply = new AppliedLog(log.getLogId(), log.getOffset(), log.getLength(),
                    log.getLogUuid());
              } else {
                logger.debug("{} a committed log {}, data may not in return buffer", secondary,
                    log.getLogId());
                logToApply = new CommittedYetAppliedLog(log.getLogId(), log.getOffset(),
                    log.getLength(), log.getLogUuid()) {
                  @Override
                  public void getData(ByteBuffer destination, int offset, int length) {
                    throw new UnsupportedOperationException();
                  }

                  @Override
                  public void getData(ByteBuf destination, int offsetInLog, int length) {
                    ByteBuf src = response.getResponseUnitDataWithoutRetain(finalIndex);
                    int logOffsetInUnit = (int) (log.getOffset() - response.getMetadata()
                        .getResponseUnits(finalIndex).getOffset());
                    src.readerIndex(src.readerIndex() + logOffsetInUnit + offsetInLog);
                    src.readBytes(destination, destination.writerIndex(), length);
                  }
                };
              }

              applier.addLog(logToApply);
            } else {
              logger.warn("a strange log id={}, offset={}, length={}, status={}", log.getLogId(),
                  log.getOffset(), log.getLength(), log.getLogStatus());
            }
          }
        }
        if (!isUnitFree) {
          try {
            applier.apply().get();
          } catch (LogsNotInSamePageException | LogsNotInRightOrderException
              | DestinationNotInSamePageException e) {
            logger.error("apply failed", e);
            throw new MergeFailedException(e);
          } catch (InterruptedException | ExecutionException e) {
            logger.error("unexpected error", e);
            throw new MergeFailedException(e);
          }
        }
      }
      returnResponse = buildResponse(baseResponse, data);
      return returnResponse;
    } finally {
      if (returnResponse == null) {
        for (ByteBuf buf : data) {
          if (buf != null) {
            buf.release();
          }
        }
      }
    }

  }

  private PyReadResponse buildResponse(PyReadResponse firstResponse, ByteBuf[] data) {
    ByteBuf returnData = null;
    CompositeByteBuf compositeByteBuf = null;
    PbReadResponse.Builder responseBuilder = PbReadResponse.newBuilder();
    responseBuilder.setRequestId(firstResponse.getMetadata().getRequestId());
    for (int i = 0; i < data.length; i++) {
      PbReadResponseUnit.Builder unitBuilder = firstResponse.getMetadata().getResponseUnits(i)
          .toBuilder();
      unitBuilder.clearLogsToMerge();
      unitBuilder.clearChecksum();
      if (data[i] == null) {
        unitBuilder.setResult(PbIoUnitResult.FREE);
      } else {
        data[i].writerIndex(data[i].capacity());
        unitBuilder.setResult(PbIoUnitResult.OK);
        unitBuilder.setChecksum(0L);
        if (returnData == null) {
          returnData = data[i];
        } else {
          if (compositeByteBuf == null) {
            compositeByteBuf = new CompositeByteBuf(allocator, true, data.length);
            compositeByteBuf.addComponent(true, returnData);
          }
          compositeByteBuf.addComponent(true, data[i]);
        }
      }
      responseBuilder.addResponseUnits(unitBuilder);
    }

    if (compositeByteBuf != null) {
      returnData = compositeByteBuf;
    }

    return new PyReadResponse(responseBuilder.build(), returnData, true);
  }

  private boolean enoughResponse(MergeReadType type, Map<InstanceId, PyReadResponse> responses) {
    for (InstanceId member : responses.keySet()) {
      Validate.isTrue(membership.isSecondary(member) || membership.isJoiningSecondary(member)
              || membership.isPrimary(member),
          "not primary or secondary, not possible %s %s", member, membership);
    }

    int expect;
    if (type == MergeReadType.StablePrimary) {
      expect = membership.getWriteSecondaries().size();
    } else if (type == MergeReadType.UnstablePrimary) {
      if (membership.isPrimary(myself)) {
        expect = membership.getWriteSecondaries().size() + 1;
      } else {
        expect = membership.getWriteSecondaries().size();
      }
    } else {
      throw new IllegalArgumentException("unknown type: " + type);
    }

    if (expect == responses.size()) {
      return true;
    } else if (expect > responses.size()) {
      logger.warn("not enough response, expect {}, but got {}", expect, responses.keySet());
      return false;
    } else {
      logger.error("what's wrong here? {} {} {}", type, membership, responses);
      throw new IllegalArgumentException("response size larger than expect ?");
    }

  }

  private CompletableFuture<PyReadResponse> readFromMembers(MergeReadType type,
      Set<InstanceId> members) {
    CompletableFuture<PyReadResponse> future = new CompletableFuture<>();

    AtomicInteger count = new AtomicInteger(members.size());
    Map<InstanceId, PyReadResponse> responses = new ConcurrentHashMap<>();

    PbReadRequest readRequest = this.readRequest;

    for (InstanceId secondary : members) {
      logger.debug("going to read from secondary {}", secondary);

      communicator.read(secondary, readRequest)
          .thenAccept(pyReadResponse -> {
            logger.debug("got response from {}", secondary);
            responses.put(secondary, pyReadResponse);
            if (count.decrementAndGet() == 0) {
              CompletableFuture<PyReadResponse> mergedFuture;
              if (type == MergeReadType.StablePrimary) {
                mergedFuture = primaryProcess(responses);
              } else if (type == MergeReadType.UnstablePrimary) {
                mergedFuture = tempPrimaryProcess(responses);
              } else {
                logger.error("unknown type {}", type);
                future.completeExceptionally(new IllegalArgumentException("unknown type: " + type));
                return;
              }
              mergedFuture
                  .thenAccept(future::complete)
                  .exceptionally(t -> {
                    future.completeExceptionally(t);
                    return null;
                  })
                  .whenComplete((avoid, throwable) -> {
                    for (PyReadResponse response : responses.values()) {
                      response.release();
                    }
                  });
            }
          })
          .exceptionally(throwable -> {
            future.completeExceptionally(throwable);
            return null;
          });
    }

    return future;
  }

  private enum MergeReadType {
    StablePrimary, UnstablePrimary
  }

}

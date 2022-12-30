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

import io.netty.buffer.ByteBuf;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.InvalidSegmentStatusException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.exception.NoAvailableBufferException;
import py.netty.core.MethodCallback;
import py.netty.datanode.PyWriteRequest;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;

public abstract class WriteTask extends IoTask implements Comparable<WriteTask> {
  private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);
  private static final int DEFAULT_MAX_TIME_FOR_FAST_BUFFER = 200;
  private static final int DEFAULT_INIT_TIME_FOR_FAST_BUFFER = 10;
  protected final boolean infiniteTimeoutAllocatingFastBuffer;
  protected final DataNodeConfiguration cfg;
  protected PyWriteRequest request;
  protected MethodCallback<PbWriteResponse> callback;
  protected PbWriteResponse.Builder responseBuilder;
  protected PbIoUnitResult[] unitResults;
  protected SegmentUnit segmentUnit;
  protected PlalEngine plalEngine;
  protected int logTimeoutMs;

  protected WriteTask(PyWriteRequest request, MethodCallback<PbWriteResponse> callback,
      PbWriteResponse.Builder responseBuilder, PbIoUnitResult[] unitResults,
      SegmentUnit segmentUnit,
      PlalEngine plalEngine, boolean infiniteTimeoutAllocatingFastBuffer,
      DataNodeConfiguration cfg) {
    super(segmentUnit);
    this.callback = callback;
    this.request = request;
    this.responseBuilder = responseBuilder;
    this.unitResults = unitResults;
    this.segmentUnit = segmentUnit;
    this.plalEngine = plalEngine;
    this.infiniteTimeoutAllocatingFastBuffer = infiniteTimeoutAllocatingFastBuffer;
    this.cfg = cfg;
  }

  public int getTimeout() {
    if (infiniteTimeoutAllocatingFastBuffer) {
      return Integer.MAX_VALUE;
    }
    int times = (request.getMetadata().getFailTimes() + 1) * DEFAULT_INIT_TIME_FOR_FAST_BUFFER;
    if (times > DEFAULT_MAX_TIME_FOR_FAST_BUFFER) {
      return DEFAULT_MAX_TIME_FOR_FAST_BUFFER;
    } else {
      return times;
    }
  }

  @Override
  public void run() {
    try {
      process();
    } catch (Exception t) {
      logger.warn("caught a throwable for {}", segmentUnit, t);
      failToProcess(t);
    }
  }

  private long requestTime() {
    return request.getMetadata().getRequestTime();
  }

  private long requestId() {
    return request.getMetadata().getRequestId();
  }

  private void checkSegmentUnitStatus() throws InvalidSegmentStatusException {
    SegmentUnitStatus segmentUnitStatus = segmentUnit.getSegmentUnitMetadata()
        .getStatus();
    if (segmentUnitStatus.isFinalStatus()) {
      logger.error("segment unit status is error {} {}", segmentUnit.getSegId(),
          segmentUnitStatus);
      throw new InvalidSegmentStatusException();
    }
  }

  protected MutationLogEntry createLog(PbWriteRequestUnit writeUnit, ByteBuf data,
      boolean isPrimary) throws NoAvailableBufferException, InvalidSegmentStatusException {
    checkSegmentUnitStatus();
    MutationLogEntry entry;
    int timeout = getTimeout();
    int createLogTimeout = Math.min(timeout, DEFAULT_MAX_TIME_FOR_FAST_BUFFER);
    int maxTime = Math.max(1, timeout / createLogTimeout);
    long beginTimeMs = System.currentTimeMillis();
    try {
      while (true) {
        try {
          entry = segmentUnit.getSegmentLogMetadata().createLog(writeUnit, data,
              segmentUnit.getArchive().getArchiveId(), isPrimary, createLogTimeout);
          return entry;
        } catch (NoAvailableBufferException e) {
          checkSegmentUnitStatus();
          maxTime--;
          if (maxTime == 0) {
            throw new NoAvailableBufferException();
          }
        }
      }
    } finally {
      long cost = System.currentTimeMillis() - beginTimeMs;
      if (cost > cfg.getAllocateFastBufferDebugLatencyThreshold()) {
        logger.warn("allocate fast buffer cost too much {}, segId {}", cost,
            segmentUnit.getSegId());
      }
    }
  }

  public abstract void failToProcess(Exception e);

  protected abstract void process() throws Exception;

  @Override
  public int compareTo(@Nonnull WriteTask o) {
    return Long.compare(requestTime(), o.requestTime());
  }

  protected MethodCallback<PbWriteResponse> getCallback() {
    return callback;
  }
}

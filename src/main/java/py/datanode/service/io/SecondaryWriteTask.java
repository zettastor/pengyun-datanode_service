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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.common.struct.Pair;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.broadcast.BrokenLogWithoutLogId;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.netty.core.MethodCallback;
import py.netty.datanode.PyWriteRequest;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbWriteRequest;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Broadcastlog.PbWriteResponse.Builder;

/**
 * What secondary does during a writing. <br>
 * Create logs without log id and keep them but not inserted into the log sequence until primary
 * told us the log id. Or if the log IDs came earlier than the data, insert them directly.
 *
 */
public class SecondaryWriteTask extends WriteTask {
  private static final Logger logger = LoggerFactory.getLogger(SecondaryWriteTask.class);

  private boolean stable;
  private String className;

  public SecondaryWriteTask(PyWriteRequest request, MethodCallback<PbWriteResponse> callback,
      Builder responseBuilder,
      PbIoUnitResult[] unitResults, SegmentUnit segmentUnit, PlalEngine plalEngine, boolean stable,
      DataNodeConfiguration cfg, boolean infiniteTimeoutAllocatingFastBuffer) {
    super(request, callback, responseBuilder, unitResults, segmentUnit, plalEngine,
        infiniteTimeoutAllocatingFastBuffer, cfg);
    Validate.notNull(cfg);
    this.stable = stable;

    this.className = "SecondaryWriteTask";
  }

  @Override
  protected void process() throws Exception {
    logger.debug("process write request: {} for segId: {}", request.getMetadata(),
        segmentUnit.getSegId());

    boolean nofree = false;
    SegmentLogMetadata segLogMetadata = segmentUnit.getSegmentLogMetadata();
    PbWriteRequest metadata = request.getMetadata();

    Long requestId = metadata.getRequestId();

    SortedMap<MutationLogEntry, Boolean> mapLogsToNewlyCreated = new TreeMap<>();
    Map<MutationLogEntry, Integer> mapLogToIndex = new HashMap<>();
    for (int i = 0; i < unitResults.length; i++) {
      if (unitResults[i] != PbIoUnitResult.SKIP) {
        continue;
      }

      PbWriteRequestUnit writeUnit = metadata.getRequestUnits(i);
      if (nofree) {
        segLogMetadata.markLogBroken(writeUnit.getLogUuid(),
            BrokenLogWithoutLogId.BrokenReason.ResourceExhausted);
        unitResults[i] = PbIoUnitResult.EXHAUSTED;
        continue;
      }

      Pair<Long, Boolean> logIdAndAlreadyExists = segLogMetadata
          .getNewOrOldLogId(writeUnit.getLogUuid(), false);
      if (logIdAndAlreadyExists.getSecond()) {
        unitResults[i] = stable ? PbIoUnitResult.OK : PbIoUnitResult.SECONDARY_NOT_STABLE;
        continue;
      }

      MutationLogEntry entry = segLogMetadata.getLogWithoutLogId(writeUnit.getLogUuid());
      if (entry != null) {
        Validate.isTrue(entry.getOffset() == writeUnit.getOffset());
        Validate.isTrue(entry.getLength() == writeUnit.getLength());
        Validate.isTrue(entry.getChecksum() == writeUnit.getChecksum());
        unitResults[i] = stable ? PbIoUnitResult.OK : PbIoUnitResult.SECONDARY_NOT_STABLE;
      } else {
        try {
          entry = createLog(writeUnit, request.getRequestUnitData(i), false);
          Validate.notNull(entry);
          Validate.isTrue(LogImage.INVALID_LOG_ID == logIdAndAlreadyExists.getFirst());
          entry.setLogId(LogImage.INVALID_LOG_ID);
          mapLogsToNewlyCreated.put(entry, true);
          mapLogToIndex.put(entry, i);
        } catch (Exception e) {
          logger.warn("caught an exception for {}, failed times {}", writeUnit.getLogUuid(),
              request.getMetadata().getFailTimes(), e);
          segLogMetadata.markLogBroken(writeUnit.getLogUuid(),
              BrokenLogWithoutLogId.BrokenReason.ResourceExhausted);
          unitResults[i] = PbIoUnitResult.EXHAUSTED;
          nofree = true;
        }
      }

    }

    List<MutationLogEntry> logsWithoutLogId = new LinkedList<>();
    for (Entry<MutationLogEntry, Boolean> logAndNewlyCreated : mapLogsToNewlyCreated.entrySet()) {
      MutationLogEntry log = logAndNewlyCreated.getKey();
      boolean newlyCreated = logAndNewlyCreated.getValue();
      if (newlyCreated) {
        if (log.getLogId() == LogImage.INVALID_LOG_ID) {
          logsWithoutLogId.add(log);
        }
      }

      unitResults[mapLogToIndex.get(log)] =
          stable ? PbIoUnitResult.OK : PbIoUnitResult.SECONDARY_NOT_STABLE;
    }

    segLogMetadata.saveLogsWithoutLogId(logsWithoutLogId, plalEngine);

    logger.debug("dealt with {} logs, new created {}", unitResults.length,
        mapLogsToNewlyCreated.size());

    finishWrite();
  }

  public void finishWrite() {
    for (int i = 0; i < unitResults.length; i++) {
      responseBuilder.addResponseUnits(PbRequestResponseHelper
          .buildPbWriteResponseUnitFrom(request.getMetadata().getRequestUnits(i), unitResults[i]));
    }

    Long requestId = request.getMetadata().getRequestId();

    callback.complete(responseBuilder.build());
  }

  @Override
  public void failToProcess(Exception e) {
    Long requestId = request.getMetadata().getRequestId();

    callback.fail(e);
  }

}

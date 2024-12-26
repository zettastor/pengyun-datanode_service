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

package py.datanode.service.io;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.CloneStatus;
import py.archive.segment.CloneType;
import py.datanode.exception.CloneFailedException;
import py.datanode.exception.InsertLogFailedException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.exception.NoAvailableBufferException;
import py.netty.core.MethodCallback;
import py.netty.datanode.NettyExceptionHelper;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Commitlog;
import py.proto.Commitlog.PbCommitlogRequest;
import py.proto.Commitlog.PbCommitlogResponse;

public class AddOrCommitLogsTask extends IoTask {
  private static final Logger logger = LoggerFactory.getLogger(AddOrCommitLogsTask.class);

  private final PbCommitlogRequest request;
  private final MethodCallback<PbCommitlogResponse> callback;
  private final PlalEngine plalEngine;
  private final int pageSize;

  public AddOrCommitLogsTask(SegmentUnit segmentUnit, PbCommitlogRequest request,
      MethodCallback<PbCommitlogResponse> callback,
      PlalEngine plalEngine, int pageSize) {
    super(segmentUnit);
    this.request = request;
    this.callback = callback;
    this.plalEngine = plalEngine;
    this.pageSize = pageSize;
  }

  @Override
  public void run() {
    try {
      processLogs();
    } catch (CloneFailedException e) {
      logger.warn("clone page failed", e);
      callback
          .fail(NettyExceptionHelper.buildServerProcessException(e));
      return;
    } catch (NoAvailableBufferException e) {
      logger.warn("no available fast buffer", e);
      callback
          .fail(NettyExceptionHelper.buildServerProcessException(e));
      return;
    } catch (InsertLogFailedException e) {
      logger.warn("fail to insert log", e);
      callback
          .fail(NettyExceptionHelper.buildServerProcessException(e));
      return;
    } catch (Throwable throwable) {
      logger.error("caught a throwable", throwable);
      callback.fail(NettyExceptionHelper.buildServerProcessException(throwable));
      return;
    }

    Commitlog.PbCommitlogResponse.Builder builder = Commitlog.PbCommitlogResponse
        .newBuilder();
    builder.setSuccess(true);
    builder.setRequestId(request.getRequestId());
    builder.setVolumeId(request.getVolumeId());
    builder.setSegIndex(request.getSegIndex());
    callback.complete(builder.build());
  }

  private void processLogs()
      throws CloneFailedException, NoAvailableBufferException, InsertLogFailedException {
    logger.debug("add or commit logs {}", request.getBroadcastManagersList().size());
    long pl = segmentUnit.getSegmentLogMetadata().getPlId();
    for (PbBroadcastLogManager logs : request.getBroadcastManagersList()) {
      for (PbBroadcastLog log : logs.getBroadcastLogsList()) {
        logger.debug("processing {}, {}", log.getLogId(), log.getLogStatus());
        int pageIndex = (int) (log.getOffset() / pageSize);
        segmentUnit.setPageHasBeenWritten(pageIndex);

        if (log.getLogId() < pl) {
          logger.warn("found a very old log, skipping... pl : {}, new log : {}", pl,
              log.getLogId());
          continue;
        }

        MutationLogEntry mutationLogEntry;
        mutationLogEntry = MutationLogEntryFactory
            .createLogForSyncLog(log.getLogUuid(), log.getLogId(), log.getOffset(),
                log.getData().toByteArray(), log.getChecksum());
        mutationLogEntry
            .setStatus(DataNodeRequestResponseHelper.convertFromPbStatus(log.getLogStatus()));
        if (!segmentUnit.getSegmentLogMetadata()
            .insertLogsForSecondary(plalEngine, Lists.newArrayList(mutationLogEntry), 10000)) {
          logger.error("insert log failed");
          throw new InsertLogFailedException();
        }
      }
    }
  }
}

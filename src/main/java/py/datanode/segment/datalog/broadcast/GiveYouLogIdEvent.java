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

package py.datanode.segment.datalog.broadcast;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.exception.InvalidSegmentStatusException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.broadcast.listener.CompletingLogListener;
import py.netty.core.MethodCallback;
import py.netty.datanode.NettyExceptionHelper;
import py.proto.Broadcastlog;

public class GiveYouLogIdEvent implements CompletingLogListener {
  private static final Logger logger = LoggerFactory.getLogger(GiveYouLogIdEvent.class);
  private static AtomicLong requestIdBuilder = new AtomicLong(0);
  private final MethodCallback<Broadcastlog.GiveYouLogIdResponse> callback;
  private final Map<Long, Long> mapLogUuidToLogId;
  private final Set<Long> doneLogsUuids = Sets.newConcurrentHashSet();
  private final long myInstanceId;
  private final long currentPrimaryInstanceId;
  private AtomicBoolean done = new AtomicBoolean(false);
  private SegmentUnit segmentUnit;

  public GiveYouLogIdEvent(long myInstanceId,
      MethodCallback<Broadcastlog.GiveYouLogIdResponse> callback,
      List<Broadcastlog.LogUuidAndLogId> logUuidAndLogIds, SegmentUnit segmentUnit,
      long currentPrimaryInstanceId) {
    this.segmentUnit = segmentUnit;
    this.myInstanceId = myInstanceId;
    this.callback = callback;
    this.mapLogUuidToLogId = new ConcurrentHashMap<>();
    this.currentPrimaryInstanceId = currentPrimaryInstanceId;

    for (Broadcastlog.LogUuidAndLogId logUuidAndLogId : logUuidAndLogIds) {
      if (mapLogUuidToLogId.putIfAbsent(logUuidAndLogId.getLogUuid(), logUuidAndLogId.getLogId())
          != null) {
        throw new IllegalArgumentException("log uuid should be unique !");
      }
    }
    logger.debug("start GiveYouLogIdEvent {}", this.mapLogUuidToLogId);
  }

  public Map<Long, Long> getMapLogUuidToLogId() {
    return mapLogUuidToLogId;
  }

  private void success() {
    if (!done.compareAndSet(false, true)) {
      return;
    }

    long potentialPrimaryId = segmentUnit.getPotentialPrimaryId();
    if (potentialPrimaryId != SegmentUnit.NO_POTENTIAL_PRIMARY_ID
        && potentialPrimaryId != currentPrimaryInstanceId) {
      logger.warn("it is very strange the potential id has changed from {} to {} for segId={}",
          currentPrimaryInstanceId, potentialPrimaryId, segmentUnit.getSegId());

      callback.fail(NettyExceptionHelper
          .buildServerProcessException(
              new InvalidSegmentStatusException("segId=" + segmentUnit.getSegId())));
    } else {
      logger.debug("finish GiveYouLogIdEvent {}", this.getMapLogUuidToLogId());
      callback.complete(Broadcastlog.GiveYouLogIdResponse.newBuilder()
          .setRequestId(requestIdBuilder.incrementAndGet())
          .setMyInstanceId(myInstanceId).build());
    }
  }

  @Override
  public void complete(long logUuid) {
    Validate.isTrue(mapLogUuidToLogId.containsKey(logUuid));
    doneLogsUuids.add(logUuid);

    if (doneLogsUuids.size() == mapLogUuidToLogId.size()) {
      success();
    }
  }

  @Override
  public void fail(long logUuid, Exception e) {
    fail(e);
  }

  public void fail(Exception e) {
    if (done.compareAndSet(false, true)) {
      logger.debug("failed getting log entry {} {}", doneLogsUuids, e.getMessage());
      callback.fail(NettyExceptionHelper.buildServerProcessException(e));
    }
  }
}

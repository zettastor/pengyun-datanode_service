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

package py.datanode.service.io.throttle.strategy;

import py.common.LogPoolType;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.service.io.throttle.CopyPageSampleInfo;
import py.storage.Storage;
import py.storage.impl.PriorityStorageImpl;

public class SmartIoThrottleStrategy extends IoThrottleStrategy {
  public static final float REQUIRED_FREE_RESOURCE_RATIO = 0.25F;
  public final int defaultMinCopySpeed;

  public SmartIoThrottleStrategy(DataNodeConfiguration cfg) {
    super(cfg);

    defaultMinCopySpeed = cfg.getPageCountInRawChunk();
  }

  @Override
  protected void releasePermits(CopyPageSampleInfo copyPageSampleInfo) {
    boolean hasNormalIo = copyPageSampleInfo.isHasNormalIo();
    boolean hasResource = copyPageSampleInfo.isHasResource();
    int minSpeed = defaultMinCopySpeed;
    int maxSpeed = copyPageSampleInfo.getMaxCopySpeed();

    int middle = (minSpeed + maxSpeed) / 2;
    int permits;
    int oldPermits = copyPageSampleInfo.getLastPermits();
    if (hasNormalIo) {
      if (hasResource) {
        if (oldPermits == minSpeed) {
          permits = middle;
        } else if (oldPermits == maxSpeed) {
          permits = middle;
        } else {
          permits = (oldPermits + maxSpeed) / 2;
        }
      } else {
        permits = minSpeed;
      }
    } else {
      permits = maxSpeed;
    }
    copyPageSampleInfo.setPermits(permits);
    if (hasNormalIo || !hasResource) {
      logger.warn(
          "hasNormalIo {}, hasResource {}  min {}, max {}, " 
              + "old permits {} release permits {}, sample {}",
          hasNormalIo, hasResource, minSpeed, maxSpeed, oldPermits, permits, copyPageSampleInfo);
    }
  }

  @Override
  protected boolean canSpeedUp(boolean hasIoInLastSecond, CopyPageSampleInfo copyPageSampleInfo) {
    return thereIsEnoughResource(hasIoInLastSecond, copyPageSampleInfo);
  }

  private boolean thereIsEnoughResource(boolean hasIoInLastSecond,
      CopyPageSampleInfo copyPageSampleInfo) {
    copyPageSampleInfo.setHasNormalIo(hasIoInLastSecond);
    float freeFastBufferRatio = MutationLogEntryFactory
        .getFreeRatioOfFastBuffer(copyPageSampleInfo.getArchive().getArchiveId(),
            LogPoolType.primaryLogPool);
    Storage storage = copyPageSampleInfo.getArchive().getStorage();
    float freeStorageRatio = 1.0f - ((PriorityStorageImpl) storage).getDiskUtility();
    boolean diskIsFree = hasIoInLastSecond ? freeStorageRatio > REQUIRED_FREE_RESOURCE_RATIO : true;
    boolean enough = diskIsFree && freeFastBufferRatio > REQUIRED_FREE_RESOURCE_RATIO;
    logger.info(
        "segid {}, hasIoInLastSecond {} freeFastBufferRatio " 
            + "{} freeStorageRatio {} thereIsEnoughResource {} ",
        copyPageSampleInfo, hasIoInLastSecond, freeFastBufferRatio, freeStorageRatio, enough);
    copyPageSampleInfo.setHasResource(enough);
    return enough;
  }

}

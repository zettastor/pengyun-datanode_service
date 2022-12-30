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

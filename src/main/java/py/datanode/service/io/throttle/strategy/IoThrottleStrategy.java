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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.service.io.throttle.CopyPageSampleInfo;

public abstract class IoThrottleStrategy {
  protected final Logger logger = LoggerFactory.getLogger(IoThrottleStrategy.class);

  protected DataNodeConfiguration cfg;

  IoThrottleStrategy(DataNodeConfiguration cfg) {
    this.cfg = cfg;
  }
  

  public int throttle(SegId segId, int copyCount, CopyPageSampleInfo copyPageSampleInfo) {
    if (copyPageSampleInfo.getArchive().getArchiveMetadata().getMaxMigrationSpeed()
        >= 1024 * 1024) {
      return 0;
    }

    int leftTimeToOneSecond = copyPageSampleInfo.leftTimeToNextSecond();
    if (leftTimeToOneSecond == 0) {
      releasePermits(copyPageSampleInfo);
    }

    if (!copyPageSampleInfo.acquirePermits(copyCount)) {
      logger.info(
          "in last second, segid {} cannot continue copy, " 
              + "require {} available {}, i suggest you wait {} ms for next try",
          segId, copyCount, copyPageSampleInfo.getPermits(), leftTimeToOneSecond);
      return leftTimeToOneSecond;
    } else {
      return 0;
    }
  }

  protected void releasePermits(CopyPageSampleInfo copyPageSampleInfo) {
    int permits = copyPageSampleInfo.getMaxCopySpeed();
    logger.info("release permits to {} sample info {}", permits, copyPageSampleInfo);
    copyPageSampleInfo.setPermits(permits);
  }

  public void addTotal(CopyPageSampleInfo copyPageSampleInfo, int count) {
    copyPageSampleInfo.addTotal(count);
  }

  public boolean addAlready(SegId segId, int copyCount, boolean hasIoInLastSecond,
      CopyPageSampleInfo copyPageSampleInfo) {
    copyPageSampleInfo.addAlready(copyCount);
    return canSpeedUp(hasIoInLastSecond, copyPageSampleInfo);
  }

  protected boolean canSpeedUp(boolean hasIoInLastSecond, CopyPageSampleInfo copyPageSampleInfo) {
    return true;
  }

}

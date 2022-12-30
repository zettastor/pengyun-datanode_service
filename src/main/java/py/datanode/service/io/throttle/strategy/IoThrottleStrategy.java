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

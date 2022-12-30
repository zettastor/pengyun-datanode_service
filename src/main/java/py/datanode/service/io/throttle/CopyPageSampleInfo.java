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

package py.datanode.service.io.throttle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.RawArchiveMetadata;
import py.archive.segment.SegId;
import py.datanode.archive.RawArchive;

public class CopyPageSampleInfo {
  public static int PAGE_SIZE_KB = 8;
  public static int ONE_SECOND_MS = 1000;
  public static int DEFAULT_MIN_FINISH_TIME_MS = 3000;
  private final Logger logger = LoggerFactory.getLogger(CopyPageSampleInfo.class);
  private long sessionId;
  private RawArchive archive;
  private SegId segId;
  private RawArchiveMetadata archiveMetadata;

  private long lastActiveTime;
  private long lastOneSecondTime;
  private boolean hasNormalIo;
  private boolean hasResource;

  private long startTime;
  private long initTime;
  private int count;
 
  private int averCopySpeed;
  private int currentCopySpeed;
  private int maxCopySpeed;

  private int totalToCopy;
  private int alreadyCopy;

  private int speed;

  private int permits;
  private int lastPermits;

  public CopyPageSampleInfo(RawArchive archive, SegId segId, int totalToCopy, int pageSize,
      long sessionId) {
    this.sessionId = sessionId;
    this.archive = archive;
    this.segId = segId;
    this.totalToCopy = totalToCopy;
    PAGE_SIZE_KB = pageSize / 1024;
    archiveMetadata = archive.getArchiveMetadata();
    initTime = lastActiveTime = System.currentTimeMillis();
  }

  public RawArchive getArchive() {
    return archive;
  }

  public SegId getSegId() {
    return segId;
  }

  public void addTotal(int count) {
    logger.info("add total current {}, add {}", totalToCopy, count);
    totalToCopy += count;
    markLastActiveTime();
  }

  public int getTotal() {
    return totalToCopy;
  }

  public int getAlready() {
    return alreadyCopy;
  }

  public int getSpeed() {
    return speed;
  }

  public boolean addAlready(int copyCount) {
    if (startTime == 0 && copyCount != 1) {
      startTime = System.currentTimeMillis();
    }
    logger.info("current total {} , already {}, going to add count {} segid {}", totalToCopy,
        alreadyCopy, copyCount, segId);
    if (alreadyCopy + copyCount > totalToCopy) {
      logger
          .warn("what is wrong for seg {} , total {} , already {} can not add copy count {}", segId,
              totalToCopy, alreadyCopy, copyCount);
    }
    currentCopySpeed += copyCount;
    alreadyCopy += copyCount;
    markLastActiveTime();
    return true;
  }

  public void markLastActiveTime() {
    logger.info("mark active time for seg {}, session {}", segId, sessionId);
    lastActiveTime = System.currentTimeMillis();
  }

  public long lastActiveTime() {
    return lastActiveTime;
  }

  public boolean ifOneSecondPassed() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastOneSecondTime >= ONE_SECOND_MS) {
      setCurrentCopySpeed((int) (currentTime - lastOneSecondTime));
      lastOneSecondTime = currentTime;
      currentCopySpeed = 0;
      maxCopySpeed = (int) (archiveMetadata.getMaxMigrationSpeed() / PAGE_SIZE_KB);
      return true;
    }
    return false;
  }

  public int leftTimeToNextSecond() {
    if (ifOneSecondPassed()) {
      return 0;
    } else {
      return ONE_SECOND_MS - (int) (System.currentTimeMillis() - lastOneSecondTime);
    }
  }

  public long leftTimeToFinish() {
    return averCopySpeed == 0 ? DEFAULT_MIN_FINISH_TIME_MS
        : (totalToCopy - alreadyCopy) / averCopySpeed * ONE_SECOND_MS;
  }

  public long getSessionId() {
    return sessionId;
  }

  public int getMaxCopySpeed() {
    return maxCopySpeed;
  }

  public void setCurrentCopySpeed(int interval) {
   
    speed = currentCopySpeed * PAGE_SIZE_KB / 1;
   
    if (lastOneSecondTime != 0 && interval > ONE_SECOND_MS * 1.1) {
      logger.info("time passed more than 1s {}", interval);
      speed = (int) ((float) speed / (((float) interval) / ONE_SECOND_MS));
    }
    averCopySpeed = (averCopySpeed * count + currentCopySpeed) / (++count);
    logger.debug("SegId {}  speed {} kb/s copypage {} interval {}  total {} already {}  ", segId,
        speed, currentCopySpeed, interval, totalToCopy, alreadyCopy);
  }

  public int getLastPermits() {
    return lastPermits;
  }

  public int getPermits() {
    return permits;
  }

  public void setPermits(int permits) {
    this.permits = permits;
    this.lastPermits = permits;
  }

  public boolean acquirePermits(int count) {
    if (permits >= count) {
      return (permits -= count) >= 0;
    }
    return false;
  }

  public boolean isHasNormalIo() {
    return hasNormalIo;
  }

  public void setHasNormalIo(boolean hasNormalIo) {
    this.hasNormalIo = hasNormalIo;
  }

  public boolean isHasResource() {
    return hasResource;
  }

  public void setHasResource(boolean hasResource) {
    this.hasResource = hasResource;
  }

  public String toString() {
    return "session " + sessionId + " segid " + segId + " totalToCopy " + totalToCopy
        + " alreayToCopy " + alreadyCopy + " aver speed  " + averCopySpeed * PAGE_SIZE_KB / 1024
        + " mb/s costs sec " + (System.currentTimeMillis() - startTime) / ONE_SECOND_MS
        + ", waittime " + (startTime - initTime) / ONE_SECOND_MS
        + " lastActiveTime interval " + (System.currentTimeMillis() - lastActiveTime);
  }
}

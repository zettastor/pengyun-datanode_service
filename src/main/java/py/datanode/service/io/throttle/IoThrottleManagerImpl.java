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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.struct.Pair;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.service.io.throttle.strategy.IoThrottleStrategy;
import py.datanode.service.io.throttle.strategy.SmartIoThrottleStrategy;
import py.datanode.service.io.throttle.strategy.StrictIoThrottleStrategy;
import py.io.qos.MigrationStrategy;

public class IoThrottleManagerImpl implements IoThrottleManager {
  private final Logger logger = LoggerFactory.getLogger(IoThrottleManagerImpl.class);
  protected Map<MigrationStrategy, IoThrottleStrategy> allStrategies = new HashMap<>(3);
  protected Map<SegId, CopyPageSampleInfo> mapSegIdToCopyPageSampleInfo = new HashMap<>();
  protected Map<RawArchive, SegId> mapArchiveToCopyingSegId = new HashMap<>();
  protected Map<RawArchive, Long> mapArchiveToLastIoTime = new HashMap<>();
  protected Map<RawArchive, Object> mapArchiveToLock = new HashMap<>();
  DataNodeConfiguration cfg;
  

  public IoThrottleManagerImpl(RawArchiveManager rawArchiveManager,
      DataNodeConfiguration cfg) {
    CopyPageSampleInfo.PAGE_SIZE_KB = cfg.getPageSize() / 1024;
    this.cfg = cfg;
    allStrategies.put(MigrationStrategy.Smart, new SmartIoThrottleStrategy(cfg));
    allStrategies.put(MigrationStrategy.Manual, new StrictIoThrottleStrategy(cfg));

    for (RawArchive rawArchive : rawArchiveManager.getRawArchives()) {
      mapArchiveToLastIoTime.put(rawArchive, 0L);
      mapArchiveToLock.put(rawArchive, new Object());
    }
  }

  @Override
  public Pair<Boolean, Integer> register(RawArchive archive, SegId segId, int countOfPageToCopy,
      long sessionId) {
    IoType ioType = IoType.getIoType(sessionId);

    synchronized (getLockForArchive(archive)) {
      SegId other = mapArchiveToCopyingSegId.get(archive);
      CopyPageSampleInfo otherInfo = mapSegIdToCopyPageSampleInfo.get(other);
      int timeToFinish = CopyPageSampleInfo.DEFAULT_MIN_FINISH_TIME_MS;
      timeToFinish = otherInfo == null ? timeToFinish
          : Math.min(timeToFinish, (int) otherInfo.leftTimeToFinish());

      boolean success = false;
      boolean timeout = false;
      if (other != null) {
        long interval = otherInfo == null ? Long.MAX_VALUE
            : System.currentTimeMillis() - otherInfo.lastActiveTime();
        if (interval > cfg.getMaxPushIntervalTimeMs()) {
          logger.error("timeout of seg {} {} {}", other, interval, otherInfo);
          timeout = true;
        }

        if (segId.equals(other) && !timeout) {
          if (otherInfo != null && IoType.getIoType(otherInfo.getSessionId()) == ioType) {
            if (ioType == IoType.Clone) {
              logger.warn("{} {},primary and seondary are in same archive for clone : {} {}", segId,
                  sessionId, ioType, otherInfo);
              otherInfo.markLastActiveTime();
              return new Pair(true, 0);
            } else if (ioType == IoType.CopyPage) {
              logger
                  .warn("{} {}, copypage are racing for the same archive in primary: {} {}", segId,
                      sessionId, ioType, otherInfo);
              if (!timeout) {
                return new Pair(false, timeToFinish);
              }
            } else {
              logger.warn("todo for rebalance {}", segId);
              return new Pair(false, timeToFinish);
            }
          } else {
            logger.warn("{} {}, copypage and clone  are racing for the same archive: {} {}", segId,
                sessionId, ioType, otherInfo);
            return new Pair(false, timeToFinish);
          }
        }
      }

      CopyPageSampleInfo myInfo = mapSegIdToCopyPageSampleInfo.get(segId);
      if (other == null || timeout) {
        success = true;
        mapArchiveToCopyingSegId.put(archive, segId);
        if (myInfo != null && myInfo.getSessionId() != sessionId) {
          logger.warn("session outofdate! abort ! old = {} ", myInfo);
          myInfo = null;
        }
        logger.info(
            "register, segId {}, type {}, arichve {} totalpage {}, workCount {}",
            segId, ioType, archive.getArchiveMetadata().getDeviceName(), countOfPageToCopy);
      } else {
        logger.info("other seg {} is working please wait {}, {}", other, timeToFinish, otherInfo);
      }

      if (myInfo == null) {
        myInfo = new CopyPageSampleInfo(archive, segId, countOfPageToCopy, cfg.getPageSize(),
            sessionId);
        mapSegIdToCopyPageSampleInfo.put(segId, myInfo);
      }
      myInfo.markLastActiveTime();
      return new Pair(success, timeToFinish);
    }

  }

  @Override
  public void unregister(RawArchive archive, SegId segId) {
    synchronized (getLockForArchive(archive)) {
      if (mapArchiveToCopyingSegId.remove(archive, segId)) {
        logger.info("unregister {} {} ", archive.getArchiveMetadata().getDeviceName(), segId);
      } else {
        logger.warn("can not unregister {}  i am {} exist is {}",
            archive.getArchiveMetadata().getDeviceName(), segId,
            mapArchiveToCopyingSegId.get(archive));
      }
    }
  }

  @Override
  public boolean exist(SegId segId) {
    return mapSegIdToCopyPageSampleInfo.containsKey(segId);
  }

  @Override
  public void finish(RawArchive archive, SegId segId) {
    CopyPageSampleInfo copyPageSampleInfo = mapSegIdToCopyPageSampleInfo.remove(segId);
    logger.info("finish copy {} copyPageSampleInfo {} arichve {} ",
        segId, copyPageSampleInfo, archive.getArchiveId());
    unregister(archive, segId);
  }

  @Override
  public void addTotal(SegId segId, int count) {
    CopyPageSampleInfo copyPageSampleInfo = mapSegIdToCopyPageSampleInfo.get(segId);
    if (!checkExist(segId, copyPageSampleInfo)) {
      return;
    }
    MigrationStrategy strategy = copyPageSampleInfo.getArchive().getArchiveMetadata()
        .getMigrationStrategy();
    allStrategies.get(strategy).addTotal(copyPageSampleInfo, count);
  }

  @Override
  public boolean addAlready(SegId segId, int count) {
    CopyPageSampleInfo copyPageSampleInfo = mapSegIdToCopyPageSampleInfo.get(segId);
    if (!checkExist(segId, copyPageSampleInfo)) {
      return false;
    }
    Long lastIoTime = mapArchiveToLastIoTime.get(copyPageSampleInfo.getArchive());
    boolean hasIo =
        (System.currentTimeMillis() - (lastIoTime == null ? 0 : lastIoTime.longValue())) < 1000;
    MigrationStrategy strategy = copyPageSampleInfo.getArchive().getArchiveMetadata()
        .getMigrationStrategy();
    return allStrategies.get(strategy).addAlready(segId, count, hasIo, copyPageSampleInfo);
  }

  public int throttle(SegId segId, int copyCount) {
    CopyPageSampleInfo copyPageSampleInfo = mapSegIdToCopyPageSampleInfo.get(segId);
    if (!checkExist(segId, copyPageSampleInfo)) {
      return 0;
    }
    MigrationStrategy strategy = copyPageSampleInfo.getArchive().getArchiveMetadata()
        .getMigrationStrategy();
    return allStrategies.get(strategy).throttle(segId, copyCount, copyPageSampleInfo);
  }

  @Override
  public void markNormalIoComes(RawArchive archive, boolean isWrite) {
    mapArchiveToLastIoTime.put(archive, System.currentTimeMillis());
  }

  private Object getLockForArchive(RawArchive archive) {
    return mapArchiveToLock.computeIfAbsent(archive, k -> new Object());
  }

  @Override
  public Collection<CopyPageSampleInfo> getCopyPageSampleInfos() {
    return mapSegIdToCopyPageSampleInfo.values().stream()
        .filter(
            item -> IoType.getIoType(item.getSessionId()) == IoType.CopyPage && item.getTotal() > 0)
        .collect(Collectors.toList());
  }

  private boolean checkExist(SegId segId, CopyPageSampleInfo copyPageSampleInfo) {
    if (copyPageSampleInfo == null) {
      logger.warn("seg {} was aborted or finished before!", segId, new Exception());
      return false;
    }
    return true;
  }

}

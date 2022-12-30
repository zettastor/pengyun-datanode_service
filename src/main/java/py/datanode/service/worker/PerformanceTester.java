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

package py.datanode.service.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveOptions;
import py.archive.segment.CloneType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.common.NamedThreadFactory;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveManager;
import py.datanode.archive.RawArchiveManagerImpl;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitCanDeletingCheck;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.LogIdGenerator;
import py.datanode.segment.datalog.LogIdWindow;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.persist.LogPersister;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.exception.NoAvailableBufferException;
import py.exception.StorageException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.third.rocksdb.KvStoreException;
import py.volume.CacheType;
import py.volume.VolumeType;

public class PerformanceTester {
  private static final Logger logger = LoggerFactory.getLogger(PerformanceTester.class);

  private static final Long VOLUME_ID = 1L;

  private final AtomicLong uuidGenerator = new AtomicLong(1L);

  private final PlalEngine plalEngine;
  private final LogPersister logPersister;
  private final DataNodeConfiguration cfg;
  private final RawArchiveManager archiveManager;
  private final SegmentUnitTaskExecutor catchupLogEngine;
  private final SegmentUnitManager segmentUnitManager;
  private final InstanceId myself;
  private final AtomicBoolean withSnapshot = new AtomicBoolean(false);
  private final SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck;
  private long preUuid = 0;

  public PerformanceTester(PlalEngine plalEngine, LogPersister logPersister,
      DataNodeConfiguration cfg,
      RawArchiveManager archiveManager, SegmentUnitTaskExecutor catchupLogEngine,
      SegmentUnitManager segmentUnitManager, InstanceId myself,
      SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck) {
    this.plalEngine = plalEngine;
    this.logPersister = logPersister;
    this.cfg = cfg;
    this.archiveManager = archiveManager;
    this.catchupLogEngine = catchupLogEngine;
    this.segmentUnitManager = segmentUnitManager;
    this.myself = myself;
    this.segmentUnitCanDeletingCheck = segmentUnitCanDeletingCheck;
  }

  public void start() {
    Thread thread = new Thread(this::job, "performance-tester");
    thread.start();

    ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("performance-monitor"));
    scheduler.scheduleAtFixedRate(this::monitor, 0, 1, TimeUnit.SECONDS);
  }

  public void monitor() {
    long currentUuid = uuidGenerator.get();
    logger.warn("{} logs done", currentUuid - preUuid);
    preUuid = currentUuid;

    for (SegmentUnit segmentUnit : segmentUnitManager
        .get(s -> s.getSegId().getVolumeId().getId() == VOLUME_ID)) {
      SegmentLogMetadata logMetadata = segmentUnit.getSegmentLogMetadata();
      logMetadata.setSwplIdTo(logMetadata.getClId());
      logMetadata.setSwplIdTo(logMetadata.getPlId());
    }
  }

  public void job() {
    List<SegmentUnit> segmentUnitList = new ArrayList<>();
    int index = 0;
    for (RawArchive rawArchive : archiveManager.getRawArchives()) {
      SegId segId = new SegId(VOLUME_ID, index++);
      try {
        SegmentUnit segmentUnit = createSegmentUnit(segId, rawArchive);
        segmentUnitList.add(segmentUnit);
      } catch (Exception e) {
        logger.error("error adding segment unit", e);
        System.exit(-1);
      }
    }

    logger.warn("created segment units {}", segmentUnitList);

    int step = 16;
    while (true) {
      for (int i = 0; i < ArchiveOptions.PAGE_NUMBER_PER_SEGMENT; i += step) {
        for (SegmentUnit segmentUnit : segmentUnitList) {
          List<MutationLogEntry> logs = new ArrayList<>();
          for (int j = 0; j < step; j++) {
            try {
              logs.add(createLog(segmentUnit, (i + j) * cfg.getPageSize(), cfg.getPageSize(),
                  0));
            } catch (NoAvailableBufferException ignore) {
              logger.info("no fast buffer");
            }
          }
          try {
            insertLogs(segmentUnit, logs);
          } catch (LogIdTooSmall logIdTooSmall) {
            logger.error("error insert logs", logIdTooSmall);
            System.exit(-1);
          }
        }
      }
    }
  }

  private void insertLogs(SegmentUnit segmentUnit, Collection<MutationLogEntry> logsToBeInserted)
      throws LogIdTooSmall {
    List<MutationLogEntry> logsThatCanBeApplied = new LinkedList<>();
    SegmentLogMetadata segLogMetadata = segmentUnit.getSegmentLogMetadata();
    segLogMetadata.insertOrUpdateLogs(logsToBeInserted, false, logsThatCanBeApplied);

    for (MutationLogEntry logToApply : logsThatCanBeApplied) {
      plalEngine.putLog(segLogMetadata.getSegId(), logToApply);
    }
  }

  private MutationLogEntry createLog(SegmentUnit segmentUnit, long offset, int length,
      int snapshotVersion)
      throws NoAvailableBufferException {
    long uuid = uuidGenerator.getAndIncrement();
    MutationLogEntry log = MutationLogEntryFactory
        .createLogForPrimary(uuid,
            segmentUnit.getSegmentLogMetadata().getNewOrOldLogId(uuid, true).getFirst(),
            segmentUnit.getArchive().getArchiveId(), offset, length, 0L,  2);
    log.commit();
    return log;
  }

  private SegmentUnit createSegmentUnit(SegId segId, RawArchive archive) throws Exception {
    SegmentUnit segUnit = archive.addSegmentUnit(segId,
        new SegmentMembership(myself, Arrays.asList(new InstanceId(2L), new InstanceId(3L))),
        VolumeType.REGULAR,  null, null, SegmentUnitType.Normal,
        0L, null, false, null);
    segmentUnitManager.put(segUnit);

    segUnit.getSegmentUnitMetadata().setStatus(SegmentUnitStatus.Primary);
    segUnit.getArchive().persistSegmentUnitMetaAndBitmap(segUnit.getSegmentUnitMetadata());
    segUnit.setPlalEngine(plalEngine);

    SegmentLogMetadata logMetadata = new SegmentLogMetadata(segId, cfg.getPageSize(),
        segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize(),
        Optional.of(cfg.getGiveYouLogIdTimeout()),
        new LogIdWindow(cfg.getMaxLogIdWindowSize(), cfg.getMaxLogIdWindowSplitCount()));
    segUnit.setSegmentLogMetadata(logMetadata);
    logMetadata.updateLogIdGenerator(new LogIdGenerator(0));

    logPersister.removeLogMetaData(segId);
    logPersister.persistLog(segId, MutationLogEntry.LAZY_CLONE_INIT_LOG);
    catchupLogEngine.addSegmentUnit(segId, false);
    return segUnit;
  }

}

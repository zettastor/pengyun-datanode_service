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

package py.datanode.segment.membership.statemachine.processors;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.archive.RawArchive;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalogbak.catchup.PlalDriver;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;

public class LogFlushedChecker extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(LogFlushedChecker.class);
  // The period to check if other segment units are still OK. If not
  private static final long CHECK_ROLLBACK_PERIOD = 1000L;
  // OK, need archive to rollback to GOOD status
  // Delay to check whether segment unit can be offlined again
  private static final long DEFAULT_FIXED_DELAY = 5000L;

  // store the archive id and last time to check archive need to roll back to GOOD status;
  public static Map<Long, Long> archiveWithLastTimeCheckNeedRollBack =
      new ConcurrentHashMap<Long, Long>();

  public LogFlushedChecker(StateProcessingContext context) {
    super(context);
  }

  @Override
  public StateProcessingResult process(StateProcessingContext context) {
    SegId segId = context.getSegId();
    long archiveId = context.getSegmentUnit().getArchive().getArchiveId();
    SegmentUnit segUnit = context.getSegmentUnit();
    SegmentUnitStatus currentStatus = segUnit.getSegmentUnitMetadata().getStatus();

    if (currentStatus != SegmentUnitStatus.OFFLINING) {
      logger.debug("current segment unit is not offling {}", context);
      return getFailureResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
    }

    if (segUnit.isArbiter()) {
      return becomeOfflined(context, segId);
    }

    if (cfg.isNeedCheckRollbackInOffling()) {
      Long lastTimeCheckRollBack = archiveWithLastTimeCheckNeedRollBack.get(archiveId);
      if (lastTimeCheckRollBack == null
          || System.currentTimeMillis() - lastTimeCheckRollBack > CHECK_ROLLBACK_PERIOD) {
        logger.debug("check if need to roll back: archiveId {}", archiveId);
        if (needToRollBack(context.getSegmentUnit().getArchive())) {
          logger.warn("something wrong with peer, the archive need to rollback to GOOD status");
          try {
            this.archiveManager.getRawArchive(archiveId).setArchiveStatus(ArchiveStatus.GOOD);
            archiveWithLastTimeCheckNeedRollBack.remove(archiveId);
          } catch (Exception e) {
            logger.error("when set archive to GOOD status catch an exception", e);
          }
          return getFailureResultWithZeroDelay(context, SegmentUnitStatus.Start);
        }
      }
    }

    SegmentLogMetadata logMetadata = this.mutationLogManager.getSegment(segId);

    if (logMetadata != null) {
      LogImage logImage = logMetadata.getLogImage(0);
      logger.debug("segId and logImage are {} {}", segId, logImage);

      if (!logImage.allLogsFlushedToDisk()) {
        PlalDriver.drive(logMetadata, segUnit, plalEngine);
        logger.warn(
            "for segment {}, Either PL is not same as LAL or LAL is not same as CL. logImage is {}",
            segId, logImage);
        return getSuccesslResultWithFixedDelay(context, SegmentUnitStatus.OFFLINING,
            DEFAULT_FIXED_DELAY);
      }
    }

    return becomeOfflined(context, segId);
  }

  private StateProcessingResult becomeOfflined(StateProcessingContext context, SegId segId) {
    try {
      logger.warn("the seg with segId {} can be in OFFLINED state", segId);
      archiveManager
          .updateSegmentUnitMetadata(context.getSegId(), null, SegmentUnitStatus.OFFLINED);
    } catch (Exception e) {
      logger.error("when update segment unit {} status, catch an exception", context, e);
    }

    return getSuccesslResultWithZeroDelay(context, SegmentUnitStatus.OFFLINED);
  }

  private boolean needToRollBack(RawArchive archive) {
    return false;
  }
}

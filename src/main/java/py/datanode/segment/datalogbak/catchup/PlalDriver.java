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

package py.datanode.segment.datalogbak.catchup;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.plal.engine.PlalEngine;

/**
 * A static class that is used to drive plal to pcl.
 *
 */
public class PlalDriver {
  private static final Logger logger = LoggerFactory.getLogger(PlalDriver.class);

  /**
   * Driver plal. the returned value is the number of pages that have been submitted.
   *
   */
  public static int drive(SegmentLogMetadata segLogMetadata, SegmentUnit segmentUnit,
      PlalEngine plalEngine) {
    long lalId = segLogMetadata.getLalId();
    long clId = segLogMetadata.getClId();
    if (lalId == clId) {
      return 0;
    }

    logger.info("driving plal for segId:{}, plal:{}, clId: {}", segLogMetadata.getSegId(),
        lalId, clId);

    int numLogsWillBeApplied = 0;
    try {
      Set<Integer> pagesHavingUnAppliedLogs = segLogMetadata
          .movePlalAndGetCommittedYetAppliedLogs(plalEngine.getMaxNumberOfPagesToApplyPerDrive());

      numLogsWillBeApplied = pagesHavingUnAppliedLogs.size();
      if (numLogsWillBeApplied > 0) {
        logger.debug("submit {} pages to worker thread: {}", numLogsWillBeApplied,
            segLogMetadata.getSegId());
        for (int pageIndex : pagesHavingUnAppliedLogs) {
          PageAddress pageAddress = segmentUnit.getLogicalPageAddressToApplyLog(pageIndex);
          plalEngine.putLog(pageAddress);
        }
      }

      logger.debug("We have submitted at lease {} logs at segment {} to be applied",
          numLogsWillBeApplied,
          segLogMetadata.getSegId());
      logger.debug("after driving plal for segId:{}, plal:{}, clId: {}", segLogMetadata.getSegId(),
          segLogMetadata.getLalId(), segLogMetadata.getClId());
    } catch (Throwable t) {
      logger.error("PLAL self driving failed at the segment unit {} ", segmentUnit.getSegId(), t);
    } finally {
      logger.info("nothing need to do here");
    }
    return numLogsWillBeApplied;
  }
}

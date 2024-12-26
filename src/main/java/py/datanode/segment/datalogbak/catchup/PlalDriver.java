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

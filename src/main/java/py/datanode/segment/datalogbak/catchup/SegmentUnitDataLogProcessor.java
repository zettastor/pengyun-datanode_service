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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.recurring.SegmentUnitProcessResult;
import py.archive.segment.recurring.SegmentUnitProcessor;
import py.archive.segment.recurring.SegmentUnitTaskContext;

public class SegmentUnitDataLogProcessor extends SegmentUnitProcessor {
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitDataLogProcessor.class);

  private final LogDriver firstLogDriver;

  public SegmentUnitDataLogProcessor(SegmentUnitTaskContext context, LogDriver logDriver) {
    super(context);
    firstLogDriver = logDriver;
  }

  public SegmentUnitProcessResult process() {
    SegmentUnitProcessResult result = new SegmentUnitProcessResult(getContext());
    long delayToExecute = LogDriver.ExecuteLevel.SLOWLY.getDelay();

    if (firstLogDriver == null) {
      logger.info("no log driver, do nothing and sleep {} ms", delayToExecute);
      result.setExecutionSuccess(false);

      result.setDelayToExecute(delayToExecute);
      return result;
    }

    try {
      LogDriver.ExecuteLevel executeLvl = firstLogDriver.drive();
      result.setExecutionSuccess(true);
      delayToExecute = executeLvl.getDelay();
    } catch (Throwable t) {
      logger.warn("caught unknown exception when driver data logs", t);
      result.setExecutionSuccess(false);
      result.setExecutionException(t);
    }
    result.setDelayToExecute(delayToExecute);
    return result;
  }
}

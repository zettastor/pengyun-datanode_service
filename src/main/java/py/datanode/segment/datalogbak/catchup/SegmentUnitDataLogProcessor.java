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

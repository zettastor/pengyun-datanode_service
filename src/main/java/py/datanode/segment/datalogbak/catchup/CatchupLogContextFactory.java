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

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.recurring.SegmentUnitProcessResult;
import py.archive.segment.recurring.SegmentUnitTaskContext;
import py.archive.segment.recurring.SegmentUnitTaskContextFactory;
import py.archive.segment.recurring.SegmentUnitTaskType;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;

public class CatchupLogContextFactory implements SegmentUnitTaskContextFactory {
  private static final Logger logger = LoggerFactory.getLogger(CatchupLogContextFactory.class);
  private static final int PCL_DELAY_TIME_AFTER_REJECTION = 25;
  private static final int PPL_DELAY_TIME_AFTER_REJECTION = 30;
  private final SegmentUnitManager segmentUnitManager;

  public CatchupLogContextFactory(SegmentUnitManager segmentUnitManager) {
    this.segmentUnitManager = segmentUnitManager;
  }

  @Override
  public List<SegmentUnitTaskContext> generateProcessingContext(SegId segId) {
    List<SegmentUnitTaskContext> contexts = new ArrayList<SegmentUnitTaskContext>(1);
    CatchupLogContextKey keyForPclDriver = new CatchupLogContextKey(segId,
        CatchupLogContextKey.CatchupLogDriverType.PCL);
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);

    CatchupLogContext context = new CatchupLogContext(keyForPclDriver, segmentUnit);
    context.setType(SegmentUnitTaskType.PCL);
    context.setDelayAfterReject(PCL_DELAY_TIME_AFTER_REJECTION);
    contexts.add(context);
    logger.debug("add context {}", context);
    return contexts;
  }

  @Override
  public List<SegmentUnitTaskContext> generateProcessingContext(SegmentUnitProcessResult result) {
    CatchupLogContext context = (CatchupLogContext) result.getContext();
    List<SegmentUnitTaskContext> contexts = new ArrayList<SegmentUnitTaskContext>();

    if (!result.executionSuccess()) {
      context.incFailureTimes();
    }
    context.updateDelay(result.getDelayToExecute());
    contexts.add(context);

    logger.debug("context is {}", context);

    if (context.getKey().getLogDriverType() == CatchupLogContextKey.CatchupLogDriverType.PCL
        && context.isFirstTime()) {
      CatchupLogContextKey keyForPplDriver = new CatchupLogContextKey(context.getSegId(),
          CatchupLogContextKey.CatchupLogDriverType.PPL);

      CatchupLogContext pplContext = new CatchupLogContext(keyForPplDriver,
          context.getSegmentUnit());
      pplContext.updateDelayWithForce(0L);
      pplContext.setDelayAfterReject(PPL_DELAY_TIME_AFTER_REJECTION);
      pplContext.setType(SegmentUnitTaskType.PPL);
      contexts.add(pplContext);
      logger.debug("add PPL context {}", pplContext);

      context.setFirstTime(false);
    }
    return contexts;
  }
}

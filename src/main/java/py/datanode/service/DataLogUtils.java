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

package py.datanode.service;

import com.google.common.collect.RangeSet;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs.PlanToApplyLog;

public class DataLogUtils {
  private static final Logger logger = LoggerFactory.getLogger(DataLogUtils.class);

  public static List<MutationLogEntry> applyLogs(byte[] readBufferToReturn, int offsetAtPage,
      int length, int pageSize,
      PlansToApplyLogs plans, PageAddress pageAddress, SegmentUnit segmentUnit)
      throws Exception {
    List<MutationLogEntry> logsToApply = new ArrayList<MutationLogEntry>();
    if (plans == null || plans.getPlans().size() == 0) {
      return logsToApply;
    }

    if (plans.isMigrating()) {
      Validate.isTrue(false,
          "I am a primary" + segmentUnit.getSegId()
              + ", and I should not have any migration going on");
    }

    long largerId = Long.MAX_VALUE;
    logger.debug("there are {} logs in the same page {}", plans.getPlans().size(), pageAddress);

    for (PlanToApplyLog plan : plans.getPlans()) {
      MutationLogEntry logToApply = plan.getLog();
      Validate.notNull(logToApply);
      logger.info("apply the log: {}, the plan {}", logToApply, plan);
      logsToApply.add(logToApply);

      RangeSet<Integer> rangeSet = plan.getRangesToApply();
      if (logToApply.isApplied()) {
        if (!(rangeSet == null || rangeSet.isEmpty())) {
          logger.error("log has been applied but its ranges are not empty {} ", plan);
        }
      } else {
        try {
          long id = logToApply.getLogId();
          Validate.isTrue(logToApply.isFinalStatus() && largerId > id);

          logToApply.applyLogData(readBufferToReturn, offsetAtPage, length, pageSize, rangeSet);

          largerId = id;
        } catch (Exception e) {
          logger.error("Fail to applying log {} to the page {} and the ranges are {}, plans: {}",
              logToApply,
              pageAddress, rangeSet, plans.getPlans(), e);
          throw e;
        }
      }
    }

    return logsToApply;
  }

  public static List<MutationLogEntry> applyLogs(ByteBuf readBufferToReturn, int offsetAtPage,
      int length, int pageSize,
      PlansToApplyLogs plans, PageAddress pageAddress, SegmentUnit segmentUnit)
      throws Exception {
    List<MutationLogEntry> logsToApply = new ArrayList<MutationLogEntry>();
    if (plans == null || plans.getPlans().size() == 0) {
      return logsToApply;
    }

    if (plans.isMigrating()) {
      Validate.isTrue(false,
          "I am a primary " + segmentUnit.getSegId()
              + ", and I should not have any migration going on");
    }

    long largerId = Long.MAX_VALUE;
    logger.debug("there are {} logs in the same page {}", plans.getPlans().size(), pageAddress);

    for (PlanToApplyLog plan : plans.getPlans()) {
      MutationLogEntry logToApply = plan.getLog();
      Validate.notNull(logToApply);
      logger.info("apply the log: {}, the plan {}", logToApply, plan);
      logsToApply.add(logToApply);

      RangeSet<Integer> rangeSet = plan.getRangesToApply();
      if (logToApply.isApplied()) {
        if (!(rangeSet == null || rangeSet.isEmpty())) {
          logger.error("log has been applied but its ranges are not empty {} ", plan);
        }
      } else {
        try {
          logToApply.applyLogData(readBufferToReturn, offsetAtPage, length, pageSize, rangeSet);
        } catch (Exception e) {
          logger.error("Fail to applying log {} to the page {} and the ranges are {}, plans: {}",
              logToApply,
              pageAddress, rangeSet, plans.getPlans(), e);
          throw e;
        }
      }
    }

    return logsToApply;
  }
}

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

package py.datanode.segment.datalog.plal.engine;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.datanode.exception.InvalidLogStatusException;
import py.datanode.exception.LogsNotInRightOrderException;
import py.datanode.exception.LogsNotInSamePageException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs.PlanToApplyLog;
import py.datanode.segment.datalog.plal.engine.PlansToApplyLogs.WhatToDo;

public class PlansToApplyLogsBuilder {
  private static final Logger logger = LoggerFactory.getLogger(PlansToApplyLogsBuilder.class);

  public static PlansToApplyLogs generate(PageAddress pageAddress, List<MutationLogEntry> logs,
      int pageSize, SegmentUnit segmentUnit)
      throws LogsNotInRightOrderException, LogsNotInSamePageException, InvalidLogStatusException {
    return generate(pageAddress, logs, pageSize, true, segmentUnit);
  }

  public static PlansToApplyLogs generate(PageAddress pageAddress, List<MutationLogEntry> logs,
      int pageSize,
      boolean needToApply, SegmentUnit segmentUnit)
      throws LogsNotInRightOrderException, LogsNotInSamePageException, InvalidLogStatusException {
    logger.debug("begin generate plans to apply logs size {}", logs.size());
    if (logs == null || logs.isEmpty()) {
      return new EmptyPlans();
    }

    long maxLogId = Long.MAX_VALUE;

    long pageLogicOffsetInSegment = pageAddress.getLogicOffsetInSegment(pageSize);
   
    EffectivePlans plans = new EffectivePlans();
    RangeSet<Integer> rangesCovered = TreeRangeSet.create();
    long sizeOfNeedToApply = 0;

    boolean allPlansAreEmpty = true;

    Optional<MutationLogEntry> basicLogs = Optional.empty();
   
    int numLogsHavingEmptyPlans = 0;
    for (MutationLogEntry log : logs) {
      if (log.getLogId() > maxLogId) {
        throw new LogsNotInRightOrderException(log.toString());
      }
     
      int logOffsetAtPage = (int) (log.getOffset() - pageLogicOffsetInSegment);
      final int logLengthAtPage = log.getLength();
      int logOffsetAtPageWithCloneVolume = 0;

      if (logOffsetAtPage < 0 || logOffsetAtPage >= pageSize) {
        throw new LogsNotInSamePageException("log is " + log.toString() + " logOffsetAtPage is "
            + logOffsetAtPage + " page size is " + pageSize);
      }

      if (log.getStatus() != LogStatus.Committed) {
        throw new InvalidLogStatusException(log.toString());
      }
      maxLogId = log.getLogId();
      
     
      logger.debug("begin calculate one log range, offset {}, length {}, log is applied {}",
          logOffsetAtPage, logLengthAtPage, log.isApplied());
      Range<Integer> dataRangeForPageData = Range
          .closedOpen(logOffsetAtPage, logOffsetAtPage + logLengthAtPage);
      if (!log.isApplied()) {
        RangeSet<Integer> complementRangeSet = rangesCovered.complement();
       
        RangeSet<Integer> rangeSetForPageData = complementRangeSet
            .subRangeSet(dataRangeForPageData);

        RangeSet<Integer> tempRangeSetForLogData = null;

        if (!rangeSetForPageData.isEmpty()) {
          tempRangeSetForLogData = TreeRangeSet.create();
          for (Range<Integer> range : rangeSetForPageData.asRanges()) {
            if (range.isEmpty()) {
              continue;
            }

            int distanceBetweenLogStartAndLogRead =
                logOffsetAtPage - logOffsetAtPageWithCloneVolume;
            tempRangeSetForLogData
                .add(Range.range(range.lowerEndpoint() - distanceBetweenLogStartAndLogRead,
                    range.lowerBoundType(),
                    range.upperEndpoint() - distanceBetweenLogStartAndLogRead,
                    range.upperBoundType()));
            sizeOfNeedToApply += (range.upperEndpoint() - range.lowerEndpoint());
          }
        }

        if (tempRangeSetForLogData == null || tempRangeSetForLogData.isEmpty()) {
         
          if (needToApply && allPlansAreEmpty) {
            log.apply();
            log.setPersisted();
            MutationLogEntryFactory.releaseLogData(log);
          }
          numLogsHavingEmptyPlans++;
        } else {
         
          allPlansAreEmpty = false;
        }
       
        plans.addPlan(log, tempRangeSetForLogData);
      }
      rangesCovered.add(dataRangeForPageData);
    }

    if (numLogsHavingEmptyPlans == plans.getPlans().size()) {
      return new EmptyPlans();
    } else if (plans.isEmpty()) {
      return new EmptyPlans();
    }

    if (sizeOfNeedToApply == pageSize) {
      plans.setWhatToDo(WhatToDo.NoNeedToLoadPage);
    } else {
      plans.setWhatToDo(WhatToDo.LoadPage);
    }

    return plans;
  }

  protected static class EmptyPlans implements PlansToApplyLogs {
    @Override
    public WhatToDo getWhatToDo() {
      return WhatToDo.Nothing;
    }

    @Override
    public String toString() {
      return "EmptyPlans []";
    }

    @Override
    public List<? extends PlanToApplyLog> getPlans() {
      return new ArrayList<DummyPlanToApplyLog>();
    }

    public int getSnapshotId() {
      throw new NotImplementedException("this is empty Plans");
    }

    public void setSnapshotId(int snapshotId) {
      throw new NotImplementedException("this is empty Plans");
    }

    @Override
    public boolean isMigrating() {
      throw new NotImplementedException("this is empty Plans");
    }

    @Override
    public void canApplyWhenMigrating(boolean migrating) {
      throw new NotImplementedException("this is empty Plans");
    }

    @Override
    public WhatToSave getWhatToSave() {
      return WhatToSave.Nothing;
    }

    @Override
    public void setWhatToSave(WhatToSave whatToSave) {
      throw new NotImplementedException("this is empty Plans");
    }

  }

  private static class EffectivePlans implements PlansToApplyLogs {
    private WhatToDo whatToDo;
    private WhatToSave whatToSave;
    private List<Plan> plans;
    private boolean migrating;
    private MultiPageAddress logicalAddress;
    private ChunkLogsCollection chunkLogsCollection;

    EffectivePlans() {
      this.whatToDo = null;
      this.whatToSave = WhatToSave.Nothing;
      this.plans = new ArrayList<Plan>();
    }

    void addPlan(MutationLogEntry log, RangeSet<Integer> ranges) {
     
      if (log == null) {
        throw new RuntimeException("log or ranges can't be null");
      }

      plans.add(new Plan(log, ranges));
    }

    @Override
    public WhatToDo getWhatToDo() {
      return this.whatToDo;
    }

    void setWhatToDo(WhatToDo whatToDo) {
      this.whatToDo = whatToDo;
    }

    @Override
    public List<? extends PlanToApplyLog> getPlans() {
      return this.plans;
    }

    @Override
    public String toString() {
      return "EffectivePlans{"
          + "whatToDo=" + whatToDo
          + ", whatToSave=" + whatToSave
          + ", plans=" + plans
          + ", migrating=" + migrating
          + '}';
    }

    public boolean isEmpty() {
      return plans == null || plans.isEmpty();
    }

    public boolean isMigrating() {
      return migrating;
    }

    public void canApplyWhenMigrating(boolean migrating) {
      this.migrating = migrating;
    }

    @Override
    public WhatToSave getWhatToSave() {
      return whatToSave;
    }

    @Override
    public void setWhatToSave(WhatToSave whatToSave) {
      this.whatToSave = whatToSave;
    }

    @Override
    public ChunkLogsCollection getChunkLogsCollection() {
      return chunkLogsCollection;
    }

    @Override
    public long getMaxLogId() {
      return plans.size() > 0 ? plans.get(plans.size() - 1).getLog().getLogId() : -1;
    }

    private class Plan implements PlanToApplyLog {
      final MutationLogEntry log;
     
     
     
     
     
     
      final RangeSet<Integer> ranges;

      Plan(MutationLogEntry log, RangeSet<Integer> ranges) {
        this.log = log;
        this.ranges = ranges;
      }

      @Override
      public MutationLogEntry getLog() {
        return this.log;
      }

      @Override
      public RangeSet<Integer> getRangesToApply() {
        return this.ranges;
      }

      @Override
      public String toString() {
        return "Plan [log=" + log + ", ranges=" + ranges + "]";
      }
    }

  }

  private class DummyPlanToApplyLog implements PlanToApplyLog {
    @Override
    public MutationLogEntry getLog() {
      return null;
    }

    @Override
    public RangeSet<Integer> getRangesToApply() {
      return null;
    }
  }

}

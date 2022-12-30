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

package py.datanode.segment.datalog.algorithm;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongPredicate;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.exception.DestinationNotInSamePageException;
import py.datanode.exception.LogsNotInRightOrderException;
import py.datanode.exception.LogsNotInSamePageException;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;

public abstract class DataLogsApplier {
  private static final Logger logger = LoggerFactory.getLogger(DataLogsApplier.class);

  private final SortedMap<Long, DataLog> dataLogs = new TreeMap<>(
      (o1, o2) -> -Long.compare(o1, o2));
  private final long destinationPos;
  private final int destinationLength;
  private final int pageSize;
  private LongPredicate pageClonedPredicate;

  public DataLogsApplier(long destinationPos, int destinationLength, int pageSize) {
    this.destinationPos = destinationPos;
    this.destinationLength = destinationLength;
    this.pageSize = pageSize;
  }

  public static Plans generatePlans(Iterable<DataLog> logs, long pageStartOffset, int pageSize,
      LongPredicate pageClonedPredicate)
      throws LogsNotInRightOrderException, LogsNotInSamePageException {
    RangeSet<Integer> rangesCovered = TreeRangeSet.create();
    long maxLogId = Long.MAX_VALUE;

    Plans plans = new EffectivePlans();

    DataLog basicLogForClone = null;
    RangeSet<Integer> rangeForBasicLog = null;

    for (DataLog log : logs) {
      if (log.getLogId() > maxLogId) {
        throw new LogsNotInRightOrderException(logs.toString());
      }

      if (log.getLogStatus() != LogStatus.Committed) {
        logger.error("a not committed log ?? {}", log);
        continue;
      }

      int logOffsetAtPage = (int) (log.getOffset() - pageStartOffset);
      int logLengthAtPage = log.getLength();

      if (logOffsetAtPage < 0 || logOffsetAtPage >= pageSize) {
        throw new LogsNotInSamePageException(
            String
                .format("log %s, pageStartOffset %s, pageSize %s", log, pageStartOffset, pageSize));
      }

      maxLogId = log.getLogId();
      Range<Integer> dataRangeForPageData = Range
          .closedOpen(logOffsetAtPage, logOffsetAtPage + logLengthAtPage);
      if (!log.isApplied()) {
        RangeSet<Integer> complementRangeSet = rangesCovered.complement();
        RangeSet<Integer> rangeSetForPageData = complementRangeSet
            .subRangeSet(dataRangeForPageData);

        if (basicLogForClone == log) {
         
          rangeForBasicLog = TreeRangeSet.create(rangeSetForPageData);
        } else if (!rangeSetForPageData.isEmpty()) {
          plans.addPlan(log, ImmutableRangeSet.copyOf(rangeSetForPageData));
        } else {
          plans.addPlan(log, null);
        }
      }
      rangesCovered.add(dataRangeForPageData);

    }

    if (basicLogForClone != null) {
      RangeSet<Integer> complementRangeSet = rangesCovered.complement()
          .subRangeSet(Range.closedOpen(0, pageSize));
      rangeForBasicLog.addAll(complementRangeSet);
      plans.addPlan(basicLogForClone, ImmutableRangeSet.copyOf(rangeForBasicLog));
      plans.setWholePageCovered(true);
    } else {
      plans.setWholePageCovered(rangesCovered.encloses(Range.closedOpen(0, pageSize)));
    }

    return plans;
  }

  public void setPageClonedPredicate(LongPredicate pageClonedPredicate) {
    this.pageClonedPredicate = pageClonedPredicate;
  }

  public boolean addLog(DataLog log) {
    return dataLogs.putIfAbsent(log.getLogId(), log) == null;
  }

  public boolean containsLog(long logId) {
    return dataLogs.containsKey(logId);
  }

  public CompletableFuture<Void> apply() throws LogsNotInSamePageException,
      LogsNotInRightOrderException, DestinationNotInSamePageException {
    if (dataLogs.isEmpty()) {
      logger.warn("empty logs, just return");
      return CompletableFuture.completedFuture(null);
    }

    long pageStartOffset = destinationPos / pageSize * pageSize;

    if (pageStartOffset + pageSize < destinationPos + destinationLength) {
      throw new DestinationNotInSamePageException(String
          .format("destination %s, length %s, page size %s", destinationPos, destinationLength,
              pageSize));
    }

    int destinationOffset = (int) (destinationPos % pageSize);

    Range<Integer> destinationRange = Range
        .closedOpen(destinationOffset, destinationOffset + destinationLength);
    Plans plans = generatePlans(dataLogs.values(), pageStartOffset, pageSize, pageClonedPredicate);
    return loadPageData(plans.isWholePageCovered()).thenRun(() -> {
      for (Plan plan : plans.getPlans()) {
        DataLog log = plan.getLog();
        RangeSet<Integer> range = plan.getRangesToApply();
        if (range == null) {
          logger.debug("this log is being skipped {}", log);
          continue;
        }
        for (Range<Integer> rangeToApplyLog : range.asRanges()) {
          

          if (!destinationRange.isConnected(rangeToApplyLog)) {
            continue;
          }

          Range<Integer> wantedRange = destinationRange.intersection(rangeToApplyLog);

          if (wantedRange.isEmpty()) {
            continue;
          }

          int bufOffset = wantedRange.lowerEndpoint() - destinationRange.lowerEndpoint();

          int logOffsetInPage = (int) (log.getOffset() - pageStartOffset);
          int offsetInsideLog = wantedRange.lowerEndpoint() - logOffsetInPage;

          int wantedLength = wantedRange.upperEndpoint() - wantedRange.lowerEndpoint();

          logger.debug("applying log {}", log);
          applyLogData(log, bufOffset, offsetInsideLog, wantedLength);
        }
      }
    });
  }

  protected abstract CompletableFuture<Void> loadPageData(boolean wholePageCovered);

  protected abstract void applyLogData(DataLog log, int offsetInDestination,
      int offsetInLog, int length);

  public interface Plans {
    List<? extends Plan> getPlans();

    void addPlan(DataLog log, RangeSet<Integer> range);

    boolean isWholePageCovered();

    void setWholePageCovered(boolean wholePageCovered);
  }

  public static class EffectivePlans implements Plans {
    private boolean isWholePageCovered = false;
    private List<Plan> plans = new ArrayList<>();

    @Override
    public List<? extends Plan> getPlans() {
      return plans;
    }

    @Override
    public void addPlan(DataLog log, RangeSet<Integer> range) {
      plans.add(new Plan(log, range));
    }

    @Override
    public boolean isWholePageCovered() {
      return isWholePageCovered;
    }

    @Override
    public void setWholePageCovered(boolean wholePageCovered) {
      this.isWholePageCovered = wholePageCovered;
    }
  }

  public static class Plan {
    private DataLog log;

    private RangeSet<Integer> rangesToApply;

    public Plan(DataLog log, RangeSet<Integer> rangesToApply) {
      this.log = log;
      this.rangesToApply = rangesToApply;
    }

    public DataLog getLog() {
      return log;
    }

    public RangeSet<Integer> getRangesToApply() {
      return rangesToApply;
    }
  }

}

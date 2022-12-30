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

package py.datanode.segment.datalog.algorithm.merger;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang.Validate;
import py.common.struct.Pair;
import py.datanode.segment.datalog.algorithm.CommittedYetAppliedLog;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.instance.InstanceId;

public class TempPrimaryDataLogsMerger implements DataLogsMerger {
  private final TreeMap<Long, LogsHolder> logs = new TreeMap<>();

  public void addLog(InstanceId instance, DataLog log) {
    Validate.isTrue(!log.isApplied());

    LogsHolder logsHolder = logs
        .computeIfAbsent(log.getLogId(), whatever -> new LogsHolder(log.getLogId()));
    logsHolder.logs.add(log);
  }

  public MergeLogIterator iterator(long maxLogIdToMerge) {
    final Iterator<LogsHolder> mapIt = logs.values().iterator();

    return new MergeLogIterator() {
      @Override
      public boolean hasNext() {
        return mapIt.hasNext() && logs.floorKey(maxLogIdToMerge) != null;
      }

      @Override
      public Pair<DataLog, Set<InstanceId>> next() {
        LogsHolder log = mapIt.next();
        if (log.logId > maxLogIdToMerge) {
          throw new IllegalArgumentException();
        }
        return new Pair<>(log.merge(), new HashSet<>());
      }
    };

  }

  private class LogsHolder implements Comparable<LogsHolder> {
    final long logId;
    final List<DataLog> logs = new ArrayList<>();

    private LogsHolder(long logId) {
      this.logId = logId;
    }

    private DataLog merge() {
      if (logs.isEmpty()) {
        return null;
      }

      DataLog firstLog = logs.get(0);
      return new CommittedYetAppliedLog(firstLog.getLogId(), firstLog.getOffset(),
          firstLog.getLength(), firstLog.getLogUuid()) {
        @Override
        public void getData(ByteBuffer destination, int offset, int length) {
          firstLog.getData(destination, offset, length);
        }

        @Override
        public void getData(ByteBuf destination, int offset, int length) {
          firstLog.getData(destination, offset, length);
        }
      };
    }

    @Override
    public int compareTo(LogsHolder o) {
      return Long.compare(logId, o.logId);
    }
  }

}

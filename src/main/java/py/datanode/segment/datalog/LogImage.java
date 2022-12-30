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

package py.datanode.segment.datalog;

import java.util.List;
import py.common.DoubleLinkedList.DoubleLinkedListIterator;
import py.common.DoubleLinkedList.ListNode;

public class LogImage {
  public static final long INVALID_LOG_ID = 0;
  public static final long IMPOSSIBLE_LOG_ID = -1;
  public static final long DISCARD_LOG_ID = 1;
  private final DoubleLinkedListIterator<ListNode<MutationLogEntry>> ppl;
  private final DoubleLinkedListIterator<ListNode<MutationLogEntry>> plal;
  private final DoubleLinkedListIterator<ListNode<MutationLogEntry>> pcl;
  private final long swplId;
  private final long swclId;
  private final long maxLogId;

  private final List<Long> uncommittedLogIds;

  private final List<Long> uuidsOfLogsWithoutLogId;

  private final int numLogsFromPplToPlal;
  private final int numLogsFromPlalToPcl;
  private final int numLogsAfterPcl;

  public LogImage(long swplId, long swclId,
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> ppl,
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> plal,
      DoubleLinkedListIterator<ListNode<MutationLogEntry>> pcl, int numLogsFromPplToPlal,
      int numLogsFromPlalToPcl, int numLogsAfterPcl, List<Long> logIds,
      List<Long> uuidsOfLogsWithoutLogId,
      long maxLogId) {
    this.swplId = swplId;
    this.swclId = swclId;
    this.ppl = ppl;
    this.plal = plal;
    this.pcl = pcl;
    this.numLogsAfterPcl = numLogsAfterPcl;
    this.numLogsFromPlalToPcl = numLogsFromPlalToPcl;
    this.numLogsFromPplToPlal = numLogsFromPplToPlal;
    this.uncommittedLogIds = logIds;
    this.uuidsOfLogsWithoutLogId = uuidsOfLogsWithoutLogId;
    this.maxLogId = maxLogId;
  }

  public MutationLogEntry getPl() {
    return ppl.content() == null ? null : ppl.content().content();
  }

  public MutationLogEntry getLal() {
    return plal.content() == null ? null : plal.content().content();
  }

  public MutationLogEntry getCl() {
    return pcl.content() == null ? null : pcl.content().content();
  }

  public DoubleLinkedListIterator<ListNode<MutationLogEntry>> getCursorFromPlToLal() {
    return new DoubleLinkedListIterator<ListNode<MutationLogEntry>>(ppl, ppl.getCurrentPosition(),
        plal.getCurrentPosition());
  }

  public DoubleLinkedListIterator<ListNode<MutationLogEntry>> getCursorFromLalToCl() {
    return new DoubleLinkedListIterator<ListNode<MutationLogEntry>>(plal, plal.getCurrentPosition(),
        pcl.getCurrentPosition());
  }

  public DoubleLinkedListIterator<ListNode<MutationLogEntry>> getCursorFromLalToEnd() {
    return new DoubleLinkedListIterator<ListNode<MutationLogEntry>>(plal);
  }

  public long getPreviousGapLogsFromClToLal(int gapOfLogs) {
    long logId = INVALID_LOG_ID;
    DoubleLinkedListIterator<ListNode<MutationLogEntry>> cursor =
        new DoubleLinkedListIterator<ListNode<MutationLogEntry>>(pcl);
    do {
      cursor = cursor.previous();
    } while (cursor.hasPrevious() && --gapOfLogs > 0);

    return cursor.content().content().getLogId();
  }

  public long getSwclId() {
    return swclId;
  }

  public long getSwplId() {
    return swplId;
  }

  public long getClId() {
    MutationLogEntry cl = getCl();
    return cl == null ? INVALID_LOG_ID : cl.getLogId();
  }

  public long getPlId() {
    MutationLogEntry pl = getPl();
    return pl == null ? INVALID_LOG_ID : pl.getLogId();
  }

  public long getLalId() {
    MutationLogEntry lal = getLal();
    return lal == null ? INVALID_LOG_ID : lal.getLogId();
  }

  public List<Long> getUuidsOfLogsWithoutLogId() {
    return uuidsOfLogsWithoutLogId;
  }

  public List<Long> getUncommittedLogIds() {
    return uncommittedLogIds;
  }

  public String toString() {
    return "LogImage [ppl=" + getPlId() + ", plal=" + getLalId() + ", pcl=" + getClId()
        + ", swplId=" + swplId
        + ", swclId=" + swclId + ", numLogsFromPPLToPLAL=" + numLogsFromPplToPlal
        + ", numLogsFromPLALToPCL="
        + numLogsFromPlalToPcl + ", numLogsAfterPCL=" + numLogsAfterPcl + ", notInsertedLogs="
        + uuidsOfLogsWithoutLogId.size() + ", maxLogId=" + maxLogId + "]";
  }

  public int getNumLogsFromPplToPlal() {
    return numLogsFromPplToPlal;
  }

  public int getNumLogsFromPlalToPcl() {
    return numLogsFromPlalToPcl;
  }

  public int getNumLogsAfterPcl() {
    return numLogsAfterPcl;
  }

  public boolean isLogsFlushedForRollback() {
    return this.getNumLogsAfterPcl() <= 0 && this.getClId() == this.getLalId();

  }

  public boolean allPointerEqual() {
    long pl = this.getPlId();
    long al = this.getLalId();
    long cl = this.getClId();
    return pl == al && al == cl;
  }

  public boolean allLogsFlushedToDisk() {
    long pl = this.getPlId();
    long al = this.getLalId();
    long cl = this.getClId();

    return (pl == al && al == cl && cl >= maxLogId && uuidsOfLogsWithoutLogId.isEmpty());
  }

  public long getMaxLogId() {
    return maxLogId;
  }
}

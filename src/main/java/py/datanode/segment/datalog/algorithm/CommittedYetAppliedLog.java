
package py.datanode.segment.datalog.algorithm;

import py.datanode.segment.datalog.MutationLogEntry.LogStatus;

public abstract class CommittedYetAppliedLog extends ImmutableStatusDataLog {
  public CommittedYetAppliedLog(long logId, long offset, int length, long logUuid) {
    super(logId, offset, length, logUuid, LogStatus.Committed, false, false);
  }

}

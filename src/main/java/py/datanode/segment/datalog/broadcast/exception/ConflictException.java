
package py.datanode.segment.datalog.broadcast.exception;

import py.datanode.segment.datalog.MutationLogEntry;

public class ConflictException extends Exception {
  private MutationLogEntry log;

  public ConflictException(String message) {
    super(message);
  }

  public ConflictException(String message, MutationLogEntry log) {
    super(message);
    this.log = log;
  }

  public MutationLogEntry getConflictLog() {
    return log;
  }
}



package py.datanode.exception;

import py.datanode.segment.datalog.persist.LogStorageMetadata;

public class LogIdNotFoundException extends Exception {
  private static final long serialVersionUID = 1L;
  private LogStorageMetadata prev;
  private LogStorageMetadata next;

  public LogIdNotFoundException(String message, LogStorageMetadata prev, LogStorageMetadata next) {
    super(message);
    this.prev = prev;
    this.next = next;
  }

  public LogStorageMetadata getPrevLogStorageMetadata() {
    return prev;
  }

  public LogStorageMetadata getNextLogStorageMetadata() {
    return next;
  }

}

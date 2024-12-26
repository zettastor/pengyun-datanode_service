

package py.datanode.exception;

public class SnapshotTooOldException extends Exception {
  private static final long serialVersionUID = 1L;

  public SnapshotTooOldException() {
    super();
   
  }

  public SnapshotTooOldException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
   
  }

  public SnapshotTooOldException(String message, Throwable cause) {
    super(message, cause);
   
  }

  public SnapshotTooOldException(String message) {
    super(message);
   
  }

  public SnapshotTooOldException(Throwable cause) {
    super(cause);
   
  }

}

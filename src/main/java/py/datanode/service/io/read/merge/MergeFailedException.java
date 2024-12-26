

package py.datanode.service.io.read.merge;

public class MergeFailedException extends Exception {
  public MergeFailedException() {
    super();
  }

  public MergeFailedException(String message) {
    super(message);
  }

  public MergeFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public MergeFailedException(Throwable cause) {
    super(cause);
  }

  protected MergeFailedException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}

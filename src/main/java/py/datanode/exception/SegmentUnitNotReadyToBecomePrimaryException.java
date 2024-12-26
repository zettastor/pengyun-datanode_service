

package py.datanode.exception;

public class SegmentUnitNotReadyToBecomePrimaryException extends Exception {
  private static final long serialVersionUID = 1L;

  public SegmentUnitNotReadyToBecomePrimaryException() {
    super();
  }

  public SegmentUnitNotReadyToBecomePrimaryException(String message) {
    super(message);
  }

  public SegmentUnitNotReadyToBecomePrimaryException(String message, Throwable cause) {
    super(message, cause);
  }

  public SegmentUnitNotReadyToBecomePrimaryException(Throwable cause) {
    super(cause);
  }
}

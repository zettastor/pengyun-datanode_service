
package py.datanode.exception;

import py.archive.segment.SegmentUnitStatus;

public class SegmentLogMetadataNotExistException extends Exception {
  private static final long serialVersionUID = 1L;

  public SegmentLogMetadataNotExistException() {
    super();
  }

  public SegmentLogMetadataNotExistException(SegmentUnitStatus newStatus,
      SegmentUnitStatus currentStatus) {
    super("current status " + currentStatus + " the new status " + newStatus);
  }

  public SegmentLogMetadataNotExistException(String message) {
    super(message);
  }

  public SegmentLogMetadataNotExistException(String message, Throwable cause) {
    super(message, cause);
  }

  public SegmentLogMetadataNotExistException(Throwable cause) {
    super(cause);
  }
}

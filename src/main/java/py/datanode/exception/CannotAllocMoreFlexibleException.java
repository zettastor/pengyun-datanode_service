

package py.datanode.exception;

public class CannotAllocMoreFlexibleException extends Exception {
  private static final long serialVersionUID = 1L;

  public CannotAllocMoreFlexibleException() {
    super();
  }

  public CannotAllocMoreFlexibleException(String message) {
    super(message);
  }

  public CannotAllocMoreFlexibleException(String message, Throwable cause) {
    super(message, cause);
  }

  public CannotAllocMoreFlexibleException(Throwable cause) {
    super(cause);
  }
}

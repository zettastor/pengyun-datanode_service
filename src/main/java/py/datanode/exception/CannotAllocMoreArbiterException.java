
package py.datanode.exception;

public class CannotAllocMoreArbiterException extends Exception {
  private static final long serialVersionUID = 1L;

  public CannotAllocMoreArbiterException() {
    super();
  }

  public CannotAllocMoreArbiterException(String message) {
    super(message);
  }

  public CannotAllocMoreArbiterException(String message, Throwable cause) {
    super(message, cause);
  }

  public CannotAllocMoreArbiterException(Throwable cause) {
    super(cause);
  }
}

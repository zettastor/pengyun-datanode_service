

package py.datanode.exception;

public class InvalidLogStatusException extends Exception {
  private static final long serialVersionUID = 1L;

  public InvalidLogStatusException() {
    super();
  }

  public InvalidLogStatusException(String message) {
    super(message);
  }

  public InvalidLogStatusException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidLogStatusException(Throwable cause) {
    super(cause);
  }
}

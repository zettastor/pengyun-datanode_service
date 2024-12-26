
package py.datanode.exception;

public class LogsNotInRightOrderException extends Exception {
  private static final long serialVersionUID = 1L;

  public LogsNotInRightOrderException() {
    super();
  }

  public LogsNotInRightOrderException(String message) {
    super(message);
  }

  public LogsNotInRightOrderException(String message, Throwable cause) {
    super(message, cause);
  }

  public LogsNotInRightOrderException(Throwable cause) {
    super(cause);
  }
}

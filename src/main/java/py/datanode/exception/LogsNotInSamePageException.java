
package py.datanode.exception;

public class LogsNotInSamePageException extends Exception {
  private static final long serialVersionUID = 1L;

  public LogsNotInSamePageException() {
    super();
  }

  public LogsNotInSamePageException(String message) {
    super(message);
  }

  public LogsNotInSamePageException(String message, Throwable cause) {
    super(message, cause);
  }

  public LogsNotInSamePageException(Throwable cause) {
    super(cause);
  }
}

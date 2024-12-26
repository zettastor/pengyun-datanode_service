
package py.datanode.exception;

public class DestinationNotInSamePageException extends Exception {
  public DestinationNotInSamePageException() {
    super();
  }

  public DestinationNotInSamePageException(String message) {
    super(message);
  }

  public DestinationNotInSamePageException(String message, Throwable cause) {
    super(message, cause);
  }

  public DestinationNotInSamePageException(Throwable cause) {
    super(cause);
  }

  protected DestinationNotInSamePageException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}



package py.datanode.exception;

public class ParseFailedException extends Exception {
  public ParseFailedException() {
  }

  public ParseFailedException(String message) {
    super(message);
  }

  public ParseFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParseFailedException(Throwable cause) {
    super(cause);
  }

  public ParseFailedException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}

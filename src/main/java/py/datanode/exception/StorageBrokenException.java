

package py.datanode.exception;

public class StorageBrokenException extends Exception {
  private static final long serialVersionUID = 1L;

  public StorageBrokenException() {
    super();
  }

  public StorageBrokenException(String message) {
    super(message);
  }

  public StorageBrokenException(String message, Throwable cause) {
    super(message, cause);
  }

  public StorageBrokenException(Throwable cause) {
    super(cause);
  }
}

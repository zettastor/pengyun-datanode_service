
package py.datanode.exception;

public class RollbackFailedException extends Exception {
  public RollbackFailedException(String message) {
    super(message);
  }

  public RollbackFailedException(Throwable cause) {
    super(cause);
  }

}

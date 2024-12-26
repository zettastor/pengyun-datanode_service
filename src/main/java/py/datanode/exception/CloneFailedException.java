
package py.datanode.exception;

public class CloneFailedException extends Exception {
  private static final long serialVersionUID = 1L;

  public CloneFailedException() {
    super();
  }

  public CloneFailedException(String message) {
    super(message);
  }

  public CloneFailedException(Throwable cause) {
    super(cause);
  }
}

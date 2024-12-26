
package py.datanode.exception;

public class CopyPageAbortException extends Exception {
  public CopyPageAbortException(Throwable cause) {
    super(cause);
  }

  public CopyPageAbortException(String message) {
    super(message);
  }
}

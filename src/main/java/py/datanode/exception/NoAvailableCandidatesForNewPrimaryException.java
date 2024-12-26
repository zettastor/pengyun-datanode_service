

package py.datanode.exception;

public class NoAvailableCandidatesForNewPrimaryException extends Exception {
  private static final long serialVersionUID = 1L;

  public NoAvailableCandidatesForNewPrimaryException() {
    super();
  }

  public NoAvailableCandidatesForNewPrimaryException(String message) {
    super(message);
  }

  public NoAvailableCandidatesForNewPrimaryException(String message, Throwable cause) {
    super(message, cause);
  }

  public NoAvailableCandidatesForNewPrimaryException(Throwable cause) {
    super(cause);
  }
}

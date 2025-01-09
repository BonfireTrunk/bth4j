package today.bonfire.oss.bth4j.exceptions;

public class OperationNotAllowed extends RuntimeException {

  private OperationNotAllowed() {}

  public OperationNotAllowed(String message) {
    super(message);
  }

  public OperationNotAllowed(String message, Throwable cause) {
    super(message, cause);
  }

}

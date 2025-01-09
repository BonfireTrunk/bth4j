package today.bonfire.oss.bth4j.exceptions;

public class TaskErrorException extends RuntimeException {

  private TaskErrorException() {}

  public TaskErrorException(String message) {
    super(message);
  }

  public TaskErrorException(String message, Throwable cause) {
    super(message, cause);
  }

}

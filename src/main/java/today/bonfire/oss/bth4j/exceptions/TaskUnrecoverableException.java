package today.bonfire.oss.bth4j.exceptions;

/**
 * Exception thrown when a running task has failed.
 * the failed task is not retired again
 */
public class TaskUnrecoverableException extends RuntimeException {

  private TaskUnrecoverableException() {}

  public TaskUnrecoverableException(String message) {
    super(message);
  }

  public TaskUnrecoverableException(String message, Throwable cause) {
    super(message, cause);
  }

}

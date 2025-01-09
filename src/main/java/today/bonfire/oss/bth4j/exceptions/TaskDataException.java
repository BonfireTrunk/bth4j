package today.bonfire.oss.bth4j.exceptions;

/**
 * Exception thrown when a running task has failed.
 * the failed task is not retired again
 */
public class TaskDataException extends RuntimeException {

  private TaskDataException() {}

  public TaskDataException(String message) {
    super(message);
  }

  public TaskDataException(String message, Throwable cause) {
    super(message, cause);
  }

}

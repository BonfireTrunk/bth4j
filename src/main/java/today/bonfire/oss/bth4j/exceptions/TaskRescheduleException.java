package today.bonfire.oss.bth4j.exceptions;

import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Exception thrown when a running task has failed.
 * the failed task is retried again
 */
@Accessors(fluent = true)
public class TaskRescheduleException extends RuntimeException {

  @Getter
  private long delay = 0;

  private TaskRescheduleException() {}

  public TaskRescheduleException(long seconds) {
    super("Reschedule task in " + seconds + " seconds");
    delay = seconds;
  }

  public TaskRescheduleException(String message, Throwable cause) {
    super(message, cause);
  }

}

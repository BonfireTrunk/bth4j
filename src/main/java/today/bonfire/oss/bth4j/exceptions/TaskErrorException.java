package today.bonfire.oss.bth4j.exceptions;

/**
 * Exception for invalid task definitions (malformed task string, invalid builder input) and for
 * task failures that should be retried. When thrown by a task processor, the task stays in progress
 * and is retried by the maintenance service after the stale task timeout.
 */
public class TaskErrorException extends RuntimeException {

    private TaskErrorException() {}

    public TaskErrorException(String message) {
        super(message);
    }

    public TaskErrorException(String message, Throwable cause) {
        super(message, cause);
    }
}

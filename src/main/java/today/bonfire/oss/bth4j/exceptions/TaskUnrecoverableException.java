package today.bonfire.oss.bth4j.exceptions;

/**
 * Exception thrown by a task processor when the task has failed and must not be retried.
 * The task is moved to the dead queue.
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

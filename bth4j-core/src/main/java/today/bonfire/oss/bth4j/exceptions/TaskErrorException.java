package today.bonfire.oss.bth4j.exceptions;

/**
 * Exception for invalid task definitions and configuration found at runtime. When thrown by a task
 * processor it counts as a failed attempt like any other exception.
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

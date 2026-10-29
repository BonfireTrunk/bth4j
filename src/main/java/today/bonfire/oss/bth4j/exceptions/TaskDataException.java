package today.bonfire.oss.bth4j.exceptions;

/**
 * Exception thrown when the data of a task cannot be read or converted.
 * The task is not retried, it is moved to the dead queue.
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

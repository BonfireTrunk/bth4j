package today.bonfire.oss.bth4j.exceptions;

/**
 * Thrown inside a processor when the store reports that this worker no longer holds the task, for example
 * from {@link today.bonfire.oss.bth4j.TaskContext#step}. The task already belongs to another worker; the
 * processor should stop.
 */
public class TaskLeaseLostException extends RuntimeException {
    public TaskLeaseLostException(String message) {
        super(message);
    }
}

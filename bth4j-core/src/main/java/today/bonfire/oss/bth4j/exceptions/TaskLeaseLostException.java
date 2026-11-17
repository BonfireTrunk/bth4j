package today.bonfire.oss.bth4j.exceptions;

/**
 * Thrown inside a processor when the store reports that this runner no longer holds the task, for example
 * from {@link today.bonfire.oss.bth4j.TaskContext#step}. The task already belongs to another runner; the
 * processor should stop.
 */
public final class TaskLeaseLostException extends TaskSignal {
    public TaskLeaseLostException(String message) {
        super(message);
    }
}

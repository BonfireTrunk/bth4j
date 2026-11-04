package today.bonfire.oss.bth4j.exceptions;

/** Thrown by {@link today.bonfire.oss.bth4j.TaskContext#checkCancelled()}; ends the task as cancelled. */
public final class TaskCancelledException extends TaskSignal {
    public TaskCancelledException(String message) {
        super(message);
    }
}

package today.bonfire.oss.bth4j.exceptions;

/** Thrown by {@link today.bonfire.oss.bth4j.TaskContext#checkCancelled()}; ends the task as cancelled. */
public class TaskCancelledException extends RuntimeException {
    public TaskCancelledException(String message) {
        super(message);
    }
}

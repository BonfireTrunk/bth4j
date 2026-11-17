package today.bonfire.oss.bth4j.exceptions;

/**
 * Thrown when a stored value of a task cannot be read, such as a malformed number, state code or JSON field. The
 * message names the task but does not quote the stored value.
 * <p>
 * The store itself works, so this is not a {@link TaskStoreException}: repeating the call fails the same way until
 * the task is repaired or deleted.
 */
public class UnreadableTaskException extends Bth4jException {
    private final long taskId;

    public UnreadableTaskException(long taskId, String message, Throwable cause) {
        super(message, cause);
        this.taskId = taskId;
    }

    public long taskId() {
        return taskId;
    }
}

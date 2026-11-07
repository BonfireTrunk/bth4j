package today.bonfire.oss.bth4j.spi;

/**
 * Thrown when a stored value of a task cannot be read, such as a malformed number, state code or JSON field. The
 * message names the task but does not quote the stored value.
 */
public class UnreadableTaskException extends TaskStoreException {
    private final long taskId;

    public UnreadableTaskException(long taskId, String message, Throwable cause) {
        super(message, cause);
        this.taskId = taskId;
    }

    public long taskId() {
        return taskId;
    }
}

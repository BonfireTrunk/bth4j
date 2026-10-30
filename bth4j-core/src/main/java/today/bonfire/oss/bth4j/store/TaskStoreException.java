package today.bonfire.oss.bth4j.store;

/** Thrown by a {@link TaskStore} when the underlying storage cannot be reached or rejects an operation. */
public class TaskStoreException extends RuntimeException {
    public TaskStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}

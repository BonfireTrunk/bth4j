package today.bonfire.oss.bth4j.exceptions;

/**
 * The store could not be reached while storing a task, so the caller does not know whether the task was
 * stored. {@link #id()} is the id the task has if it was; enqueueing the same request again with that id is
 * safe, the store answers accepted either way.
 */
public class EnqueueFailedException extends RuntimeException {
    private final long id;

    public EnqueueFailedException(long id, Throwable cause) {
        super("Enqueue of task " + id + " failed: " + cause.getMessage(), cause);
        this.id = id;
    }

    public long id() {
        return id;
    }
}

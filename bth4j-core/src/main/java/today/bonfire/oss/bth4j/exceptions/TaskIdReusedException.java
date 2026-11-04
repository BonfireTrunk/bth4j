package today.bonfire.oss.bth4j.exceptions;

/**
 * A request set an id with {@link today.bonfire.oss.bth4j.TaskRequest.Builder#id(long)} that already belongs to
 * a task with other content (another type, queue, payload or unique key). Reusing an id for different content
 * is a bug in the caller, so nothing was stored under it; only the same request may be enqueued again with its id.
 */
public class TaskIdReusedException extends RuntimeException {
    private final long id;

    public TaskIdReusedException(long id, String reason) {
        super("Task id " + id + " is already used by a different task: " + reason);
        this.id = id;
    }

    /** @return the id that was reused */
    public long id() {
        return id;
    }
}

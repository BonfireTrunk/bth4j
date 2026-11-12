package today.bonfire.oss.bth4j.exceptions;

/**
 * A store cannot be reached, or it refuses: a wrong schema version at start, no queue or type ids left. A stored
 * task that cannot be read is not a store failure, see {@link UnreadableTaskException}.
 */
public class TaskStoreException extends Bth4jException {
    public TaskStoreException(String message) {
        super(message);
    }

    public TaskStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}

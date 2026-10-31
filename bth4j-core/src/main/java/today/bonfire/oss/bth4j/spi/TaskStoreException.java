package today.bonfire.oss.bth4j.spi;

/** Thrown when a store cannot be reached or refuses to start. */
public class TaskStoreException extends RuntimeException {
    public TaskStoreException(String message) {
        super(message);
    }

    public TaskStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}

package today.bonfire.oss.bth4j.exceptions;

/** A request was rejected before reaching the store: missing field, size limit, bad value. */
public class TaskValidationException extends RuntimeException {
    public TaskValidationException(String message) {
        super(message);
    }
}

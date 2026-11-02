package today.bonfire.oss.bth4j.exceptions;

/**
 * Stands in for a processor's exception in task events when the runner uses
 * {@link today.bonfire.oss.bth4j.FailureDetail#CLASS_NAME_ONLY}. It has no message, no cause and no stack
 * trace; {@link #toString()} and {@link #type()} give the class name of the original exception.
 */
public final class RedactedFailure extends RuntimeException {
    private final String type;

    public RedactedFailure(String type) {
        super(null, null, false, false);
        this.type = type;
    }

    /** @return the class name of the exception that was replaced */
    public String type() {
        return type;
    }

    @Override
    public String toString() {
        return type;
    }
}

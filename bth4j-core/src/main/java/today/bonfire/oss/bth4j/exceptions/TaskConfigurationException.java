package today.bonfire.oss.bth4j.exceptions;

/**
 * A setting of a runtime object was refused: a builder, a store, an executor, a policy, the admin HTTP server or
 * the metrics binder.
 */
public class TaskConfigurationException extends Bth4jException {

    public TaskConfigurationException(String message) {
        super(message);
    }

    public TaskConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}

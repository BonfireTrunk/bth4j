package today.bonfire.oss.bth4j.exceptions;

public class TaskConfigurationError extends RuntimeException {
    private static final String PREFIX = "ERR11_Error in configuration ";

    public TaskConfigurationError(String message) {
        super(PREFIX + message);
    }

    public TaskConfigurationError(String message, Throwable cause) {
        super(PREFIX + message, cause);
    }
}

package today.bonfire.oss.bth4j.exceptions;

public class TaskConfigurationException extends RuntimeException {
    private static final String PREFIX = "ERR11_Error in configuration ";

    public TaskConfigurationException(String message) {
        super(PREFIX + message);
    }

    public TaskConfigurationException(String message, Throwable cause) {
        super(PREFIX + message, cause);
    }
}

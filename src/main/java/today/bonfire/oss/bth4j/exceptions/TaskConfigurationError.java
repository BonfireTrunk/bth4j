package today.bonfire.oss.bth4j.exceptions;

public class TaskConfigurationError extends RuntimeException {

  private TaskConfigurationError() {}

  public TaskConfigurationError(String message) {
    super(Errors.CONFIGURATION_ERROR + message);
  }

  public TaskConfigurationError(String message, Throwable cause) {
    super(Errors.CONFIGURATION_ERROR + message, cause);
  }

}

package today.bonfire.oss.bth4j.exceptions;

public class Errors {
  public static final String OPERATION_NOT_ALLOWED = "ERR10_Operation not allowed ";
  public static final String CONFIGURATION_ERROR   = "ERR11_Error in configuration ";
  public static final String DATASTORE_ERROR       = "ERR12_Error in redis connection ";

  public static class Tasks {
    public static final String CANNOT_CREATE_TASK  = "ERR01_Cannot create task ";
    public static final String CANNOT_PARSE_TASK   = "ERR02_Cannot parse task ";
    public static final String CANNOT_PROCESS_TASK = "ERR03_Cannot process task ";
  }
}

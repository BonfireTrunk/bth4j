package today.bonfire.oss.bth4j.exceptions;

/**
 * Data that is sent or stored was refused: a request, a schedule definition, an argument of an admin call or a
 * listing cursor with a missing field, a size over its limit or a bad value.
 */
public class TaskValidationException extends Bth4jException {
    public TaskValidationException(String message) {
        super(message);
    }
}

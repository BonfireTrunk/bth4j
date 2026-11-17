package today.bonfire.oss.bth4j.exceptions;

/**
 * The base of everything bth4j throws to refuse a call or to report a failure of its own, so that one
 * {@code catch (Bth4jException e)} covers them all.
 *
 * <table>
 * <caption>What is thrown when</caption>
 * <tr><th>Exception</th><th>Thrown for</th></tr>
 * <tr><td>{@link TaskValidationException}</td><td>data that is sent or stored: a request, a schedule definition,
 * an argument of an admin call, a listing cursor</td></tr>
 * <tr><td>{@link TaskConfigurationException}</td><td>a setting of a runtime object: a builder, a store, an
 * executor, a policy, the admin HTTP server, the metrics binder</td></tr>
 * <tr><td>{@link TaskStoreException}</td><td>a store that cannot be reached or that refuses</td></tr>
 * <tr><td>{@link UnreadableTaskException}</td><td>a stored task with a value that cannot be read</td></tr>
 * <tr><td>{@link TaskIdReusedException}</td><td>a caller id that belongs to a task with other content</td></tr>
 * <tr><td>{@link EnqueueFailedException}</td><td>an enqueue that could not complete; carries what was written</td></tr>
 * <tr><td>{@link TaskDataException}</td><td>a payload or step result that cannot be converted</td></tr>
 * </table>
 *
 * Two things stay outside on purpose. The signals a processor throws to steer the runner ({@link TaskSignal})
 * are not failures of bth4j, and a {@code catch (Bth4jException e)} in a processor must not swallow them. Using
 * an object in a state it is not in, such as starting a stopped runner, is an {@link IllegalStateException}.
 */
public abstract class Bth4jException extends RuntimeException {

    protected Bth4jException(String message) {
        super(message);
    }

    protected Bth4jException(String message, Throwable cause) {
        super(message, cause);
    }
}

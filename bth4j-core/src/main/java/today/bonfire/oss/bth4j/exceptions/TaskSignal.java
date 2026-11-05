package today.bonfire.oss.bth4j.exceptions;

/**
 * An exception that tells the runner what to do with the task rather than reporting a failure: reschedule it,
 * end it as dead, stop because the lease is gone, or end it as cancelled. A processor that wraps exceptions
 * (for example to keep error text out of the store) must let these through unchanged:
 *
 * <pre>{@code
 * try {
 *     work();
 * } catch (TaskSignal signal) {
 *     throw signal;
 * } catch (RuntimeException e) {
 *     throw new IllegalStateException("work failed");
 * }
 * }</pre>
 */
public abstract sealed class TaskSignal extends RuntimeException
        permits TaskRescheduleException, TaskUnrecoverableException, TaskLeaseLostException, TaskCancelledException {

    TaskSignal(String message) {
        super(message);
    }

    TaskSignal(String message, Throwable cause) {
        super(message, cause);
    }
}

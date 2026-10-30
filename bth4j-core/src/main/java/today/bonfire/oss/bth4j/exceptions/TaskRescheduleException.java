package today.bonfire.oss.bth4j.exceptions;

/**
 * Exception thrown by a task processor to ask for the task to be run again after a delay.
 * The original task is replaced by a new task (new id) that carries the same data and is
 * scheduled to run after {@link #delay()} seconds.
 */
public class TaskRescheduleException extends RuntimeException {

    /** delay in seconds before the task is run again */
    private long delay = 0;

    private TaskRescheduleException() {}

    /**
     * @param seconds delay in seconds before the task runs again, must not be negative
     * @throws IllegalArgumentException if seconds is negative
     */
    public TaskRescheduleException(long seconds) {
        super("Reschedule task in " + seconds + " seconds");
        delay = requireNonNegative(seconds);
    }

    /**
     * Reschedules the task without a delay.
     */
    public TaskRescheduleException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param seconds delay in seconds before the task runs again, must not be negative
     * @param message description of why the task is rescheduled
     * @param cause   the underlying cause
     * @throws IllegalArgumentException if seconds is negative
     */
    public TaskRescheduleException(long seconds, String message, Throwable cause) {
        super(message, cause);
        delay = requireNonNegative(seconds);
    }

    public long delay() {
        return delay;
    }

    private static long requireNonNegative(long seconds) {
        if (seconds < 0) {
            throw new IllegalArgumentException("Reschedule delay cannot be negative: " + seconds);
        }
        return seconds;
    }
}

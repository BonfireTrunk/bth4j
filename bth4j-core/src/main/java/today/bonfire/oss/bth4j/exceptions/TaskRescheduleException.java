package today.bonfire.oss.bth4j.exceptions;

import java.time.Duration;

/**
 * Thrown by a task processor to run the task again after a delay without counting the attempt. The task
 * keeps its id and payload.
 */
public class TaskRescheduleException extends RuntimeException {

    private final Duration delay;

    /** @param delay how long to wait before the task runs again, not negative */
    public TaskRescheduleException(Duration delay) {
        this(delay, "Reschedule task in " + delay, null);
    }

    public TaskRescheduleException(Duration delay, String message, Throwable cause) {
        super(message, cause);
        if (delay == null || delay.isNegative()) {
            throw new IllegalArgumentException("Reschedule delay cannot be null or negative: " + delay);
        }
        this.delay = delay;
    }

    public Duration delay() {
        return delay;
    }
}

package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;

/**
 * What a runner reports to its {@link TaskObserver}. Event kinds may be added in a minor release, so a
 * {@code switch} over events needs a {@code default} branch that ignores a kind it does not know. The event
 * records and the view records are for reading: a component may be added to one in a minor release, which breaks
 * only code that constructs it or takes it apart. {@code outcome} tells whether the store applied the transition
 * ({@link Outcome#APPLIED}), rejected it because the lease was lost meanwhile ({@link Outcome#LOST}), or failed
 * so that the result is not known ({@link Outcome#UNKNOWN}). A {@code queue} or {@code type} that could not be
 * resolved to a name is the text {@code unknown}.
 */
public interface TaskEvent {

    enum Outcome {
        APPLIED,
        LOST,
        /**
         * The store call failed (for example a timeout or a lost reply), so the transition may or may not have
         * been committed. When it was not, the lease expires and maintenance retries the task.
         */
        UNKNOWN
    }

    /** A task passed its {@link TaskView#startBy()} before it could run and was moved to EXPIRED. */
    record Expired(long id, String queue, String type) implements TaskEvent {}

    /** A task was claimed and its processor is about to run. */
    record Started(TaskView task, int workerId) implements TaskEvent {}

    /** The processor returned normally. */
    record Succeeded(TaskView task, Duration runTime, Outcome outcome) implements TaskEvent {}

    /**
     * The processor failed and the task will run again at {@code retryAt}. With {@link Outcome#LOST} this
     * worker's lease was gone and nothing was written; with {@link Outcome#UNKNOWN} the store call failed and
     * the failure may not have been written. Either way the store's maintenance decides whether and when the
     * task runs again, and {@code retryAt} is not that time: it is the time this worker asked for after a
     * failed attempt, or the time the lost lease was noticed when the processor was stopped for it.
     */
    record Retrying(TaskView task, Throwable error, Instant retryAt, Outcome outcome) implements TaskEvent {}

    /** The task ended as dead: attempts exhausted, unrecoverable error, or the retry policy said so. */
    record Died(TaskView task, Throwable error, String reason, Outcome outcome) implements TaskEvent {}

    /** The processor asked for the task to run again later without counting the attempt. */
    record Rescheduled(TaskView task, Instant due, Outcome outcome) implements TaskEvent {}

    /** The task ended as cancelled. */
    record Cancelled(TaskView task, String reason, Outcome outcome) implements TaskEvent {}

    /** Maintenance found an expired lease; the task was retried or died. */
    record LeaseExpired(long id, String queue, String type, int attempts, int maxAttempts, boolean dead)
            implements TaskEvent {}

    /** A claimed task could not be run because this runner has no processor for its type, and was put back. */
    record Deferred(long id, String queue, String type, String reason) implements TaskEvent {}

    /** An occurrence of a recurring definition was created. */
    record ScheduleFired(String scheduleId, long taskId, Instant fireAt) implements TaskEvent {}

    /** Maintenance removed finished tasks past their retention. */
    record Purged(today.bonfire.oss.bth4j.spi.TaskState state, long count) implements TaskEvent {}
}

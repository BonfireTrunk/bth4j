package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;

/**
 * What a runner reports to its {@link TaskObserver}. The vocabulary is fixed; new event kinds are added, not
 * changed. {@code outcome} tells whether the store applied the transition ({@link Outcome#APPLIED}) or the
 * lease was lost meanwhile ({@link Outcome#LOST}).
 */
public sealed interface TaskEvent {

    enum Outcome {
        APPLIED,
        LOST
    }

    /** A task passed its {@link TaskView#startBy()} before it could run and was moved to EXPIRED. */
    record Expired(long id) implements TaskEvent {}

    /** A task was claimed and its processor is about to run. */
    record Started(TaskView task, int workerId) implements TaskEvent {}

    /** The processor returned normally. */
    record Succeeded(TaskView task, Duration runTime, Outcome outcome) implements TaskEvent {}

    /** The processor failed and the task will run again at {@code retryAt}. */
    record Retrying(TaskView task, Throwable error, Instant retryAt, Outcome outcome) implements TaskEvent {}

    /** The task ended as dead: attempts exhausted, unrecoverable error, or the retry policy said so. */
    record Died(TaskView task, Throwable error, String reason, Outcome outcome) implements TaskEvent {}

    /** The processor asked for the task to run again later without counting the attempt. */
    record Rescheduled(TaskView task, Instant due, Outcome outcome) implements TaskEvent {}

    /** The task ended as cancelled. */
    record Cancelled(TaskView task, String reason, Outcome outcome) implements TaskEvent {}

    /** Maintenance found an expired lease; the task was retried or died. */
    record LeaseExpired(long id, int attempts, int maxAttempts, boolean dead) implements TaskEvent {}

    /** A claimed task could not be run (no processor, undecodable payload) and was put back. */
    record Deferred(long id, String event, String reason) implements TaskEvent {}

    /** An occurrence of a recurring definition was created. */
    record ScheduleFired(String scheduleId, long taskId, Instant fireAt) implements TaskEvent {}

    /** Maintenance removed finished tasks past their retention. */
    record Purged(today.bonfire.oss.bth4j.spi.TaskState state, long count) implements TaskEvent {}
}

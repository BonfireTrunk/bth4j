package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

/** Decides what happens after a failed attempt. */
@FunctionalInterface
public interface RetryPolicy {

    /**
     * @param task    the task that failed; {@link TaskView#attempts()} counts the failed attempt
     * @param error   what went wrong; null when the lease expired
     * @return when to try again, or {@link Decision#dead} to stop; the runner ends the task as dead anyway when
     *         the attempts are exhausted, and waits at most {@link TaskRequest#MAX_TIME_FROM_NOW}
     */
    Decision decide(TaskView task, Throwable error);

    sealed interface Decision {
        /** A null or negative delay counts as none. */
        record RetryAfter(Duration delay) implements Decision {
            public RetryAfter {
                if (delay == null || delay.isNegative()) delay = Duration.ZERO;
            }
        }

        record Dead(String reason) implements Decision {}

        static Decision retryAfter(Duration delay) {
            return new RetryAfter(delay);
        }

        static Decision dead(String reason) {
            return new Dead(reason);
        }
    }

    /**
     * Exponential backoff with jitter: {@code base}, doubling per attempt up to {@code cap}, each varied by up to
     * 20 percent. A base or cap longer than {@link TaskRequest#MAX_TIME_FROM_NOW} counts as that long.
     *
     * @throws TaskConfigurationException when the base or the cap is null or negative
     */
    static RetryPolicy exponential(Duration base, Duration cap) {
        long baseMs = millis(base, "base");
        long capMs = millis(cap, "cap");
        return (task, error) -> {
            int doublings = Math.min(62, Math.max(1, task.attempts()) - 1);
            // compared before the shift, so that the doubling cannot overflow
            long ms = baseMs <= capMs >> doublings ? baseMs << doublings : capMs;
            long jitter = (long) (ms * 0.2 * ThreadLocalRandom.current().nextDouble(-1, 1));
            return Decision.retryAfter(Duration.ofMillis(Math.max(0, ms + jitter)));
        };
    }

    /** 1 s, 2 s, 4 s ... capped at one hour, with jitter. */
    static RetryPolicy defaultPolicy() {
        return exponential(Duration.ofSeconds(1), Duration.ofHours(1));
    }

    /** @throws TaskConfigurationException when the delay is null or negative */
    static RetryPolicy fixed(Duration delay) {
        millis(delay, "delay");
        return (task, error) -> Decision.retryAfter(delay);
    }

    private static long millis(Duration duration, String name) {
        if (duration == null || duration.isNegative()) {
            throw new TaskConfigurationException(name + " cannot be null or negative");
        }
        var longest = TaskRequest.MAX_TIME_FROM_NOW;
        return (duration.compareTo(longest) > 0 ? longest : duration).toMillis();
    }
}

package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/** Decides what happens after a failed attempt. */
@FunctionalInterface
public interface RetryPolicy {

    /**
     * @param task    the task that failed; {@link TaskView#attempts()} counts the failed attempt
     * @param error   what went wrong; null when the lease expired
     * @return when to try again, or {@link Decision#dead} to stop; the runner ends the task as dead anyway when
     *         the attempts are exhausted
     */
    Decision decide(TaskView task, Throwable error);

    sealed interface Decision {
        record RetryAfter(Duration delay) implements Decision {}

        record Dead(String reason) implements Decision {}

        static Decision retryAfter(Duration delay) {
            return new RetryAfter(delay == null || delay.isNegative() ? Duration.ZERO : delay);
        }

        static Decision dead(String reason) {
            return new Dead(reason);
        }
    }

    /**
     * Exponential backoff with jitter: {@code base}, doubling per attempt up to {@code cap}, each varied by up to
     * 20 percent.
     */
    static RetryPolicy exponential(Duration base, Duration cap) {
        return (task, error) -> {
            int failed = Math.max(1, task.attempts());
            long ms = Math.min(cap.toMillis(), base.toMillis() << Math.min(20, failed - 1));
            long jitter = (long) (ms * 0.2 * ThreadLocalRandom.current().nextDouble(-1, 1));
            return Decision.retryAfter(Duration.ofMillis(Math.max(0, ms + jitter)));
        };
    }

    /** 1 s, 2 s, 4 s ... capped at one hour, with jitter. */
    static RetryPolicy defaultPolicy() {
        return exponential(Duration.ofSeconds(1), Duration.ofHours(1));
    }

    static RetryPolicy fixed(Duration delay) {
        return (task, error) -> Decision.retryAfter(delay);
    }
}

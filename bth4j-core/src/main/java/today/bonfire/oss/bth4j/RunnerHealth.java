package today.bonfire.oss.bth4j;

import java.time.Instant;

/**
 * A snapshot of one runner.
 *
 * @param runningTasks the attempts running on this runner, including those whose processor returned and whose outcome
 *     is still being written; with the built-in executors up to twice the concurrency limit
 * @param abandonedAttempts the running attempts that passed their maximum run time without stopping: their leases are
 *     no longer renewed, but each still holds its slots until it ends
 */
public record RunnerHealth(
        Status status,
        int workerId,
        int runningTasks,
        int abandonedAttempts,
        Instant lastClaimAt,
        Instant lastMaintenanceAt,
        Instant lastStoreErrorAt,
        String lastStoreError,
        int issueCount) {

    public enum Status {
        /** built, {@code start()} not called yet */
        NEW,
        RUNNING,
        /** started, and claiming no new tasks after {@code quiet()} */
        QUIET,
        /** {@code stop()} was called and a task, a claim or a service of this runner has not ended yet */
        STOPPING,
        /** {@code stop()} was called and nothing of this runner runs any more */
        STOPPED
    }
}

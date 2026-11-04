package today.bonfire.oss.bth4j;

import java.time.Instant;

/**
 * A snapshot of one runner.
 *
 * @param running the attempts running on this runner
 * @param abandoned the running attempts that passed their maximum run time without stopping: their leases are
 *     no longer renewed, but each still holds its executor and queue slot until its processor returns
 */
public record RunnerHealth(
        Status status,
        int workerId,
        int running,
        int abandoned,
        Instant lastClaimAt,
        Instant lastMaintenanceAt,
        Instant lastStoreErrorAt,
        String lastStoreError,
        int issueCount) {

    public enum Status {
        NEW,
        RUNNING,
        QUIET,
        STOPPED
    }
}

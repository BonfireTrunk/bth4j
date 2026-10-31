package today.bonfire.oss.bth4j;

import java.time.Instant;

/** A snapshot of one runner. */
public record RunnerHealth(
        Status status,
        int workerId,
        int running,
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

package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/** What one runner knows about itself: last activity per service and the issues it noticed. */
final class RunnerStatus {
    static final int MAX_ISSUES = 100;

    private final Clock clock;
    private final ArrayDeque<Issue> issues = new ArrayDeque<>();
    private final ReentrantLock issuesLock = new ReentrantLock();
    private volatile Instant lastClaimAt;
    private volatile Instant lastMaintenanceAt;
    private volatile Instant lastStoreErrorAt;
    private volatile String lastStoreError;
    /** The last store error recorded as an issue; guarded by issuesLock. */
    private Throwable lastRecorded;

    RunnerStatus(Clock clock) {
        this.clock = clock;
    }

    void claimed() {
        lastClaimAt = clock.instant();
    }

    void maintained() {
        lastMaintenanceAt = clock.instant();
    }

    /**
     * Records a store error. Every task of a failed acknowledgement batch reports the same error; it becomes one
     * issue, so a batch does not push the other issues out.
     *
     * @return false when this error was already recorded
     */
    boolean storeError(Throwable error) {
        lastStoreErrorAt = clock.instant();
        lastStoreError = error.toString();
        issuesLock.lock();
        try {
            if (sameError(error, lastRecorded)) return false;
            lastRecorded = error;
        } finally {
            issuesLock.unlock();
        }
        issue("store-error", error.toString(), null);
        return true;
    }

    /** The batch hands an Error to its waiting tasks wrapped, one wrapper each, around the same cause. */
    private static boolean sameError(Throwable error, Throwable last) {
        if (last == null) return false;
        var cause = error.getCause();
        return error == last || cause != null && (cause == last || cause == last.getCause());
    }

    void issue(String kind, String detail, Long taskId) {
        var issue = new Issue(clock.instant(), kind, detail, taskId);
        issuesLock.lock();
        try {
            if (issues.size() == MAX_ISSUES) issues.removeFirst();
            issues.addLast(issue);
        } finally {
            issuesLock.unlock();
        }
    }

    List<Issue> issues() {
        issuesLock.lock();
        try {
            return new ArrayList<>(issues);
        } finally {
            issuesLock.unlock();
        }
    }

    int issueCount() {
        issuesLock.lock();
        try {
            return issues.size();
        } finally {
            issuesLock.unlock();
        }
    }

    Instant lastClaimAt() {
        return lastClaimAt;
    }

    Instant lastMaintenanceAt() {
        return lastMaintenanceAt;
    }

    Instant lastStoreErrorAt() {
        return lastStoreErrorAt;
    }

    String lastStoreError() {
        return lastStoreError;
    }
}

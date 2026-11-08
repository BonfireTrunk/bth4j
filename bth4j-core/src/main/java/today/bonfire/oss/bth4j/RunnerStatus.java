package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    /** The store-error issue of each distinct error text and how often it was seen; guarded by issuesLock. */
    private final LinkedHashMap<String, StoreErrorIssue> storeErrors = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, StoreErrorIssue> eldest) {
            return size() > MAX_ISSUES;
        }
    };

    private record StoreErrorIssue(Issue issue, int count) {}

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
     * issue, so a batch does not push the other issues out. An outage repeats the same error text: those errors
     * share one issue, which carries how often it was seen and the time of the latest, and moves to the end.
     *
     * @return false when this error was already recorded or only repeats an earlier one
     */
    boolean storeError(Throwable error) {
        var now = clock.instant();
        lastStoreErrorAt = now;
        var text = error.toString();
        lastStoreError = text;
        issuesLock.lock();
        try {
            if (sameError(error, lastRecorded)) return false;
            lastRecorded = error;
            var previous = storeErrors.get(text);
            if (previous == null) {
                var issue = new Issue(now, "store-error", text, null);
                storeErrors.put(text, new StoreErrorIssue(issue, 1));
                append(issue);
                return true;
            }
            issues.remove(previous.issue());
            int count = previous.count() + 1;
            var issue = new Issue(now, "store-error", text + " (" + count + " times, latest at " + now + ")", null);
            storeErrors.put(text, new StoreErrorIssue(issue, count));
            append(issue);
            return false;
        } finally {
            issuesLock.unlock();
        }
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
            append(issue);
        } finally {
            issuesLock.unlock();
        }
    }

    private void append(Issue issue) {
        if (issues.size() == MAX_ISSUES) issues.removeFirst();
        issues.addLast(issue);
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

package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import today.bonfire.oss.bth4j.Issue;

/** What one runner knows about itself: last activity per service and the issues it noticed. */
public final class RunnerStatus {
    public static final int MAX_ISSUES = 100;

    private final Clock clock;
    private final ArrayDeque<Issue> issues = new ArrayDeque<>();
    private volatile Instant lastClaimAt;
    private volatile Instant lastMaintenanceAt;
    private volatile Instant lastStoreErrorAt;
    private volatile String lastStoreError;

    public RunnerStatus(Clock clock) {
        this.clock = clock;
    }

    public void claimed() {
        lastClaimAt = clock.instant();
    }

    public void maintained() {
        lastMaintenanceAt = clock.instant();
    }

    public void storeError(Throwable error) {
        lastStoreErrorAt = clock.instant();
        lastStoreError = error.toString();
        issue("store-error", error.toString(), null);
    }

    public void issue(String kind, String detail, Long taskId) {
        var issue = new Issue(clock.instant(), kind, detail, taskId);
        synchronized (issues) {
            if (issues.size() == MAX_ISSUES) issues.removeFirst();
            issues.addLast(issue);
        }
    }

    public List<Issue> issues() {
        synchronized (issues) {
            return new ArrayList<>(issues);
        }
    }

    public int issueCount() {
        synchronized (issues) {
            return issues.size();
        }
    }

    public Instant lastClaimAt() {
        return lastClaimAt;
    }

    public Instant lastMaintenanceAt() {
        return lastMaintenanceAt;
    }

    public Instant lastStoreErrorAt() {
        return lastStoreErrorAt;
    }

    public String lastStoreError() {
        return lastStoreError;
    }
}

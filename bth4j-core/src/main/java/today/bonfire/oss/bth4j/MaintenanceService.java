package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.TaskEvent.Outcome;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * Housekeeping, every check interval: ends tasks whose lease expired (retried, dead, or cancelled when a
 * cancel request was pending), expires tasks that missed their start-by time and records this runner's
 * heartbeat. Every purge interval: purges finished tasks past their retention, releases unique keys whose
 * window passed and deletes stale runner rows. Jobs run on the scheduler thread itself; {@link #stopThread()}
 * returns at once and {@link #waitTillDone(Duration)} waits for a job in progress. A job that works through
 * many batches does not hold the others back: a purge runs a cycle between two batches, and a cycle writes
 * the heartbeat again between two batches, whenever a check interval has passed.
 */
class MaintenanceService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(Loggers.MAINTENANCE);

    private final ScheduledExecutorService executor;
    private final FullStore store;
    private final Names names;
    private final RetryPolicies retryPolicies;
    private final Observers observer;
    private final Clock clock;
    private final RunnerStatus status;
    private final Duration checkInterval;
    private final Duration purgeInterval;
    private final Map<TaskState, Duration> retention;
    private final boolean keepDead;
    private final FailureDetail failureDetail;
    private final int batchSize;
    private final int purgeBatchSize;
    private final int runnerId;
    private final String runnerLabel;
    private final Set<Integer> runnerQueueIds;
    private final Set<Integer> runnerTypeIds;
    private final IntSupplier running;
    private final BooleanSupplier accepting;
    private final Duration runnerStaleTimeout;
    private final LongSupplier nanoTime;
    private long lastCycleEnd;
    private long lastHeartbeat;

    MaintenanceService(Builder b) {
        super(b.group, b.threadName);
        this.store = b.store;
        this.names = b.names;
        this.retryPolicies = b.retryPolicies;
        this.observer = b.observer;
        this.clock = b.clock;
        this.status = b.status != null ? b.status : new RunnerStatus(b.clock);
        this.checkInterval = b.checkInterval;
        this.purgeInterval = b.purgeInterval;
        this.retention = Map.copyOf(b.retention);
        this.keepDead = b.keepDead;
        this.failureDetail = b.failureDetail;
        this.batchSize = b.batchSize;
        this.purgeBatchSize = b.purgeBatchSize;
        this.runnerId = b.runnerId;
        this.runnerLabel = b.runnerLabel;
        this.runnerQueueIds = b.runnerQueueIds;
        this.runnerTypeIds = b.runnerTypeIds;
        this.running = b.running;
        this.accepting = b.accepting;
        this.runnerStaleTimeout = b.runnerStaleTimeout;
        this.nanoTime = b.nanoTime;
        this.lastCycleEnd = nanoTime.getAsLong();
        this.lastHeartbeat = lastCycleEnd;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(b.group, r, b.threadName + "-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void run() {
        var delay = checkInterval.toMillis();
        try {
            executor.scheduleWithFixedDelay(survivingErrors(this::cycle), delay, delay, TimeUnit.MILLISECONDS);
            executor.scheduleWithFixedDelay(
                    survivingErrors(this::purge), delay, purgeInterval.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // stopped before this thread got to run: there is nothing left to schedule
            if (!executor.isShutdown()) throw e;
        }
    }

    /** A periodic job that throws is never run again by its executor, so an Error must not leave the job. */
    private static Runnable survivingErrors(Runnable job) {
        return () -> {
            try {
                job.run();
            } catch (Error e) {
                log.error("Maintenance job failed, it runs again at its next interval", e);
            }
        };
    }

    void cycle() {
        // first, so that a long backlog of expired leases cannot make this runner look gone
        boolean seen = heartbeat();
        reclaimExpiredLeases();
        expireMissedStarts();
        if (seen) status.maintained();
        lastCycleEnd = nanoTime.getAsLong();
    }

    /** Between two batches of a long purge, so that it does not hold back the heartbeat, lease recovery and expiry. */
    private void cycleIfDue() {
        if (nanoTime.getAsLong() - lastCycleEnd >= checkInterval.toNanos()) cycle();
    }

    /** Between two batches of a long backlog, so that it does not make this runner look gone. */
    private void heartbeatIfDue() {
        if (nanoTime.getAsLong() - lastHeartbeat >= checkInterval.toNanos() && heartbeat()) status.maintained();
    }

    private void reclaimExpiredLeases() {
        try {
            int count;
            int handled;
            boolean more;
            do {
                var expired = store.reclaim(batchSize);
                count = expired.size();
                handled = 0;
                for (var e : expired) {
                    try {
                        handleExpired(e);
                        handled++;
                    } catch (RuntimeException ex) {
                        log.error("Could not handle the expired lease of task {}", e.id(), ex);
                    }
                }
                // a lease that could not be handled stays expired, so a batch of only those would come back at once
                more = count == batchSize && handled > 0 && canContinueProcessing;
                if (more) heartbeatIfDue();
            } while (more);
        } catch (Exception e) {
            status.storeError(e);
            log.error("Error in reclaiming expired leases", e);
        }
    }

    private void handleExpired(ExpiredLease e) {
        TaskRecord record;
        try {
            record = store.task(e.id()).orElse(null);
        } catch (UnreadableTaskException unreadable) {
            endUnreadable(e, unreadable);
            return;
        }
        if (e.cancelRequested()) {
            cancelExpired(e, record);
            return;
        }
        Instant retryAt = null;
        if (e.attempts() < e.maxAttempts()
                && record != null
                && decide(record) instanceof RetryPolicy.Decision.RetryAfter r) {
            retryAt = StoredTime.after(clock.instant(), r.delay());
        }
        var failure = new FailureInfo(
                e.attempts(), clock.instant(), "lease-expired", "lease expired after attempt " + e.attempts(), null);
        var pause = RecurringTaskService.pauseEffect(record);
        var outcome = store.failExpired(e.id(), e.revision(), failure, retryAt, keepDead, pause);
        log.warn("Lease of task {} expired after attempt {}: {}", e.id(), e.attempts(), outcome);
        if (outcome != FailOutcome.LOST) {
            observer.emit(leaseExpired(e, outcome == FailOutcome.DEAD));
        }
        if (outcome == FailOutcome.DEAD) pauseParkedSchedule(record, TaskState.DEAD, pause);
    }

    private TaskEvent.LeaseExpired leaseExpired(ExpiredLease e, boolean dead) {
        return new TaskEvent.LeaseExpired(
                e.id(),
                names.queueNameOrUnknown(e.queueId()),
                names.typeNameOrUnknown(e.typeId()),
                e.attempts(),
                e.maxAttempts(),
                dead);
    }

    /**
     * The store paused the definition of a fixed-delay occurrence that ended here, in the same step as the
     * transition, because nothing would re-arm it; this makes that visible so an operator can resume it.
     */
    private void pauseParkedSchedule(TaskRecord record, TaskState ended, ScheduleEffect pause) {
        if (pause == null || !pause.isApplied()) return;
        var detail = "schedule " + record.scheduleId() + " paused: occurrence ended " + ended;
        status.issue(Issue.Kind.SCHEDULE_PAUSED, detail, record.id());
        log.warn(detail);
    }

    /**
     * A task with a stored value that cannot be read could never run, so it ends DEAD whatever attempts it has
     * left. A claim leaves such a task out and ends its lease, which brings it here.
     */
    private void endUnreadable(ExpiredLease e, UnreadableTaskException unreadable) {
        var failure =
                new FailureInfo(e.attempts(), clock.instant(), "unreadable-record", unreadable.getMessage(), null);
        var outcome = store.failExpired(e.id(), e.revision(), failure, null, keepDead, null);
        if (outcome == FailOutcome.LOST) return;
        status.issue(Issue.Kind.CORRUPT_TASK, unreadable.getMessage(), e.id());
        log.error("Task {} has a stored value that cannot be read, it ended {}", e.id(), outcome, unreadable);
        observer.emit(leaseExpired(e, true));
    }

    /** A cancelled task whose runner died must not run again. */
    private void cancelExpired(ExpiredLease e, TaskRecord record) {
        var reason = record != null && record.cancelReason() != null ? record.cancelReason() : "cancel requested";
        var pause = RecurringTaskService.pauseEffect(record);
        var applied = store.finishCancelledExpired(e.id(), e.revision(), reason, true, pause);
        log.warn("Lease of cancelled task {} expired, task ended as cancelled: {}", e.id(), applied);
        if (applied && record != null) {
            observer.emit(new TaskEvent.Cancelled(names.view(record), reason, Outcome.APPLIED));
            pauseParkedSchedule(record, TaskState.CANCELLED, pause);
        }
    }

    /** An Error from the policy, fatal or not, is logged and answered with an immediate retry; it never ends the maintenance thread. */
    private RetryPolicy.Decision decide(TaskRecord record) {
        var policy = retryPolicies.named(record.retryPolicy());
        if (policy == null) {
            status.issue(Issue.Kind.RETRY_POLICY, "unknown retry policy " + record.retryPolicy(), record.id());
            log.warn("Task {} names unknown retry policy {}, using the default", record.id(), record.retryPolicy());
            policy = retryPolicies.defaultPolicy();
        }
        try {
            var d = policy.decide(names.view(record), null);
            return d != null ? d : RetryPolicy.Decision.retryAfter(Duration.ZERO);
        } catch (RuntimeException | Error policyError) {
            if (failureDetail == FailureDetail.FULL) {
                log.error("Retry policy failed for expired task {}, retrying at once", record.id(), policyError);
            } else {
                log.error(
                        "Retry policy failed for expired task {}, retrying at once: {}",
                        record.id(),
                        Failures.describe(failureDetail, policyError));
            }
            status.issue(Issue.Kind.RETRY_POLICY, Failures.describe(failureDetail, policyError), record.id());
            return RetryPolicy.Decision.retryAfter(Duration.ZERO);
        }
    }

    private void expireMissedStarts() {
        try {
            int count;
            boolean more;
            do {
                var tasks = store.expire(batchSize);
                count = tasks.size();
                for (var t : tasks) {
                    observer.emit(new TaskEvent.Expired(
                            t.id(), names.queueNameOrUnknown(t.queueId()), names.typeNameOrUnknown(t.typeId())));
                }
                if (count > 0) log.info("Expired {} tasks that missed their start-by time", count);
                more = count == batchSize && canContinueProcessing;
                if (more) heartbeatIfDue();
            } while (more);
        } catch (Exception e) {
            status.storeError(e);
            log.error("Error in expiring tasks", e);
        }
    }

    void heartbeatNow() {
        try {
            executor.execute(survivingErrors(() -> {
                if (heartbeat()) status.maintained();
            }));
        } catch (RejectedExecutionException e) {
            // A concurrent stop leaves the next service generation to publish the state.
            if (!executor.isShutdown()) throw e;
        }
    }

    /** @return false when the store call failed */
    private boolean heartbeat() {
        lastHeartbeat = nanoTime.getAsLong();
        try {
            if (runnerId > 0) {
                store.heartbeat(
                        runnerId,
                        runnerLabel,
                        runnerQueueIds,
                        runnerTypeIds,
                        running.getAsInt(),
                        accepting.getAsBoolean());
            }
            return true;
        } catch (Exception e) {
            status.storeError(e);
            log.error("Heartbeat of runner {} failed", runnerId, e);
            return false;
        }
    }

    void purge() {
        try {
            for (var entry : retention.entrySet()) {
                long removed = 0;
                long batch;
                boolean more;
                do {
                    batch = store.purge(entry.getKey(), entry.getValue(), purgeBatchSize);
                    removed += batch;
                    more = batch == purgeBatchSize && canContinueProcessing;
                    if (more) cycleIfDue();
                } while (more);
                if (removed > 0) {
                    log.info("Purged {} {} tasks", removed, entry.getKey());
                    observer.emit(new TaskEvent.Purged(entry.getKey(), removed));
                }
            }
            int released;
            boolean more;
            do {
                released = store.releaseKeys(purgeBatchSize);
                more = released == purgeBatchSize && canContinueProcessing;
                if (more) cycleIfDue();
            } while (more);
            int runners = store.purgeRunners(runnerStaleTimeout);
            if (runners > 0) log.info("Removed {} runners without a heartbeat for {}", runners, runnerStaleTimeout);
        } catch (Exception e) {
            status.storeError(e);
            log.error("Error in purging finished tasks", e);
        }
    }

    /** Stops the maintenance schedule without waiting; a job in progress finishes. */
    @Override
    void stopThread() {
        super.stopThread();
        executor.shutdown();
    }

    @Override
    void waitTillDone() throws InterruptedException {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /** A job still running when the timeout elapses is interrupted. @return true when terminated in time */
    @Override
    boolean waitTillDone(Duration timeout) throws InterruptedException {
        if (executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
            return true;
        }
        executor.shutdownNow();
        return false;
    }

    @Override
    boolean isRunning() {
        return !executor.isShutdown();
    }

    /** The thread only schedules the jobs and ends at once; a job still in a store call runs on the executor. */
    @Override
    boolean isDone() {
        return !isAlive() && executor.isTerminated();
    }

    static class Builder {
        private ThreadGroup group;
        private String threadName;
        private FullStore store;
        private Names names;
        private RetryPolicies retryPolicies = RetryPolicies.of(RetryPolicy.defaultPolicy());
        private Observers observer = new Observers(null);
        private Clock clock = Clock.systemUTC();
        private RunnerStatus status;
        private Duration checkInterval = Duration.ofSeconds(5);
        private Duration purgeInterval = Duration.ofMinutes(1);
        private Map<TaskState, Duration> retention = Map.of();
        private boolean keepDead = true;
        private FailureDetail failureDetail = FailureDetail.FULL;
        private int batchSize = 100;
        private int purgeBatchSize = 500;
        private int runnerId;
        private String runnerLabel;
        private Set<Integer> runnerQueueIds = Set.of();
        private Set<Integer> runnerTypeIds = Set.of();
        private IntSupplier running = () -> 0;
        private BooleanSupplier accepting = () -> true;
        private Duration runnerStaleTimeout = Duration.ofMinutes(10);
        private LongSupplier nanoTime = System::nanoTime;

        Builder group(ThreadGroup group) {
            this.group = group;
            return this;
        }

        Builder threadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        Builder store(FullStore store) {
            this.store = store;
            return this;
        }

        Builder names(Names names) {
            this.names = names;
            return this;
        }

        Builder retryPolicies(RetryPolicies retryPolicies) {
            this.retryPolicies = retryPolicies;
            return this;
        }

        Builder observer(Observers observer) {
            this.observer = observer;
            return this;
        }

        Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        Builder status(RunnerStatus status) {
            this.status = status;
            return this;
        }

        Builder checkInterval(Duration checkInterval) {
            this.checkInterval = checkInterval;
            return this;
        }

        Builder purgeInterval(Duration purgeInterval) {
            this.purgeInterval = purgeInterval;
            return this;
        }

        /** Retention per terminal state; a state that is missing is never purged by this runner. */
        Builder retention(Map<TaskState, Duration> retention) {
            this.retention = retention;
            return this;
        }

        Builder keepDead(boolean keepDead) {
            this.keepDead = keepDead;
            return this;
        }

        Builder failureDetail(FailureDetail failureDetail) {
            this.failureDetail = failureDetail;
            return this;
        }

        Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        Builder purgeBatchSize(int purgeBatchSize) {
            this.purgeBatchSize = purgeBatchSize;
            return this;
        }

        /**
         * The runner whose heartbeat this service records, with what it registered so that the heartbeat can
         * restore a purged row; 0 records none.
         */
        Builder runner(int runnerId, String label, Set<Integer> queueIds, Set<Integer> typeIds, IntSupplier running) {
            this.runnerId = runnerId;
            this.runnerLabel = label;
            this.runnerQueueIds = queueIds;
            this.runnerTypeIds = typeIds;
            this.running = running;
            return this;
        }

        Builder accepting(BooleanSupplier accepting) {
            this.accepting = accepting;
            return this;
        }

        Builder runnerStaleTimeout(Duration runnerStaleTimeout) {
            this.runnerStaleTimeout = runnerStaleTimeout;
            return this;
        }

        /** The source of elapsed time between maintenance cycles; a test passes its own. */
        Builder nanoTime(LongSupplier nanoTime) {
            this.nanoTime = nanoTime;
            return this;
        }

        MaintenanceService create() {
            if (group == null || threadName == null)
                throw new TaskConfigurationException("group and threadName are required");
            if (store == null || names == null) throw new TaskConfigurationException("store and names are required");
            return new MaintenanceService(this);
        }
    }
}

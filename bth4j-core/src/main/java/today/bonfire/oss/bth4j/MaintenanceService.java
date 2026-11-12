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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.TaskEvent.Outcome;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * Housekeeping, every check interval: ends tasks whose lease expired (retried, dead, or cancelled when a
 * cancel request was pending), expires tasks that missed their start-by time and records this worker's
 * heartbeat. Every purge interval: purges finished tasks past their retention, releases unique keys whose
 * window passed and deletes stale worker rows. Jobs run on the scheduler thread itself; {@link #stopThread()}
 * returns at once and {@link #waitTillDone(Duration)} waits for a job in progress.
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
    private final int workerId;
    private final String workerLabel;
    private final Set<Integer> workerQueueIds;
    private final Set<Integer> workerTypeIds;
    private final IntSupplier running;
    private final BooleanSupplier accepting;
    private final Duration workerStaleAfter;

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
        this.workerId = b.workerId;
        this.workerLabel = b.workerLabel;
        this.workerQueueIds = b.workerQueueIds;
        this.workerTypeIds = b.workerTypeIds;
        this.running = b.running;
        this.accepting = b.accepting;
        this.workerStaleAfter = b.workerStaleAfter;
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
        // first, so that a long backlog of expired leases cannot make this worker look gone
        boolean seen = heartbeat();
        reclaimExpiredLeases();
        expireMissedStarts();
        if (seen) status.maintained();
    }

    private void reclaimExpiredLeases() {
        try {
            int count;
            int handled;
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
            } while (count == batchSize && handled > 0 && canContinueProcessing);
        } catch (Exception e) {
            status.storeError(e);
            log.error("Error in reclaiming expired leases", e);
        }
    }

    private void handleExpired(ExpiredLease e) {
        TaskRecord record;
        try {
            record = store.getTask(e.id()).orElse(null);
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
            observer.emit(
                    new TaskEvent.LeaseExpired(e.id(), e.attempts(), e.maxAttempts(), outcome == FailOutcome.DEAD));
        }
        if (outcome == FailOutcome.DEAD) pauseParkedSchedule(record, TaskState.DEAD, pause);
    }

    /**
     * The store paused the definition of a fixed-delay occurrence that ended here, in the same step as the
     * transition, because nothing would re-arm it; this makes that visible so an operator can resume it.
     */
    private void pauseParkedSchedule(TaskRecord record, TaskState ended, ScheduleEffect pause) {
        if (pause == null || !pause.applied()) return;
        var detail = "schedule " + record.scheduleId() + " paused: occurrence ended " + ended;
        status.issue("schedule-paused", detail, record.id());
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
        status.issue("corrupt-task", unreadable.getMessage(), e.id());
        log.error("Task {} has a stored value that cannot be read, it ended {}", e.id(), outcome, unreadable);
        observer.emit(new TaskEvent.LeaseExpired(e.id(), e.attempts(), e.maxAttempts(), true));
    }

    /** A cancelled task whose worker died must not run again. */
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
            status.issue("retry-policy", "unknown retry policy " + record.retryPolicy(), record.id());
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
            status.issue("retry-policy", Failures.describe(failureDetail, policyError), record.id());
            return RetryPolicy.Decision.retryAfter(Duration.ZERO);
        }
    }

    private void expireMissedStarts() {
        try {
            int count;
            do {
                var ids = store.expire(batchSize);
                count = ids.size();
                for (var id : ids) {
                    observer.emit(new TaskEvent.Expired(id));
                }
                if (count > 0) log.info("Expired {} tasks that missed their start-by time", count);
            } while (count == batchSize && canContinueProcessing);
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
        try {
            if (workerId > 0) {
                store.heartbeat(
                        workerId,
                        workerLabel,
                        workerQueueIds,
                        workerTypeIds,
                        running.getAsInt(),
                        accepting.getAsBoolean());
            }
            return true;
        } catch (Exception e) {
            status.storeError(e);
            log.error("Heartbeat of worker {} failed", workerId, e);
            return false;
        }
    }

    void purge() {
        try {
            for (var entry : retention.entrySet()) {
                long removed = 0;
                long batch;
                do {
                    batch = store.purge(entry.getKey(), entry.getValue(), purgeBatchSize);
                    removed += batch;
                } while (batch == purgeBatchSize && canContinueProcessing);
                if (removed > 0) {
                    log.info("Purged {} {} tasks", removed, entry.getKey());
                    observer.emit(new TaskEvent.Purged(entry.getKey(), removed));
                }
            }
            int released;
            do {
                released = store.releaseKeys(purgeBatchSize);
            } while (released == purgeBatchSize && canContinueProcessing);
            int workers = store.purgeWorkers(workerStaleAfter);
            if (workers > 0) log.info("Removed {} workers without a heartbeat for {}", workers, workerStaleAfter);
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
        private int workerId;
        private String workerLabel;
        private Set<Integer> workerQueueIds = Set.of();
        private Set<Integer> workerTypeIds = Set.of();
        private IntSupplier running = () -> 0;
        private BooleanSupplier accepting = () -> true;
        private Duration workerStaleAfter = Duration.ofMinutes(10);

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
         * The worker whose heartbeat this service records, with what it registered so that the heartbeat can
         * restore a purged row; 0 records none.
         */
        Builder worker(int workerId, String label, Set<Integer> queueIds, Set<Integer> typeIds, IntSupplier running) {
            this.workerId = workerId;
            this.workerLabel = label;
            this.workerQueueIds = queueIds;
            this.workerTypeIds = typeIds;
            this.running = running;
            return this;
        }

        Builder accepting(BooleanSupplier accepting) {
            this.accepting = accepting;
            return this;
        }

        Builder workerStaleAfter(Duration workerStaleAfter) {
            this.workerStaleAfter = workerStaleAfter;
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

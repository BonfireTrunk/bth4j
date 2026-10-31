package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.RetryPolicy;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.TaskEvent.Outcome;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * Housekeeping, every check interval: ends tasks whose lease expired (retried, dead, or cancelled when a
 * cancel request was pending), expires tasks that missed their start-by time and records this worker's
 * heartbeat. Every purge interval: purges finished tasks past their retention, releases unique keys whose
 * window passed and deletes stale worker rows. Jobs run on the scheduler thread itself; {@link #stopThread()}
 * returns at once and {@link #waitTillDone(Duration)} waits for a job in progress.
 */
public class MaintenanceService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(MaintenanceService.class);

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
    private final int batchSize;
    private final int purgeBatchSize;
    private final int workerId;
    private final IntSupplier running;
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
        this.batchSize = b.batchSize;
        this.purgeBatchSize = b.purgeBatchSize;
        this.workerId = b.workerId;
        this.running = b.running;
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
        executor.scheduleWithFixedDelay(this::cycle, delay, delay, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(this::purge, delay, purgeInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void cycle() {
        reclaimExpiredLeases();
        expireMissedStarts();
        heartbeat();
    }

    private void reclaimExpiredLeases() {
        try {
            int count;
            do {
                var expired = store.reclaim(batchSize);
                count = expired.size();
                for (var e : expired) {
                    try {
                        handleExpired(e);
                    } catch (RuntimeException ex) {
                        log.error("Could not handle the expired lease of task {}", e.id(), ex);
                    }
                }
            } while (count == batchSize && canContinueProcessing);
        } catch (Exception e) {
            status.storeError(e);
            log.error("Error in reclaiming expired leases", e);
        }
    }

    private void handleExpired(ExpiredLease e) {
        if (e.cancelRequested()) {
            cancelExpired(e);
            return;
        }
        Instant retryAt = null;
        var record = store.getTask(e.id()).orElse(null);
        if (e.attempts() < e.maxAttempts()
                && record != null
                && decide(record) instanceof RetryPolicy.Decision.RetryAfter r) {
            retryAt = clock.instant().plus(r.delay());
        }
        var failure = new FailureInfo(
                e.attempts(), clock.instant(), "lease-expired", "lease expired after attempt " + e.attempts(), null);
        var outcome = store.failExpired(e.id(), e.revision(), failure, retryAt, keepDead);
        log.warn("Lease of task {} expired after attempt {}: {}", e.id(), e.attempts(), outcome);
        if (outcome != FailOutcome.LOST) {
            observer.emit(
                    new TaskEvent.LeaseExpired(e.id(), e.attempts(), e.maxAttempts(), outcome == FailOutcome.DEAD));
        }
        if (outcome == FailOutcome.DEAD) pauseParkedSchedule(record, TaskState.DEAD);
    }

    /**
     * A fixed-delay definition is re-armed by its occurrence's own execution, so an occurrence that ends here
     * would leave it parked forever; pausing makes that visible and lets an operator resume it.
     */
    private void pauseParkedSchedule(TaskRecord record, TaskState ended) {
        if (!RecurringTaskService.isFixedDelayOccurrence(record)) return;
        try {
            store.pauseRecurring(record.scheduleId(), true);
            var detail = "schedule " + record.scheduleId() + " paused: occurrence ended " + ended;
            status.issue("schedule-paused", detail, record.id());
            log.warn(detail);
        } catch (RuntimeException e) {
            status.storeError(e);
            log.error("Could not pause schedule {} after task {}", record.scheduleId(), record.id(), e);
        }
    }

    /** A cancelled task whose worker died must not run again. */
    private void cancelExpired(ExpiredLease e) {
        var record = store.getTask(e.id()).orElse(null);
        var reason = record != null && record.cancelReason() != null ? record.cancelReason() : "cancel requested";
        var applied = store.finishCancelled(e.id(), e.revision(), reason, true);
        log.warn("Lease of cancelled task {} expired, task ended as cancelled: {}", e.id(), applied);
        if (applied && record != null) {
            observer.emit(new TaskEvent.Cancelled(names.view(record), reason, Outcome.APPLIED));
            pauseParkedSchedule(record, TaskState.CANCELLED);
        }
    }

    private RetryPolicy.Decision decide(TaskRecord record) {
        var policy = retryPolicies.named(record.retryPolicy());
        if (policy == null) policy = retryPolicies.defaultPolicy();
        try {
            var d = policy.decide(names.view(record), null);
            return d != null ? d : RetryPolicy.Decision.retryAfter(Duration.ZERO);
        } catch (RuntimeException policyError) {
            log.error("Retry policy failed for expired task {}, retrying at once", record.id(), policyError);
            status.issue("retry-policy", policyError.toString(), record.id());
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

    private void heartbeat() {
        try {
            if (workerId > 0) store.heartbeat(workerId, running.getAsInt());
            status.maintained();
        } catch (Exception e) {
            status.storeError(e);
            log.error("Heartbeat of worker {} failed", workerId, e);
        }
    }

    private void purge() {
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
    public void stopThread() {
        super.stopThread();
        executor.shutdown();
    }

    @Override
    public void waitTillDone() throws InterruptedException {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /** A job still running when the timeout elapses is interrupted. @return true when terminated in time */
    @Override
    public boolean waitTillDone(Duration timeout) throws InterruptedException {
        if (executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
            return true;
        }
        executor.shutdownNow();
        return false;
    }

    @Override
    public boolean isRunning() {
        return !executor.isShutdown();
    }

    public static class Builder {
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
        private int batchSize = 100;
        private int purgeBatchSize = 500;
        private int workerId;
        private IntSupplier running = () -> 0;
        private Duration workerStaleAfter = Duration.ofMinutes(10);

        public Builder group(ThreadGroup group) {
            this.group = group;
            return this;
        }

        public Builder threadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        public Builder store(FullStore store) {
            this.store = store;
            return this;
        }

        public Builder names(Names names) {
            this.names = names;
            return this;
        }

        public Builder retryPolicies(RetryPolicies retryPolicies) {
            this.retryPolicies = retryPolicies;
            return this;
        }

        public Builder observer(Observers observer) {
            this.observer = observer;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder status(RunnerStatus status) {
            this.status = status;
            return this;
        }

        public Builder checkInterval(Duration checkInterval) {
            this.checkInterval = checkInterval;
            return this;
        }

        public Builder purgeInterval(Duration purgeInterval) {
            this.purgeInterval = purgeInterval;
            return this;
        }

        /** Retention per terminal state; a state that is missing is never purged by this runner. */
        public Builder retention(Map<TaskState, Duration> retention) {
            this.retention = retention;
            return this;
        }

        public Builder keepDead(boolean keepDead) {
            this.keepDead = keepDead;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder purgeBatchSize(int purgeBatchSize) {
            this.purgeBatchSize = purgeBatchSize;
            return this;
        }

        /** The worker whose heartbeat this service records; 0 records none. */
        public Builder worker(int workerId, IntSupplier running) {
            this.workerId = workerId;
            this.running = running;
            return this;
        }

        public Builder workerStaleAfter(Duration workerStaleAfter) {
            this.workerStaleAfter = workerStaleAfter;
            return this;
        }

        public MaintenanceService create() {
            if (group == null || threadName == null)
                throw new TaskConfigurationError("group and threadName are required");
            if (store == null || names == null) throw new TaskConfigurationError("store and names are required");
            return new MaintenanceService(this);
        }
    }
}

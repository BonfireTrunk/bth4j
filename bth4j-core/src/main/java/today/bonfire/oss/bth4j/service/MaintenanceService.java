package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.RetryPolicy;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * Housekeeping: fails tasks whose lease expired (so they are retried or die), purges finished tasks past
 * their retention and releases unique keys whose window passed. Jobs run on the scheduler thread itself;
 * {@link #stopThread()} returns at once and {@link #waitTillDone(Duration)} waits for a job in progress.
 */
public class MaintenanceService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(MaintenanceService.class);

    private final ScheduledExecutorService executor;
    private final FullStore store;
    private final Names names;
    private final RetryPolicy retryPolicy;
    private final Observers observer;
    private final Clock clock;
    private final Duration checkInterval;
    private final Duration purgeInterval;
    private final Map<TaskState, Duration> retention;
    private final boolean keepDead;
    private final int batchSize;
    private final int purgeBatchSize;

    MaintenanceService(Builder b) {
        super(b.group, b.threadName);
        this.store = b.store;
        this.names = b.names;
        this.retryPolicy = b.retryPolicy;
        this.observer = b.observer;
        this.clock = b.clock;
        this.checkInterval = b.checkInterval;
        this.purgeInterval = b.purgeInterval;
        this.retention = Map.copyOf(b.retention);
        this.keepDead = b.keepDead;
        this.batchSize = b.batchSize;
        this.purgeBatchSize = b.purgeBatchSize;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(b.group, r, b.threadName + "-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void run() {
        var delay = checkInterval.toMillis();
        executor.scheduleWithFixedDelay(this::reclaimExpiredLeases, delay, delay, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(this::purge, delay, purgeInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void reclaimExpiredLeases() {
        try {
            int count;
            do {
                var expired = store.reclaim(batchSize);
                count = expired.size();
                for (var e : expired) {
                    handleExpired(e);
                }
            } while (count == batchSize && canContinueProcessing);
        } catch (Exception e) {
            log.error("Error in reclaiming expired leases", e);
        }
    }

    private void handleExpired(ExpiredLease e) {
        Instant retryAt = null;
        if (e.attempts() < e.maxAttempts()) {
            var record = store.getTask(e.id()).orElse(null);
            if (record != null) {
                var decision = retryPolicy.decide(names.view(record), null);
                if (decision instanceof RetryPolicy.Decision.RetryAfter r) {
                    retryAt = clock.instant().plus(r.delay());
                }
            }
        }
        var failure = new FailureInfo(
                e.attempts(), clock.instant(), "lease-expired", "lease expired after attempt " + e.attempts(), null);
        var outcome = store.failExpired(e.id(), e.revision(), failure, retryAt, keepDead);
        log.warn("Lease of task {} expired after attempt {}: {}", e.id(), e.attempts(), outcome);
        if (outcome != FailOutcome.LOST) {
            observer.emit(
                    new TaskEvent.LeaseExpired(e.id(), e.attempts(), e.maxAttempts(), outcome == FailOutcome.DEAD));
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
        } catch (Exception e) {
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
        private RetryPolicy retryPolicy = RetryPolicy.defaultPolicy();
        private Observers observer = new Observers(null);
        private Clock clock = Clock.systemUTC();
        private Duration checkInterval = Duration.ofSeconds(5);
        private Duration purgeInterval = Duration.ofMinutes(1);
        private Map<TaskState, Duration> retention = Map.of();
        private boolean keepDead = true;
        private int batchSize = 100;
        private int purgeBatchSize = 500;

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

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
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

        public MaintenanceService create() {
            if (group == null || threadName == null)
                throw new TaskConfigurationError("group and threadName are required");
            if (store == null || names == null) throw new TaskConfigurationError("store and names are required");
            return new MaintenanceService(this);
        }
    }
}

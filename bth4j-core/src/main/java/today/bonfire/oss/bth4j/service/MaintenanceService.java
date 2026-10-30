package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.CustomThread;

/**
 * Housekeeping: fails tasks whose lease expired (so they are retried or die) and purges dead tasks
 * past their retention. Jobs run on the scheduler thread itself, so shutting the scheduler down waits
 * for a job in progress.
 */
public class MaintenanceService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(MaintenanceService.class);
    private static final Duration DEAD_PURGE_INTERVAL = Duration.ofHours(1);
    private static final long SHUTDOWN_WAIT_SECONDS = 30;

    private final ScheduledExecutorService executor;
    private final TaskOps taskOps;
    private final Duration checkInterval;
    private final Duration deadRetention;
    private final Function<Integer, Duration> taskRetryDelay;
    private final int batchSize;

    private final AtomicBoolean executorShutdownStarted = new AtomicBoolean(false);

    MaintenanceService(Builder builder) {
        super(builder.group, builder.threadName);
        this.taskOps = builder.taskOps;
        this.checkInterval = builder.checkInterval;
        this.deadRetention = builder.deadRetention;
        this.taskRetryDelay = builder.taskRetryDelay;
        this.batchSize = builder.batchSize;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(builder.group, r, builder.threadName + "-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void run() {
        var delay = checkInterval.toMillis();
        executor.scheduleWithFixedDelay(this::reclaimExpiredLeases, delay, delay, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(
                this::purgeDeadTasks, delay, DEAD_PURGE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void reclaimExpiredLeases() {
        try {
            log.trace("Maintenance Service: reclaiming expired leases");
            int count;
            do {
                var expired = taskOps.reclaim(batchSize);
                count = expired.size();
                for (var e : expired) {
                    var delay = retryDelay(e.attempts());
                    var outcome = taskOps.fail(e.id(), e.token(), delay, "lease expired after attempt " + e.attempts());
                    log.warn("Lease of task {} expired after attempt {}: {}", e.id(), e.attempts(), outcome);
                }
            } while (count == batchSize && canContinueProcessing);
        } catch (Exception e) {
            log.error("Error in reclaiming expired leases", e);
        }
    }

    private Duration retryDelay(int attempts) {
        var delay = taskRetryDelay.apply(attempts);
        return delay == null || delay.isNegative() ? Duration.ZERO : delay;
    }

    private void purgeDeadTasks() {
        try {
            var removed = taskOps.purgeDeadTasks(Instant.now().minus(deadRetention));
            if (removed > 0) {
                log.info("Maintenance Service: purged {} dead tasks", removed);
            }
        } catch (Exception e) {
            log.error("Error in purging dead tasks", e);
        }
    }

    /**
     * Stops the maintenance schedule. The scheduler is shut down right away and {@link #waitTillDone()}
     * returns once it has terminated, which includes a job that is still running.
     */
    @Override
    public void stopThread() {
        super.stopThread();
        shutdownExecutor();
    }

    /**
     * Shuts down the scheduler once and always counts down the done latch, also on timeout or interrupt.
     */
    private void shutdownExecutor() {
        if (!executorShutdownStarted.compareAndSet(false, true)) {
            return;
        }
        try {
            log.info("Maintenance Service: Stopping executor");
            executor.shutdown();
            if (executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
                log.info("Maintenance Service: Executor stopped");
            } else {
                log.error("Maintenance Service: Executor failed to stop within {} seconds", SHUTDOWN_WAIT_SECONDS);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            doneLatch.countDown();
        }
    }

    @Override
    public boolean isRunning() {
        return !executor.isShutdown();
    }

    public static class Builder {
        private ThreadGroup group;
        private String threadName;
        private TaskOps taskOps;
        private Duration checkInterval = Duration.ofSeconds(30);
        private Duration deadRetention = Duration.ofDays(30);
        private Function<Integer, Duration> taskRetryDelay = attempts -> Duration.ZERO;
        private int batchSize = 100;

        public Builder() {}

        public Builder setGroup(ThreadGroup group) {
            this.group = group;
            return this;
        }

        public Builder setThreadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        public Builder setTaskOps(TaskOps taskOps) {
            this.taskOps = taskOps;
            return this;
        }

        public Builder setCheckInterval(Duration checkInterval) {
            this.checkInterval = checkInterval;
            return this;
        }

        public Builder setDeadRetention(Duration deadRetention) {
            this.deadRetention = deadRetention;
            return this;
        }

        public Builder setTaskRetryDelay(Function<Integer, Duration> taskRetryDelay) {
            this.taskRetryDelay = taskRetryDelay;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public MaintenanceService create() {
            if (group == null) {
                throw new TaskConfigurationError("ThreadGroup is required");
            }
            if (threadName == null) {
                throw new TaskConfigurationError("ThreadName is required");
            }
            return new MaintenanceService(this);
        }
    }
}

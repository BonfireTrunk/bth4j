package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.spi.MaintenanceStore;

/** Moves scheduled tasks whose time has come into their queues. Any number of instances may run it. */
class ScheduledTaskService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(Loggers.PROMOTION);
    private static final Duration SHORTEST_WAIT = Duration.ofMillis(1);

    private final MaintenanceStore store;
    private final Clock clock;
    private final RunnerStatus status;
    private final Duration pollInterval;
    private final int batchSize;

    /**
     * The wait before the store is asked again about a task that is due by this clock and was not moved, which
     * happens while the store's clock is behind this one. It doubles with every such round, up to the poll
     * interval, so that a skew of seconds costs a few calls and not one per millisecond.
     */
    private Duration overdueWait = SHORTEST_WAIT;

    ScheduledTaskService(Builder builder) {
        super(builder.group, builder.threadName);
        this.store = builder.store;
        this.clock = builder.clock;
        this.status = builder.status;
        this.pollInterval = builder.pollInterval;
        this.batchSize = builder.batchSize;
    }

    @Override
    public void run() {
        try {
            while (this.canContinueProcessing) {
                try {
                    var r = store.promote(batchSize);
                    if (r.moved() > 0) {
                        log.trace("Moved {} scheduled tasks to their queues", r.moved());
                        overdueWait = SHORTEST_WAIT;
                    }
                    if (r.moved() == batchSize) {
                        continue;
                    }
                    Thread.sleep(sleepTime(r.nextDue()));
                } catch (InterruptedException e) {
                    if (this.canContinueProcessing) {
                        log.error("Scheduled task service Thread interrupted", e);
                    } else {
                        log.debug("Scheduled task service Thread interrupted while stopping");
                    }
                } catch (Exception e) {
                    status.storeError(e);
                    log.error("Scheduled task service Thread error", e);
                    sleepAfterError();
                }
            }
        } finally {
            doneLatch.countDown();
        }
    }

    Duration sleepTime(Instant nextDue) {
        if (nextDue == null) {
            overdueWait = SHORTEST_WAIT;
            return pollInterval;
        }
        // by the runner's clock, which is the one the store's due times follow
        var untilDue = Duration.between(clock.instant(), nextDue);
        if (untilDue.isPositive()) {
            overdueWait = SHORTEST_WAIT;
            return shorter(untilDue.compareTo(SHORTEST_WAIT) < 0 ? SHORTEST_WAIT : untilDue, pollInterval);
        }
        var wait = shorter(overdueWait, pollInterval);
        overdueWait = shorter(overdueWait.multipliedBy(2), pollInterval);
        return wait;
    }

    private static Duration shorter(Duration a, Duration b) {
        return a.compareTo(b) < 0 ? a : b;
    }

    private void sleepAfterError() {
        try {
            Thread.sleep(pollInterval);
        } catch (InterruptedException e) {
            if (this.canContinueProcessing) {
                log.error("Scheduled task service Thread interrupted", e);
            } else {
                log.debug("Scheduled task service Thread interrupted while stopping");
            }
        }
    }

    static class Builder {
        private ThreadGroup group;
        private String threadName;
        private MaintenanceStore store;
        private Clock clock = Clock.systemUTC();
        private RunnerStatus status;
        private Duration pollInterval = Duration.ofSeconds(1);
        private int batchSize = 100;

        Builder() {}

        Builder setGroup(ThreadGroup group) {
            this.group = group;
            return this;
        }

        Builder setThreadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        Builder setStore(MaintenanceStore store) {
            this.store = store;
            return this;
        }

        Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        Builder setStatus(RunnerStatus status) {
            this.status = status;
            return this;
        }

        Builder setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        ScheduledTaskService create() {
            return new ScheduledTaskService(this);
        }
    }
}

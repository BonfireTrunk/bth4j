package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.store.DueSchedule;

/**
 * Creates the upcoming occurrences of recurring definitions. Any number of instances may run it; the
 * commit of an occurrence is atomic and compares the definition revision and fire time.
 */
public class RecurringTaskService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(RecurringTaskService.class);
    private static final int MAX_ROUNDS_PER_CYCLE = 100;

    private final TaskOps taskOps;
    private final Duration checkInterval;
    private final Duration queueAheadBy;
    private final int batchSize;

    RecurringTaskService(Builder builder) {
        super(builder.group, builder.threadName);
        this.taskOps = builder.taskOps;
        this.checkInterval = builder.checkInterval;
        this.queueAheadBy = builder.queueAheadBy;
        this.batchSize = builder.batchSize;
    }

    @Override
    public void run() {
        try {
            while (this.canContinueProcessing) {
                try {
                    queueUpcomingTasks();
                    Thread.sleep(checkInterval);
                } catch (InterruptedException e) {
                    if (this.canContinueProcessing) {
                        log.error("Recurring task service Thread interrupted", e);
                    } else {
                        log.debug("Recurring task service Thread interrupted while stopping");
                    }
                } catch (Exception e) {
                    log.error("Recurring task service Thread error", e);
                    sleepAfterError();
                }
            }
        } finally {
            doneLatch.countDown();
        }
    }

    private void sleepAfterError() {
        try {
            Thread.sleep(checkInterval);
        } catch (InterruptedException e) {
            if (this.canContinueProcessing) {
                log.error("Recurring task service Thread interrupted", e);
            } else {
                log.debug("Recurring task service Thread interrupted while stopping");
            }
        }
    }

    private void queueUpcomingTasks() {
        var queued = 0;
        try {
            for (var round = 0; round < MAX_ROUNDS_PER_CYCLE && this.canContinueProcessing; round++) {
                var due = taskOps.cronDue(Instant.now().plus(queueAheadBy), batchSize);
                var committed = 0;
                for (var d : due) {
                    if (queueIfDue(d)) {
                        committed++;
                    }
                }
                queued += committed;
                // nothing committed means every due definition is broken or was taken by another instance
                if (committed == 0) {
                    break;
                }
            }
        } finally {
            if (queued > 0) {
                log.info("Recurring task service queued {} tasks", queued);
            }
        }
    }

    /**
     * A bad definition (for example an unparseable cron expression) is logged and skipped so that it
     * cannot block the others.
     *
     * @return true when an occurrence was committed
     */
    private boolean queueIfDue(DueSchedule d) {
        try {
            var cron = d.definition().cron();
            var now = Instant.now();
            var after = d.fireAt().isAfter(now) ? d.fireAt() : now;
            var next = Task.nextExecution(cron, after);
            if (next.isEmpty()) {
                log.error("Recurring task {} never fires again, removing it", d.scheduleId());
                taskOps.deleteRecurringTask(d.scheduleId());
                return false;
            }
            return taskOps.cronCommit(d, next.get());
        } catch (Exception e) {
            log.error("Skipping recurring task {}", d.scheduleId(), e);
            return false;
        }
    }

    public static class Builder {
        private ThreadGroup group;
        private String threadName;
        private TaskOps taskOps;
        private Duration checkInterval = Duration.ofSeconds(10);
        private Duration queueAheadBy = Duration.ofSeconds(30);
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

        public Builder setQueueAheadBy(Duration queueAheadBy) {
            this.queueAheadBy = queueAheadBy;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public RecurringTaskService create() {
            return new RecurringTaskService(this);
        }
    }
}

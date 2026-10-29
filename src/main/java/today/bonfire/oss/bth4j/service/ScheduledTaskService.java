package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.executor.CustomThread;

/** Moves scheduled tasks whose time has come into their queues. Any number of instances may run it. */
public class ScheduledTaskService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskService.class);

    private final TaskOps taskOps;
    private final Duration pollInterval;
    private final int batchSize;

    ScheduledTaskService(Builder builder) {
        super(builder.group, builder.threadName);
        this.taskOps = builder.taskOps;
        this.pollInterval = builder.pollInterval;
        this.batchSize = builder.batchSize;
    }

    @Override
    public void run() {
        try {
            while (this.canContinueProcessing) {
                try {
                    var r = taskOps.promote(batchSize);
                    if (r.moved() > 0) {
                        log.trace("Moved {} scheduled tasks to their queues", r.moved());
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
                    log.error("Scheduled task service Thread error", e);
                    sleepAfterError();
                }
            }
        } finally {
            doneLatch.countDown();
        }
    }

    private Duration sleepTime(Instant nextDue) {
        if (nextDue == null) {
            return pollInterval;
        }
        var untilDue = Duration.between(Instant.now(), nextDue);
        var wait = untilDue.compareTo(Duration.ofMillis(1)) < 0 ? Duration.ofMillis(1) : untilDue;
        return wait.compareTo(pollInterval) < 0 ? wait : pollInterval;
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

    public static class Builder {
        private ThreadGroup group;
        private String threadName;
        private TaskOps taskOps;
        private Duration pollInterval = Duration.ofSeconds(1);
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

        public Builder setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public ScheduledTaskService create() {
            return new ScheduledTaskService(this);
        }
    }
}

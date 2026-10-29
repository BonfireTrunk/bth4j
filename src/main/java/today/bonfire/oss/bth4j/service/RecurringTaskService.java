package today.bonfire.oss.bth4j.service;

import java.time.Instant;
import java.util.Collections;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.common.THC;
import today.bonfire.oss.bth4j.executor.CustomThread;

public class RecurringTaskService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(RecurringTaskService.class);

    private final long checkInterval;
    private final long lockTimeout;
    private final long queueAheadBy;
    private final TaskOps taskOps;
    private final THC.Keys keys;
    private final Function<String, Event> eventParser;

    RecurringTaskService(Builder builder) {
        super(builder.group, builder.threadName);
        this.checkInterval = builder.checkInterval;
        this.lockTimeout = builder.lockTimeout;
        this.queueAheadBy = builder.queueTaskAheadDuration;
        this.taskOps = builder.taskOps;
        this.keys = builder.keys;
        this.eventParser = builder.eventParser;
    }

    @Override
    public void run() {
        try {
            while (this.canContinueProcessing) {
                try {
                    if (taskOps.acquireLock(keys.LOCK_RECURRING_TASKS, lockTimeout)) {
                        queueUpcomingTasks();
                    }

                    Thread.sleep(checkInterval);
                } catch (InterruptedException e) {
                    if (this.canContinueProcessing) {
                        log.error("Recurring task service Thread interrupted", e);
                    } else {
                        log.debug("Recurring task service Thread interrupted while stopping");
                    }
                } catch (Exception e) {
                    log.error("Recurring task service Thread error", e);
                }
            }
        } finally {
            doneLatch.countDown();
        }
    }

    /**
     * Scans all recurring task definitions and queues the ones that are due within the queue-ahead window.
     * Must be called with the lock held. The lock (valid for {@code lockTimeout} seconds) is refreshed after
     * every scan page and released on exit, also on exceptions, unless it could not be refreshed
     * (it may then belong to another instance).
     */
    private void queueUpcomingTasks() {
        var lockHeld = true;
        var itemsQueued = 0;
        try {
            var cursor = "0";
            do {
                var r = taskOps.scanHashSet(keys.RECURRING_TASK_SET, cursor);
                cursor = r.getCursor();
                var entries = r.getResult();

                for (var entry : entries) {
                    if (queueIfDue(entry.getKey(), entry.getValue())) {
                        itemsQueued++;
                    }
                }
                log.trace("Recurring task service analysed {} tasks", entries.size());

                if (!"0".equals(cursor)) {
                    // more pages to scan, make sure the lock does not expire in between
                    if (!taskOps.refreshLock(keys.LOCK_RECURRING_TASKS, lockTimeout)) {
                        log.error("Unable to refresh lock on recurring tasks");
                        lockHeld = false;
                        break;
                    }
                }
            } while (!"0".equals(cursor) && this.canContinueProcessing);
        } finally {
            if (lockHeld) {
                taskOps.releaseLock(keys.LOCK_RECURRING_TASKS);
            }
        }
        if (itemsQueued > 0) {
            log.info("Recurring task service queued {} tasks", itemsQueued);
        }
    }

    /**
     * Queues one instance of the recurring task if it is due within the queue-ahead window.
     * A malformed entry is logged and skipped so that it cannot block the other recurring tasks.
     *
     * @param taskString        task string of the recurring task, the hash key
     * @param lastExecutionTime epoch seconds of the last queued execution, the hash value
     * @return true if a task was queued
     */
    private boolean queueIfDue(String taskString, String lastExecutionTime) {
        try {
            var lastExecution = Instant.ofEpochSecond(Long.parseLong(lastExecutionTime));
            var task = new Task(taskString, eventParser);
            var nextExecutionTime = task.getNextExecutionTime(lastExecution);
            // only queue tasks that will run in the next queueAheadBy seconds
            if (Instant.now().plusSeconds(queueAheadBy).isAfter(nextExecutionTime)) {
                // queue the task based on the next execution time
                taskOps.addTaskToQueue(
                        Task.Builder.newTask()
                                .event(task.event())
                                .queueName(task.queueName())
                                .accountId(task.accountId())
                                .executeAtTimeStamp(nextExecutionTime)
                                .build(),
                        Collections.emptyMap());
                taskOps.updateExecutionTimeForRecurringTasks(taskString, nextExecutionTime);
                return true;
            }
        } catch (Exception e) {
            log.error("Skipping recurring task entry {}", taskString, e);
        }
        return false;
    }

    public static class Builder {

        private long checkInterval;
        private long queueTaskAheadDuration;
        private ThreadGroup group;
        private String threadName;
        private TaskOps taskOps;
        private THC.Keys keys;
        private Function<String, Event> eventParser;

        private long lockTimeout;

        public Builder() {}

        public Builder setGroup(ThreadGroup group) {
            this.group = group;
            return this;
        }

        public Builder setThreadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        public Builder setCheckInterval(long checkInterval) {
            this.checkInterval = checkInterval;
            return this;
        }

        public Builder setLockTimeout(long timeout) {
            this.lockTimeout = timeout;
            return this;
        }

        public Builder setTaskOps(TaskOps taskOps) {
            this.taskOps = taskOps;
            return this;
        }

        public Builder setKeys(THC.Keys keys) {
            this.keys = keys;
            return this;
        }

        public Builder setEventParser(Function<String, Event> eventParser) {
            this.eventParser = eventParser;
            return this;
        }

        public RecurringTaskService create() {
            return new RecurringTaskService(this);
        }

        public Builder setQueueTaskAheadBy(long duration) {
            this.queueTaskAheadDuration = duration;
            return this;
        }
    }
}

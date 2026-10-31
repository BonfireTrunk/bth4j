package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.common.Cron;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.ScheduleStore;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * Creates the occurrences of recurring definitions as they come due. Any number of instances may run it;
 * the commit of an occurrence is atomic and compares the definition revision and fire time, so each
 * occurrence is created once.
 */
public class RecurringTaskService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(RecurringTaskService.class);
    private static final int MAX_ROUNDS_PER_CYCLE = 100;
    private static final Duration NEVER = Duration.ofDays(365L * 100);

    private final ScheduleStore store;
    private final IdAllocator ids;
    private final Observers observer;
    private final Clock clock;
    private final Duration checkInterval;
    private final int batchSize;

    RecurringTaskService(Builder b) {
        super(b.group, b.threadName);
        this.store = b.store;
        this.ids = b.ids;
        this.observer = b.observer;
        this.clock = b.clock;
        this.checkInterval = b.checkInterval;
        this.batchSize = b.batchSize;
    }

    @Override
    public void run() {
        try {
            while (this.canContinueProcessing) {
                try {
                    queueDueOccurrences();
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

    private void queueDueOccurrences() {
        var queued = 0;
        try {
            for (var round = 0; round < MAX_ROUNDS_PER_CYCLE && this.canContinueProcessing; round++) {
                var due = store.dueRecurring(clock.instant().plus(checkInterval), batchSize);
                var committed = 0;
                for (var d : due) {
                    if (commit(d)) {
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
    private boolean commit(DueSchedule d) {
        try {
            var def = d.definition();
            var now = clock.instant();
            var after = d.fireAt().isAfter(now) ? d.fireAt() : now;
            // a definition that never fires again still gets this occurrence; parking its next fire far
            // ahead keeps it out of dueRecurring without a revision-checked delete
            var next = Cron.next(def.cron(), after).orElseGet(() -> {
                log.warn("Recurring task {} never fires again after {}", d.scheduleId(), d.fireAt());
                return d.fireAt().plus(NEVER);
            });
            var occurrence = TaskRecord.builder()
                    .id(ids.nextId())
                    .queueId(def.queueId())
                    .eventId(def.eventId())
                    .payloadVersion(def.payloadVersion())
                    .state(TaskState.SCHEDULED)
                    .maxAttempts(def.maxAttempts())
                    .created(now)
                    .due(d.fireAt())
                    .data(def.data())
                    .account(def.account())
                    .scheduleId(d.scheduleId())
                    .build();
            var committed = store.commitRecurring(d, next, occurrence);
            if (committed) {
                observer.emit(new TaskEvent.ScheduleFired(d.scheduleId(), occurrence.id(), d.fireAt()));
            }
            return committed;
        } catch (Exception e) {
            log.error("Skipping recurring task {}", d.scheduleId(), e);
            return false;
        }
    }

    public static class Builder {
        private ThreadGroup group;
        private String threadName;
        private ScheduleStore store;
        private IdAllocator ids;
        private Observers observer = new Observers(null);
        private Clock clock = Clock.systemUTC();
        private Duration checkInterval = Duration.ofSeconds(10);
        private int batchSize = 100;

        public Builder group(ThreadGroup group) {
            this.group = group;
            return this;
        }

        public Builder threadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        public Builder store(ScheduleStore store) {
            this.store = store;
            return this;
        }

        public Builder ids(IdAllocator ids) {
            this.ids = ids;
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

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public RecurringTaskService create() {
            if (group == null || threadName == null)
                throw new TaskConfigurationError("group and threadName are required");
            if (store == null || ids == null) throw new TaskConfigurationError("store and ids are required");
            return new RecurringTaskService(this);
        }
    }
}

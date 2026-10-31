package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.common.Cron;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.InspectionStore;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.ScheduleStore;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * Creates the occurrences of recurring definitions as they come due. Any number of instances may run it;
 * the commit of an occurrence is atomic and compares the definition revision and fire time, so each
 * occurrence is created once.
 * <p>
 * A cron fire time that is older than the misfire threshold (twice the check interval, at least one minute)
 * is handled by the definition's misfire policy. A fixed-delay definition fires once and is parked; the
 * occurrence re-arms it when it ends (see {@link TaskExecution}). An occurrence of a definition with overlap
 * protection carries the unique key {@code sched:<id>}, so the store skips it while the previous one runs.
 */
public class RecurringTaskService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(RecurringTaskService.class);
    private static final int MAX_ROUNDS_PER_CYCLE = 100;
    private static final Duration NEVER = Duration.ofDays(365L * 100);
    private static final Duration MIN_MISFIRE_THRESHOLD = Duration.ofMinutes(1);

    /** Header of a fixed-delay occurrence holding the delay as an ISO-8601 duration. */
    public static final String FIXED_DELAY_HEADER = "bth4j.fixed-delay";

    public static final String OVERLAP_KEY_PREFIX = "sched:";

    /** True for an occurrence of a fixed-delay definition; its definition is parked until the occurrence ends. */
    public static boolean isFixedDelayOccurrence(TaskRecord r) {
        return r != null
                && r.scheduleId() != null
                && r.headers() != null
                && r.headers().containsKey(FIXED_DELAY_HEADER);
    }

    private final ScheduleStore store;
    private final InspectionStore inspection;
    private final IdAllocator ids;
    private final Observers observer;
    private final Clock clock;
    private final RunnerStatus status;
    private final Duration checkInterval;
    private final Duration misfireThreshold;
    private final int batchSize;

    RecurringTaskService(Builder b) {
        super(b.group, b.threadName);
        this.store = b.store;
        this.inspection = b.inspection;
        this.ids = b.ids;
        this.observer = b.observer;
        this.clock = b.clock;
        this.status = b.status != null ? b.status : new RunnerStatus(b.clock);
        this.checkInterval = b.checkInterval;
        var twice = b.checkInterval.multipliedBy(2);
        this.misfireThreshold = twice.compareTo(MIN_MISFIRE_THRESHOLD) > 0 ? twice : MIN_MISFIRE_THRESHOLD;
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
                    status.storeError(e);
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
            if (def.isFixedDelay()) {
                return commitOccurrence(d, d.fireAt(), d.fireAt().plus(NEVER));
            }
            var zone = Cron.zone(def.zone());
            var late = Duration.between(d.fireAt(), now);
            if (late.compareTo(misfireThreshold) > 0) {
                return misfired(d, now, zone, late);
            }
            var after = d.fireAt().isAfter(now) ? d.fireAt() : now;
            return commitOccurrence(d, d.fireAt(), nextOrPark(def, after, zone, d.fireAt()));
        } catch (Exception e) {
            log.error("Skipping recurring task {}", d.scheduleId(), e);
            return false;
        }
    }

    private boolean misfired(DueSchedule d, Instant now, ZoneId zone, Duration late) {
        var def = d.definition();
        var misfire = def.misfire() == null ? RecurringDefinition.MISFIRE_FIRE_ONCE_NOW : def.misfire();
        switch (misfire) {
            case RecurringDefinition.MISFIRE_SKIP -> {
                var next = nextOrPark(def, now, zone, d.fireAt());
                store.setNextFire(d.scheduleId(), next);
                misfireIssue(d, late, "skipped, next at " + next);
                return false;
            }
            case RecurringDefinition.MISFIRE_CATCH_UP -> {
                var fires = new ArrayList<Instant>();
                var fire = d.fireAt();
                while (fire != null && !fire.isAfter(now) && fires.size() <= def.catchUpMax()) {
                    fires.add(fire);
                    fire = Cron.next(def.cron(), fire, zone).orElse(null);
                }
                if (fires.size() > def.catchUpMax()) {
                    var resume = fires.get(fires.size() - def.catchUpMax());
                    store.setNextFire(d.scheduleId(), resume);
                    misfireIssue(d, late, "catching up the last " + def.catchUpMax() + " fires from " + resume);
                    return false;
                }
                // each missed fire gets its own occurrence; the next round picks the following one up
                return commitOccurrence(d, d.fireAt(), nextOrPark(def, d.fireAt(), zone, d.fireAt()));
            }
            default -> {
                misfireIssue(d, late, "firing once now");
                return commitOccurrence(d, now, nextOrPark(def, now, zone, d.fireAt()));
            }
        }
    }

    private void misfireIssue(DueSchedule d, Duration late, String action) {
        var detail = "schedule " + d.scheduleId() + " missed its fire at " + d.fireAt() + " by " + late + ": " + action;
        log.warn(detail);
        status.issue("schedule-misfire", detail, null);
    }

    /**
     * A definition that never fires again still gets this occurrence; parking its next fire far ahead keeps it
     * out of dueRecurring without a revision-checked delete.
     */
    private Instant nextOrPark(RecurringDefinition def, Instant after, ZoneId zone, Instant fireAt) {
        return Cron.next(def.cron(), after, zone).orElseGet(() -> {
            log.warn("Recurring task {} never fires again after {}", def.scheduleId(), fireAt);
            return fireAt.plus(NEVER);
        });
    }

    private boolean commitOccurrence(DueSchedule d, Instant due, Instant next) {
        var def = d.definition();
        var builder = TaskRecord.builder()
                .id(ids.nextId())
                .queueId(def.queueId())
                .eventId(def.eventId())
                .payloadVersion(def.payloadVersion())
                .state(due.isAfter(clock.instant()) ? TaskState.SCHEDULED : TaskState.QUEUED)
                .maxAttempts(def.maxAttempts())
                .created(clock.instant())
                .due(due)
                .data(def.data())
                .account(def.account())
                .scheduleId(d.scheduleId());
        if (def.isFixedDelay()) {
            builder.headers(Map.of(
                    FIXED_DELAY_HEADER, Duration.ofMillis(def.fixedDelayMs()).toString()));
        }
        if (def.overlapSkip()) {
            builder.uniqueKey(OVERLAP_KEY_PREFIX + d.scheduleId());
        }
        var occurrence = builder.build();
        var committed = store.commitRecurring(d, next, occurrence);
        if (committed) {
            if (def.overlapSkip()
                    && inspection != null
                    && inspection.getTask(occurrence.id()).isEmpty()) {
                var detail = "schedule " + d.scheduleId() + " skipped its fire at " + d.fireAt()
                        + ": the previous occurrence has not finished";
                log.info(detail);
                status.issue("schedule-overlap", detail, null);
            } else {
                observer.emit(new TaskEvent.ScheduleFired(d.scheduleId(), occurrence.id(), d.fireAt()));
            }
        }
        return committed;
    }

    public static class Builder {
        private ThreadGroup group;
        private String threadName;
        private ScheduleStore store;
        private InspectionStore inspection;
        private IdAllocator ids;
        private Observers observer = new Observers(null);
        private Clock clock = Clock.systemUTC();
        private RunnerStatus status;
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

        /** Lets the service tell a skipped overlapping occurrence from a created one. Optional. */
        public Builder inspection(InspectionStore inspection) {
            this.inspection = inspection;
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

        public Builder status(RunnerStatus status) {
            this.status = status;
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

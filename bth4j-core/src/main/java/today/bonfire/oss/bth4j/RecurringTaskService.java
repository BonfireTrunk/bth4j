package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.common.Cron;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.CommitOutcome;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.InspectionStore;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.ScheduleStore;
import today.bonfire.oss.bth4j.spi.StoredSchedule;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * Creates the occurrences of recurring definitions as they come due. Any number of instances may run it;
 * the commit of an occurrence is atomic and compares the definition revision and fire time, so each
 * occurrence is created once.
 * <p>
 * A cron fire time that is older than the misfire threshold (twice the check interval, at least one minute)
 * is handled by the definition's misfire policy. A fixed-delay definition fires once and is parked; the
 * occurrence re-arms it when it ends (see {@link TaskExecution}). An occurrence of a definition with overlap
 * protection carries the unique key {@code sched:<id>}, so the store skips it while the previous one runs.
 * <p>
 * An occurrence is created up to one check interval before its fire time. A fire with overlap protection is
 * created that early only while no earlier occurrence is open; otherwise it waits for its fire time, and is
 * skipped only when the earlier one is still open then.
 */
class RecurringTaskService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(Loggers.RECURRING);
    private static final int MAX_ROUNDS_PER_CYCLE = 100;
    private static final Duration NEVER = Duration.ofDays(365L * 100);
    private static final Duration MIN_MISFIRE_THRESHOLD = Duration.ofMinutes(1);
    /** Fires walked to find the last ones a catch-up may create, per definition and check. */
    private static final int MAX_CATCH_UP_SCAN = 100_000;
    /** How far a definition that cannot be evaluated is pushed ahead, at least. */
    private static final Duration BROKEN_RETRY = Duration.ofHours(1);

    /** What a definition's turn achieved: nothing, a moved fire time without an occurrence, or an occurrence. */
    private enum Progress {
        NONE,
        ADVANCED,
        QUEUED
    }

    /** Header of a fixed-delay occurrence holding the delay as an ISO-8601 duration. */
    static final String FIXED_DELAY_HEADER = "bth4j.fixed-delay";

    static final String OVERLAP_KEY_PREFIX = "sched:";

    /** True for an occurrence of a fixed-delay definition; its definition is parked until the occurrence ends. */
    static boolean isFixedDelayOccurrence(TaskRecord r) {
        return r != null
                && r.scheduleId() != null
                && r.headers() != null
                && r.headers().containsKey(FIXED_DELAY_HEADER);
    }

    /** The re-arm to apply when this fixed-delay occurrence succeeds; null for any other task. */
    static ScheduleEffect rearmEffect(TaskRecord r, Instant nextFire) {
        return isFixedDelayOccurrence(r) ? ScheduleEffect.rearm(r.scheduleId(), r.id(), nextFire) : null;
    }

    /** The pause to apply when this fixed-delay occurrence ends without succeeding; null for any other task. */
    static ScheduleEffect pauseEffect(TaskRecord r) {
        return isFixedDelayOccurrence(r) ? ScheduleEffect.pause(r.scheduleId(), r.id()) : null;
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
    /** What the scan of a catch-up established, per definition, so that its later rounds do not scan again. */
    private final Map<String, CatchUpWindow> catchUpWindows = new HashMap<>();

    private long cycle;
    private final OutageLog outage = new OutageLog(log, "Recurring task service", System::nanoTime);

    /** The fires one scan found missed, for a revision; {@code cycle} is the last check that used it. */
    private record CatchUpWindow(long revision, Instant first, Instant last, long cycle) {
        boolean covers(long revision, Instant fire) {
            return this.revision == revision && !fire.isBefore(first) && !fire.isAfter(last);
        }
    }

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
                    outage.failed(e, "Recurring task service Thread error");
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

    void queueDueOccurrences() {
        var queued = 0;
        cycle++;
        try {
            for (var round = 0; round < MAX_ROUNDS_PER_CYCLE && this.canContinueProcessing; round++) {
                var due = store.dueSchedules(clock.instant().plus(checkInterval), batchSize);
                var progressed = 0;
                for (var d : due) {
                    Progress progress;
                    try {
                        progress = commit(d);
                    } catch (TaskStoreException e) {
                        status.storeError(e);
                        outage.failed(e, "Recurring task service could not reach the store, ending this round");
                        return;
                    }
                    if (progress == Progress.QUEUED) queued++;
                    if (progress != Progress.NONE) progressed++;
                }
                outage.succeeded();
                // no progress means every due definition was taken by another instance or could not be moved
                if (progressed == 0) {
                    break;
                }
            }
        } finally {
            // a definition that stopped catching up (deleted, paused, or its fires no longer late) was not used
            // in this check, so its window goes now instead of staying for good
            if (!catchUpWindows.isEmpty()) catchUpWindows.values().removeIf(w -> w.cycle() != cycle);
            if (queued > 0) {
                log.info("Recurring task service queued {} tasks", queued);
            }
        }
    }

    /**
     * A definition that cannot be evaluated (for example an unparseable cron expression or a removed zone) is
     * pushed ahead and reported, so that it cannot block the others behind it. A store failure is not the
     * definition's fault: it ends the round and moves nothing.
     */
    private Progress commit(DueSchedule d) {
        try {
            var def = d.definition();
            var now = clock.instant();
            if (def.isFixedDelay()) {
                return commitOccurrence(d, d.fireAt().plus(NEVER));
            }
            var zone = Cron.zone(def.zone());
            var late = Duration.between(d.fireAt(), now);
            // a fire an operator asked for is wanted however late this check comes
            if (!d.triggered() && late.compareTo(misfireThreshold) > 0) {
                return misfired(d, now, zone, late);
            }
            // catch-up gives every missed fire its own occurrence, also the ones that are late by less than the
            // threshold; the other policies continue from now
            var catchesUp = StoredSchedule.misfire(def.misfire(), def.catchUpMaxFires()) instanceof Misfire.CatchUp;
            var ahead = d.fireAt().isAfter(now);
            if (ahead && def.overlapSkip() && earlierOccurrenceMayBeOpen(d)) {
                return Progress.NONE;
            }
            var after = ahead || catchesUp ? d.fireAt() : now;
            return commitOccurrence(d, nextOrPark(def, after, zone, d.fireAt()));
        } catch (TaskStoreException e) {
            throw e;
        } catch (TaskValidationException e) {
            log.error("Recurring task {} cannot be evaluated: {}", d.scheduleId(), e.getMessage());
            return pushAhead(d, e);
        } catch (Exception e) {
            log.error("Recurring task {} cannot be evaluated", d.scheduleId(), e);
            return pushAhead(d, e);
        }
    }

    /** Without an inspection store the service cannot tell, so the fire waits for its time. */
    private boolean earlierOccurrenceMayBeOpen(DueSchedule d) {
        return inspection == null
                || inspection.taskByKey(OVERLAP_KEY_PREFIX + d.scheduleId()).isPresent();
    }

    private Progress pushAhead(DueSchedule d, Exception cause) {
        var twice = checkInterval.multipliedBy(2);
        var next = clock.instant().plus(twice.compareTo(BROKEN_RETRY) > 0 ? twice : BROKEN_RETRY);
        try {
            if (!store.setNextFire(d.scheduleId(), d.definition().revision(), d.fireAt(), next)) {
                return Progress.NONE;
            }
        } catch (TaskStoreException e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("Recurring task {} could not be moved ahead", d.scheduleId(), e);
            return Progress.NONE;
        }
        var detail = "schedule " + d.scheduleId() + " cannot be evaluated ("
                + cause.getClass().getSimpleName() + "), its next fire moved from " + d.fireAt() + " to " + next;
        status.issue(Issue.Kind.SCHEDULE_BROKEN, detail, null);
        return Progress.ADVANCED;
    }

    private Progress misfired(DueSchedule d, Instant now, ZoneId zone, Duration late) {
        var def = d.definition();
        var misfire = StoredSchedule.misfire(def.misfire(), def.catchUpMaxFires());
        switch (misfire) {
            case Misfire.Skip s -> {
                var next = nextOrPark(def, now, zone, d.fireAt());
                if (store.setNextFire(d.scheduleId(), def.revision(), d.fireAt(), next)) {
                    misfireIssue(d, late, "skipped, next at " + next);
                    return Progress.ADVANCED;
                }
                return Progress.NONE;
            }
            case Misfire.CatchUp c -> {
                return catchUp(d, now, zone, late, c.maxFires());
            }
            case Misfire.FireOnceNow f -> {
                misfireIssue(d, late, "firing once now");
                return commitOccurrence(d, nextOrPark(def, now, zone, d.fireAt()));
            }
        }
    }

    /**
     * Walks the missed fires keeping the last {@code maxFires}. When more were missed the next fire moves to the
     * first of those at once, in one step whatever the downtime; the following rounds create their occurrences.
     * Past the scan limit the missed fires are not counted: the definition fires once now, like
     * {@code FireOnceNow}. The scan is kept for the rounds that create the fires it found, so that a catch-up
     * walks the fires once; fires that fall due meanwhile are counted by the scan after it.
     */
    private Progress catchUp(DueSchedule d, Instant now, ZoneId zone, Duration late, int maxFires) {
        var def = d.definition();
        var kept = catchUpWindows.remove(d.scheduleId());
        if (kept != null && kept.covers(def.revision(), d.fireAt())) {
            if (!d.fireAt().equals(kept.last())) {
                catchUpWindows.put(
                        d.scheduleId(), new CatchUpWindow(kept.revision(), kept.first(), kept.last(), cycle));
            }
            return commitOccurrence(d, nextOrPark(def, d.fireAt(), zone, d.fireAt()));
        }
        var window = new ArrayDeque<Instant>(Math.min(maxFires, MAX_CATCH_UP_SCAN) + 1);
        var fire = d.fireAt();
        int scanned = 0;
        while (fire != null && !fire.isAfter(now) && scanned < MAX_CATCH_UP_SCAN) {
            window.addLast(fire);
            if (window.size() > maxFires) window.removeFirst();
            scanned++;
            fire = Cron.next(def.cron(), fire, zone).orElse(null);
        }
        if (fire != null && !fire.isAfter(now)) {
            misfireIssue(d, late, "more than " + MAX_CATCH_UP_SCAN + " fires missed, firing once now");
            return commitOccurrence(d, nextOrPark(def, now, zone, d.fireAt()));
        }
        if (scanned > maxFires) {
            var resume = window.getFirst();
            if (store.setNextFire(d.scheduleId(), def.revision(), d.fireAt(), resume)) {
                catchUpWindows.put(d.scheduleId(), new CatchUpWindow(def.revision(), resume, window.getLast(), cycle));
                misfireIssue(d, late, "catching up the last " + maxFires + " fires from " + resume);
                return Progress.ADVANCED;
            }
            return Progress.NONE;
        }
        // each missed fire gets its own occurrence; the next round picks the following one up
        if (!d.fireAt().equals(window.getLast())) {
            catchUpWindows.put(
                    d.scheduleId(), new CatchUpWindow(def.revision(), window.getFirst(), window.getLast(), cycle));
        }
        return commitOccurrence(d, nextOrPark(def, d.fireAt(), zone, d.fireAt()));
    }

    private void misfireIssue(DueSchedule d, Duration late, String action) {
        var detail = "schedule " + d.scheduleId() + " missed its fire at " + d.fireAt() + " by " + late + ": " + action;
        log.warn(detail);
        status.issue(Issue.Kind.SCHEDULE_MISFIRE, detail, null);
    }

    /**
     * A definition that never fires again still gets this occurrence; parking its next fire far ahead keeps it
     * out of dueSchedules without a revision-checked delete.
     */
    private Instant nextOrPark(StoredSchedule def, Instant after, ZoneId zone, Instant fireAt) {
        return Cron.next(def.cron(), after, zone).orElseGet(() -> {
            log.warn("Recurring task {} never fires again after {}", def.scheduleId(), fireAt);
            return fireAt.plus(NEVER);
        });
    }

    private Progress commitOccurrence(DueSchedule d, Instant next) {
        var def = d.definition();
        var builder = TaskRecord.builder()
                .id(ids.nextId())
                .revision(IdAllocator.firstRevision())
                .queueId(def.queueId())
                .typeId(def.typeId())
                .payloadVersion(def.payloadVersion())
                .state(d.fireAt().isAfter(clock.instant()) ? TaskState.SCHEDULED : TaskState.QUEUED)
                .maxAttempts(def.maxAttempts())
                .maxRunTime(def.maxRunTime())
                .createdAt(clock.instant())
                .dueAt(d.fireAt())
                .encodedPayload(def.encodedPayload())
                .account(def.account())
                .scheduleId(d.scheduleId());
        if (def.isFixedDelay()) {
            builder.headers(Map.of(FIXED_DELAY_HEADER, def.fixedDelay().toString()));
        }
        if (def.overlapSkip()) {
            builder.uniqueKey(OVERLAP_KEY_PREFIX + d.scheduleId());
        }
        var occurrence = builder.build();
        var outcome = store.commitSchedule(d, next, occurrence);
        if (outcome == CommitOutcome.SKIPPED) {
            var detail = "schedule " + d.scheduleId() + " skipped its fire at " + d.fireAt()
                    + ": the previous occurrence has not finished";
            log.info(detail);
            status.issue(Issue.Kind.SCHEDULE_OVERLAP, detail, null);
        } else if (outcome == CommitOutcome.STORED) {
            observer.emit(new TaskEvent.ScheduleFired(d.scheduleId(), occurrence.id(), d.fireAt()));
        }
        return outcome.isCommitted() ? Progress.QUEUED : Progress.NONE;
    }

    static class Builder {
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

        Builder group(ThreadGroup group) {
            this.group = group;
            return this;
        }

        Builder threadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        Builder store(ScheduleStore store) {
            this.store = store;
            return this;
        }

        /** Lets the service tell a skipped overlapping occurrence from a created one. Optional. */
        Builder inspection(InspectionStore inspection) {
            this.inspection = inspection;
            return this;
        }

        Builder ids(IdAllocator ids) {
            this.ids = ids;
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

        Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        RecurringTaskService create() {
            if (group == null || threadName == null)
                throw new TaskConfigurationException("group and threadName are required");
            if (store == null || ids == null) throw new TaskConfigurationException("store and ids are required");
            return new RecurringTaskService(this);
        }
    }
}

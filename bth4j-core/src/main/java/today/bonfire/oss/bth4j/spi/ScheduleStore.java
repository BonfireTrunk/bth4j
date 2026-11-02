package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Recurring definitions. Each occurrence is a task of its own, created exactly once across all runners. */
public interface ScheduleStore extends TaskStore {

    /**
     * Creates or replaces a definition. The store assigns the revision. When a definition with that id exists,
     * its timing (cron expression, zone and fixed delay) is unchanged and either the occurrence it has in
     * flight (see {@link #commitRecurring}) is not finished or its stored next fire time is not later than
     * {@code keepUntil}, the stored next fire time is kept; otherwise it becomes {@code nextFire}. A stored
     * time later than {@code keepUntil} with no occurrence in flight has skipped fires: a fixed-delay
     * definition whose re-arm was lost, or a time pushed far out by a stale write. Changed timing forgets the
     * occurrence in flight.
     */
    void putRecurring(RecurringDefinition definition, Instant nextFire, Instant keepUntil);

    default void putRecurring(RecurringDefinition definition, Instant nextFire) {
        putRecurring(definition, nextFire, nextFire);
    }

    /** @return true when a definition with that id existed */
    boolean deleteRecurring(String scheduleId);

    /** @return every definition's id with its next fire time */
    Map<String, Instant> recurringSchedules();

    /** @return definitions whose next fire time is at or before {@code upTo}, up to the limit */
    List<DueSchedule> dueRecurring(Instant upTo, int limit);

    /**
     * Stores the occurrence (SCHEDULED for {@link DueSchedule#fireAt()}) and advances the definition to
     * {@code nextFire}, unless the definition's revision or next fire time changed meanwhile. When the
     * occurrence names a unique key that another task holds, the definition is still advanced and the
     * method still reports true, but the occurrence is not stored; this is how an overlapping occurrence is
     * skipped. For a fixed-delay definition the store records the stored occurrence as the one in flight, which
     * is what a {@link ScheduleEffect} of that occurrence is checked against; a skipped occurrence leaves the
     * recorded one alone, so the running occurrence still re-arms the definition.
     *
     * @return true when committed
     */
    boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord occurrence);

    /** @return every definition with its next fire time, in id order */
    List<RecurringSchedule> recurringDefinitions();

    /**
     * Pauses or resumes a definition. A paused definition keeps its next fire time but is left out of
     * {@link #dueRecurring}.
     *
     * @return false when no definition with that id exists
     */
    boolean pauseRecurring(String scheduleId, boolean paused);

    /**
     * Operator write: replaces the next fire time (trigger now) and assigns a new revision, so that a runner
     * write based on the old revision, such as the re-arm of an occurrence that is still running, does nothing.
     *
     * @return false when no definition with that id exists
     */
    boolean setNextFire(String scheduleId, Instant nextFire);

    /**
     * Runner write for a misfire: replaces the next fire time and keeps the revision, but only while the
     * definition still has {@code revision}, still has {@code expectedFire} as its next fire time, and is not
     * paused.
     *
     * @return true when the next fire time was replaced
     */
    boolean setNextFire(String scheduleId, long revision, Instant expectedFire, Instant nextFire);
}

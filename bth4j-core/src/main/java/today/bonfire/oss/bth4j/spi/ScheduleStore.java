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
     * occurrence in flight. A put that would write nothing (every field of the definition is stored already, the
     * next fire time is kept, and no finished occurrence is recorded as in flight) keeps the revision. The paused
     * flag of an existing definition is not compared, because a put never changes it.
     *
     * @return whether the definition was created, changed or found unchanged, with the next fire time it has
     */
    PutRecurringResult putRecurring(RecurringDefinition definition, Instant nextFire, Instant keepUntil);

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
     * skipped. When a task with the occurrence's id exists, nothing is written and the method reports false: ids
     * are handed out once, so that task is not an occurrence of this fire. For a fixed-delay definition the store records the stored occurrence as the one in flight, which
     * is what a {@link ScheduleEffect} of that occurrence is checked against; a skipped occurrence leaves the
     * recorded one alone, so the running occurrence still re-arms the definition.
     *
     * @return true when committed
     */
    boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord occurrence);

    /** @return every definition with its next fire time, in id order */
    List<RecurringSchedule> recurringDefinitions();

    /**
     * Pauses a definition. A paused definition keeps its next fire time but is left out of {@link #dueRecurring}.
     *
     * @return false when no definition with that id exists
     */
    boolean pauseRecurring(String scheduleId);

    /**
     * Resumes a paused definition in one step. A {@code nextFire} that is not null replaces its next fire time and
     * assigns a new revision, as {@link #setNextFire(String, Instant)} does; a fixed-delay definition needs one,
     * because it parks its next fire while paused. While the recorded occurrence is not finished, {@code nextFire}
     * is ignored and the stored time and revision stay: the end of that occurrence sets the next fire, and a time
     * given here would start a second occurrence beside it. The recorded occurrence is kept. A definition that is
     * not paused is left unchanged.
     *
     * @return false when no definition with that id exists
     */
    boolean resumeRecurring(String scheduleId, Instant nextFire);

    /**
     * Operator write: replaces the next fire time (trigger now) and assigns a new revision, so that a runner
     * write based on the old revision does nothing. It also forgets the recorded occurrence, so that the effect
     * of an occurrence that is still running (a re-arm or a pause) does nothing either; when the triggered
     * occurrence is skipped because that occurrence still holds the key, the skipped commit records it again.
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

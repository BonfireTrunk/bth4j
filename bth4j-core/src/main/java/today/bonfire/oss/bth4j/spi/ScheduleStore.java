package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import today.bonfire.oss.bth4j.PutScheduleResult;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * Schedules: the definitions of recurring tasks. Each occurrence is a task of its own, created exactly once
 * across all runners.
 */
public interface ScheduleStore extends TaskStore {

    /**
     * Creates or replaces a definition. The store assigns the revision. When a definition with that id exists,
     * its timing (cron expression, zone and fixed delay) is unchanged and either the occurrence it has in
     * flight (see {@link #commitSchedule}) is not finished or its stored next fire time is not later than
     * {@code keepUntil}, the stored next fire time is kept; otherwise it becomes {@code nextFireAt}. A stored
     * time later than {@code keepUntil} with no occurrence in flight has skipped fires: a fixed-delay
     * definition whose re-arm was lost, or a time pushed far out by a stale write. Changed timing forgets the
     * occurrence in flight. A put that would write nothing (every field of the definition is stored already, the
     * next fire time is kept, and no finished occurrence is recorded as in flight) keeps the revision. The paused
     * flag of an existing definition is not compared, because a put never changes it.
     *
     * @return whether the definition was created, changed or found unchanged, with the next fire time it has
     */
    PutScheduleResult putSchedule(StoredSchedule definition, Instant nextFireAt, Instant keepUntil);

    /** @return true when a definition with that id existed */
    boolean deleteSchedule(String scheduleId);

    /** @return every definition's id with its next fire time */
    Map<String, Instant> nextFireTimes();

    /**
     * @param limit at least 1
     * @return definitions whose next fire time is at or before {@code upTo}, up to the limit
     * @throws TaskValidationException if the limit is below 1
     */
    List<DueSchedule> dueSchedules(Instant upTo, int limit);

    /**
     * Stores the occurrence (SCHEDULED for {@link DueSchedule#fireAt()}) and advances the definition to
     * {@code nextFireAt}, unless the definition's revision or next fire time changed meanwhile. An occurrence
     * that names no schedule is stored with the id of this one. When the
     * occurrence names a unique key that another task holds, the definition is still advanced and the
     * method reports {@link CommitOutcome#SKIPPED}, but the occurrence is not stored; this is how an overlapping
     * occurrence is skipped. When a task with the occurrence's id exists, nothing is written and the method
     * reports {@link CommitOutcome#NOT_COMMITTED}: ids
     * are handed out once, so that task is not an occurrence of this fire. For a fixed-delay definition the store records the stored occurrence as the one in flight, which
     * is what a {@link ScheduleEffect} of that occurrence is checked against; a skipped occurrence leaves the
     * recorded one alone, so the running occurrence still re-arms the definition. When none is recorded and the
     * holder of the key is an open task of this schedule, the holder is recorded.
     *
     * @return whether the definition advanced, and whether the occurrence was stored with it
     */
    CommitOutcome commitSchedule(DueSchedule due, Instant nextFireAt, TaskRecord occurrence);

    /**
     * The order of {@link #schedules()}: by the code points of the ids, which is also the byte order
     * of their UTF-8 form, so a database that compares bytes and a store that compares in Java list alike.
     */
    Comparator<String> ID_ORDER = (a, b) -> {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int x = a.codePointAt(i);
            int y = b.codePointAt(j);
            if (x != y) return Integer.compare(x, y);
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    };

    /** @return every definition with its next fire time, in {@link #ID_ORDER} */
    List<ScheduleEntry> schedules();

    /**
     * Pauses a definition. A paused definition keeps its next fire time but is left out of {@link #dueSchedules}.
     *
     * @return false when no definition with that id exists
     */
    boolean pauseSchedule(String scheduleId);

    /**
     * Resumes a paused definition in one step. A {@code nextFireAt} that is not null replaces its next fire time and
     * assigns a new revision, as {@link #triggerSchedule} does; a fixed-delay definition needs one,
     * because it parks its next fire while paused. While the recorded occurrence is not finished, {@code nextFireAt}
     * is ignored and the stored time and revision stay: the end of that occurrence sets the next fire, and a time
     * given here would start a second occurrence beside it. The recorded occurrence is kept. A definition that is
     * not paused is left unchanged.
     *
     * @return false when no definition with that id exists
     */
    boolean resumeSchedule(String scheduleId, Instant nextFireAt);

    /**
     * Operator write: makes the definition fire at {@code fireAt} (trigger now). It replaces the next fire time
     * and assigns a new revision, so that a runner write based on the old revision does nothing. It also forgets
     * the recorded occurrence, so that the effect of an occurrence that is still running (a re-arm or a pause)
     * does nothing either; when the triggered occurrence is skipped because that occurrence still holds the key,
     * the skipped commit records it again.
     * <p>
     * The fire time is marked as triggered: {@link #dueSchedules} reports it with
     * {@link DueSchedule#triggered()}, so that the runner does not treat it as a missed fire. The mark lasts as
     * long as this fire time: every other write that replaces the next fire time removes it
     * ({@link #commitSchedule}, {@link #setNextFire}, a {@link ScheduleEffect} that re-arms, a
     * {@link #putSchedule} or {@link #resumeSchedule} that stores another time), and a write that keeps the
     * time keeps the mark.
     *
     * @return false when no definition with that id exists
     */
    boolean triggerSchedule(String scheduleId, Instant fireAt);

    /**
     * Runner write for a misfire: replaces the next fire time and keeps the revision, but only while the
     * definition still has {@code revision}, still has {@code expectedFire} as its next fire time, and is not
     * paused.
     *
     * @return true when the next fire time was replaced
     */
    boolean setNextFire(String scheduleId, long revision, Instant expectedFire, Instant nextFireAt);
}

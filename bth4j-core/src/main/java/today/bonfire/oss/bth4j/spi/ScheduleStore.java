package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Recurring definitions. Each occurrence is a task of its own, created exactly once across all runners. */
public interface ScheduleStore extends TaskStore {

    /**
     * Creates or replaces a definition. The store assigns the revision. When a definition with that id exists
     * and its cron expression is unchanged, the stored next fire time is kept; otherwise it becomes
     * {@code nextFire}.
     */
    void putRecurring(RecurringDefinition definition, Instant nextFire);

    /** @return true when a definition with that id existed */
    boolean deleteRecurring(String scheduleId);

    /** @return every definition's id with its next fire time */
    Map<String, Instant> recurringSchedules();

    /** @return definitions whose next fire time is at or before {@code upTo}, up to the limit */
    List<DueSchedule> dueRecurring(Instant upTo, int limit);

    /**
     * Stores the occurrence (SCHEDULED for {@link DueSchedule#fireAt()}) and advances the definition to
     * {@code nextFire}, unless the definition's revision or next fire time changed meanwhile.
     *
     * @return true when committed
     */
    boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord occurrence);
}

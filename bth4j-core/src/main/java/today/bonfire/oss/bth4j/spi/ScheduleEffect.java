package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.util.Objects;

/**
 * A change to a recurring definition that the end of its fixed-delay occurrence applies in the same atomic step
 * as the task transition, so that a process dying right after the transition cannot leave the definition
 * parked. The store records the id of the in-flight occurrence of a fixed-delay definition when
 * {@link ScheduleStore#commitRecurring} stores it. The effect applies only while that id equals
 * {@code occurrenceId}, and then clears it; otherwise it does nothing, because the definition was replaced, its
 * timing changed or a newer occurrence took over. A re-arm of a paused definition stores the next fire time
 * but leaves the definition paused; resuming makes it due.
 *
 * @param occurrenceId the id of the task whose transition carries the effect
 * @param nextFire the new next fire time, or null to pause the definition
 */
public record ScheduleEffect(String scheduleId, long occurrenceId, Instant nextFire) {

    public static ScheduleEffect rearm(String scheduleId, long occurrenceId, Instant nextFire) {
        return new ScheduleEffect(scheduleId, occurrenceId, Objects.requireNonNull(nextFire));
    }

    public static ScheduleEffect pause(String scheduleId, long occurrenceId) {
        return new ScheduleEffect(scheduleId, occurrenceId, null);
    }

    public boolean pauses() {
        return nextFire == null;
    }
}

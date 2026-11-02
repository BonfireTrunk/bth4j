package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.util.Objects;

/**
 * A change to a recurring definition that a task transition applies in the same atomic step, so that a process
 * dying right after the transition cannot leave the definition parked. It applies only when the definition
 * still has {@code revision}; otherwise it does nothing, because an operator replaced the definition
 * meanwhile. A re-arm also does nothing while the definition is paused.
 *
 * @param nextFire the new next fire time, or null to pause the definition
 */
public record ScheduleEffect(String scheduleId, long revision, Instant nextFire) {

    public static ScheduleEffect rearm(String scheduleId, long revision, Instant nextFire) {
        return new ScheduleEffect(scheduleId, revision, Objects.requireNonNull(nextFire));
    }

    public static ScheduleEffect pause(String scheduleId, long revision) {
        return new ScheduleEffect(scheduleId, revision, null);
    }

    public boolean pauses() {
        return nextFire == null;
    }
}

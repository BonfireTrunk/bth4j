package today.bonfire.oss.bth4j.spi;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A change to a recurring definition that the end of its fixed-delay occurrence applies in the same atomic step
 * as the task transition, so that a process dying right after the transition cannot leave the definition
 * parked. The store records the id of the in-flight occurrence of a fixed-delay definition when
 * {@link ScheduleStore#commitRecurring} stores it. The effect applies only while that id equals
 * {@code occurrenceId}, and then clears it; otherwise it does nothing, because the definition was replaced, its
 * timing changed or a newer occurrence took over. A re-arm of a paused definition stores the next fire time
 * but leaves the definition paused; resuming makes it due.
 * <p>
 * The store reports whether it changed the definition through {@link #applied()}, which is valid once the
 * store call that took the effect has returned. An instance is for one call.
 */
public final class ScheduleEffect {
    private final String scheduleId;
    private final long occurrenceId;
    private final Instant nextFire;
    private final AtomicBoolean applied = new AtomicBoolean();

    private ScheduleEffect(String scheduleId, long occurrenceId, Instant nextFire) {
        this.scheduleId = Objects.requireNonNull(scheduleId);
        this.occurrenceId = occurrenceId;
        this.nextFire = nextFire;
    }

    /** Sets the next fire time to {@code nextFire} when the occurrence {@code occurrenceId} ends. */
    public static ScheduleEffect rearm(String scheduleId, long occurrenceId, Instant nextFire) {
        return new ScheduleEffect(scheduleId, occurrenceId, Objects.requireNonNull(nextFire));
    }

    /** Pauses the definition when the occurrence {@code occurrenceId} ends. */
    public static ScheduleEffect pause(String scheduleId, long occurrenceId) {
        return new ScheduleEffect(scheduleId, occurrenceId, null);
    }

    public String scheduleId() {
        return scheduleId;
    }

    public long occurrenceId() {
        return occurrenceId;
    }

    /** @return the new next fire time, or null when the effect pauses the definition */
    public Instant nextFire() {
        return nextFire;
    }

    public boolean pauses() {
        return nextFire == null;
    }

    /** @return true when the store changed the definition */
    public boolean applied() {
        return applied.get();
    }

    /** For stores: records that the definition was changed. */
    public void markApplied() {
        applied.set(true);
    }

    @Override
    public String toString() {
        return "ScheduleEffect[" + scheduleId + ", occurrence " + occurrenceId + ", "
                + (pauses() ? "pause" : "next " + nextFire) + "]";
    }
}

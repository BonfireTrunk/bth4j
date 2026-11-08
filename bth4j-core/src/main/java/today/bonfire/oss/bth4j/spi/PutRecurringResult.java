package today.bonfire.oss.bth4j.spi;

import java.time.Instant;

/**
 * Result of {@link ScheduleStore#putRecurring}.
 *
 * @param outcome  whether the definition was created, changed or found unchanged
 * @param nextFire the next fire time stored after the put: the one passed in, or the stored one it kept
 */
public record PutRecurringResult(PutRecurringOutcome outcome, Instant nextFire) {}

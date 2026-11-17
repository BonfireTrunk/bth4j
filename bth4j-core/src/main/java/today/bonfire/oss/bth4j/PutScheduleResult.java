package today.bonfire.oss.bth4j;

import java.time.Instant;

/**
 * Result of {@link TaskClient#putSchedule}.
 *
 * @param outcome    whether the definition was created, changed or found unchanged
 * @param nextFireAt the next fire time stored after the put: the one worked out for this put, or the stored one
 *                   it kept
 */
public record PutScheduleResult(PutScheduleOutcome outcome, Instant nextFireAt) {}

package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;

/**
 * A recurring definition as applications and the admin API see it.
 *
 * @param revision the revision the store assigned; an operator write sends it back and is refused when the
 *     definition was replaced or re-timed meanwhile (see {@link ScheduleOutcome})
 * @param zone the time zone id as stored, or null for a fixed-delay definition; it is text so that a definition
 *     whose zone this JVM does not know is still listed
 */
public record ScheduleView(
        String id,
        long revision,
        String typeName,
        String queue,
        String account,
        String cron,
        String zone,
        Misfire misfire,
        Duration fixedDelay,
        Overlap overlap,
        boolean paused,
        Instant nextFireAt,
        int maxAttempts,
        Duration maxRunTime) {}

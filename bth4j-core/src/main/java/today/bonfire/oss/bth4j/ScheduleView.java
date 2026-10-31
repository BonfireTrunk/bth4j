package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;

/** A recurring definition as applications and the admin API see it. */
public record ScheduleView(
        String id,
        String event,
        String queue,
        String account,
        String cron,
        String zone,
        String misfire,
        Duration fixedDelay,
        boolean overlapSkip,
        boolean paused,
        Instant nextFire,
        int maxAttempts) {}

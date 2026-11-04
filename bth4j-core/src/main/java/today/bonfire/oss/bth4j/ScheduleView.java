package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/** A recurring definition as applications and the admin API see it. */
public record ScheduleView(
        String id,
        String type,
        String queue,
        String account,
        String cron,
        ZoneId zone,
        Misfire misfire,
        Duration fixedDelay,
        Overlap overlap,
        boolean paused,
        Instant nextFire,
        int maxAttempts,
        Duration maxRunTime) {}

package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.ZoneId;
import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * A recurring definition: a task type and payload created on a cron expression or a fixed delay. Identified by
 * {@code id}; {@link TaskClient#putSchedule} creates or replaces it.
 * <p>
 * Cron definitions fire by wall-clock time in {@code zone} (UTC by default). When a runner was down past a
 * fire time, {@link #misfire} decides what happens (one occurrence now by default). Fixed-delay definitions
 * fire {@code fixedDelay} after the previous occurrence ended; a failed occurrence pauses the definition.
 * {@link Overlap#SKIP} prevents a new occurrence while the previous one has not finished.
 *
 * @param cron       five-field unix expression; null for a fixed-delay definition
 * @param fixedDelay delay between the end of an occurrence and the next; null for a cron definition
 */
public record ScheduleDefinition<T>(
        String id,
        TaskType<T> type,
        T payload,
        String cron,
        Duration fixedDelay,
        ZoneId zone,
        Misfire misfire,
        Overlap overlap,
        String queue,
        String account,
        Integer maxAttempts) {

    public static final Duration MIN_FIXED_DELAY = Duration.ofSeconds(1);

    public ScheduleDefinition {
        if (StringUtils.isBlank(id) || id.length() > 128) {
            throw new TaskValidationException("schedule id must be 1 to 128 characters");
        }
        if (type == null) throw new TaskValidationException("type is required");
        if (StringUtils.isBlank(cron) == (fixedDelay == null)) {
            throw new TaskValidationException("exactly one of cron and fixedDelay is required");
        }
        if (fixedDelay != null && fixedDelay.compareTo(MIN_FIXED_DELAY) < 0) {
            throw new TaskValidationException("fixedDelay must be at least " + MIN_FIXED_DELAY);
        }
        if (type.hasPayload() && payload == null) {
            throw new TaskValidationException("Task type " + type + " needs a payload");
        }
        if (maxAttempts != null && (maxAttempts < 1 || maxAttempts > Short.MAX_VALUE)) {
            throw new TaskValidationException("maxAttempts must be between 1 and " + Short.MAX_VALUE);
        }
        if (misfire == null) misfire = Misfire.FIRE_ONCE_NOW;
        if (overlap == null) overlap = Overlap.ALLOW;
    }

    public static <T> ScheduleDefinition<T> of(String id, TaskType<T> type, T payload, String cron) {
        return new ScheduleDefinition<>(id, type, payload, cron, null, null, null, null, null, null, null);
    }

    public static ScheduleDefinition<Void> of(String id, TaskType<Void> type, String cron) {
        return of(id, type, null, cron);
    }

    /** A definition that fires {@code delay} after each occurrence ends, starting {@code delay} from now. */
    public static <T> ScheduleDefinition<T> every(String id, TaskType<T> type, T payload, Duration delay) {
        return new ScheduleDefinition<>(id, type, payload, null, delay, null, null, null, null, null, null);
    }

    public static ScheduleDefinition<Void> every(String id, TaskType<Void> type, Duration delay) {
        return every(id, type, null, delay);
    }

    public boolean isFixedDelay() {
        return fixedDelay != null;
    }

    public ScheduleDefinition<T> queue(String queue) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts);
    }

    public ScheduleDefinition<T> account(String account) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts);
    }

    public ScheduleDefinition<T> maxAttempts(int maxAttempts) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts);
    }

    /** The zone the cron expression is evaluated in. Ignored by fixed-delay definitions. */
    public ScheduleDefinition<T> zone(ZoneId zone) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts);
    }

    public ScheduleDefinition<T> misfire(Misfire misfire) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts);
    }

    public ScheduleDefinition<T> overlap(Overlap overlap) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts);
    }
}

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
 * {@link Overlap#SKIP} prevents a new occurrence while the previous one has not finished. A cron definition
 * has no default for it, the caller chooses; a fixed-delay definition defaults to {@link Overlap#ALLOW}, which
 * only matters for a fire an operator triggers while an occurrence runs.
 *
 * @param cron       five-field unix expression; null for a fixed-delay definition
 * @param fixedDelay delay between the end of an occurrence and the next, at least {@link #MIN_FIXED_DELAY} and at
 *                   most {@link TaskRequest#MAX_TIME_FROM_NOW}; null for a cron definition
 * @param maxRunTime     maximum run time of each occurrence's attempts; null for the runner's
 *                   {@code defaultMaxRunTime}
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
        Integer maxAttempts,
        Duration maxRunTime) {

    public static final Duration MIN_FIXED_DELAY = Duration.ofSeconds(1);

    public ScheduleDefinition {
        if (StringUtils.isBlank(id) || id.length() > 128) {
            throw new TaskValidationException("schedule id must be 1 to 128 characters");
        }
        StoredText.require("schedule id", id);
        TaskRequest.requireAccount(account);
        if (type == null) throw new TaskValidationException("type is required");
        // a blank cron is no cron: stored as text it would differ from the same definition given with null
        if (StringUtils.isBlank(cron)) cron = null;
        if ((cron == null) == (fixedDelay == null)) {
            throw new TaskValidationException("exactly one of cron and fixedDelay is required");
        }
        if (fixedDelay != null && fixedDelay.compareTo(MIN_FIXED_DELAY) < 0) {
            throw new TaskValidationException("fixedDelay must be at least " + MIN_FIXED_DELAY);
        }
        StoredTime.require("fixedDelay", fixedDelay);
        if (type.hasPayload() && payload == null) {
            throw new TaskValidationException("Task type " + type + " needs a payload");
        }
        if (maxAttempts != null && (maxAttempts < 1 || maxAttempts > TaskRequest.MAX_ATTEMPTS)) {
            throw new TaskValidationException("maxAttempts must be between 1 and " + TaskRequest.MAX_ATTEMPTS);
        }
        TaskRequest.requireMaxRunTime(maxRunTime);
        if (misfire == null) misfire = Misfire.FIRE_ONCE_NOW;
        if (overlap == null) {
            if (fixedDelay == null) {
                throw new TaskValidationException(
                        "a cron schedule needs an overlap rule: Overlap.ALLOW or Overlap.SKIP");
            }
            overlap = Overlap.ALLOW;
        }
    }

    /**
     * A definition that fires on a cron expression.
     *
     * @param overlap what happens to a fire while the previous occurrence has not finished: {@link Overlap#ALLOW}
     *     runs both, {@link Overlap#SKIP} drops the fire
     */
    public static <T> ScheduleDefinition<T> of(String id, TaskType<T> type, T payload, String cron, Overlap overlap) {
        return new ScheduleDefinition<>(id, type, payload, cron, null, null, null, overlap, null, null, null, null);
    }

    public static ScheduleDefinition<Void> of(String id, TaskType<Void> type, String cron, Overlap overlap) {
        return of(id, type, null, cron, overlap);
    }

    /** A definition that fires {@code delay} after each occurrence ends, starting {@code delay} from now. */
    public static <T> ScheduleDefinition<T> every(String id, TaskType<T> type, T payload, Duration delay) {
        return new ScheduleDefinition<>(id, type, payload, null, delay, null, null, null, null, null, null, null);
    }

    public static ScheduleDefinition<Void> every(String id, TaskType<Void> type, Duration delay) {
        return every(id, type, null, delay);
    }

    public boolean isFixedDelay() {
        return fixedDelay != null;
    }

    public ScheduleDefinition<T> withQueue(String queue) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts, maxRunTime);
    }

    public ScheduleDefinition<T> withAccount(String account) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts, maxRunTime);
    }

    public ScheduleDefinition<T> withMaxAttempts(int maxAttempts) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts, maxRunTime);
    }

    /** An occurrence's attempt running longer than this is cancelled, like {@link TaskRequest.Builder#maxRunTime}. */
    public ScheduleDefinition<T> withMaxRunTime(Duration maxRunTime) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts, maxRunTime);
    }

    /** The zone the cron expression is evaluated in. Ignored by fixed-delay definitions. */
    public ScheduleDefinition<T> withZone(ZoneId zone) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts, maxRunTime);
    }

    public ScheduleDefinition<T> withMisfire(Misfire misfire) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts, maxRunTime);
    }

    public ScheduleDefinition<T> withOverlap(Overlap overlap) {
        return new ScheduleDefinition<>(
                id, type, payload, cron, fixedDelay, zone, misfire, overlap, queue, account, maxAttempts, maxRunTime);
    }
}

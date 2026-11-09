package today.bonfire.oss.bth4j.common;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * 5-field unix cron expressions. Fire times are computed in a zone: a wall-clock time that a DST change skips
 * fires at the first valid instant after it, and one that repeats fires once.
 */
public final class Cron {

    private static final CronParser PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    private static final int MAX_CACHED = 256;
    /** The most recently used parsed expressions; synchronized, an access-ordered map changes on every read. */
    private static final Map<String, ExecutionTime> EXECUTION_TIMES =
            Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, ExecutionTime> eldest) {
                    return size() > MAX_CACHED;
                }
            });
    /** Transitions examined for a gap before giving up; a leap-day expression needs about eight. */
    private static final int MAX_TRANSITIONS = 64;

    private Cron() {}

    /** @throws TaskValidationException when the expression does not parse */
    public static void validate(String expression) {
        parse(expression);
    }

    /** {@link #next(String, Instant, ZoneId)} in UTC. */
    public static Optional<Instant> next(String expression, Instant after) {
        return next(expression, after, ZoneOffset.UTC);
    }

    /**
     * @return the next fire time strictly after {@code after}, or empty when the expression never fires again
     * @throws TaskValidationException when the expression does not parse
     */
    public static Optional<Instant> next(String expression, Instant after, ZoneId zone) {
        var executionTime =
                EXECUTION_TIMES.computeIfAbsent(normalize(expression), c -> ExecutionTime.forCron(parse(c)));
        var z = zone == null ? ZoneOffset.UTC : zone;
        var next =
                executionTime.nextExecution(ZonedDateTime.ofInstant(after, z)).map(ZonedDateTime::toInstant);
        // cron-utils drops a wall-clock match that falls inside a DST gap; every transition before the next
        // match is examined, because the first one after `after` may be a repeated hour and the gap behind it
        var rules = z.getRules();
        var transition = rules.nextTransition(after);
        for (int i = 0;
                i < MAX_TRANSITIONS
                        && transition != null
                        && (next.isEmpty() || transition.getInstant().isBefore(next.get()));
                i++, transition = rules.nextTransition(transition.getInstant())) {
            if (!transition.isGap()) continue;
            // evaluate the expression in the offset before the gap and, when a match lands in the gap, fire at
            // the moment the gap ends
            var gap = transition;
            var skipped = executionTime
                    .nextExecution(ZonedDateTime.ofInstant(gap.getInstant(), gap.getOffsetBefore())
                            .minusNanos(1))
                    .map(ZonedDateTime::toLocalDateTime)
                    .filter(t -> !t.isBefore(gap.getDateTimeBefore()) && t.isBefore(gap.getDateTimeAfter()));
            if (skipped.isPresent()) return Optional.of(gap.getInstant());
        }
        return next;
    }

    /** The parsed expressions currently cached; for tests. */
    static int cached() {
        return EXECUTION_TIMES.size();
    }

    private static com.cronutils.model.Cron parse(String expression) {
        try {
            return PARSER.parse(expression);
        } catch (RuntimeException e) {
            throw new TaskValidationException(
                    "invalid cron expression (5 field unix format expected): " + expression + ": " + e.getMessage());
        }
    }

    /** Spaces around and between the fields do not make another expression. */
    private static String normalize(String expression) {
        return expression.trim().replaceAll("\\s+", " ");
    }

    /** @throws TaskValidationException when the zone id is unknown */
    public static ZoneId zone(String id) {
        if (id == null) return ZoneOffset.UTC;
        try {
            return ZoneId.of(id);
        } catch (RuntimeException e) {
            throw new TaskValidationException("unknown time zone " + id);
        }
    }
}

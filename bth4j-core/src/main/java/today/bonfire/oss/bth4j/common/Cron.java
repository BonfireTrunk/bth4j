package today.bonfire.oss.bth4j.common;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * 5-field unix cron expressions. Fire times are the wall-clock times of a zone that match the expression. Where
 * the zone's clocks are set forward, the matches in the skipped stretch fire once together, at the moment of the
 * change. Where they are set back, an expression whose minute or hour field starts with {@code *} fires in both
 * passes of the repeated stretch and keeps its rhythm, and every other expression fires in the first pass only.
 */
public final class Cron {

    private static final CronParser PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    private static final int MAX_CACHED = 256;
    /** The most recently used parsed expressions; synchronized, an access-ordered map changes on every read. */
    private static final Map<String, Parsed> PARSED = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Parsed> eldest) {
            return size() > MAX_CACHED;
        }
    });

    /** @param everyMinuteOrHour the minute or the hour field starts with a wildcard */
    private record Parsed(ExecutionTime time, boolean everyMinuteOrHour) {

        static Parsed of(String normalized) {
            var time = ExecutionTime.forCron(parse(normalized));
            var fields = normalized.split(" ");
            return new Parsed(time, fields[0].startsWith("*") || fields[1].startsWith("*"));
        }

        /** The next wall-clock time after {@code local} that matches, whatever the zone. */
        Optional<LocalDateTime> nextMatch(LocalDateTime local) {
            return time.nextExecution(local.atZone(ZoneOffset.UTC)).map(ZonedDateTime::toLocalDateTime);
        }
    }

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
        var parsed = PARSED.computeIfAbsent(normalize(expression), Parsed::of);
        var rules = (zone == null ? ZoneOffset.UTC : zone).getRules();
        // the expression is matched against wall-clock times only and the zone's rules place each match: the
        // stretch of one offset is searched up to the next change of the offset, then the stretch behind it
        var offset = rules.getOffset(after);
        var change = rules.nextTransition(after);
        var match = parsed.nextMatch(LocalDateTime.ofInstant(after, offset));
        while (match.isPresent()) {
            var local = match.get();
            if (change != null && !local.isBefore(change.getDateTimeBefore())) {
                if (change.isGap() && local.isBefore(change.getDateTimeAfter())) {
                    return Optional.of(change.getInstant());
                }
                offset = change.getOffsetAfter();
                if (change.isOverlap() && parsed.everyMinuteOrHour()) {
                    match = parsed.nextMatch(change.getDateTimeAfter().minusNanos(1));
                }
                change = rules.nextTransition(change.getInstant());
                continue;
            }
            var repeated = parsed.everyMinuteOrHour() ? null : secondPass(rules, local, offset);
            if (repeated == null) return Optional.of(local.toInstant(offset));
            match = parsed.nextMatch(repeated.getDateTimeBefore().minusNanos(1));
        }
        return Optional.empty();
    }

    /** The change that set the clocks back to {@code local} at {@code offset}, or null when it is not a repeat. */
    private static ZoneOffsetTransition secondPass(ZoneRules rules, LocalDateTime local, ZoneOffset offset) {
        var change = rules.getTransition(local);
        return change != null && change.isOverlap() && change.getOffsetAfter().equals(offset) ? change : null;
    }

    /** The parsed expressions currently cached; for tests. */
    static int cached() {
        return PARSED.size();
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

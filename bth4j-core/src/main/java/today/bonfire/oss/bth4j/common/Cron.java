package today.bonfire.oss.bth4j.common;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/** 5-field unix cron expressions, evaluated in UTC. */
public final class Cron {

    private static final CronParser PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    private static final Map<String, ExecutionTime> EXECUTION_TIMES = new ConcurrentHashMap<>();

    private Cron() {}

    /** @throws TaskValidationException when the expression does not parse */
    public static void validate(String expression) {
        try {
            PARSER.parse(expression);
        } catch (RuntimeException e) {
            throw new TaskValidationException(
                    "invalid cron expression (5 field unix format expected): " + expression + ": " + e.getMessage());
        }
    }

    /** @return the next fire time strictly after {@code after}, or empty when the expression never fires again */
    public static Optional<Instant> next(String expression, Instant after) {
        var executionTime = EXECUTION_TIMES.computeIfAbsent(expression, c -> ExecutionTime.forCron(PARSER.parse(c)));
        return executionTime
                .nextExecution(ZonedDateTime.ofInstant(after, ZoneOffset.UTC))
                .map(ZonedDateTime::toInstant);
    }
}

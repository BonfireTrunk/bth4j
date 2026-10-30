package today.bonfire.oss.bth4j.service;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.common.Random;
import today.bonfire.oss.bth4j.exceptions.Errors;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.store.TaskRecord;

/**
 * A task as submitted by the application or as claimed from Redis.
 * <p>
 * A submitted task has an id, an event, an account id, optionally a queue, an execution time and, for a
 * recurring definition, a cron expression. A claimed task additionally knows its attempt count, the
 * maximum number of attempts, the schedule it came from and its serialized data.
 */
public final class Task {

    private static final CronParser CRON_PARSER =
            new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    private static final Map<String, ExecutionTime> EXECUTION_TIMES = new ConcurrentHashMap<>();

    private final String uniqueId;
    private final Event event;
    private final String accountId;
    private final String queueName;
    private final String cronExpression;
    private final Instant executeAtTimeStamp;
    private final int attempts;
    private final int maxAttempts;
    private final String scheduleId;
    private final String data;
    private final String error;
    private final String token;

    private Task(Builder b) {
        this.uniqueId = b.uniqueId;
        this.event = b.event;
        this.accountId = String.valueOf(b.accountId);
        this.queueName = b.queueName;
        this.cronExpression = b.cronExpression;
        this.executeAtTimeStamp = b.executeAtTimeStamp;
        this.attempts = 0;
        this.maxAttempts = b.event.retryCount() + 1;
        this.scheduleId = null;
        this.data = null;
        this.error = null;
        this.token = null;
    }

    private Task(TaskRecord r, Function<String, Event> eventParser) {
        this.uniqueId = r.id();
        this.event = r.event() == null ? null : eventParser.apply(r.event());
        if (this.event == null) {
            throw new TaskErrorException(
                    "Event parser returned null for event value '" + r.event() + "' of task " + r.id());
        }
        this.accountId = r.account();
        this.queueName = r.queue();
        this.cronExpression = null;
        this.executeAtTimeStamp = r.due();
        this.attempts = r.attempts();
        this.maxAttempts = r.maxAttempts() > 0 ? r.maxAttempts() : this.event.retryCount() + 1;
        this.scheduleId = r.scheduleId();
        this.data = r.data();
        this.error = r.error();
        this.token = r.token();
    }

    /**
     * Builds a task from its stored record.
     *
     * @throws TaskErrorException if the event value is unknown to the parser
     */
    static Task from(TaskRecord record, Function<String, Event> eventParser) {
        return new Task(record, eventParser);
    }

    public String uniqueId() {
        return uniqueId;
    }

    public Event event() {
        return event;
    }

    public String accountId() {
        return accountId;
    }

    /** @return the logical queue name, or null when the task was built without one and not yet enqueued */
    public String queueName() {
        return queueName;
    }

    /** @return the cron expression of a recurring definition, null for any other task */
    public String cronExpression() {
        return cronExpression;
    }

    /** @return when the task is due to run, null for a task that runs as soon as possible */
    public Instant executeAtTimeStamp() {
        return executeAtTimeStamp;
    }

    public boolean isExecutionTimeSet() {
        return executeAtTimeStamp != null;
    }

    /** @return number of times the task was claimed for execution, including the current one */
    public int attempts() {
        return attempts;
    }

    /** @return the number of attempts after which the task is moved to the dead set, {@code Event.retryCount() + 1} */
    public int maxAttempts() {
        return maxAttempts;
    }

    /** @return the id of the recurring definition that created this task, or null */
    public String scheduleId() {
        return scheduleId;
    }

    /** @return the serialized data of the task, or null when it has none */
    public String data() {
        return data;
    }

    /** @return the message of the last failure, or null */
    public String lastError() {
        return error;
    }

    String token() {
        return token;
    }

    /** @return the next execution time of this recurring definition after the given time */
    public Instant getNextExecutionTime(Instant after) {
        return nextExecution(cronExpression, after)
                .orElseThrow(() -> new TaskErrorException("Cron expression never fires: " + cronExpression));
    }

    static Optional<Instant> nextExecution(String cronExpression, Instant after) {
        var executionTime =
                EXECUTION_TIMES.computeIfAbsent(cronExpression, c -> ExecutionTime.forCron(CRON_PARSER.parse(c)));
        return executionTime
                .nextExecution(ZonedDateTime.ofInstant(after, ZoneOffset.UTC))
                .map(ZonedDateTime::toInstant);
    }

    static void validateCronExpression(String cronExpression) {
        try {
            CRON_PARSER.parse(cronExpression);
        } catch (RuntimeException e) {
            throw new TaskErrorException(
                    Errors.Tasks.CANNOT_CREATE_TASK + "invalid cron expression (5 field unix format expected): "
                            + cronExpression,
                    e);
        }
    }

    @Override
    public String toString() {
        return "Task[" + uniqueId + ", event=" + event.value() + ", account=" + accountId + ", queue=" + queueName
                + (executeAtTimeStamp == null ? "" : ", due=" + executeAtTimeStamp)
                + (cronExpression == null ? "" : ", cron=" + cronExpression)
                + ", attempts=" + attempts + "/" + maxAttempts + ']';
    }

    public static class Builder {

        private String uniqueId;
        private Event event;
        private Object accountId;
        private Instant executeAtTimeStamp;
        private String queueName;
        private String cronExpression;

        private Builder() {}

        public static Builder newTask() {
            return newTask(null);
        }

        /**
         * @param uniqueId id of the task. A random id is generated if null or blank.
         */
        public static Builder newTask(String uniqueId) {
            var b = new Builder();
            b.uniqueId = StringUtils.isBlank(uniqueId) ? Random.tuid() : uniqueId;
            return b;
        }

        public Builder event(Event taskEvent) {
            this.event = taskEvent;
            return this;
        }

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder accountId(Object accountId) {
            this.accountId = accountId;
            return this;
        }

        public Builder executeAtTimeStamp(Instant executeAtTimeStamp) {
            this.executeAtTimeStamp = executeAtTimeStamp;
            return this;
        }

        /**
         * @param seconds delay in seconds
         */
        public Builder executeAfter(long seconds) {
            return executeAfter(Duration.ofSeconds(seconds));
        }

        public Builder executeAfter(Duration delay) {
            this.executeAtTimeStamp = Instant.now().plus(delay);
            return this;
        }

        /**
         * @param cronExpression 5 field unix cron expression. Required for a recurring definition.
         */
        public Builder cronExpression(String cronExpression) {
            this.cronExpression = cronExpression;
            return this;
        }

        /**
         * @throws TaskErrorException if the account id or event is missing, or if the event is recurring and
         *                            the cron expression is missing or invalid. A recurring event without a cron
         *                            expression is accepted when an execution time is set (a single run).
         */
        public Task build() {
            if (ObjectUtils.isEmpty(accountId)) {
                throw new TaskErrorException(Errors.Tasks.CANNOT_CREATE_TASK + "accountId cannot be empty");
            }
            if (event == null) {
                throw new TaskErrorException(Errors.Tasks.CANNOT_CREATE_TASK + "event cannot be null");
            }
            if (event.isRecurring()) {
                if (StringUtils.isBlank(cronExpression)) {
                    if (executeAtTimeStamp == null) {
                        throw new TaskErrorException(
                                Errors.Tasks.CANNOT_CREATE_TASK + "a recurring task requires a cron expression");
                    }
                } else {
                    validateCronExpression(cronExpression);
                }
            } else if (cronExpression != null) {
                throw new TaskErrorException(
                        Errors.Tasks.CANNOT_CREATE_TASK + "only a recurring event can have a cron expression");
            }
            return new Task(this);
        }
    }
}

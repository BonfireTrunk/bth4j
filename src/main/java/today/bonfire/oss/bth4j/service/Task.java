package today.bonfire.oss.bth4j.service;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Function;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.common.Random;
import today.bonfire.oss.bth4j.exceptions.Errors;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;

/**
 * A Task class which holds the task details and the data associated with it.
 * <p>
 * The task is stored in Redis as a string with the parts joined by {@code |}, so the unique id,
 * event value, account id, queue name and cron expression must not contain a {@code |}.
 * This is enforced when the task is built; a {@link TaskErrorException} is thrown otherwise.
 */
@Slf4j
@Getter
@Accessors(fluent = true)
public class Task {

    private static final String TASK_STRING_SEPARATOR = "|";

    private static final CronParser cronParser =
            new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));

    private String uniqueId;
    private Event event;
    private Object accountId;
    private String queueName;
    private String cronExpression;
    private String taskString;

    @Setter
    private Instant executeAtTimeStamp;

    private Object data;

    /**
     * list key is based on
     *
     * <pre>
     *   JID|EVENT_ID|ACCOUNT_ID|QUEUE_NAME
     *   1   2        3          4
     *
     * the string order is important, since the key is based on the order of the string.
     * to supply below items for proper taskString,
     * JID - unique id, will also act as a task ID
     * EVENT_ID - the event, is an integer
     * ACCOUNT_ID - can specify an userID, if account id doesn't make sense for your app or just empty if Not applicable
     * QUEUE_NAME - the name of the queue to which the task will be queued initially.
     * if not set, the default queue(first queue in the available queue list) will be used.
     * If the task fails, it may be queued to a retry queue.
     * The Above 4 items  are in same order always.
     *
     * In case of cron job, the task string will be
     * JID|EVENT_ID|ACCOUNT_ID|QUEUE_NAME|CRON_EXPRESSION
     * 1   2        3          4          5
     *
     * </pre>
     */
    public Task(String ts, Function<String, Event> eventParser) {
        if (StringUtils.isBlank(ts)) return;
        this.taskString = ts;
        var split = StringUtils.splitPreserveAllTokens(ts, TASK_STRING_SEPARATOR);
        if (split.length < 3 || split.length > 5) throw new TaskErrorException("Invalid task string: " + ts);

        this.uniqueId = split[0];
        event = eventParser.apply(split[1]);
        if (event == null) {
            throw new TaskErrorException(
                    "Event parser returned null for event value '" + split[1] + "' in task string: " + ts);
        }
        accountId = split[2];
        if (split.length >= 4) {
            queueName = split[3];
            if (queueName.isBlank()) queueName = null;
        }
        if (split.length > 4 && event.isRecurring()) {
            cronExpression = split[4];
        }
    }

    protected Task(Builder builder) {
        this.uniqueId = builder.getUniqueId();
        this.event = builder.getEvent();
        this.accountId = builder.getAccountId();
        this.executeAtTimeStamp = builder.getExecuteAtTimeStamp();
        this.queueName = builder.getQueueName();
        this.cronExpression = builder.getCronExpression();
        this.taskString = buildTaskString();
    }

    @Override
    public String toString() {
        return taskString;
    }

    /** useful to check is task is set or just empty */
    public boolean isNULL() {
        return uniqueId == null;
    }

    /**
     * can call this method when you have made any changes to the task params
     * else prefer calling the {@link #taskString}
     * If you are trying to insert into queues for the first time you should call this method.
     */
    public String buildTaskString() {
        rejectSeparator("uniqueId", uniqueId);
        rejectSeparator("event value", event.value());
        rejectSeparator("accountId", String.valueOf(accountId));
        rejectSeparator("queueName", queueName);
        rejectSeparator("cronExpression", cronExpression);
        var sb = new StringBuilder();
        sb.append(uniqueId)
                .append(TASK_STRING_SEPARATOR)
                .append(event.value())
                .append(TASK_STRING_SEPARATOR)
                .append(accountId);

        if (event.isRecurring() && StringUtils.isNotBlank(cronExpression)) {
            validateCronExpression(cronExpression);
            sb.append(TASK_STRING_SEPARATOR).append(queueName == null ? "" : queueName);
            sb.append(TASK_STRING_SEPARATOR).append(cronExpression);
        } else {
            if (queueName != null) {
                sb.append(TASK_STRING_SEPARATOR).append(queueName);
            }
        }
        return sb.toString();
    }

    private static void rejectSeparator(String name, String value) {
        if (value != null && value.contains(TASK_STRING_SEPARATOR)) {
            throw new TaskErrorException(Errors.Tasks.CANNOT_CREATE_TASK + name + " must not contain '"
                    + TASK_STRING_SEPARATOR + "': " + value);
        }
    }

    private static void validateCronExpression(String cronExpression) {
        try {
            cronParser.parse(cronExpression);
        } catch (RuntimeException e) {
            throw new TaskErrorException(
                    Errors.Tasks.CANNOT_CREATE_TASK + "invalid cron expression (5 field unix format expected): "
                            + cronExpression,
                    e);
        }
    }

    public boolean isExecutionTimeSet() {
        return executeAtTimeStamp != null;
    }

    public <T> Task setData(T data) {
        this.data = data;
        return this;
    }

    /**
     * call only if you are sure the task is recurring.
     */
    public Instant getNextExecutionTime(Instant previousExecutionTime) {
        var cron = cronParser.parse(cronExpression);
        return ExecutionTime.forCron(cron)
                .nextExecution(ZonedDateTime.ofInstant(previousExecutionTime, ZoneOffset.UTC))
                .orElse(ZonedDateTime.now().plusYears(100_000L)) // used so that it can be skipped
                .toInstant();
    }

    /**
     * @return data - that was associated with the task
     *     may not always have data associated with it.
     *     Data is populated in {@link TaskRunnerWrapper} class for execution purpose.
     *     refer {@link TaskRunnerWrapper} for more information.
     */
    public <T> T getData(Class<T> clazz) {
        try {
            return clazz.cast(data);
        } catch (ClassCastException e) {
            throw new TaskDataException("Data type mismatch. Expected " + clazz.getName() + " but found "
                    + (data == null ? "null" : data.getClass().getName()));
        }
    }

    @Getter
    @Accessors(fluent = false)
    public static class Builder {

        private String uniqueId = null;
        private Event event = null;
        private Object accountId = null;
        private Instant executeAtTimeStamp = null;
        private String queueName = null;
        private String cronExpression = null;

        public Builder() {}

        public static Builder newTask() {
            return newTask(null);
        }

        /**
         * @param uniqueId id for the task. A random id is generated if null or blank.
         *                 It must not contain {@code |}.
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
         * set to execute task after a fixed delay
         *
         * @param seconds delay in seconds
         */
        public Builder executeAfter(long seconds) {
            this.executeAtTimeStamp = Instant.now().plusSeconds(seconds);
            return this;
        }

        /**
         * @param cronExpression 5 field unix cron expression. Required for the definition of a recurring task.
         */
        public Builder cronExpression(String cronExpression) {
            this.cronExpression = cronExpression;
            return this;
        }

        /**
         * Builds the task after validating it.
         *
         * @throws TaskErrorException if the account id or event is missing, if any part of the task contains
         *                            the separator {@code |}, or if the event is recurring and the cron expression is
         *                            invalid. A recurring event without a cron expression is only accepted when an
         *                            execution time is set, that is for a single run of a recurring task.
         */
        public Task build() {
            if (ObjectUtils.isEmpty(accountId)) {
                log.error("accountId cannot be null");
                throw new TaskErrorException(Errors.Tasks.CANNOT_CREATE_TASK);
            }
            if (ObjectUtils.isEmpty(event)) {
                log.error("eventId cannot be null");
                throw new TaskErrorException(Errors.Tasks.CANNOT_CREATE_TASK);
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
            }
            return new Task(this);
        }
    }
}

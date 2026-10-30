package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.store.DueSchedule;
import today.bonfire.oss.bth4j.store.ExpiredLease;
import today.bonfire.oss.bth4j.store.FailOutcome;
import today.bonfire.oss.bth4j.store.PromoteResult;
import today.bonfire.oss.bth4j.store.RecurringDefinition;
import today.bonfire.oss.bth4j.store.TaskRecord;
import today.bonfire.oss.bth4j.store.TaskStore;

/**
 * All task operations of a runner, on top of its {@link TaskStore}. Applications use the public methods to
 * enqueue tasks, manage recurring definitions and inspect queues; the services use the package-private
 * transitions. This class validates queues, serializes data and parses events; the store makes the
 * transitions atomic (see {@code docs/design-3.0.md}).
 */
public class TaskOps {
    private static final Logger log = LoggerFactory.getLogger(TaskOps.class);
    private static final int MAX_ERROR_LENGTH = 1000;

    private final TaskStore store;
    private final JsonMapper mapper;
    private final Function<String, Event> eventParser;
    private final QueuesHolder queuesHolder;
    private final Duration deadRetention;

    /**
     * Normally created by {@link BackgroundRunner}; applications obtain the instance from the runner
     * through {@code taskOps()}.
     *
     * @param deadRetention how long a dead task is kept
     */
    public TaskOps(
            TaskStore store,
            JsonMapper jsonMapper,
            Function<String, Event> eventParser,
            QueuesHolder queuesHolder,
            Duration deadRetention) {
        this.store = store;
        this.mapper = jsonMapper;
        this.eventParser = eventParser;
        this.queuesHolder = queuesHolder;
        this.deadRetention = deadRetention;
    }

    // ---- enqueue -------------------------------------------------------------------------------------

    /**
     * Stores the task and puts it into its queue, or into the scheduled set when it has an execution time.
     *
     * @return true when stored, false when a task with the same id already exists
     * @throws TaskErrorException if the queue of the task is not an available queue
     */
    public boolean enqueue(Task task) {
        return enqueue(task, null);
    }

    /**
     * Stores the task with its data. The data is serialized with the configured {@link JsonMapper} and
     * handed to the processor registered for the event, deserialized to the processor's data class.
     *
     * @return true when stored, false when a task with the same id already exists
     * @throws TaskDataException  if the data cannot be serialized, or no mapper is configured
     * @throws TaskErrorException if the queue of the task is not an available queue
     */
    public boolean enqueue(Task task, Object data) {
        return enqueueSerialized(task, data == null ? null : toJson(task, data));
    }

    private String toJson(Task task, Object data) {
        if (mapper == null) {
            throw new TaskDataException("No JsonMapper configured, cannot store data for task " + task.uniqueId());
        }
        try {
            return mapper.toJson(data);
        } catch (Exception e) {
            throw new TaskDataException("Unable to serialize data for task: " + task.uniqueId(), e);
        }
    }

    boolean enqueueSerialized(Task task, String json) {
        var queue = queuesHolder.getValidQueue(task.queueName());
        var record = TaskRecord.newTask(
                task.uniqueId(),
                task.event().value(),
                task.accountId(),
                queue,
                json,
                task.maxAttempts(),
                task.isExecutionTimeSet() ? task.executeAtTimeStamp() : null);
        if (store.enqueue(record)) {
            log.debug("Enqueued {} to {}", task, queue);
            return true;
        }
        log.debug("Task {} already exists, not enqueued", task.uniqueId());
        return false;
    }

    // ---- recurring definitions -----------------------------------------------------------------------

    /**
     * Registers (or updates) a recurring definition. The task's id is the schedule id; its event must be
     * recurring and it must have a cron expression. Each occurrence is created in the scheduled set ahead
     * of its time and runs in the task's queue with the given data.
     * Registering the same id again with the same cron expression keeps the next fire time; a changed
     * expression resets it.
     *
     * @return the schedule id
     */
    public String addRecurringTask(Task task, Object data) {
        if (!task.event().isRecurring() || StringUtils.isBlank(task.cronExpression())) {
            throw new TaskErrorException("A recurring definition needs a recurring event and a cron expression");
        }
        var queue = queuesHolder.getValidQueue(task.queueName());
        var next = task.getNextExecutionTime(Instant.now());
        var definition = new RecurringDefinition(
                task.uniqueId(),
                task.event().value(),
                task.accountId(),
                queue,
                task.cronExpression(),
                data == null ? null : toJson(task, data),
                task.maxAttempts(),
                0);
        store.putRecurring(definition, next);
        log.info("Registered recurring task {} next at {}", task, next);
        return task.uniqueId();
    }

    public String addRecurringTask(Task task) {
        return addRecurringTask(task, null);
    }

    /** @return true when a definition with that id existed */
    public boolean deleteRecurringTask(String scheduleId) {
        var deleted = store.deleteRecurring(scheduleId);
        log.info("Deleted recurring task {}: {}", scheduleId, deleted);
        return deleted;
    }

    /** @return the number of definitions removed */
    public long deleteAllRecurringTasks() {
        long removed = 0;
        for (var id : store.recurringSchedules().keySet()) {
            if (deleteRecurringTask(id)) removed++;
        }
        return removed;
    }

    /** @return the recurring definitions with their next fire time */
    public Map<String, Instant> recurringTasks() {
        return store.recurringSchedules();
    }

    // ---- data and inspection -------------------------------------------------------------------------

    /**
     * @return the serialized data of the task, or null if the id is blank or the task has no data or does not exist
     */
    public String getDataForTask(String uniqueId) {
        if (StringUtils.isBlank(uniqueId)) return null;
        return store.getTask(uniqueId).map(TaskRecord::data).orElse(null);
    }

    /**
     * @return the data of the task converted to the given type, or null if there is none
     * @throws TaskDataException if the conversion fails
     */
    public <T> T getDataForTask(String uniqueId, Class<T> clazz) {
        if (StringUtils.isBlank(uniqueId) || clazz == null) return null;
        return fromJson(uniqueId, getDataForTask(uniqueId), clazz);
    }

    /**
     * Converts the data carried by a claimed task to the given type without reading the store.
     *
     * @return the converted data, or null when the task has none
     * @throws TaskDataException if the conversion fails
     */
    public <T> T decodeData(Task task, Class<T> clazz) {
        return fromJson(task.uniqueId(), task.data(), clazz);
    }

    private <T> T fromJson(String id, String json, Class<T> clazz) {
        if (json == null) return null;
        if (mapper == null) {
            throw new TaskDataException("No JsonMapper configured, cannot read data for task " + id);
        }
        try {
            return mapper.fromJson(json, clazz);
        } catch (Exception e) {
            throw new TaskDataException("Error reading data for task: " + id, e);
        }
    }

    /** @return the task with that id, in whatever state it is, if it exists */
    public Optional<Task> getTask(String uniqueId) {
        return store.getTask(uniqueId).map(r -> Task.from(r, eventParser));
    }

    public long queueSize(String queueName) {
        return store.queueSize(queuesHolder.getValidQueue(queueName));
    }

    public long leasedCount() {
        return store.leasedCount();
    }

    public long scheduledCount() {
        return store.scheduledCount();
    }

    public long deadCount() {
        return store.deadCount();
    }

    /** @return the oldest dead tasks, up to the limit */
    public List<Task> deadTasks(int limit) {
        var result = new ArrayList<Task>();
        for (var r : store.deadTasks(limit)) {
            result.add(Task.from(r, eventParser));
        }
        return result;
    }

    /** Puts a dead task back into its queue with a fresh attempt count. @return true when it was dead */
    public boolean retryDeadTask(String uniqueId) {
        return store.retryDeadTask(uniqueId);
    }

    /** Removes dead tasks that died before the given time. @return the number removed */
    public long purgeDeadTasks(Instant before) {
        return store.purgeDeadTasks(before);
    }

    // ---- transitions used by the services --------------------------------------------------------------

    /**
     * Claims up to {@code count} tasks from the queues, in the given order, for {@code lease}.
     *
     * @param tokenBase unique per call; the store appends an index per task
     */
    List<Task> claim(List<String> queues, int count, String tokenBase, Duration lease) {
        var records = store.claim(queues, count, tokenBase, lease);
        var tasks = new ArrayList<Task>(records.size());
        for (var r : records) {
            try {
                tasks.add(Task.from(r, eventParser));
            } catch (RuntimeException e) {
                // the task is leased with a token nobody holds; the lease expires and reclaim decides
                log.error("Claimed task {} cannot be read and will be retried after its lease expires", r.id(), e);
            }
        }
        return tasks;
    }

    /** Extends the lease of the given tasks. @return the number of leases still owned */
    int renew(Collection<Task> tasks, Duration lease) {
        if (tasks.isEmpty()) return 0;
        var tokens = new LinkedHashMap<String, String>();
        for (var t : tasks) {
            tokens.put(t.uniqueId(), t.token());
        }
        return store.renew(tokens, lease);
    }

    /** Completes the task. @return false when the lease was lost */
    boolean ack(Task task) {
        return store.ack(task.uniqueId(), task.token());
    }

    /**
     * Records a failed attempt: the task is scheduled again after {@code retryDelay}, or moved to the dead set
     * when its attempts are exhausted or {@code retryDelay} is null.
     */
    FailOutcome fail(Task task, Duration retryDelay, String error) {
        return fail(task.uniqueId(), task.token(), retryDelay, error);
    }

    FailOutcome fail(String id, String token, Duration retryDelay, String error) {
        return store.fail(
                id,
                token,
                retryDelay,
                StringUtils.abbreviate(StringUtils.defaultString(error), MAX_ERROR_LENGTH),
                deadRetention);
    }

    /** Moves the task to the scheduled set for the given time. @return false when the lease was lost */
    boolean reschedule(Task task, Instant due) {
        return store.reschedule(task.uniqueId(), task.token(), due);
    }

    /** Returns a claimed task to the front of its queue. @return false when the lease was lost */
    boolean unclaim(Task task) {
        return store.unclaim(task.uniqueId(), task.token());
    }

    /** Moves due scheduled tasks into their queues. */
    PromoteResult promote(int limit) {
        return store.promote(limit);
    }

    /** @return leases that expired, oldest first, up to the limit */
    List<ExpiredLease> reclaim(int limit) {
        return store.reclaim(limit);
    }

    /** @return recurring definitions whose next fire time is at or before the given time */
    List<DueSchedule> cronDue(Instant upTo, int limit) {
        return store.dueRecurring(upTo, limit);
    }

    /**
     * Creates the occurrence of a due schedule, with id {@code <scheduleId>:<fireAtMillis>}, and advances the
     * schedule to {@code nextFire}, unless the definition changed or was deleted meanwhile.
     *
     * @return true when committed
     */
    boolean cronCommit(DueSchedule due, Instant nextFire) {
        var d = due.definition();
        var occurrence = TaskRecord.newTask(
                        due.scheduleId() + ":" + due.fireAt().toEpochMilli(),
                        d.event(),
                        d.account(),
                        d.queue(),
                        d.data(),
                        d.maxAttempts(),
                        due.fireAt())
                .withScheduleId(due.scheduleId());
        return store.commitRecurring(due, nextFire, occurrence);
    }

    TaskStore store() {
        return store;
    }

    QueuesHolder queuesHolder() {
        return queuesHolder;
    }

    Function<String, Event> eventParser() {
        return eventParser;
    }
}

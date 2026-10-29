package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.Keys;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;

/**
 * All Redis operations of a runner. Applications use the public methods to enqueue tasks, manage
 * recurring definitions and inspect queues; the services use the package-private transitions, each
 * of which is one atomic script (see {@code docs/design-3.0.md}).
 */
public class TaskOps {
    private static final Logger log = LoggerFactory.getLogger(TaskOps.class);

    /** Result of a failure transition. */
    enum FailOutcome {
        RETRY,
        DEAD,
        /** the lease token no longer matched, another instance owns the task now */
        LOST
    }

    /** Expired lease as returned by {@link #reclaim(int)}. */
    record ExpiredLease(String id, String token, int attempts, int maxAttempts, Event event) {}

    /** A recurring definition due for its next occurrence. */
    record DueSchedule(String scheduleId, Instant fireAt, int revision, Map<String, String> definition) {}

    record PromoteResult(int moved, Instant nextDue) {}

    private final UnifiedJedis jedis;
    private final JsonMapper mapper;
    private final Keys keys;
    private final Function<String, Event> eventParser;
    private final QueuesHolder queuesHolder;
    private final ScriptRunner scripts;
    private final long deadRetentionSeconds;

    /**
     * Normally created by {@link BackgroundRunner}; applications obtain the instance from the runner
     * through {@code taskOps()}.
     *
     * @param deadRetention how long the hash of a dead task is kept
     */
    public TaskOps(
            Keys keys,
            UnifiedJedis jedis,
            JsonMapper jsonMapper,
            Function<String, Event> eventParser,
            QueuesHolder queuesHolder,
            Duration deadRetention) {
        this.keys = keys;
        this.jedis = jedis;
        this.mapper = jsonMapper;
        this.eventParser = eventParser;
        this.queuesHolder = queuesHolder;
        this.scripts = new ScriptRunner(jedis);
        this.deadRetentionSeconds = Math.max(1, deadRetention.toSeconds());
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
        var args = new ArrayList<String>();
        args.add(task.uniqueId());
        args.add(
                task.isExecutionTimeSet()
                        ? String.valueOf(task.executeAtTimeStamp().toEpochMilli())
                        : "");
        args.add(keys.wakeChannel);
        args.add(queue);
        args.addAll(taskFields(task, queue, json));
        var r = (Long) scripts.run(
                RedisScripts.ENQUEUE, List.of(keys.task(task.uniqueId()), keys.scheduled, keys.queue(queue)), args);
        if (r == 1) {
            log.debug("Enqueued {} to {}", task, queue);
            return true;
        }
        log.debug("Task {} already exists, not enqueued", task.uniqueId());
        return false;
    }

    private static List<String> taskFields(Task task, String queue, String json) {
        var fields = new ArrayList<String>();
        fields.add(Task.F_EVENT);
        fields.add(task.event().value());
        fields.add(Task.F_ACCOUNT);
        fields.add(task.accountId());
        fields.add(Task.F_QUEUE);
        fields.add(queue);
        fields.add(Task.F_ATTEMPTS);
        fields.add("0");
        fields.add(Task.F_MAX);
        fields.add(String.valueOf(task.maxAttempts()));
        fields.add(Task.F_CREATED);
        fields.add(String.valueOf(System.currentTimeMillis()));
        if (json != null) {
            fields.add(Task.F_DATA);
            fields.add(json);
        }
        return fields;
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
        var args = new ArrayList<String>();
        args.add(task.uniqueId());
        args.add(keys.cronPrefix);
        args.add(String.valueOf(next.toEpochMilli()));
        args.add(Task.F_EVENT);
        args.add(task.event().value());
        args.add(Task.F_ACCOUNT);
        args.add(task.accountId());
        args.add(Task.F_QUEUE);
        args.add(queue);
        args.add(Task.F_CRON);
        args.add(task.cronExpression());
        args.add(Task.F_MAX);
        args.add(String.valueOf(task.maxAttempts()));
        if (data != null) {
            args.add(Task.F_DATA);
            args.add(toJson(task, data));
        }
        scripts.run(RedisScripts.CRON_PUT, List.of(keys.cron), args);
        log.info("Registered recurring task {} next at {}", task, next);
        return task.uniqueId();
    }

    public String addRecurringTask(Task task) {
        return addRecurringTask(task, null);
    }

    /** @return true when a definition with that id existed */
    public boolean deleteRecurringTask(String scheduleId) {
        var r = (Long) scripts.run(RedisScripts.CRON_DELETE, List.of(keys.cron), List.of(scheduleId, keys.cronPrefix));
        log.info("Deleted recurring task {}: {}", scheduleId, r == 1);
        return r == 1;
    }

    /** @return the number of definitions removed */
    public long deleteAllRecurringTasks() {
        long removed = 0;
        for (var id : jedis.zrange(keys.cron, 0, -1)) {
            if (deleteRecurringTask(id)) removed++;
        }
        return removed;
    }

    /** @return the recurring definitions with their next fire time */
    public Map<String, Instant> recurringTasks() {
        var result = new HashMap<String, Instant>();
        jedis.zrangeWithScores(keys.cron, 0, -1)
                .forEach(t -> result.put(t.getElement(), Instant.ofEpochMilli((long) t.getScore())));
        return result;
    }

    // ---- data and inspection -------------------------------------------------------------------------

    /**
     * @return the serialized data of the task, or null if the id is blank or the task has no data or does not exist
     */
    public String getDataForTask(String uniqueId) {
        if (StringUtils.isBlank(uniqueId)) return null;
        return jedis.hget(keys.task(uniqueId), Task.F_DATA);
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
     * Converts the data carried by a claimed task to the given type without a Redis read.
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
        var hash = jedis.hgetAll(keys.task(uniqueId));
        if (hash.isEmpty()) return Optional.empty();
        return Optional.of(Task.fromHash(uniqueId, hash, eventParser));
    }

    public long queueSize(String queueName) {
        return jedis.llen(keys.queue(queuesHolder.getValidQueue(queueName)));
    }

    public long leasedCount() {
        return jedis.zcard(keys.leased);
    }

    public long scheduledCount() {
        return jedis.zcard(keys.scheduled);
    }

    public long deadCount() {
        return jedis.zcard(keys.dead);
    }

    /** @return the oldest dead tasks, up to the limit, whose hash has not expired yet */
    public List<Task> deadTasks(int limit) {
        var result = new ArrayList<Task>();
        for (var id : jedis.zrange(keys.dead, 0, limit - 1)) {
            getTask(id).ifPresent(result::add);
        }
        return result;
    }

    /** Puts a dead task back into its queue with a fresh attempt count. @return true when it was dead */
    public boolean retryDeadTask(String uniqueId) {
        var r = (Long) scripts.run(
                RedisScripts.RETRY_DEAD,
                List.of(keys.dead),
                List.of(keys.taskPrefix, keys.queuePrefix, keys.wakeChannel, uniqueId));
        return r == 1;
    }

    /** Removes dead tasks that died before the given time. @return the number removed */
    public long purgeDeadTasks(Instant before) {
        long total = 0;
        long batch;
        do {
            batch = (Long) scripts.run(
                    RedisScripts.DEAD_PURGE,
                    List.of(keys.dead),
                    List.of(String.valueOf(before.toEpochMilli()), "500", keys.taskPrefix));
            total += batch;
        } while (batch == 500);
        return total;
    }

    // ---- transitions used by the services --------------------------------------------------------------

    /**
     * Claims up to {@code count} tasks from the queues, in the given order, for {@code lease}.
     *
     * @param tokenBase unique per call; the script appends an index per task
     */
    @SuppressWarnings("unchecked")
    List<Task> claim(List<String> queues, int count, String tokenBase, Duration lease) {
        var scriptKeys = new ArrayList<String>(queues.size() + 1);
        scriptKeys.add(keys.leased);
        queues.forEach(q -> scriptKeys.add(keys.queue(q)));
        var r = (List<Object>) scripts.run(
                RedisScripts.CLAIM,
                scriptKeys,
                List.of(
                        String.valueOf(System.currentTimeMillis()),
                        String.valueOf(lease.toMillis()),
                        tokenBase,
                        String.valueOf(count),
                        keys.taskPrefix));
        var tasks = new ArrayList<Task>(r.size());
        for (var entry : r) {
            var flat = (List<Object>) entry;
            var id = (String) flat.getFirst();
            try {
                tasks.add(Task.fromHash(id, toMap(flat, 1), eventParser));
            } catch (RuntimeException e) {
                // the task is leased with a token nobody holds; the lease expires and reclaim decides
                log.error("Claimed task {} cannot be read and will be retried after its lease expires", id, e);
            }
        }
        return tasks;
    }

    private static Map<String, String> toMap(List<Object> flat, int from) {
        var map = new HashMap<String, String>();
        for (int i = from; i + 1 < flat.size(); i += 2) {
            map.put((String) flat.get(i), (String) flat.get(i + 1));
        }
        return map;
    }

    /** Extends the lease of the given tasks. @return the number of leases still owned */
    int renew(Collection<Task> tasks, Duration lease) {
        if (tasks.isEmpty()) return 0;
        var args = new ArrayList<String>(2 + tasks.size() * 2);
        args.add(String.valueOf(System.currentTimeMillis() + lease.toMillis()));
        args.add(keys.taskPrefix);
        for (var t : tasks) {
            args.add(t.uniqueId());
            args.add(t.token());
        }
        return ((Long) scripts.run(RedisScripts.RENEW, List.of(keys.leased), args)).intValue();
    }

    /** Completes the task. @return false when the lease was lost */
    boolean ack(Task task) {
        var r = (Long) scripts.run(
                RedisScripts.ACK, List.of(keys.leased), List.of(keys.taskPrefix, task.uniqueId(), task.token()));
        return r == 1;
    }

    /**
     * Records a failed attempt: the task is scheduled again after {@code retryDelay}, or moved to the dead set
     * when its attempts are exhausted or {@code retryDelay} is null.
     */
    FailOutcome fail(Task task, Duration retryDelay, String error) {
        return fail(task.uniqueId(), task.token(), retryDelay, error);
    }

    FailOutcome fail(String id, String token, Duration retryDelay, String error) {
        var r = (String) scripts.run(
                RedisScripts.FAIL,
                List.of(keys.leased, keys.scheduled, keys.dead),
                List.of(
                        keys.taskPrefix,
                        id,
                        token,
                        String.valueOf(System.currentTimeMillis()),
                        retryDelay == null ? "dead" : String.valueOf(retryDelay.toMillis()),
                        StringUtils.abbreviate(StringUtils.defaultString(error), 1000),
                        String.valueOf(deadRetentionSeconds)));
        return FailOutcome.valueOf(r.toUpperCase());
    }

    /** Moves the task to the scheduled set for the given time. @return false when the lease was lost */
    boolean reschedule(Task task, Instant due) {
        var r = (Long) scripts.run(
                RedisScripts.RESCHEDULE,
                List.of(keys.leased, keys.scheduled),
                List.of(keys.taskPrefix, task.uniqueId(), task.token(), String.valueOf(due.toEpochMilli())));
        return r == 1;
    }

    /** Returns a claimed task to the front of its queue. @return false when the lease was lost */
    boolean unclaim(Task task) {
        var r = (Long) scripts.run(
                RedisScripts.UNCLAIM,
                List.of(keys.leased),
                List.of(keys.taskPrefix, keys.queuePrefix, task.uniqueId(), task.token()));
        return r == 1;
    }

    /** Moves due scheduled tasks into their queues. */
    @SuppressWarnings("unchecked")
    PromoteResult promote(int limit) {
        var r = (List<Object>) scripts.run(
                RedisScripts.PROMOTE,
                List.of(keys.scheduled),
                List.of(
                        String.valueOf(System.currentTimeMillis()),
                        String.valueOf(limit),
                        keys.taskPrefix,
                        keys.queuePrefix,
                        keys.wakeChannel));
        var next = (Long) r.get(1);
        return new PromoteResult(((Long) r.getFirst()).intValue(), next < 0 ? null : Instant.ofEpochMilli(next));
    }

    /** @return leases that expired, oldest first, up to the limit */
    @SuppressWarnings("unchecked")
    List<ExpiredLease> reclaim(int limit) {
        var r = (List<Object>) scripts.run(
                RedisScripts.RECLAIM,
                List.of(keys.leased),
                List.of(String.valueOf(System.currentTimeMillis()), String.valueOf(limit), keys.taskPrefix));
        var result = new ArrayList<ExpiredLease>(r.size());
        for (var entry : r) {
            var e = (List<Object>) entry;
            var eventValue = (String) e.get(4);
            var event = eventValue == null ? null : eventParser.apply(eventValue);
            result.add(new ExpiredLease(
                    (String) e.get(0),
                    (String) e.get(1),
                    Integer.parseInt((String) e.get(2)),
                    Integer.parseInt((String) e.get(3)),
                    event));
        }
        return result;
    }

    /** @return recurring definitions whose next fire time is at or before the given time */
    @SuppressWarnings("unchecked")
    List<DueSchedule> cronDue(Instant upTo, int limit) {
        var r = (List<Object>) scripts.run(
                RedisScripts.CRON_DUE,
                List.of(keys.cron),
                List.of(String.valueOf(upTo.toEpochMilli()), String.valueOf(limit), keys.cronPrefix));
        var result = new ArrayList<DueSchedule>(r.size());
        for (var entry : r) {
            var e = (List<Object>) entry;
            var definition = toMap(e, 2);
            var fireAt = Instant.ofEpochMilli((long) Double.parseDouble((String) e.get(1)));
            result.add(new DueSchedule(
                    (String) e.get(0), fireAt, Integer.parseInt(definition.getOrDefault("rev", "0")), definition));
        }
        return result;
    }

    /**
     * Creates the occurrence of a due schedule and advances the schedule to {@code nextFire}, unless the
     * definition changed or was deleted meanwhile.
     *
     * @return true when committed
     */
    boolean cronCommit(DueSchedule due, Instant nextFire) {
        var occurrenceId = due.scheduleId() + ":" + due.fireAt().toEpochMilli();
        var d = due.definition();
        var args = new ArrayList<String>();
        args.add(due.scheduleId());
        args.add(String.valueOf(due.revision()));
        args.add(String.valueOf(due.fireAt().toEpochMilli()));
        args.add(String.valueOf(nextFire.toEpochMilli()));
        args.add(occurrenceId);
        args.add(keys.cronPrefix);
        args.add(keys.taskPrefix);
        for (var field : List.of(Task.F_EVENT, Task.F_ACCOUNT, Task.F_QUEUE, Task.F_MAX, Task.F_DATA)) {
            var value = d.get(field);
            if (value != null) {
                args.add(field);
                args.add(value);
            }
        }
        args.add(Task.F_SCHEDULE);
        args.add(due.scheduleId());
        args.add(Task.F_ATTEMPTS);
        args.add("0");
        args.add(Task.F_CREATED);
        args.add(String.valueOf(System.currentTimeMillis()));
        var r = (Long) scripts.run(RedisScripts.CRON_COMMIT, List.of(keys.cron, keys.scheduled), args);
        return r == 1;
    }

    Keys keys() {
        return keys;
    }

    QueuesHolder queuesHolder() {
        return queuesHolder;
    }

    Function<String, Event> eventParser() {
        return eventParser;
    }
}

package today.bonfire.oss.bth4j.redis;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.UnifiedJedis;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.store.DueSchedule;
import today.bonfire.oss.bth4j.store.ExpiredLease;
import today.bonfire.oss.bth4j.store.FailOutcome;
import today.bonfire.oss.bth4j.store.PromoteResult;
import today.bonfire.oss.bth4j.store.RecurringDefinition;
import today.bonfire.oss.bth4j.store.TaskRecord;
import today.bonfire.oss.bth4j.store.TaskStore;

/**
 * {@link TaskStore} on Redis 7.0 or newer: one hash per task, lists as queues, sorted sets for the
 * scheduled, leased and dead states, and one Lua script per transition so that every check and its
 * writes run as one unit (see {@code docs/design-3.0.md}). All keys carry the namespace as a hash tag,
 * so the store also works on Redis Cluster. The scripts are called by SHA and loaded on demand.
 * Runners that share a namespace share the work; the wake-up channel holds one pooled connection per runner.
 */
public class RedisTaskStore implements TaskStore {
    private static final Logger log = LoggerFactory.getLogger(RedisTaskStore.class);
    private static final int PURGE_BATCH = 500;

    static final String F_EVENT = "event";
    static final String F_ACCOUNT = "account";
    static final String F_QUEUE = "queue";
    static final String F_CRON = "cron";
    static final String F_DATA = "data";
    static final String F_ATTEMPTS = "attempts";
    static final String F_MAX = "max";
    static final String F_CREATED = "created";
    static final String F_DUE = "due";
    static final String F_TOKEN = "token";
    static final String F_LEASE = "lease";
    static final String F_SCHEDULE = "schedule";
    static final String F_ERROR = "error";
    static final String F_DIED = "died";
    static final String F_REV = "rev";

    private final UnifiedJedis jedis;
    private final Keys keys;
    private final ScriptRunner scripts;

    /**
     * @param jedis     client used for all Redis operations, for example a {@code RedisClient}. The store does not
     *                  close it.
     * @param namespace prefix of all keys; runners with the same namespace share the work. Must not be blank or
     *                  contain braces.
     */
    public RedisTaskStore(UnifiedJedis jedis, String namespace) {
        if (jedis == null) throw new TaskConfigurationError("Jedis client is required");
        if (StringUtils.isBlank(namespace) || namespace.contains("{") || namespace.contains("}")) {
            throw new TaskConfigurationError("namespace cannot be blank or contain braces");
        }
        this.jedis = jedis;
        this.keys = new Keys(namespace);
        this.scripts = new ScriptRunner(jedis);
    }

    public Keys keys() {
        return keys;
    }

    @Override
    public String toString() {
        return "RedisTaskStore[" + keys.namespace + "]";
    }

    // ---- tasks and definitions ----------------------------------------------------------------------

    @Override
    public boolean enqueue(TaskRecord task) {
        var args = new ArrayList<String>();
        args.add(task.id());
        args.add(task.due() == null ? "" : String.valueOf(task.due().toEpochMilli()));
        args.add(keys.wakeChannel);
        args.add(task.queue());
        args.addAll(fields(task));
        var r = (Long) scripts.run(
                RedisScripts.ENQUEUE, List.of(keys.task(task.id()), keys.scheduled, keys.queue(task.queue())), args);
        return r == 1;
    }

    @Override
    public void putRecurring(RecurringDefinition d, Instant nextFire) {
        var args = new ArrayList<String>();
        args.add(d.scheduleId());
        args.add(keys.cronPrefix);
        args.add(String.valueOf(nextFire.toEpochMilli()));
        args.add(F_EVENT);
        args.add(d.event());
        args.add(F_ACCOUNT);
        args.add(d.account());
        args.add(F_QUEUE);
        args.add(d.queue());
        args.add(F_CRON);
        args.add(d.cron());
        args.add(F_MAX);
        args.add(String.valueOf(d.maxAttempts()));
        if (d.data() != null) {
            args.add(F_DATA);
            args.add(d.data());
        }
        scripts.run(RedisScripts.CRON_PUT, List.of(keys.cron), args);
    }

    @Override
    public boolean deleteRecurring(String scheduleId) {
        var r = (Long) scripts.run(RedisScripts.CRON_DELETE, List.of(keys.cron), List.of(scheduleId, keys.cronPrefix));
        return r == 1;
    }

    @Override
    public Map<String, Instant> recurringSchedules() {
        var result = new HashMap<String, Instant>();
        jedis.zrangeWithScores(keys.cron, 0, -1)
                .forEach(t -> result.put(t.getElement(), Instant.ofEpochMilli((long) t.getScore())));
        return result;
    }

    @Override
    public Optional<TaskRecord> getTask(String id) {
        var hash = jedis.hgetAll(keys.task(id));
        if (hash.isEmpty()) return Optional.empty();
        return Optional.of(fromHash(id, hash));
    }

    @Override
    public long queueSize(String queue) {
        return jedis.llen(keys.queue(queue));
    }

    @Override
    public long leasedCount() {
        return jedis.zcard(keys.leased);
    }

    @Override
    public long scheduledCount() {
        return jedis.zcard(keys.scheduled);
    }

    @Override
    public long deadCount() {
        return jedis.zcard(keys.dead);
    }

    /** Dead tasks whose hash already expired are skipped. */
    @Override
    public List<TaskRecord> deadTasks(int limit) {
        var result = new ArrayList<TaskRecord>();
        for (var id : jedis.zrange(keys.dead, 0, limit - 1)) {
            getTask(id).ifPresent(result::add);
        }
        return result;
    }

    @Override
    public boolean retryDeadTask(String id) {
        var r = (Long) scripts.run(
                RedisScripts.RETRY_DEAD,
                List.of(keys.dead),
                List.of(keys.taskPrefix, keys.queuePrefix, keys.wakeChannel, id));
        return r == 1;
    }

    @Override
    public long purgeDeadTasks(Instant diedBefore) {
        long total = 0;
        long batch;
        do {
            batch = (Long) scripts.run(
                    RedisScripts.DEAD_PURGE,
                    List.of(keys.dead),
                    List.of(String.valueOf(diedBefore.toEpochMilli()), String.valueOf(PURGE_BATCH), keys.taskPrefix));
            total += batch;
        } while (batch == PURGE_BATCH);
        return total;
    }

    // ---- transitions ---------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public List<TaskRecord> claim(List<String> queues, int count, String tokenBase, Duration lease) {
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
        var tasks = new ArrayList<TaskRecord>(r.size());
        for (var entry : r) {
            var flat = (List<Object>) entry;
            tasks.add(fromHash((String) flat.getFirst(), toMap(flat, 1)));
        }
        return tasks;
    }

    @Override
    public int renew(Map<String, String> tokensById, Duration lease) {
        if (tokensById.isEmpty()) return 0;
        var args = new ArrayList<String>(2 + tokensById.size() * 2);
        args.add(String.valueOf(System.currentTimeMillis() + lease.toMillis()));
        args.add(keys.taskPrefix);
        tokensById.forEach((id, token) -> {
            args.add(id);
            args.add(token);
        });
        return ((Long) scripts.run(RedisScripts.RENEW, List.of(keys.leased), args)).intValue();
    }

    @Override
    public boolean ack(String id, String token) {
        var r = (Long) scripts.run(RedisScripts.ACK, List.of(keys.leased), List.of(keys.taskPrefix, id, token));
        return r == 1;
    }

    @Override
    public FailOutcome fail(String id, String token, Duration retryDelay, String error, Duration keepDeadFor) {
        var r = (String) scripts.run(
                RedisScripts.FAIL,
                List.of(keys.leased, keys.scheduled, keys.dead),
                List.of(
                        keys.taskPrefix,
                        id,
                        token,
                        String.valueOf(System.currentTimeMillis()),
                        retryDelay == null ? "dead" : String.valueOf(retryDelay.toMillis()),
                        StringUtils.defaultString(error),
                        String.valueOf(Math.max(1, keepDeadFor.toSeconds()))));
        return FailOutcome.valueOf(r.toUpperCase());
    }

    @Override
    public boolean reschedule(String id, String token, Instant due) {
        var r = (Long) scripts.run(
                RedisScripts.RESCHEDULE,
                List.of(keys.leased, keys.scheduled),
                List.of(keys.taskPrefix, id, token, String.valueOf(due.toEpochMilli())));
        return r == 1;
    }

    @Override
    public boolean unclaim(String id, String token) {
        var r = (Long) scripts.run(
                RedisScripts.UNCLAIM, List.of(keys.leased), List.of(keys.taskPrefix, keys.queuePrefix, id, token));
        return r == 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PromoteResult promote(int limit) {
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

    @Override
    @SuppressWarnings("unchecked")
    public List<ExpiredLease> reclaim(int limit) {
        var r = (List<Object>) scripts.run(
                RedisScripts.RECLAIM,
                List.of(keys.leased),
                List.of(String.valueOf(System.currentTimeMillis()), String.valueOf(limit), keys.taskPrefix));
        var result = new ArrayList<ExpiredLease>(r.size());
        for (var entry : r) {
            var e = (List<Object>) entry;
            result.add(new ExpiredLease(
                    (String) e.get(0),
                    (String) e.get(1),
                    Integer.parseInt((String) e.get(2)),
                    Integer.parseInt((String) e.get(3)),
                    (String) e.get(4)));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DueSchedule> dueRecurring(Instant upTo, int limit) {
        var r = (List<Object>) scripts.run(
                RedisScripts.CRON_DUE,
                List.of(keys.cron),
                List.of(String.valueOf(upTo.toEpochMilli()), String.valueOf(limit), keys.cronPrefix));
        var result = new ArrayList<DueSchedule>(r.size());
        for (var entry : r) {
            var e = (List<Object>) entry;
            var id = (String) e.get(0);
            var fireAt = Instant.ofEpochMilli((long) Double.parseDouble((String) e.get(1)));
            var d = toMap(e, 2);
            var definition = new RecurringDefinition(
                    id,
                    d.get(F_EVENT),
                    d.get(F_ACCOUNT),
                    d.get(F_QUEUE),
                    d.get(F_CRON),
                    d.get(F_DATA),
                    parseInt(d.get(F_MAX), 0),
                    parseInt(d.get(F_REV), 0));
            result.add(new DueSchedule(id, fireAt, definition));
        }
        return result;
    }

    @Override
    public boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord occurrence) {
        var args = new ArrayList<String>();
        args.add(due.scheduleId());
        args.add(String.valueOf(due.definition().revision()));
        args.add(String.valueOf(due.fireAt().toEpochMilli()));
        args.add(String.valueOf(nextFire.toEpochMilli()));
        args.add(occurrence.id());
        args.add(keys.cronPrefix);
        args.add(keys.taskPrefix);
        args.addAll(fields(occurrence));
        var r = (Long) scripts.run(RedisScripts.CRON_COMMIT, List.of(keys.cron, keys.scheduled), args);
        return r == 1;
    }

    // ---- wake-up -------------------------------------------------------------------------------------

    @Override
    public AutoCloseable subscribeWake(Runnable onWake, ThreadFactory threadFactory) {
        var listener = new WakeListener(onWake);
        var thread = threadFactory.newThread(listener);
        thread.start();
        return () -> listener.stop(thread);
    }

    /**
     * Subscribes to the wake channel and calls back on every message. Reconnects with a bounded backoff
     * when the subscription fails.
     */
    private final class WakeListener implements Runnable {
        private final Runnable onWake;
        private volatile boolean listening = true;
        private volatile JedisPubSub subscription;

        WakeListener(Runnable onWake) {
            this.onWake = onWake;
        }

        @Override
        public void run() {
            long backoffMs = 100;
            while (listening) {
                var pubSub = new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        onWake.run();
                    }
                };
                subscription = pubSub;
                try {
                    jedis.subscribe(pubSub, keys.wakeChannel);
                    backoffMs = 100;
                } catch (Exception e) {
                    if (listening) {
                        log.warn("Wake-up subscription lost, retrying in {} ms", backoffMs, e);
                        try {
                            Thread.sleep(backoffMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        backoffMs = Math.min(backoffMs * 2, 10_000);
                    }
                }
            }
        }

        void stop(Thread thread) {
            listening = false;
            var s = subscription;
            if (s != null && s.isSubscribed()) {
                try {
                    s.unsubscribe();
                } catch (Exception e) {
                    log.debug("Unsubscribe failed while stopping", e);
                }
            }
            thread.interrupt();
        }
    }

    // ---- hash mapping --------------------------------------------------------------------------------

    private static List<String> fields(TaskRecord t) {
        var fields = new ArrayList<String>();
        fields.add(F_EVENT);
        fields.add(t.event());
        fields.add(F_ACCOUNT);
        fields.add(t.account());
        fields.add(F_QUEUE);
        fields.add(t.queue());
        fields.add(F_ATTEMPTS);
        fields.add(String.valueOf(t.attempts()));
        fields.add(F_MAX);
        fields.add(String.valueOf(t.maxAttempts()));
        fields.add(F_CREATED);
        fields.add(String.valueOf(t.created().toEpochMilli()));
        if (t.data() != null) {
            fields.add(F_DATA);
            fields.add(t.data());
        }
        if (t.scheduleId() != null) {
            fields.add(F_SCHEDULE);
            fields.add(t.scheduleId());
        }
        return fields;
    }

    static TaskRecord fromHash(String id, Map<String, String> h) {
        return new TaskRecord(
                id,
                h.get(F_EVENT),
                h.get(F_ACCOUNT),
                h.get(F_QUEUE),
                h.get(F_DATA),
                parseInt(h.get(F_ATTEMPTS), 0),
                parseInt(h.get(F_MAX), 0),
                parseInstant(h.get(F_CREATED)),
                parseInstant(h.get(F_DUE)),
                h.get(F_TOKEN),
                parseInstant(h.get(F_LEASE)),
                h.get(F_SCHEDULE),
                h.get(F_ERROR),
                parseInstant(h.get(F_DIED)));
    }

    private static Map<String, String> toMap(List<Object> flat, int from) {
        var map = new HashMap<String, String>();
        for (int i = from; i + 1 < flat.size(); i += 2) {
            map.put((String) flat.get(i), (String) flat.get(i + 1));
        }
        return map;
    }

    private static int parseInt(String s, int fallback) {
        return s == null ? fallback : Integer.parseInt(s);
    }

    private static Instant parseInstant(String millis) {
        return millis == null ? null : Instant.ofEpochMilli(Long.parseLong(millis));
    }
}

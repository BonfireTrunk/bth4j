package today.bonfire.oss.bth4j.redis;

import static today.bonfire.oss.bth4j.redis.Keys.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.LeaseStatus;
import today.bonfire.oss.bth4j.spi.PromoteResult;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RecordJson;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.RecurringSchedule;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.StoreInfo;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/**
 * {@link FullStore} on Redis 7.0 or newer: one hash per task, sorted sets for the queues and the state indexes,
 * and one Lua script per transition so that every check and its writes run as one unit (see
 * {@link Keys} for the layout and {@link RedisScripts} for the scripts). All keys carry the namespace as a hash
 * tag, so the store also works on Redis Cluster. Runners that share a namespace share the work.
 */
public class RedisTaskStore implements FullStore {
    private static final int MAX_FAILURES = 20;
    private static final int PAGE_CHUNK = 500;

    private final UnifiedJedis jedis;
    private final Keys keys;
    private final ScriptRunner scripts;
    private final Map<String, Integer> queueIds = new ConcurrentHashMap<>();
    private final Map<String, Integer> eventIds = new ConcurrentHashMap<>();

    /**
     * @param jedis     client used for all Redis operations, for example a {@code RedisClient}. The store does not
     *                  close it.
     * @param namespace prefix of all keys; runners with the same namespace share the work. Must not be blank or
     *                  contain braces.
     */
    public RedisTaskStore(UnifiedJedis jedis, String namespace) {
        if (jedis == null) {
            throw new IllegalArgumentException("Jedis client is required");
        }
        if (namespace == null || namespace.isBlank() || namespace.contains("{") || namespace.contains("}")) {
            throw new IllegalArgumentException("namespace cannot be blank or contain braces");
        }
        this.jedis = jedis;
        this.keys = new Keys(namespace);
        this.scripts = new ScriptRunner(jedis, keys.meta, keys.prefix);
    }

    public Keys keys() {
        return keys;
    }

    @Override
    public String toString() {
        return "RedisTaskStore[" + keys.namespace + "]";
    }

    private static <T> T guard(Supplier<T> call) {
        try {
            return call.get();
        } catch (JedisException e) {
            throw new TaskStoreException("Redis call failed: " + e.getMessage(), e);
        }
    }

    private static void guard(Runnable call) {
        guard(() -> {
            call.run();
            return null;
        });
    }

    private static String ms(Instant t) {
        return String.valueOf(t.toEpochMilli());
    }

    private static long num(Object o) {
        return o instanceof Long l ? l : Long.parseLong((String) o);
    }

    // ---- identity and registries ---------------------------------------------------------------------

    @Override
    public StoreInfo info() {
        return new StoreInfo("redis", StoreInfo.SCHEMA_VERSION);
    }

    @Override
    public void init() {
        guard(() -> {
            var wanted = String.valueOf(StoreInfo.SCHEMA_VERSION);
            jedis.hsetnx(keys.meta, SCHEMA, wanted);
            var found = jedis.hget(keys.meta, SCHEMA);
            if (!wanted.equals(found)) {
                throw new TaskStoreException(
                        "Redis namespace " + keys.namespace + " has schema version " + found + ", expected " + wanted);
            }
        });
    }

    @Override
    public long allocateIds(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be positive");
        }
        return guard(() -> jedis.incrBy(keys.seqId, count)) - count + 1;
    }

    @Override
    public int allocateWorkerId(String label) {
        var args = List.of(label == null ? "" : label);
        return (int) num(guard(() -> scripts.run(RedisScripts.WORKER_REGISTER, args)));
    }

    @Override
    public int queueId(String name) {
        return register("queue", name, queueIds);
    }

    @Override
    public int eventId(String name) {
        return register("event", name, eventIds);
    }

    private int register(String kind, String name, Map<String, Integer> cache) {
        var cached = cache.get(name);
        if (cached != null) {
            return cached;
        }
        var id = (int) num(guard(() -> scripts.run(RedisScripts.REGISTER, List.of(kind, name))));
        cache.put(name, id);
        return id;
    }

    @Override
    public Map<Integer, String> queueNames() {
        return names("queue");
    }

    @Override
    public Map<Integer, String> eventNames() {
        return names("event");
    }

    private Map<Integer, String> names(String kind) {
        var raw = guard(() -> jedis.hgetAll(keys.registryNames(kind)));
        var out = new LinkedHashMap<Integer, String>();
        raw.forEach((id, name) -> out.put(Integer.valueOf(id), name));
        return out;
    }

    // ---- execution -----------------------------------------------------------------------------------

    @Override
    public EnqueueOutcome enqueue(TaskRecord task) {
        return outcome(guard(() -> scripts.run(RedisScripts.ENQUEUE, enqueueArgs(task))));
    }

    @Override
    public List<EnqueueOutcome> enqueueAll(List<TaskRecord> tasks) {
        if (tasks.isEmpty()) {
            return List.of();
        }
        var calls = tasks.stream().map(RedisTaskStore::enqueueArgs).toList();
        return guard(() -> scripts.runAll(RedisScripts.ENQUEUE, calls)).stream()
                .map(RedisTaskStore::outcome)
                .toList();
    }

    private static List<String> enqueueArgs(TaskRecord t) {
        var args = new ArrayList<String>();
        args.add(String.valueOf(t.id()));
        args.add(t.due() == null ? "" : ms(t.due()));
        args.add(t.uniqueKey() == null ? "" : t.uniqueKey());
        args.addAll(TaskHashes.fields(t));
        return args;
    }

    @SuppressWarnings("unchecked")
    private static EnqueueOutcome outcome(Object result) {
        var r = (List<Object>) result;
        return switch ((int) num(r.get(0))) {
            case 1 -> EnqueueOutcome.STORED;
            case 0 -> EnqueueOutcome.EXISTS;
            default -> new EnqueueOutcome.DuplicateKey(num(r.get(1)));
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<TaskRecord> claim(ClaimRequest request) {
        var known = request.knownEventIds();
        if (request.count() < 1 || request.queueIds().isEmpty() || (known != null && known.isEmpty())) {
            return List.of();
        }
        var args = new ArrayList<String>();
        args.add(String.valueOf(request.count()));
        args.add(String.valueOf(request.lease().toMillis()));
        args.add(String.valueOf(request.workerId()));
        args.add(known == null ? "" : known.stream().map(String::valueOf).collect(Collectors.joining(",")));
        request.queueIds().forEach(q -> args.add(String.valueOf(q)));
        var r = (List<Object>) guard(() -> scripts.run(RedisScripts.CLAIM, args));
        var tasks = new ArrayList<TaskRecord>(r.size());
        for (var entry : r) {
            var flat = (List<Object>) entry;
            tasks.add(TaskHashes.fromHash(Long.parseLong((String) flat.getFirst()), toMap(flat, 1)));
        }
        return tasks;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<LeaseStatus> renew(List<LeaseRef> leases, Duration lease) {
        if (leases.isEmpty()) {
            return List.of();
        }
        var args = new ArrayList<String>();
        args.add(String.valueOf(lease.toMillis()));
        for (var l : leases) {
            args.add(String.valueOf(l.id()));
            args.add(String.valueOf(l.revision()));
        }
        var r = (List<Object>) guard(() -> scripts.run(RedisScripts.RENEW, args));
        var out = new ArrayList<LeaseStatus>(leases.size());
        for (var i = 0; i < leases.size(); i++) {
            var code = num(r.get(i));
            out.add(new LeaseStatus(leases.get(i).id(), code > 0, code == 2));
        }
        return out;
    }

    @Override
    public boolean ack(long id, int revision, boolean keep, boolean keepPayload) {
        var args = List.of(String.valueOf(id), String.valueOf(revision), keep ? "1" : "0", keepPayload ? "1" : "0");
        return num(guard(() -> scripts.run(RedisScripts.ACK, args))) == 1;
    }

    @Override
    public FailOutcome fail(long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead) {
        return fail(id, revision, failure, retryAt, keepDead, false);
    }

    @Override
    public FailOutcome failExpired(long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead) {
        return fail(id, revision, failure, retryAt, keepDead, true);
    }

    /**
     * The failures list is read, extended and written back whole. Only the holder of the current revision passes
     * the script's fence, and the revision does not change between this read and that write for it.
     */
    private FailOutcome fail(
            long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead, boolean expiredOnly) {
        return guard(() -> {
            var failures = new ArrayList<FailureInfo>();
            var current = RecordJson.decodeFailures(jedis.hget(keys.task(id), F_FAILURES));
            if (current != null) {
                failures.addAll(current);
            }
            failures.add(failure);
            var kept = failures.subList(Math.max(0, failures.size() - MAX_FAILURES), failures.size());
            var r = (String) scripts.run(
                    RedisScripts.FAIL,
                    List.of(
                            String.valueOf(id),
                            String.valueOf(revision),
                            retryAt == null ? "" : ms(retryAt),
                            keepDead ? "1" : "0",
                            expiredOnly ? "1" : "0",
                            RecordJson.encodeFailures(kept)));
            return FailOutcome.valueOf(r);
        });
    }

    @Override
    public boolean reschedule(long id, int revision, Instant due) {
        var args = List.of(String.valueOf(id), String.valueOf(revision), ms(due));
        return num(guard(() -> scripts.run(RedisScripts.RESCHEDULE, args))) == 1;
    }

    @Override
    public boolean unclaim(long id, int revision) {
        var args = List.of(String.valueOf(id), String.valueOf(revision));
        return num(guard(() -> scripts.run(RedisScripts.UNCLAIM, args))) == 1;
    }

    @Override
    public boolean saveSteps(long id, int revision, Map<String, StepResult> steps) {
        return setField(id, revision, F_STEPS, RecordJson.encodeSteps(steps));
    }

    @Override
    public boolean progress(long id, int revision, String progress) {
        return setField(id, revision, F_PROGRESS, progress);
    }

    private boolean setField(long id, int revision, String field, String value) {
        var args = List.of(
                String.valueOf(id),
                String.valueOf(revision),
                field,
                value == null ? "" : value,
                value == null ? "1" : "0");
        return num(guard(() -> scripts.run(RedisScripts.SET_FIELD, args))) == 1;
    }

    // ---- maintenance ---------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public PromoteResult promote(int limit) {
        var r = (List<Object>) guard(() -> scripts.run(RedisScripts.PROMOTE, List.of(String.valueOf(limit))));
        var next = num(r.get(1));
        return new PromoteResult((int) num(r.get(0)), next < 0 ? null : Instant.ofEpochMilli(next));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<ExpiredLease> reclaim(int limit) {
        var r = (List<Object>) guard(() -> scripts.run(RedisScripts.RECLAIM, List.of(String.valueOf(limit))));
        var out = new ArrayList<ExpiredLease>(r.size());
        for (var entry : r) {
            var e = (List<Object>) entry;
            out.add(new ExpiredLease(
                    num(e.get(0)),
                    (int) num(e.get(1)),
                    (int) num(e.get(2)),
                    (int) num(e.get(3)),
                    (int) num(e.get(4)),
                    num(e.get(5)) == 1));
        }
        return out;
    }

    @Override
    public long purge(TaskState state, Duration retention, int limit) {
        if (!state.isTerminal()) {
            throw new IllegalArgumentException("purge needs a terminal state, got " + state);
        }
        var args = List.of(String.valueOf(state.code()), String.valueOf(retention.toMillis()), String.valueOf(limit));
        return num(guard(() -> scripts.run(RedisScripts.PURGE, args)));
    }

    @Override
    public int releaseKeys(int limit) {
        return (int) num(guard(() -> scripts.run(RedisScripts.RELEASE_KEYS, List.of(String.valueOf(limit)))));
    }

    @Override
    public Instant nextDue() {
        var head = guard(() -> jedis.zrangeWithScores(keys.scheduled, 0, 0));
        return head.isEmpty()
                ? null
                : Instant.ofEpochMilli((long) head.getFirst().getScore());
    }

    // ---- schedules -----------------------------------------------------------------------------------

    @Override
    public void putRecurring(RecurringDefinition d, Instant nextFire) {
        var args = new ArrayList<String>();
        args.add(d.scheduleId());
        args.add(ms(nextFire));
        args.addAll(List.of(
                F_EVENT,
                String.valueOf(d.eventId()),
                F_QUEUE,
                String.valueOf(d.queueId()),
                F_PAYLOAD_VERSION,
                String.valueOf(d.payloadVersion()),
                F_MAX_ATTEMPTS,
                String.valueOf(d.maxAttempts())));
        if (d.cron() != null) {
            args.addAll(List.of(F_CRON, d.cron()));
        }
        if (d.zone() != null) {
            args.addAll(List.of(F_ZONE, d.zone()));
        }
        if (d.misfire() != null) {
            args.addAll(List.of(F_MISFIRE, d.misfire()));
        }
        if (d.catchUpMax() != 0) {
            args.addAll(List.of(F_CATCH_UP, String.valueOf(d.catchUpMax())));
        }
        if (d.fixedDelayMs() != null) {
            args.addAll(List.of(F_FIXED_DELAY, String.valueOf(d.fixedDelayMs())));
        }
        if (d.overlapSkip()) {
            args.addAll(List.of(F_OVERLAP_SKIP, "1"));
        }
        if (d.paused()) {
            args.addAll(List.of(F_PAUSED, "1"));
        }
        if (d.account() != null) {
            args.addAll(List.of(F_ACCOUNT, d.account()));
        }
        if (d.data() != null) {
            args.addAll(List.of(F_DATA, d.data()));
        }
        guard(() -> scripts.run(RedisScripts.CRON_PUT, args));
    }

    @Override
    public boolean deleteRecurring(String scheduleId) {
        return num(guard(() -> scripts.run(RedisScripts.CRON_DELETE, List.of(scheduleId)))) == 1;
    }

    @Override
    public Map<String, Instant> recurringSchedules() {
        var result = new LinkedHashMap<String, Instant>();
        recurringDefinitions().forEach(r -> result.put(r.definition().scheduleId(), r.nextFire()));
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DueSchedule> dueRecurring(Instant upTo, int limit) {
        var r = (List<Object>)
                guard(() -> scripts.run(RedisScripts.CRON_DUE, List.of(ms(upTo), String.valueOf(limit))));
        var out = new ArrayList<DueSchedule>(r.size());
        for (var entry : r) {
            var e = (List<Object>) entry;
            var id = (String) e.get(0);
            var fireAt = Instant.ofEpochMilli((long) Double.parseDouble((String) e.get(1)));
            out.add(new DueSchedule(id, fireAt, definition(id, toMap(e, 2))));
        }
        return out;
    }

    private static RecurringDefinition definition(String id, Map<String, String> d) {
        return new RecurringDefinition(
                id,
                Integer.parseInt(d.get(F_EVENT)),
                Integer.parseInt(d.get(F_PAYLOAD_VERSION)),
                Integer.parseInt(d.get(F_QUEUE)),
                d.get(F_ACCOUNT),
                d.get(F_CRON),
                d.get(F_ZONE),
                d.get(F_MISFIRE),
                d.containsKey(F_CATCH_UP) ? Integer.parseInt(d.get(F_CATCH_UP)) : 0,
                d.containsKey(F_FIXED_DELAY) ? Long.parseLong(d.get(F_FIXED_DELAY)) : null,
                "1".equals(d.get(F_OVERLAP_SKIP)),
                d.get(F_DATA),
                Integer.parseInt(d.get(F_MAX_ATTEMPTS)),
                "1".equals(d.get(F_PAUSED)),
                Long.parseLong(d.get(F_REVISION)));
    }

    @Override
    public boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord occurrence) {
        var args = new ArrayList<String>();
        args.add(due.scheduleId());
        args.add(String.valueOf(due.definition().revision()));
        args.add(ms(due.fireAt()));
        args.add(ms(nextFire));
        args.add(String.valueOf(occurrence.id()));
        args.add(occurrence.uniqueKey() == null ? "" : occurrence.uniqueKey());
        args.addAll(TaskHashes.fields(occurrence));
        return num(guard(() -> scripts.run(RedisScripts.CRON_COMMIT, args))) == 1;
    }

    // ---- inspection ----------------------------------------------------------------------------------

    @Override
    public Optional<TaskRecord> getTask(long id) {
        var hash = guard(() -> jedis.hgetAll(keys.task(id)));
        return hash.isEmpty() ? Optional.empty() : Optional.of(TaskHashes.fromHash(id, hash));
    }

    @Override
    public Map<Integer, StateCounts> counts() {
        return guard(() -> {
            var out = new LinkedHashMap<Integer, StateCounts>();
            for (var qid : sortedQueueIds()) {
                var h = jedis.hgetAll(keys.counters(qid));
                out.put(
                        qid,
                        new StateCounts(
                                count(h, 0),
                                count(h, 1),
                                count(h, 2),
                                count(h, 3),
                                count(h, 4),
                                count(h, 5),
                                count(h, 6)));
            }
            return out;
        });
    }

    private static long count(Map<String, String> h, int state) {
        var v = h.get(String.valueOf(state));
        return v == null ? 0 : Long.parseLong(v);
    }

    private List<Integer> sortedQueueIds() {
        return jedis.smembers(keys.queues).stream()
                .map(Integer::valueOf)
                .sorted()
                .toList();
    }

    /**
     * Lists from the structure that holds the state: {@code idx:<state>:<qid>} for LEASED and the terminal states,
     * {@code q:<qid>} for QUEUED and the global {@code scheduled} zset for SCHEDULED (filtered by queue in Java).
     * Order is by the zset score, then numeric id: stateChanged for the indexed states, due for QUEUED and
     * SCHEDULED. The cursor is the score read from the {@code afterId} task, so each page costs a seek plus at
     * most one chunk of 500 per zset; when that task no longer exists, ids after it are listed from the start.
     */
    @Override
    public List<TaskRecord> tasks(TaskState state, Integer queueId, long afterId, int limit) {
        if (limit < 1) {
            return List.of();
        }
        return guard(() -> {
            var cursor = afterId > 0 ? cursorScore(state, afterId) : null;
            var entries = new ArrayList<Entry>();
            if (state == TaskState.SCHEDULED) {
                entries.addAll(
                        page(keys.scheduled, cursor, afterId, limit, id -> queueId == null || queueOf(id) == queueId));
            } else {
                var qids = queueId == null ? sortedQueueIds() : List.of(queueId);
                for (var qid : qids) {
                    var zset = state == TaskState.QUEUED ? keys.queue(qid) : keys.index(state.code(), qid);
                    entries.addAll(page(zset, cursor, afterId, limit, id -> true));
                }
            }
            entries.sort(ORDER);
            var out = new ArrayList<TaskRecord>();
            for (var e : entries.subList(0, Math.min(limit, entries.size()))) {
                getTask(e.id()).ifPresent(out::add);
            }
            return out;
        });
    }

    private record Entry(double score, long id) {}

    private static final Comparator<Entry> ORDER =
            Comparator.comparingDouble(Entry::score).thenComparingLong(Entry::id);

    private Double cursorScore(TaskState state, long afterId) {
        var v = jedis.hmget(
                keys.task(afterId),
                F_STATE,
                state == TaskState.QUEUED || state == TaskState.SCHEDULED ? F_DUE : F_STATE_CHANGED);
        return v.get(0) != null && v.get(1) != null && Integer.parseInt(v.get(0)) == state.code()
                ? Double.valueOf(v.get(1))
                : null;
    }

    private int queueOf(long id) {
        var q = jedis.hget(keys.task(id), F_QUEUE);
        return q == null ? -1 : Integer.parseInt(q);
    }

    /**
     * Reads one zset in (score, numeric id) order after the cursor. Redis breaks score ties by member bytes, so a
     * chunk's trailing score group is re-read whole before it is ordered by number.
     */
    private List<Entry> page(
            String zset, Double cursor, long afterId, int limit, java.util.function.LongPredicate accept) {
        var out = new ArrayList<Entry>();
        var min = cursor == null ? "-inf" : String.valueOf((long) cursor.doubleValue());
        while (out.size() < limit) {
            var chunk = jedis.zrangeByScoreWithScores(zset, min, "+inf", 0, PAGE_CHUNK);
            if (chunk.isEmpty()) {
                break;
            }
            var full = chunk.size() == PAGE_CHUNK;
            var lastScore = chunk.getLast().getScore();
            String nextMin = null;
            if (full) {
                var kept = chunk.stream().filter(t -> t.getScore() < lastScore).toList();
                if (kept.isEmpty()) {
                    var group = (long) lastScore;
                    chunk = jedis.zrangeByScoreWithScores(
                            zset, String.valueOf(group), String.valueOf(group), 0, Integer.MAX_VALUE);
                    nextMin = "(" + group;
                } else {
                    chunk = kept;
                    nextMin = String.valueOf((long) lastScore);
                }
            }
            var ordered = new ArrayList<Entry>(chunk.size());
            for (var t : chunk) {
                ordered.add(new Entry(t.getScore(), Long.parseLong(t.getElement())));
            }
            ordered.sort(ORDER);
            for (var e : ordered) {
                var after = cursor == null ? e.id() > afterId : ORDER.compare(e, new Entry(cursor, afterId)) > 0;
                if (after && accept.test(e.id())) {
                    out.add(e);
                }
            }
            if (nextMin == null) {
                break;
            }
            min = nextMin;
        }
        return out;
    }

    private static Map<String, String> toMap(List<Object> flat, int from) {
        var map = new HashMap<String, String>();
        for (var i = from; i + 1 < flat.size(); i += 2) {
            map.put((String) flat.get(i), (String) flat.get(i + 1));
        }
        return map;
    }

    // ---- wave 2: cancel, expiry, admin, workers ------------------------------------------------------

    private static String reason(String reason) {
        return reason == null ? "" : reason;
    }

    @Override
    public boolean finishCancelled(long id, int revision, String reason, boolean keepPayload) {
        var args = List.of(String.valueOf(id), String.valueOf(revision), reason(reason), keepPayload ? "1" : "0");
        return num(guard(() -> scripts.run(RedisScripts.FINISH_CANCELLED, args))) == 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Long> expire(int limit) {
        var r = (List<Object>) guard(() -> scripts.run(RedisScripts.EXPIRE, List.of(String.valueOf(limit))));
        return r.stream().map(o -> Long.parseLong((String) o)).toList();
    }

    @Override
    public int purgeWorkers(Duration staleAfter) {
        var args = List.of(String.valueOf(staleAfter.toMillis()));
        return (int) num(guard(() -> scripts.run(RedisScripts.PURGE_WORKERS, args)));
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason) {
        var args = List.of(String.valueOf(id), String.valueOf(revision), reason(reason));
        return CancelOutcome.valueOf((String) guard(() -> scripts.run(RedisScripts.CANCEL, args)));
    }

    @Override
    public boolean retry(long id, int revision) {
        var args = List.of(String.valueOf(id), String.valueOf(revision));
        return num(guard(() -> scripts.run(RedisScripts.RETRY, args))) == 1;
    }

    @Override
    public boolean delete(long id, int revision) {
        var args = List.of(String.valueOf(id), String.valueOf(revision));
        return num(guard(() -> scripts.run(RedisScripts.DELETE, args))) == 1;
    }

    @Override
    public void pauseQueue(int queueId, boolean paused) {
        guard(() -> {
            if (paused) {
                jedis.sadd(keys.paused, String.valueOf(queueId));
            } else {
                jedis.srem(keys.paused, String.valueOf(queueId));
            }
        });
    }

    @Override
    public List<QueueInfo> queues() {
        return guard(() -> {
            var paused = jedis.smembers(keys.paused);
            return names("queue").entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> new QueueInfo(e.getKey(), e.getValue(), paused.contains(String.valueOf(e.getKey()))))
                    .toList();
        });
    }

    @Override
    public void heartbeat(int workerId, int running) {
        var args = List.of(String.valueOf(workerId), String.valueOf(running));
        guard(() -> scripts.run(RedisScripts.HEARTBEAT, args));
    }

    @Override
    public List<WorkerInfo> workers() {
        return guard(() -> {
            var ids = jedis.zrange(keys.workers, 0, -1).stream()
                    .map(Integer::valueOf)
                    .sorted()
                    .toList();
            var hashes = new ArrayList<Response<Map<String, String>>>(ids.size());
            try (var pipeline = jedis.pipelined()) {
                ids.forEach(id -> hashes.add(pipeline.hgetAll(keys.worker(id))));
                pipeline.sync();
            }
            var out = new ArrayList<WorkerInfo>(ids.size());
            for (var i = 0; i < ids.size(); i++) {
                var h = hashes.get(i).get();
                if (h.isEmpty()) {
                    continue;
                }
                out.add(new WorkerInfo(
                        ids.get(i),
                        h.getOrDefault(F_LABEL, ""),
                        Instant.ofEpochMilli(Long.parseLong(h.get(F_STARTED))),
                        Instant.ofEpochMilli(Long.parseLong(h.get(F_SEEN))),
                        Integer.parseInt(h.getOrDefault(F_RUNNING, "0"))));
            }
            return out;
        });
    }

    // ---- wave 2: schedule controls -------------------------------------------------------------------

    @Override
    public List<RecurringSchedule> recurringDefinitions() {
        return guard(() -> {
            var ids = jedis.smembers(keys.cronDefinitions).stream().sorted().toList();
            var hashes = new ArrayList<Response<Map<String, String>>>(ids.size());
            try (var pipeline = jedis.pipelined()) {
                ids.forEach(id -> hashes.add(pipeline.hgetAll(keys.cronDefinition(id))));
                pipeline.sync();
            }
            var out = new ArrayList<RecurringSchedule>(ids.size());
            for (var i = 0; i < ids.size(); i++) {
                var h = hashes.get(i).get();
                if (!h.isEmpty()) {
                    out.add(new RecurringSchedule(
                            definition(ids.get(i), h), Instant.ofEpochMilli(Long.parseLong(h.get(F_NEXT)))));
                }
            }
            return out;
        });
    }

    @Override
    public boolean pauseRecurring(String scheduleId, boolean paused) {
        var args = List.of(scheduleId, paused ? "1" : "0");
        return num(guard(() -> scripts.run(RedisScripts.CRON_PAUSE, args))) == 1;
    }

    @Override
    public boolean setNextFire(String scheduleId, Instant nextFire) {
        var args = List.of(scheduleId, ms(nextFire));
        return num(guard(() -> scripts.run(RedisScripts.CRON_SET_NEXT, args))) == 1;
    }
}

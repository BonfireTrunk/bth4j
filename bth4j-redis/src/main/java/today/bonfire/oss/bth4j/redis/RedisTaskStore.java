package today.bonfire.oss.bth4j.redis;

import static today.bonfire.oss.bth4j.redis.Keys.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.resps.Tuple;
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
import today.bonfire.oss.bth4j.spi.RetryOutcome;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.StoreInfo;
import today.bonfire.oss.bth4j.spi.TaskPage;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/**
 * {@link FullStore} on Redis 7.4 or newer and Valkey 9 or newer (hash field expiry is required; tested on
 * Redis 8 and Valkey 9): one hash per task with the payload, steps and failures in keys of their own, sorted
 * sets for the queues and the state indexes, and one Lua script per transition so that every check and its
 * writes run as one unit (see {@link Keys} for the layout and {@link RedisScripts} for the scripts). All keys
 * carry the namespace as a hash tag, so the store also works on Redis Cluster. Runners that share a namespace
 * share the work.
 */
public class RedisTaskStore implements FullStore {
    /**
     * The key layout this class writes, stored in the namespace's {@code meta} hash. {@link #init} refuses a
     * namespace written with another one; there is no migration between layouts.
     */
    public static final int LAYOUT_VERSION = 2;

    private static final int PAGE_CHUNK = 500;

    /** Leases per RENEW call, so that one call blocks Redis for well under a millisecond or two. */
    private static final int RENEW_CHUNK = 256;

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
        return new StoreInfo("redis", LAYOUT_VERSION);
    }

    @Override
    public void init() {
        guard(() -> {
            var wanted = String.valueOf(LAYOUT_VERSION);
            jedis.hsetnx(keys.meta, SCHEMA, wanted);
            var found = jedis.hget(keys.meta, SCHEMA);
            if (!wanted.equals(found)) {
                throw new TaskStoreException(
                        "Redis namespace " + keys.namespace + " has layout version " + found + ", expected " + wanted);
            }
        });
    }

    @Override
    public long idHighWaterMark() {
        var last = guard(() -> jedis.get(keys.seqId));
        return last == null ? 1 : Long.parseLong(last) + 1;
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
        var raw = guard(() -> jedis.hgetAll(keys.registry(kind)));
        var out = new TreeMap<Integer, String>();
        raw.forEach((name, id) -> out.put(Integer.valueOf(id), name));
        return out;
    }

    /** Redis server time in ms, so that every comparison with a score written by a script uses the same clock. */
    private long serverNow() {
        @SuppressWarnings("unchecked")
        var t = (List<byte[]>) jedis.sendCommand(keys.meta, Protocol.Command.TIME);
        return Long.parseLong(new String(t.get(0), StandardCharsets.US_ASCII)) * 1000
                + Long.parseLong(new String(t.get(1), StandardCharsets.US_ASCII)) / 1000;
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

    static List<String> enqueueArgs(TaskRecord t) {
        var args = new ArrayList<String>();
        args.add(String.valueOf(t.id()));
        args.add(t.due() == null ? "" : ms(t.due()));
        args.add(t.uniqueKey() == null ? "" : t.uniqueKey());
        args.addAll(storeArgs(t));
        args.add(Keys.F_WRITE_TOKEN);
        // decimal, so the hash listpack stores it as an integer
        args.add(Long.toString(ThreadLocalRandom.current().nextLong()));
        return args;
    }

    /** queue id, start-by ms or '', payload flag and payload, then the hash fields: what the script's store needs after the key */
    private static List<String> storeArgs(TaskRecord t) {
        var args = new ArrayList<String>();
        args.add(String.valueOf(t.queueId()));
        args.add(t.startBy() == null ? "" : ms(t.startBy()));
        args.add(t.data() == null ? "" : "1");
        args.add(t.data() == null ? "" : t.data());
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

    /**
     * The script returns the hash fields as they were before the claim and the time it used; the fields it
     * wrote (state, stateChanged, revision, attempts, claimedAt, lease, worker) are filled in here.
     */
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
        var at = num(r.get(0));
        var lease = String.valueOf(at + request.lease().toMillis());
        var tasks = new ArrayList<TaskRecord>(r.size() - 1);
        for (var entry : r.subList(1, r.size())) {
            var e = (List<Object>) entry;
            var fields = RedisScripts.CLAIM_FIELDS;
            var h = new HashMap<String, String>();
            for (var i = 0; i < fields.length; i++) {
                var v = e.get(4 + i);
                if (v != null) h.put(fields[i], (String) v);
            }
            h.put(F_STATE, String.valueOf(TaskState.LEASED.code()));
            h.put(F_STATE_CHANGED, String.valueOf(at));
            h.put(F_REVISION, String.valueOf(Integer.parseInt(h.get(F_REVISION)) + 1));
            h.put(F_ATTEMPTS, String.valueOf(Integer.parseInt(h.getOrDefault(F_ATTEMPTS, "0")) + 1));
            h.put(F_CLAIMED_AT, String.valueOf(at));
            h.put(F_LEASE, lease);
            h.put(F_WORKER, String.valueOf(request.workerId()));
            tasks.add(TaskHashes.fromHash(
                    Long.parseLong((String) e.get(0)), h, (String) e.get(1), (String) e.get(2), (List<String>)
                            (List<?>) e.get(3)));
        }
        return tasks;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<LeaseStatus> renew(List<LeaseRef> leases, Duration lease) {
        if (leases.isEmpty()) {
            return List.of();
        }
        var calls = new ArrayList<List<String>>();
        for (var from = 0; from < leases.size(); from += RENEW_CHUNK) {
            var args = new ArrayList<String>();
            args.add(String.valueOf(lease.toMillis()));
            for (var l : leases.subList(from, Math.min(from + RENEW_CHUNK, leases.size()))) {
                args.add(String.valueOf(l.id()));
                args.add(String.valueOf(l.revision()));
            }
            calls.add(args);
        }
        var replies = guard(() -> scripts.runAll(RedisScripts.RENEW, calls));
        var out = new ArrayList<LeaseStatus>(leases.size());
        for (var c = 0; c < replies.size(); c++) {
            var r = (List<Object>) replies.get(c);
            for (var i = 0; i < r.size(); i++) {
                var ref = leases.get(c * RENEW_CHUNK + i);
                var code = num(r.get(i));
                out.add(new LeaseStatus(ref.id(), code > 0, code == 2));
            }
        }
        return out;
    }

    @Override
    public boolean ack(long id, int revision, boolean keep, boolean keepPayload, ScheduleEffect effect) {
        var args = new ArrayList<>(
                List.of(String.valueOf(id), String.valueOf(revision), keep ? "1" : "0", keepPayload ? "1" : "0"));
        args.addAll(effectArgs(effect));
        return applied(effect, num(guard(() -> scripts.run(RedisScripts.ACK, args))));
    }

    /** Scripts answer 1 for a done transition and 2 when the schedule effect changed the definition too. */
    private static boolean applied(ScheduleEffect effect, long code) {
        if (code == 2 && effect != null) effect.markApplied();
        return code >= 1;
    }

    private static List<String> effectArgs(ScheduleEffect effect) {
        if (effect == null) return List.of("", "", "");
        return List.of(
                effect.scheduleId(),
                String.valueOf(effect.occurrenceId()),
                effect.pauses() ? "" : ms(effect.nextFire()));
    }

    @Override
    public FailOutcome fail(
            long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead, ScheduleEffect effect) {
        return fail(id, revision, failure, retryAt, keepDead, false, effect);
    }

    @Override
    public FailOutcome failExpired(
            long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead, ScheduleEffect effect) {
        return fail(id, revision, failure, retryAt, keepDead, true, effect);
    }

    private FailOutcome fail(
            long id,
            int revision,
            FailureInfo failure,
            Instant retryAt,
            boolean keepDead,
            boolean expiredOnly,
            ScheduleEffect effect) {
        var args = new ArrayList<>(List.of(
                String.valueOf(id),
                String.valueOf(revision),
                retryAt == null ? "" : ms(retryAt),
                keepDead ? "1" : "0",
                expiredOnly ? "1" : "0",
                RecordJson.encodeFailure(failure)));
        args.addAll(effectArgs(effect));
        var r = (String) guard(() -> scripts.run(RedisScripts.FAIL, args));
        if (r.endsWith("+")) {
            effect.markApplied();
            r = r.substring(0, r.length() - 1);
        }
        return FailOutcome.valueOf(r);
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
        return write(id, revision, "steps", RecordJson.encodeSteps(steps));
    }

    @Override
    public boolean progress(long id, int revision, String progress) {
        return write(id, revision, F_PROGRESS, progress);
    }

    private boolean write(long id, int revision, String target, String value) {
        var args = List.of(String.valueOf(id), String.valueOf(revision), target, value == null ? "" : value);
        return num(guard(() -> scripts.run(RedisScripts.WRITE, args))) == 1;
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
        return guard(() -> {
            var ids = jedis.zrangeByScore(keys.leased, "-inf", String.valueOf(serverNow()), 0, limit);
            var fields = new ArrayList<Response<List<String>>>(ids.size());
            try (var pipeline = jedis.pipelined()) {
                ids.forEach(id -> fields.add(pipeline.hmget(
                        keys.task(Long.parseLong(id)), F_REVISION, F_ATTEMPTS, F_MAX_ATTEMPTS, F_EVENT, F_CANCEL_AT)));
                pipeline.sync();
            }
            var out = new ArrayList<ExpiredLease>(ids.size());
            var orphans = new ArrayList<String>();
            for (var i = 0; i < ids.size(); i++) {
                var v = fields.get(i).get();
                if (v.get(0) == null) {
                    orphans.add(ids.get(i));
                    continue;
                }
                out.add(new ExpiredLease(
                        Long.parseLong(ids.get(i)),
                        Integer.parseInt(v.get(0)),
                        Integer.parseInt(v.get(1)),
                        Integer.parseInt(v.get(2)),
                        Integer.parseInt(v.get(3)),
                        v.get(4) != null));
            }
            // a leased entry whose hash is gone cannot be reclaimed; failExpired would find no task anyway
            if (!orphans.isEmpty()) jedis.zrem(keys.leased, orphans.toArray(String[]::new));
            return out;
        });
    }

    @Override
    public long purge(TaskState state, Duration retention, int limit) {
        if (!state.isTerminal()) {
            throw new IllegalArgumentException("purge needs a terminal state, got " + state);
        }
        var args = List.of(String.valueOf(state.code()), String.valueOf(retention.toMillis()), String.valueOf(limit));
        return num(guard(() -> scripts.run(RedisScripts.PURGE, args)));
    }

    /** The key fields of a finished task expire with the key itself (hash field expiry), so there is nothing to do. */
    @Override
    public int releaseKeys(int limit) {
        return 0;
    }

    @Override
    public Instant nextDue() {
        return guard(() -> {
            var heads = new ArrayList<Response<List<Tuple>>>();
            try (var pipeline = jedis.pipelined()) {
                for (var qid : sortedQueueIds()) {
                    heads.add(pipeline.zrangeWithScores(keys.scheduled(qid), 0, 0));
                }
                pipeline.sync();
            }
            Instant next = null;
            for (var head : heads) {
                var h = head.get();
                if (!h.isEmpty()) {
                    var due = Instant.ofEpochMilli((long) h.getFirst().getScore());
                    if (next == null || due.isBefore(next)) next = due;
                }
            }
            return next;
        });
    }

    // ---- schedules -----------------------------------------------------------------------------------

    @Override
    public void putRecurring(RecurringDefinition d, Instant nextFire, Instant keepUntil) {
        var args = new ArrayList<String>();
        args.add(d.scheduleId());
        args.add(ms(nextFire));
        args.add(ms(keepUntil));
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
        return guard(() -> {
            var due = jedis.zrangeByScoreWithScores(keys.cron, "-inf", ms(upTo), 0, limit);
            var hashes = new ArrayList<Response<Map<String, String>>>(due.size());
            try (var pipeline = jedis.pipelined()) {
                due.forEach(t -> hashes.add(pipeline.hgetAll(keys.cronDefinition(t.getElement()))));
                pipeline.sync();
            }
            var out = new ArrayList<DueSchedule>(due.size());
            for (var i = 0; i < due.size(); i++) {
                var h = hashes.get(i).get();
                // deleted between the two reads: CRON_DELETE removed the hash, the zset entry went with it
                if (h.isEmpty()) continue;
                var id = due.get(i).getElement();
                out.add(new DueSchedule(
                        id, Instant.ofEpochMilli((long) due.get(i).getScore()), definition(id, h)));
            }
            return out;
        });
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
        args.addAll(storeArgs(occurrence));
        return num(guard(() -> scripts.run(RedisScripts.CRON_COMMIT, args))) == 1;
    }

    // ---- inspection ----------------------------------------------------------------------------------

    @Override
    public Optional<TaskRecord> getTask(long id) {
        var found = load(List.of(id));
        return found.isEmpty() ? Optional.empty() : Optional.of(found.getFirst());
    }

    @Override
    public Optional<TaskRecord> taskByKey(String key) {
        String named = null;
        for (var attempt = 0; attempt < 2; attempt++) {
            var holder = guard(() -> jedis.get(keys.uniqueKey(key)));
            if (holder == null) return Optional.empty();
            if (holder.equals(named)) return Optional.empty();
            named = holder;
            var record = getTask(Long.parseLong(holder));
            if (record.isPresent() && holdsKey(record.get(), key)) return record;
        }
        return Optional.empty();
    }

    /** The two reads of {@link #taskByKey} are not atomic: the record may have lost the key to a newer task since. */
    private static boolean holdsKey(TaskRecord record, String key) {
        return key.equals(record.uniqueKey()) && (!record.state().isTerminal() || record.keyUntil() != null);
    }

    /** Reads the hash, payload, steps and failures of every id in one pipeline; ids without a hash are left out. */
    private List<TaskRecord> load(List<Long> ids) {
        return guard(() -> {
            var hashes = new ArrayList<Response<Map<String, String>>>(ids.size());
            var data = new ArrayList<Response<String>>(ids.size());
            var steps = new ArrayList<Response<String>>(ids.size());
            var failures = new ArrayList<Response<List<String>>>(ids.size());
            try (var pipeline = jedis.pipelined()) {
                for (var id : ids) {
                    hashes.add(pipeline.hgetAll(keys.task(id)));
                    data.add(pipeline.get(keys.data(id)));
                    steps.add(pipeline.get(keys.steps(id)));
                    failures.add(pipeline.lrange(keys.failures(id), 0, -1));
                }
                pipeline.sync();
            }
            var out = new ArrayList<TaskRecord>(ids.size());
            for (var i = 0; i < ids.size(); i++) {
                var h = hashes.get(i).get();
                if (h.isEmpty()) continue;
                out.add(TaskHashes.fromHash(
                        ids.get(i),
                        h,
                        data.get(i).get(),
                        steps.get(i).get(),
                        failures.get(i).get()));
            }
            return out;
        });
    }

    /** One ZCARD per state and queue, pipelined: the zsets are the counts, so they cannot drift. */
    @Override
    public Map<Integer, StateCounts> counts() {
        return guard(() -> {
            var qids = sortedQueueIds();
            var cards = new ArrayList<Response<Long>>(qids.size() * 7);
            try (var pipeline = jedis.pipelined()) {
                for (var qid : qids) {
                    cards.add(pipeline.zcard(keys.queue(qid)));
                    cards.add(pipeline.zcard(keys.scheduled(qid)));
                    for (var state = 2; state <= 6; state++) {
                        cards.add(pipeline.zcard(keys.index(state, qid)));
                    }
                }
                pipeline.sync();
            }
            var out = new LinkedHashMap<Integer, StateCounts>();
            for (var i = 0; i < qids.size(); i++) {
                var c = cards.subList(i * 7, i * 7 + 7);
                out.put(
                        qids.get(i),
                        new StateCounts(
                                c.get(0).get(),
                                c.get(1).get(),
                                c.get(2).get(),
                                c.get(3).get(),
                                c.get(4).get(),
                                c.get(5).get(),
                                c.get(6).get()));
            }
            return out;
        });
    }

    private List<Integer> sortedQueueIds() {
        return jedis.hvals(keys.registry("queue")).stream()
                .map(Integer::valueOf)
                .sorted()
                .toList();
    }

    /**
     * Lists from the structure that holds the state: {@code idx:<state>:<qid>} for LEASED and the terminal states,
     * {@code q:<qid>} for QUEUED and {@code sched:<qid>} for SCHEDULED. Order is by the zset score, then numeric
     * id: stateChanged for the indexed states, due for QUEUED and SCHEDULED. Best effort for {@code afterId}: the
     * cursor is the score the {@code afterId} task has in the zset being paged (not its {@code due} field, which a deferral leaves
     * behind), so each page costs a seek plus at most one chunk of 500 per zset; when that task is no longer in
     * that state, ids after it are listed from the start, which can repeat tasks. The {@code String} cursor form
     * carries the score itself and has no such case.
     */
    @Override
    public List<TaskRecord> tasks(TaskState state, Integer queueId, long afterId, int limit) {
        if (limit < 1) {
            return List.of();
        }
        return guard(() -> {
            var cursor = afterId > 0 ? cursorScore(state, afterId) : null;
            return load(listing(state, queueId, cursor, afterId, limit).stream()
                    .map(Entry::id)
                    .toList());
        });
    }

    /** The cursor is {@code <score>:<id>} of the last entry of the page, the score it has in the zset being paged. */
    @Override
    public TaskPage tasks(TaskState state, Integer queueId, String cursor, int limit) {
        var position = cursor == null ? null : decodeCursor(cursor);
        if (limit < 1) {
            return new TaskPage(List.of(), null);
        }
        return guard(() -> {
            var probe = (int) Math.min((long) limit + 1, Integer.MAX_VALUE);
            var found = position == null
                    ? listing(state, queueId, null, 0, probe)
                    : listing(state, queueId, position.score(), position.id(), probe);
            var entries = found.subList(0, Math.min(limit, found.size()));
            var items = load(entries.stream().map(Entry::id).toList());
            var last = entries.isEmpty() ? null : entries.getLast();
            var next = found.size() > limit ? (long) last.score() + ":" + last.id() : null;
            return new TaskPage(items, next);
        });
    }

    private static Entry decodeCursor(String cursor) {
        var colon = cursor.indexOf(':');
        try {
            if (colon > 0) {
                return new Entry(
                        Long.parseLong(cursor.substring(0, colon)), Long.parseLong(cursor.substring(colon + 1)));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("malformed cursor");
        }
        throw new IllegalArgumentException("malformed cursor");
    }

    private List<Entry> listing(TaskState state, Integer queueId, Double cursor, long afterId, int limit) {
        var entries = new ArrayList<Entry>();
        var qids = queueId == null ? sortedQueueIds() : List.of(queueId);
        for (var qid : qids) {
            entries.addAll(page(stateZset(state, qid), cursor, afterId, limit));
        }
        entries.sort(ORDER);
        return entries.subList(0, Math.min(limit, entries.size()));
    }

    private record Entry(double score, long id) {}

    private static final Comparator<Entry> ORDER =
            Comparator.comparingDouble(Entry::score).thenComparingLong(Entry::id);

    private String stateZset(TaskState state, int qid) {
        return switch (state) {
            case QUEUED -> keys.queue(qid);
            case SCHEDULED -> keys.scheduled(qid);
            default -> keys.index(state.code(), qid);
        };
    }

    private Double cursorScore(TaskState state, long afterId) {
        var v = jedis.hmget(keys.task(afterId), F_STATE, F_QUEUE);
        if (v.get(0) == null || v.get(1) == null || Integer.parseInt(v.get(0)) != state.code()) {
            return null;
        }
        return jedis.zscore(stateZset(state, Integer.parseInt(v.get(1))), String.valueOf(afterId));
    }

    /**
     * Reads one zset in (score, numeric id) order after the cursor. Redis breaks score ties by member bytes, so a
     * chunk's trailing score group is re-read whole before it is ordered by number.
     */
    private List<Entry> page(String zset, Double cursor, long afterId, int limit) {
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
                if (after) {
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

    // ---- wave 2: cancel, expiry, admin, workers ------------------------------------------------------

    private static String reason(String reason) {
        return reason == null ? "" : reason;
    }

    @Override
    public boolean finishCancelled(long id, int revision, String reason, boolean keepPayload, ScheduleEffect effect) {
        return finishCancelled(id, revision, reason, keepPayload, false, effect);
    }

    @Override
    public boolean finishCancelledExpired(
            long id, int revision, String reason, boolean keepPayload, ScheduleEffect effect) {
        return finishCancelled(id, revision, reason, keepPayload, true, effect);
    }

    private boolean finishCancelled(
            long id, int revision, String reason, boolean keepPayload, boolean expiredOnly, ScheduleEffect effect) {
        var args = new ArrayList<>(List.of(
                String.valueOf(id),
                String.valueOf(revision),
                reason(reason),
                keepPayload ? "1" : "0",
                expiredOnly ? "1" : "0"));
        args.addAll(effectArgs(effect));
        return applied(effect, num(guard(() -> scripts.run(RedisScripts.FINISH_CANCELLED, args))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Long> expire(int limit) {
        var r = (List<Object>) guard(() -> scripts.run(RedisScripts.EXPIRE, List.of(String.valueOf(limit))));
        return r.stream().map(o -> Long.parseLong((String) o)).toList();
    }

    @Override
    public int purgeWorkers(Duration staleAfter) {
        var args = List.of(String.valueOf(staleAfter.toMillis()), String.valueOf(RedisScripts.PURGE_WORKERS_PAGE));
        var total = 0;
        int page;
        do {
            page = (int) num(guard(() -> scripts.run(RedisScripts.PURGE_WORKERS, args)));
            total += page;
        } while (page == RedisScripts.PURGE_WORKERS_PAGE);
        return total;
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason, ScheduleEffect effect) {
        var args = new ArrayList<>(List.of(String.valueOf(id), String.valueOf(revision), reason(reason)));
        args.addAll(effectArgs(effect));
        var r = (String) guard(() -> scripts.run(RedisScripts.CANCEL, args));
        if (r.endsWith("+")) {
            effect.markApplied();
            r = r.substring(0, r.length() - 1);
        }
        return CancelOutcome.valueOf(r);
    }

    @Override
    public RetryOutcome retry(long id, int revision) {
        var args = List.of(String.valueOf(id), String.valueOf(revision));
        return switch ((int) num(guard(() -> scripts.run(RedisScripts.RETRY, args)))) {
            case 1 -> RetryOutcome.RETRIED;
            case 2 -> RetryOutcome.DUPLICATE;
            default -> RetryOutcome.STALE;
        };
    }

    @Override
    public boolean delete(long id, int revision, ScheduleEffect effect) {
        var args = new ArrayList<>(List.of(String.valueOf(id), String.valueOf(revision)));
        args.addAll(effectArgs(effect));
        return applied(effect, num(guard(() -> scripts.run(RedisScripts.DELETE, args))));
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

    /**
     * ZADD XX only touches a registered worker, so one that purgeWorkers removed is not brought back. CH reports
     * 0 both for an unknown worker and for one whose score did not change (two heartbeats in the same ms), so
     * a 0 is settled with ZSCORE.
     */
    @Override
    public void heartbeat(int workerId, int running) {
        guard(() -> {
            var at = serverNow();
            var id = String.valueOf(workerId);
            if (jedis.zadd(keys.workers, at, id, ZAddParams.zAddParams().xx().ch()) == 0
                    && jedis.zscore(keys.workers, id) == null) {
                return;
            }
            jedis.hset(keys.worker(workerId), Map.of(F_SEEN, String.valueOf(at), F_RUNNING, String.valueOf(running)));
        });
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

    @Override
    public boolean setNextFire(String scheduleId, long revision, Instant expectedFire, Instant nextFire) {
        var args = List.of(scheduleId, String.valueOf(revision), ms(expectedFire), ms(nextFire));
        return num(guard(() -> scripts.run(RedisScripts.CRON_ADVANCE, args))) == 1;
    }
}

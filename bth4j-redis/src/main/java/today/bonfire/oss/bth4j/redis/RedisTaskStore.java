package today.bonfire.oss.bth4j.redis;

import static today.bonfire.oss.bth4j.redis.Keys.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.resps.Tuple;
import today.bonfire.oss.bth4j.CancelOutcome;
import today.bonfire.oss.bth4j.DeleteOutcome;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.Loggers;
import today.bonfire.oss.bth4j.PutScheduleOutcome;
import today.bonfire.oss.bth4j.PutScheduleResult;
import today.bonfire.oss.bth4j.QueueInfo;
import today.bonfire.oss.bth4j.RetryOutcome;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.internal.Json;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.CommitOutcome;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.ExpiredTask;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.LeaseStatus;
import today.bonfire.oss.bth4j.spi.PromoteResult;
import today.bonfire.oss.bth4j.spi.RecordJson;
import today.bonfire.oss.bth4j.spi.RunnerRecord;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.ScheduleEntry;
import today.bonfire.oss.bth4j.spi.ScheduleStore;
import today.bonfire.oss.bth4j.spi.StoreArguments;
import today.bonfire.oss.bth4j.spi.StoreInfo;
import today.bonfire.oss.bth4j.spi.StoredSchedule;
import today.bonfire.oss.bth4j.spi.TaskPage;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * {@link FullStore} on Redis 7.4 or newer and Valkey 9 or newer (hash field expiry is required; tested on
 * Redis 8 and Valkey 9): one hash per task with the payload, steps and failures in keys of their own, sorted
 * sets for the queues and the state indexes, and one Lua script per transition so that every check and its
 * writes run as one unit (see {@link Keys} for the layout and {@link RedisScripts} for the scripts). All keys
 * carry the namespace as a hash tag, so a namespace lies in one slot of a Redis Cluster; the store is not
 * tested on a cluster, and the scripts derive their keys, so that slot must not be resharded while runners use
 * it. Runners that share a namespace share the work.
 */
public final class RedisTaskStore implements FullStore {
    private static final Logger log = LoggerFactory.getLogger(Loggers.REDIS_STORE);

    /**
     * The version of the keys this class writes, stored in the namespace's {@code meta} hash. {@link #init}
     * refuses a namespace with any other version.
     */
    public static final int SCHEMA_VERSION = 6;

    private static final int PAGE_CHUNK = 500;

    /** Leases per RENEW or ACK_ALL call, so that one call blocks Redis for well under a millisecond or two. */
    private static final int LEASE_CHUNK = 256;

    /**
     * Tasks per ENQUEUE_ALL call, and the argument characters after which a call takes no more tasks, so that
     * one call blocks Redis only briefly even with large payloads.
     */
    static final int ENQUEUE_CHUNK = 64;

    private static final int ENQUEUE_CHUNK_CHARS = 256 * 1024;

    private final UnifiedJedis jedis;
    private final Keys keys;
    private final ScriptRunner scripts;
    private final Map<String, Integer> queueIds = new ConcurrentHashMap<>();
    private final Map<String, Integer> typeIds = new ConcurrentHashMap<>();

    /** The last known type set a claim named; a runner passes the same set instance on every claim. */
    private volatile TypeSet lastTypeSet;

    private record TypeSet(Set<Integer> ids, String digest) {}

    /**
     * @param jedis     client used for all Redis operations, for example a {@code RedisClient}. The store does not
     *                  close it.
     * @param namespace prefix of all keys; runners with the same namespace share the work. Must not be blank or
     *                  contain braces.
     * @throws TaskConfigurationException when the client is null or the namespace is not allowed
     */
    public RedisTaskStore(UnifiedJedis jedis, String namespace) {
        if (jedis == null) {
            throw new TaskConfigurationException("Jedis client is required");
        }
        if (namespace == null || namespace.isBlank() || namespace.contains("{") || namespace.contains("}")) {
            throw new TaskConfigurationException("namespace cannot be blank or contain braces");
        }
        this.jedis = jedis;
        this.keys = new Keys(namespace);
        this.scripts = new ScriptRunner(jedis, keys.meta, keys.prefix);
    }

    Keys keys() {
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
        return new StoreInfo("redis", SCHEMA_VERSION);
    }

    /**
     * Checks that the server evicts no keys and has hash field expiry, then writes the schema version of a new
     * namespace and checks it on an existing one. The eviction policy comes from {@code INFO memory}; when the
     * server refuses that command (an ACL without {@code +info}, a renamed or disabled command) or its reply has no
     * {@code maxmemory_policy}, init logs a warning to {@link Loggers#REDIS_STORE} and goes on without the check.
     *
     * @throws TaskConfigurationException when the server's {@code maxmemory-policy} is not {@code noeviction}, or
     *                                    the server does not know {@code HPEXPIREAT}
     * @throws TaskStoreException         when the namespace has a schema version other than {@value #SCHEMA_VERSION}, or
     *                                    holds keys of the store without a schema version
     */
    @Override
    public void init() {
        checkEvictionPolicy();
        checkHashFieldExpiry();
        var version = String.valueOf(SCHEMA_VERSION);
        var found = (String) guard(() -> scripts.run(RedisScripts.INIT, List.of(version)));
        if (found.isEmpty()) {
            throw new TaskStoreException("Redis namespace " + keys.namespace
                    + " holds keys without a schema version, this library needs schema version " + SCHEMA_VERSION);
        }
        if (!version.equals(found)) {
            throw new TaskStoreException("Redis namespace " + keys.namespace + " has schema version " + found
                    + ", expected " + SCHEMA_VERSION);
        }
    }

    private void checkEvictionPolicy() {
        String memory;
        try {
            memory = jedis.info("memory");
        } catch (JedisDataException e) {
            log.warn(
                    "Redis refused INFO memory ({}), so init did not check that maxmemory-policy is noeviction;"
                            + " under any other policy Redis can evict task data and unique keys",
                    e.getMessage());
            return;
        } catch (JedisException e) {
            throw new TaskStoreException("Redis call failed: " + e.getMessage(), e);
        }
        var policy = infoField(memory, "maxmemory_policy");
        if (policy == null) {
            log.warn("Redis INFO memory has no maxmemory_policy, so init did not check that it is noeviction;"
                    + " under any other policy Redis can evict task data and unique keys");
            return;
        }
        if (!"noeviction".equals(policy)) {
            throw new TaskConfigurationException("the Redis maxmemory-policy is " + policy
                    + " and this store needs noeviction: under any other policy Redis can evict task data and the"
                    + " keys that hold unique-key windows when it reaches maxmemory. Set it in redis.conf, with"
                    + " CONFIG SET maxmemory-policy noeviction, or in the eviction setting of a managed database");
        }
    }

    /**
     * The scripts end a kept key window with {@code HPEXPIREAT}, after they wrote the task. A server without the
     * command would fail them there and leave the transition half applied, so it is refused here.
     */
    private void checkHashFieldExpiry() {
        try {
            // the field is never written, so the call changes nothing on a server that knows the command
            jedis.hpexpireAt(keys.meta, System.currentTimeMillis(), "hash-field-expiry-check");
        } catch (JedisDataException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("ERR unknown command")) {
                throw new TaskConfigurationException(
                        "the server does not know HPEXPIREAT and this store needs hash field expiry: use Redis 7.4"
                                + " or newer, or Valkey 9 or newer",
                        e);
            }
            throw new TaskStoreException("Redis call failed: " + e.getMessage(), e);
        } catch (JedisException e) {
            throw new TaskStoreException("Redis call failed: " + e.getMessage(), e);
        }
    }

    private static String infoField(String info, String name) {
        if (info == null) return null;
        var prefix = name + ":";
        for (var line : info.split("\r?\n")) {
            if (line.startsWith(prefix)) return line.substring(prefix.length()).strip();
        }
        return null;
    }

    @Override
    public long idHighWaterMark() {
        var last = guard(() -> jedis.get(keys.seqId));
        return last == null ? 1 : Long.parseLong(last) + 1;
    }

    @Override
    public long allocateIds(int count) {
        StoreArguments.requireCount(count);
        return guard(() -> jedis.incrBy(keys.seqId, count)) - count + 1;
    }

    @Override
    public int allocateRunnerId(String label, Set<Integer> queueIds, Set<Integer> typeIds) {
        StoreArguments.requireLabel(label);
        var args = List.of(label, Json.write(queueIds), Json.write(typeIds));
        return (int) num(guard(() -> scripts.run(RedisScripts.RUNNER_REGISTER, args)));
    }

    @Override
    public int queueId(String name) {
        return register("queue", name, queueIds);
    }

    @Override
    public int typeId(String name) {
        return register("type", name, typeIds);
    }

    private int register(String kind, String name, Map<String, Integer> cache) {
        StoreArguments.requireName(kind, name);
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
    public Map<Integer, String> typeNames() {
        return names("type");
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
    @SuppressWarnings("unchecked")
    public List<EnqueueOutcome> enqueueAll(List<TaskRecord> tasks) {
        if (tasks.isEmpty()) {
            return List.of();
        }
        var calls = new ArrayList<List<String>>();
        List<String> call = null;
        var count = 0;
        var chars = 0L;
        for (var task : tasks) {
            var args = enqueueArgs(task);
            var size = 0L;
            for (var a : args) size += a.length();
            if (call == null || count == ENQUEUE_CHUNK || chars + size > ENQUEUE_CHUNK_CHARS) {
                call = new ArrayList<>();
                calls.add(call);
                count = 0;
                chars = 0;
            }
            call.add(String.valueOf(args.size()));
            call.addAll(args);
            count++;
            chars += size;
        }
        var out = new ArrayList<EnqueueOutcome>(tasks.size());
        for (var reply : guard(() -> scripts.runAll(RedisScripts.ENQUEUE_ALL, calls))) {
            for (var r : (List<Object>) reply) out.add(outcome(r));
        }
        return out;
    }

    static List<String> enqueueArgs(TaskRecord t) {
        StoreArguments.requireKey(t.uniqueKey());
        var args = new ArrayList<String>();
        args.add(String.valueOf(t.id()));
        args.add(t.dueAt() == null ? "" : ms(t.dueAt()));
        args.add(t.uniqueKey() == null ? "" : t.uniqueKey());
        args.addAll(storeArgs(t));
        args.add(Keys.F_WRITE_TOKEN);
        // decimal, so the hash listpack stores it as an integer
        args.add(Long.toString(ThreadLocalRandom.current().nextLong()));
        return args;
    }

    /**
     * queue id, start-by ms or '', payload flag and payload, then the hash fields: what the script's store needs
     * after the key. A new task starts without run fields ({@link TaskRecord#asNew()}).
     */
    private static List<String> storeArgs(TaskRecord task) {
        var t = task.asNew();
        var args = new ArrayList<String>();
        args.add(String.valueOf(t.queueId()));
        args.add(t.startBy() == null ? "" : ms(t.startBy()));
        args.add(t.encodedPayload() == null ? "" : "1");
        args.add(t.encodedPayload() == null ? "" : t.encodedPayload());
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
     * wrote (state, stateChanged, revision, attempts, claimedAt, lease, runner) are filled in here.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<TaskRecord> claim(ClaimRequest request) {
        var known = request.knownTypeIds();
        if (request.count() < 1 || request.grants().isEmpty() || (known != null && known.isEmpty())) {
            return List.of();
        }
        var args = new ArrayList<String>();
        args.add(String.valueOf(request.count()));
        args.add(String.valueOf(request.lease().toMillis()));
        args.add(String.valueOf(request.runnerId()));
        var types = known == null ? null : typeSet(known);
        args.add(types == null ? "" : types.digest());
        request.grants().forEach(g -> {
            args.add(String.valueOf(g.queueId()));
            args.add(String.valueOf(g.share()));
            args.add(String.valueOf(g.cap()));
        });
        var r = claimReply(guard(() -> scripts.run(RedisScripts.CLAIM, args)));
        if (num(r.get(0)) < 0) {
            var setArgs = new ArrayList<String>(types.ids().size() + 1);
            setArgs.add(types.digest());
            types.ids().forEach(id -> setArgs.add(String.valueOf(id)));
            r = claimReply(guard(() -> {
                scripts.run(RedisScripts.TYPE_SET, setArgs);
                return scripts.run(RedisScripts.CLAIM, args);
            }));
            if (num(r.get(0)) < 0) throw new TaskStoreException("Redis type set " + types.digest() + " vanished");
        }
        var at = num(r.get(0));
        var leaseUntil = at + request.lease().toMillis();
        var tasks = new ArrayList<TaskRecord>(r.size() - 1);
        List<String> unreadable = null;
        for (var entry : r.subList(1, r.size())) {
            try {
                tasks.add(TaskHashes.fromClaim(entry, at, leaseUntil, request.runnerId()));
            } catch (UnreadableTaskException e) {
                if (unreadable == null) unreadable = new ArrayList<>();
                unreadable.add(String.valueOf(e.taskId()));
            }
        }
        if (unreadable != null) endLeases(at, request.runnerId(), unreadable);
        return tasks;
    }

    /**
     * Ends the leases this claim just took on tasks it cannot read, so maintenance takes them over at once. When
     * that fails the leases lapse on their own; the readable tasks of the claim are returned either way.
     */
    private void endLeases(long claimedAt, int runnerId, List<String> ids) {
        var args = new ArrayList<String>(ids.size() + 2);
        args.add(String.valueOf(claimedAt));
        args.add(String.valueOf(runnerId));
        args.addAll(ids);
        try {
            guard(() -> scripts.run(RedisScripts.END_LEASES, args));
        } catch (TaskStoreException ignored) {
            // the leases lapse on their own
        }
    }

    /** The CLAIM reply as a list whose first value is a number: the claim time, or -1 for a missing type set. */
    private static List<?> claimReply(Object reply) {
        if (reply instanceof List<?> r
                && !r.isEmpty()
                && (r.getFirst() instanceof Long || r.getFirst() instanceof String)) {
            try {
                num(r.getFirst());
                return r;
            } catch (NumberFormatException e) {
                throw new TaskStoreException(
                        "claim reply starts with " + r.getFirst() + ", expected the claim time", e);
            }
        }
        throw new TaskStoreException("claim reply is not a list starting with the claim time: "
                + (reply instanceof List<?> l
                        ? l.size() + " values"
                        : reply == null ? "null" : reply.getClass().getSimpleName()));
    }

    /** Names a set of type ids by a digest of the sorted ids, so that runners with the same types share one set. */
    private TypeSet typeSet(Set<Integer> ids) {
        var last = lastTypeSet;
        if (last != null && (last.ids() == ids || last.ids().equals(ids))) return last;
        var text = ids.stream().sorted().map(String::valueOf).collect(Collectors.joining(","));
        try {
            var digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.US_ASCII));
            var made = new TypeSet(ids, HexFormat.of().formatHex(digest, 0, 16));
            lastTypeSet = made;
            return made;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<LeaseStatus> renew(List<LeaseRef> leases, Duration lease) {
        if (leases.isEmpty()) {
            return List.of();
        }
        var calls = new ArrayList<List<String>>();
        for (var from = 0; from < leases.size(); from += LEASE_CHUNK) {
            var args = new ArrayList<String>();
            args.add(String.valueOf(lease.toMillis()));
            for (var l : leases.subList(from, Math.min(from + LEASE_CHUNK, leases.size()))) {
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
                var ref = leases.get(c * LEASE_CHUNK + i);
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

    @Override
    @SuppressWarnings("unchecked")
    public List<Boolean> ackAll(List<LeaseRef> leases, boolean keep, boolean keepPayload) {
        if (leases.isEmpty()) {
            return List.of();
        }
        var calls = new ArrayList<List<String>>();
        for (var from = 0; from < leases.size(); from += LEASE_CHUNK) {
            var args = new ArrayList<String>();
            args.add(keep ? "1" : "0");
            args.add(keepPayload ? "1" : "0");
            for (var l : leases.subList(from, Math.min(from + LEASE_CHUNK, leases.size()))) {
                args.add(String.valueOf(l.id()));
                args.add(String.valueOf(l.revision()));
            }
            calls.add(args);
        }
        var out = new ArrayList<Boolean>(leases.size());
        for (var reply : guard(() -> scripts.runAll(RedisScripts.ACK_ALL, calls))) {
            for (var code : (List<Object>) reply) out.add(num(code) == 1);
        }
        return out;
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
                effect.isPause() ? "" : ms(effect.nextFireAt()));
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
        StoreArguments.requireDue(due);
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
        var args = List.of(String.valueOf(id), String.valueOf(revision), target, optionalText(value));
        return num(guard(() -> scripts.run(RedisScripts.WRITE, args))) == 1;
    }

    // ---- maintenance ---------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public PromoteResult promote(int limit) {
        StoreArguments.requireLimit(limit);
        var r = (List<Object>) guard(() -> scripts.run(RedisScripts.PROMOTE, List.of(String.valueOf(limit))));
        var next = num(r.get(1));
        return new PromoteResult((int) num(r.get(0)), next < 0 ? null : Instant.ofEpochMilli(next));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<ExpiredLease> reclaim(int limit) {
        StoreArguments.requireLimit(limit);
        return guard(() -> {
            var ids = jedis.zrangeByScore(keys.leased, "-inf", String.valueOf(serverNow()), 0, limit);
            var fields = new ArrayList<Response<List<String>>>(ids.size());
            try (var pipeline = jedis.pipelined()) {
                ids.forEach(id -> fields.add(pipeline.hmget(
                        keys.task(Long.parseLong(id)),
                        F_REVISION,
                        F_ATTEMPTS,
                        F_MAX_ATTEMPTS,
                        F_QUEUE,
                        F_TYPE,
                        F_CANCEL_AT)));
                pipeline.sync();
            }
            var out = new ArrayList<ExpiredLease>(ids.size());
            var orphans = new ArrayList<String>();
            var unfenced = new ArrayList<String>();
            for (var i = 0; i < ids.size(); i++) {
                var v = fields.get(i).get();
                if (v.get(0) == null) {
                    orphans.add(ids.get(i));
                    continue;
                }
                var lease = expiredLease(Long.parseLong(ids.get(i)), v);
                if (lease != null) {
                    out.add(lease);
                } else {
                    unfenced.add(ids.get(i));
                }
            }
            // a leased entry whose hash is gone cannot be reclaimed
            if (!orphans.isEmpty()) jedis.zrem(keys.leased, orphans.toArray(String[]::new));
            // one whose revision cannot be read gets a readable one and stays, so the next reclaim ends it
            if (!unfenced.isEmpty()) {
                scripts.run(RedisScripts.REPAIR_REVISION, unfenced);
            }
            return out;
        });
    }

    /**
     * One reclaimed lease from its hash fields (rev, att, max, q, ty, cra). A task whose attempt counts cannot be read
     * comes back with its revision and zero counts, so that maintenance finds it unreadable and ends it; a queue or
     * type id that cannot be read is 0. One whose revision cannot be read cannot be fenced: it is null, and reclaim
     * gives it a readable revision for the next round.
     */
    private static ExpiredLease expiredLease(long id, List<String> v) {
        int revision;
        try {
            revision = Integer.parseInt(v.get(0));
        } catch (NumberFormatException e) {
            return null;
        }
        int attempts = 0;
        int maxAttempts = 0;
        try {
            attempts = Integer.parseInt(v.get(1));
            maxAttempts = Integer.parseInt(v.get(2));
        } catch (NumberFormatException e) {
            attempts = 0;
            maxAttempts = 0;
        }
        return new ExpiredLease(
                id, revision, attempts, maxAttempts, idOrZero(v.get(3)), idOrZero(v.get(4)), v.get(5) != null);
    }

    /** A queue or type id that cannot be read is 0, which is never a registered id. */
    private static int idOrZero(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public long purge(TaskState state, Duration retention, int limit) {
        StoreArguments.requireTerminal(state);
        StoreArguments.requireLimit(limit);
        var args = List.of(String.valueOf(state.code()), String.valueOf(retention.toMillis()), String.valueOf(limit));
        return num(guard(() -> scripts.run(RedisScripts.PURGE, args)));
    }

    /** The key fields of a finished task expire with the key itself (hash field expiry), so there is nothing to do. */
    @Override
    public int releaseKeys(int limit) {
        StoreArguments.requireLimit(limit);
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
    public PutScheduleResult putSchedule(StoredSchedule d, Instant nextFire, Instant keepUntil) {
        var args = new ArrayList<String>();
        args.add(d.scheduleId());
        args.add(ms(nextFire));
        args.add(ms(keepUntil));
        args.addAll(List.of(
                F_TYPE,
                String.valueOf(d.typeId()),
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
        if (d.catchUpMaxFires() != 0) {
            args.addAll(List.of(F_CATCH_UP, String.valueOf(d.catchUpMaxFires())));
        }
        if (d.fixedDelay() != null) {
            args.addAll(List.of(F_FIXED_DELAY, String.valueOf(d.fixedDelay().toMillis())));
        }
        if (d.maxRunTime() != null) {
            args.addAll(List.of(F_MAX_RUN_TIME, String.valueOf(d.maxRunTime().toMillis())));
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
        if (d.encodedPayload() != null) {
            args.addAll(List.of(F_DATA, d.encodedPayload()));
        }
        var r = (List<?>) guard(() -> scripts.run(RedisScripts.CRON_PUT, args));
        return new PutScheduleResult(
                PutScheduleOutcome.valueOf((String) r.get(0)), Instant.ofEpochMilli(num(r.get(1))));
    }

    @Override
    public boolean deleteSchedule(String scheduleId) {
        return num(guard(() -> scripts.run(RedisScripts.CRON_DELETE, List.of(scheduleId)))) == 1;
    }

    @Override
    public Map<String, Instant> nextFireTimes() {
        var result = new LinkedHashMap<String, Instant>();
        schedules().forEach(r -> result.put(r.definition().scheduleId(), r.nextFireAt()));
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DueSchedule> dueSchedules(Instant upTo, int limit) {
        StoreArguments.requireLimit(limit);
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
                        id,
                        Instant.ofEpochMilli((long) due.get(i).getScore()),
                        definition(id, h),
                        "1".equals(h.get(F_TRIGGERED))));
            }
            return out;
        });
    }

    private static StoredSchedule definition(String id, Map<String, String> d) {
        return new StoredSchedule(
                id,
                Integer.parseInt(d.get(F_TYPE)),
                Integer.parseInt(d.get(F_PAYLOAD_VERSION)),
                Integer.parseInt(d.get(F_QUEUE)),
                d.get(F_ACCOUNT),
                d.get(F_CRON),
                d.get(F_ZONE),
                d.get(F_MISFIRE),
                d.containsKey(F_CATCH_UP) ? Integer.parseInt(d.get(F_CATCH_UP)) : 0,
                d.containsKey(F_FIXED_DELAY) ? Duration.ofMillis(Long.parseLong(d.get(F_FIXED_DELAY))) : null,
                "1".equals(d.get(F_OVERLAP_SKIP)),
                d.get(F_DATA),
                Integer.parseInt(d.get(F_MAX_ATTEMPTS)),
                d.containsKey(F_MAX_RUN_TIME) ? Duration.ofMillis(Long.parseLong(d.get(F_MAX_RUN_TIME))) : null,
                "1".equals(d.get(F_PAUSED)),
                Long.parseLong(d.get(F_REVISION)));
    }

    @Override
    public CommitOutcome commitSchedule(DueSchedule due, Instant nextFire, TaskRecord given) {
        StoreArguments.requireKey(given.uniqueKey());
        var occurrence = given.scheduleId() != null
                ? given
                : given.toBuilder().scheduleId(due.scheduleId()).build();
        var args = new ArrayList<String>();
        args.add(due.scheduleId());
        args.add(String.valueOf(due.definition().revision()));
        args.add(ms(due.fireAt()));
        args.add(ms(nextFire));
        args.add(String.valueOf(occurrence.id()));
        args.add(occurrence.uniqueKey() == null ? "" : occurrence.uniqueKey());
        args.addAll(storeArgs(occurrence));
        return switch ((int) num(guard(() -> scripts.run(RedisScripts.CRON_COMMIT, args)))) {
            case 1 -> CommitOutcome.STORED;
            case 2 -> CommitOutcome.SKIPPED;
            default -> CommitOutcome.NOT_COMMITTED;
        };
    }

    // ---- inspection ----------------------------------------------------------------------------------

    @Override
    public Optional<TaskRecord> task(long id) {
        var found = load(List.of(id), false);
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
            var record = task(Long.parseLong(holder));
            if (record.isPresent() && holdsKey(record.get(), key)) return record;
        }
        return Optional.empty();
    }

    /** The {@code k:} string names the holder for as long as the key is taken, also after the task was removed. */
    @Override
    public boolean holdsKey(long id, String key) {
        return String.valueOf(id).equals(guard(() -> jedis.get(keys.uniqueKey(key))));
    }

    /** The two reads of {@link #taskByKey} are not atomic: the record may have lost the key to a newer task since. */
    private static boolean holdsKey(TaskRecord record, String key) {
        return key.equals(record.uniqueKey()) && (!record.state().isTerminal() || record.keyUntil() != null);
    }

    /** Reads the hash, payload, steps and failures of every id in one pipeline; ids without a hash are left out. */
    /** @param skipUnreadable leave out a task with a stored value that cannot be read instead of throwing */
    private List<TaskRecord> load(List<Long> ids, boolean skipUnreadable) {
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
                try {
                    out.add(TaskHashes.fromHash(
                            ids.get(i),
                            h,
                            data.get(i).get(),
                            steps.get(i).get(),
                            failures.get(i).get()));
                } catch (UnreadableTaskException e) {
                    if (!skipUnreadable) throw e;
                }
            }
            return out;
        });
    }

    /** Counts come from state zset cardinalities in one atomic read. */
    @Override
    @SuppressWarnings("unchecked")
    public Map<Integer, StateCounts> counts() {
        var rows = (List<List<Object>>) guard(() -> scripts.run(RedisScripts.COUNTS, List.of()));
        var out = new TreeMap<Integer, StateCounts>();
        for (var row : rows) {
            out.put(
                    (int) num(row.get(0)),
                    new StateCounts(
                            num(row.get(1)),
                            num(row.get(2)),
                            num(row.get(3)),
                            num(row.get(4)),
                            num(row.get(5)),
                            num(row.get(6)),
                            num(row.get(7))));
        }
        return out;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Integer, Map<Integer, Long>> queuedByType() {
        var rows = (List<List<Object>>) guard(() -> scripts.run(RedisScripts.QUEUED_BY_TYPE, List.of()));
        var out = new TreeMap<Integer, Map<Integer, Long>>();
        for (var row : rows) {
            var counts = new TreeMap<Integer, Long>();
            for (var pair : (List<List<Object>>) row.get(1)) {
                counts.put((int) num(pair.get(0)), num(pair.get(1)));
            }
            out.put((int) num(row.get(0)), counts);
        }
        return out;
    }

    private List<Integer> sortedQueueIds() {
        return jedis.hvals(keys.registry("queue")).stream()
                .map(Integer::valueOf)
                .sorted()
                .toList();
    }

    /**
     * Lists from the structures that hold the state: {@code idx:<state>:<qid>} for LEASED and the terminal
     * states, {@code q:<qid>:<tid>} of the types in
     * {@code active:<qid>} for QUEUED and {@code sched:<qid>} for SCHEDULED. Order is by the zset score, then the id
     * as text, which is the order a claim takes them in: stateChanged for the indexed states, due for QUEUED and
     * SCHEDULED. The cursor is
     * {@code <score>:<id>} of the last entry of the page. Pages merge the zsets of each queue, preserving the same
     * cursor when a task or an entire type leaves the queue.
     */
    @Override
    public TaskPage tasks(TaskState state, Integer queueId, String cursor, int limit) {
        StoreArguments.requireLimit(limit);
        var position = cursor == null ? null : decodeCursor(cursor);
        return guard(() -> {
            var probe = (int) Math.min((long) limit + 1, Integer.MAX_VALUE);
            var found = listing(state, queueId, position, probe);
            var entries = found.subList(0, Math.min(limit, found.size()));
            // a task can leave the state between the read of the ids and the read of the records
            var items = load(entries.stream().map(Entry::id).toList(), true).stream()
                    .filter(t -> t.state() == state)
                    .toList();
            var last = entries.isEmpty() ? null : entries.getLast();
            var next = found.size() > limit ? (long) last.score() + ":" + last.member() : null;
            return new TaskPage(items, next);
        });
    }

    private static Entry decodeCursor(String cursor) {
        var colon = cursor.indexOf(':');
        try {
            if (colon > 0) {
                return new Entry(
                        Long.parseLong(cursor.substring(0, colon)),
                        Long.toString(Long.parseLong(cursor.substring(colon + 1))));
            }
        } catch (NumberFormatException e) {
            throw new TaskValidationException("malformed cursor");
        }
        throw new TaskValidationException("malformed cursor");
    }

    private List<Entry> listing(TaskState state, Integer queueId, Entry after, int limit) {
        var entries = new ArrayList<Entry>();
        var qids = queueId == null ? sortedQueueIds() : List.of(queueId);
        for (var qid : qids) {
            for (var zset : stateZsets(state, qid)) {
                entries.addAll(page(zset, after, limit));
            }
        }
        var ordered = entries.stream().sorted(ORDER).toList();
        return ordered.subList(0, Math.min(limit, ordered.size()));
    }

    /** A zset entry: the member is the id as the decimal text Redis orders equal scores by. */
    private record Entry(double score, String member) {
        long id() {
            return Long.parseLong(member);
        }
    }

    private static final Comparator<Entry> ORDER =
            Comparator.comparingDouble(Entry::score).thenComparing(Entry::member);

    private List<String> stateZsets(TaskState state, int qid) {
        return switch (state) {
            case QUEUED ->
                jedis.smembers(keys.activeTypes(qid)).stream()
                        .map(tid -> keys.queue(qid, Integer.parseInt(tid)))
                        .toList();
            case SCHEDULED -> List.of(keys.scheduled(qid));
            default -> List.of(keys.index(state.code(), qid));
        };
    }

    /**
     * Reads one zset after the cursor in the order Redis keeps it: score, then the id as text. The script finds
     * the cursor's place by rank, so tasks that share a score are never read as a whole group, however many they
     * are. A chunk is no longer than the page needs.
     */
    @SuppressWarnings("unchecked")
    private List<Entry> page(String zset, Entry after, int limit) {
        var out = new ArrayList<Entry>();
        var position = after;
        while (out.size() < limit) {
            var size = Math.min(PAGE_CHUNK, limit - out.size());
            var args = List.of(
                    zset,
                    position == null ? "" : String.valueOf((long) position.score()),
                    position == null ? "" : position.member(),
                    String.valueOf(size));
            var chunk = (List<Object>) scripts.run(RedisScripts.LIST, args);
            for (var i = 0; i < chunk.size(); i += 2) {
                out.add(new Entry(Double.parseDouble((String) chunk.get(i + 1)), (String) chunk.get(i)));
            }
            if (chunk.size() < size * 2) {
                break;
            }
            position = out.getLast();
        }
        return out;
    }

    // ---- wave 2: cancel, expiry, admin, runners ------------------------------------------------------

    /** An optional text for a script: '' for null, else the text after a one-character prefix, so '' is kept. */
    private static String optionalText(String text) {
        return text == null ? "" : "=" + text;
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
                optionalText(reason),
                keepPayload ? "1" : "0",
                expiredOnly ? "1" : "0"));
        args.addAll(effectArgs(effect));
        return applied(effect, num(guard(() -> scripts.run(RedisScripts.FINISH_CANCELLED, args))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<ExpiredTask> expire(int limit) {
        StoreArguments.requireLimit(limit);
        var r = (List<Object>) guard(() -> scripts.run(RedisScripts.EXPIRE, List.of(String.valueOf(limit))));
        var out = new ArrayList<ExpiredTask>(r.size() / 3);
        for (var i = 0; i < r.size(); i += 3) {
            long id = Long.parseLong((String) r.get(i));
            out.add(new ExpiredTask(id, idOrZero((String) r.get(i + 1)), idOrZero((String) r.get(i + 2))));
        }
        return out;
    }

    @Override
    public int purgeRunners(Duration staleAfter) {
        var args = List.of(String.valueOf(staleAfter.toMillis()), String.valueOf(RedisScripts.PURGE_RUNNERS_PAGE));
        var total = 0;
        int page;
        do {
            page = (int) num(guard(() -> scripts.run(RedisScripts.PURGE_RUNNERS, args)));
            total += page;
        } while (page == RedisScripts.PURGE_RUNNERS_PAGE);
        return total;
    }

    @Override
    public CancelOutcome cancel(long id, int revision, String reason, ScheduleEffect effect) {
        var args = new ArrayList<>(List.of(String.valueOf(id), String.valueOf(revision), optionalText(reason)));
        args.addAll(effectArgs(effect));
        return CancelOutcome.valueOf(outcome(effect, (String) guard(() -> scripts.run(RedisScripts.CANCEL, args))));
    }

    /** Takes the trailing plus of a script answer, which says the schedule effect changed the definition, off. */
    private static String outcome(ScheduleEffect effect, String answer) {
        if (!answer.endsWith("+")) return answer;
        effect.markApplied();
        return answer.substring(0, answer.length() - 1);
    }

    @Override
    public RetryOutcome retry(long id, int revision) {
        var args = List.of(String.valueOf(id), String.valueOf(revision));
        var answer = (String) guard(() -> scripts.run(RedisScripts.RETRY, args));
        if (answer.equals("KEY_HELD")) {
            throw new TaskStoreException("Cannot retry task " + id + ": its unique key is held by another task");
        }
        return RetryOutcome.valueOf(answer);
    }

    @Override
    public DeleteOutcome delete(long id, int revision, ScheduleEffect effect) {
        var args = new ArrayList<>(List.of(String.valueOf(id), String.valueOf(revision)));
        args.addAll(effectArgs(effect));
        return DeleteOutcome.valueOf(outcome(effect, (String) guard(() -> scripts.run(RedisScripts.DELETE, args))));
    }

    @Override
    public void pauseQueue(int queueId, boolean paused) {
        guard(() -> {
            if (paused) {
                if (!jedis.hvals(keys.registry("queue")).contains(String.valueOf(queueId))) return;
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
    public void heartbeat(
            int runnerId, String label, Set<Integer> queueIds, Set<Integer> typeIds, int running, boolean accepting) {
        StoreArguments.requireLabel(label);
        guard(() -> scripts.run(
                RedisScripts.HEARTBEAT,
                List.of(
                        String.valueOf(runnerId),
                        String.valueOf(running),
                        accepting ? "1" : "0",
                        label,
                        Json.write(queueIds),
                        Json.write(typeIds))));
    }

    @Override
    public List<RunnerRecord> runners() {
        return guard(() -> {
            var ids = jedis.zrange(keys.runners, 0, -1).stream()
                    .map(Integer::valueOf)
                    .sorted()
                    .toList();
            var hashes = new ArrayList<Response<Map<String, String>>>(ids.size());
            try (var pipeline = jedis.pipelined()) {
                ids.forEach(id -> hashes.add(pipeline.hgetAll(keys.runner(id))));
                pipeline.sync();
            }
            var out = new ArrayList<RunnerRecord>(ids.size());
            for (var i = 0; i < ids.size(); i++) {
                var h = hashes.get(i).get();
                if (h.isEmpty()) {
                    continue;
                }
                out.add(new RunnerRecord(
                        ids.get(i),
                        h.getOrDefault(F_LABEL, ""),
                        Instant.ofEpochMilli(Long.parseLong(h.get(F_STARTED))),
                        Instant.ofEpochMilli(Long.parseLong(h.get(F_SEEN))),
                        Integer.parseInt(h.getOrDefault(F_RUNNING, "0")),
                        "1".equals(h.get(F_ACCEPTING)),
                        runnerIds(h.get(F_QUEUES)),
                        runnerIds(h.get(F_TYPES))));
            }
            return out;
        });
    }

    private static Set<Integer> runnerIds(String value) {
        var ids = new HashSet<Integer>();
        for (var id : (List<?>) Json.parse(value)) ids.add(((Number) id).intValue());
        return ids;
    }

    // ---- wave 2: schedule controls -------------------------------------------------------------------

    @Override
    public List<ScheduleEntry> schedules() {
        return guard(() -> {
            var ids = jedis.smembers(keys.cronDefinitions).stream()
                    .sorted(ScheduleStore.ID_ORDER)
                    .toList();
            var hashes = new ArrayList<Response<Map<String, String>>>(ids.size());
            try (var pipeline = jedis.pipelined()) {
                ids.forEach(id -> hashes.add(pipeline.hgetAll(keys.cronDefinition(id))));
                pipeline.sync();
            }
            var out = new ArrayList<ScheduleEntry>(ids.size());
            for (var i = 0; i < ids.size(); i++) {
                var h = hashes.get(i).get();
                if (!h.isEmpty()) {
                    out.add(new ScheduleEntry(
                            definition(ids.get(i), h), Instant.ofEpochMilli(Long.parseLong(h.get(F_NEXT)))));
                }
            }
            return out;
        });
    }

    @Override
    public boolean pauseSchedule(String scheduleId) {
        return num(guard(() -> scripts.run(RedisScripts.CRON_PAUSE, List.of(scheduleId)))) == 1;
    }

    @Override
    public boolean resumeSchedule(String scheduleId, Instant nextFire) {
        var args = List.of(scheduleId, nextFire == null ? "" : ms(nextFire));
        return num(guard(() -> scripts.run(RedisScripts.CRON_RESUME, args))) == 1;
    }

    @Override
    public boolean triggerSchedule(String scheduleId, Instant fireAt) {
        var args = List.of(scheduleId, ms(fireAt));
        return num(guard(() -> scripts.run(RedisScripts.CRON_TRIGGER, args))) == 1;
    }

    @Override
    public boolean setNextFire(String scheduleId, long revision, Instant expectedFire, Instant nextFire) {
        var args = List.of(scheduleId, String.valueOf(revision), ms(expectedFire), ms(nextFire));
        return num(guard(() -> scripts.run(RedisScripts.CRON_ADVANCE, args))) == 1;
    }
}

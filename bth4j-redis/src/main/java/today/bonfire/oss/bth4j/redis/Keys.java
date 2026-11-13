package today.bonfire.oss.bth4j.redis;

/**
 * Redis key names and task hash fields of one namespace. Every key carries the namespace as a hash tag
 * ({@code {ns}:}) so that all of them live in the same Redis Cluster slot, which the scripts rely on: they
 * receive the prefix as an argument and derive every other key from it.
 * <p>
 * Keys, with {@code qid} a queue id, {@code tid} a type id and {@code state} a
 * {@link today.bonfire.oss.bth4j.TaskState} code:
 * <ul>
 * <li>{@code meta} hash (field {@code schema}); {@code seq:id}, {@code seq:worker}, {@code seq:queue},
 * {@code seq:type} counters</li>
 * <li>{@code reg:queue} and {@code reg:type} hashes, name to id; listings invert them with one HGETALL and
 * purge walks the queue ids with HVALS</li>
 * <li>{@code w:<id>} hash (label, started, seen, running, accepting, queues, types); {@code workers} zset of
 * runner ids scored by the last heartbeat ms, which {@code purgeRunners} walks</li>
 * <li>{@code paused} set of queue ids that CLAIM skips</li>
 * <li>{@code startby} zset of QUEUED and SCHEDULED ids that have a {@code sb}, scored by it, so that expiry finds
 * them without scanning; an id leaves it on claim, cancel, delete and expire and returns when a task goes back to
 * QUEUED or SCHEDULED</li>
 * <li>{@code t:<id>} task hash, only non-null fields; it stays a listpack while every value is at most
 * {@code hash-max-listpack-value} bytes (default 64), which a long header map, account or unique key exceeds,
 * and then it is a hashtable (about 5 times the memory); never expires, only purge removes it. {@code hs} is 1
 * while {@code st:<id>} exists and {@code fc} is 1 while {@code f:<id>} exists, set by the scripts that write
 * those keys, so that CLAIM reads them only for tasks that have them. {@code wt} is the enqueue write token, which lets a
 * transport resend return stored without writing again. {@code lease} is only meaningful while LEASED and
 * {@code wid} only while LEASED or terminal: the transitions leave them in place and the reader ignores them</li>
 * <li>{@code d:<id>} string with the payload; {@code st:<id>} string with the steps JSON; {@code f:<id>} list
 * of failures, one JSON object each, the last {@link RedisScripts#MAX_FAILURES} kept</li>
 * <li>{@code q:<qid>:<tid>} zset of QUEUED ids of one queue and type scored by due ms; {@code active:<qid>} set
 * of the type ids whose zset in that queue is not empty, kept in step by every script that writes one, so that
 * readers never look at types without queued tasks; {@code sched:<qid>} zset of SCHEDULED ids scored by due ms;
 * {@code leased} zset scored by lease expiry ms. The type id is stored in task and recurring-definition hashes as
 * {@code ty}</li>
 * <li>{@code ts:<digest>} set of the type ids one runner handles, named by a digest of the sorted ids and written
 * by the store when a claim finds it missing; a claim intersects it with {@code active:<qid>}. A claim that finds
 * less than half a day left on it extends it to a day, so it expires between twelve and twenty-four hours after
 * the last claim that named it</li>
 * <li>{@code idx:<state>:<qid>} zset scored by stateChanged ms, for LEASED and terminal tasks, which purge reads.
 * Counts are the cardinalities of these zsets, so they cannot drift</li>
 * <li>{@code k:<key>} string holding the id of the task that owns a unique key, with a PEXPIREAT when the key
 * is kept for a window after the task ended. It outlives the task when the task is removed inside the window,
 * and keeps the key taken on its own. The {@code uk}, {@code kk} and {@code ku} fields of a kept task carry the
 * same expiry (HPEXPIREAT), so the store needs Redis 7.4 or Valkey 9 at least</li>
 * <li>{@code cron} zset of next fire ms of the definitions that are not paused, {@code cron:<sid>} hash (field
 * {@code next} always holds the next fire, also while paused), {@code crondefs} sorted set of every definition id (all scores 0, so it is ordered by the bytes of the id),
 * {@code cronrev} counter</li>
 * </ul>
 */
final class Keys {
    public static final String SCHEMA = "schema";

    public static final String F_REVISION = "rev";
    public static final String F_QUEUE = "q";
    public static final String F_WRITE_TOKEN = "wt";
    public static final String F_TYPE = "ty";
    public static final String F_PAYLOAD_VERSION = "pv";
    public static final String F_STATE = "state";
    public static final String F_ATTEMPTS = "att";
    public static final String F_MAX_ATTEMPTS = "max";
    public static final String F_CREATED = "created";
    public static final String F_DUE = "due";
    public static final String F_STATE_CHANGED = "sc";
    public static final String F_DATA = "data";
    public static final String F_RUNNER = "wid";
    public static final String F_LEASE = "lease";
    public static final String F_CLAIMED_AT = "ca";
    public static final String F_ACCOUNT = "acc";
    public static final String F_HEADERS = "hdr";
    public static final String F_UNIQUE_KEY = "uk";
    public static final String F_KEY_KEEP = "kk";
    public static final String F_KEY_UNTIL = "ku";
    public static final String F_RETRY_POLICY = "rp";
    public static final String F_SCHEDULE = "sid";
    public static final String F_RETRY_OF = "rof";
    public static final String F_START_BY = "sb";
    public static final String F_MAX_RUN_TIME = "mr";
    public static final String F_CANCEL_AT = "cra";
    public static final String F_CANCEL_REASON = "crs";
    public static final String F_PROGRESS = "prg";

    public static final String F_CRON = "cron";
    public static final String F_NEXT = "next";
    public static final String F_ZONE = "zone";
    public static final String F_MISFIRE = "mf";
    public static final String F_CATCH_UP = "cum";
    public static final String F_FIXED_DELAY = "fd";
    public static final String F_OVERLAP_SKIP = "os";
    public static final String F_PAUSED = "paused";
    public static final String F_TRIGGERED = "trg";

    public static final String F_LABEL = "label";
    public static final String F_STARTED = "started";
    public static final String F_SEEN = "seen";
    public static final String F_RUNNING = "running";
    public static final String F_ACCEPTING = "accepting";
    public static final String F_QUEUES = "queues";
    public static final String F_TYPES = "types";

    public final String namespace;
    public final String prefix;
    public final String meta;
    public final String seqId;
    public final String seqRunner;
    public final String leased;
    public final String cron;
    public final String cronDefinitions;
    public final String paused;
    public final String startBy;
    public final String runners;
    /** counter behind recurring definition revisions; it never resets, so a recreated definition cannot reuse one */
    public final String cronRevision;

    public Keys(String namespace) {
        this.namespace = namespace;
        this.prefix = "{" + namespace + "}:";
        this.meta = prefix + "meta";
        this.seqId = prefix + "seq:id";
        this.seqRunner = prefix + "seq:worker";
        this.leased = prefix + "leased";
        this.cron = prefix + "cron";
        this.cronDefinitions = prefix + "crondefs";
        this.paused = prefix + "paused";
        this.startBy = prefix + "startby";
        this.runners = prefix + "workers";
        this.cronRevision = prefix + "cronrev";
    }

    public String task(long id) {
        return prefix + "t:" + id;
    }

    public String data(long id) {
        return prefix + "d:" + id;
    }

    public String steps(long id) {
        return prefix + "st:" + id;
    }

    public String failures(long id) {
        return prefix + "f:" + id;
    }

    public String queue(int qid, int tid) {
        return prefix + "q:" + qid + ":" + tid;
    }

    public String scheduled(int qid) {
        return prefix + "sched:" + qid;
    }

    public String index(int state, int qid) {
        return prefix + "idx:" + state + ":" + qid;
    }

    public String activeTypes(int qid) {
        return prefix + "active:" + qid;
    }

    public String typeSet(String digest) {
        return prefix + "ts:" + digest;
    }

    public String uniqueKey(String key) {
        return prefix + "k:" + key;
    }

    public String runner(int id) {
        return prefix + "w:" + id;
    }

    public String cronDefinition(String scheduleId) {
        return prefix + "cron:" + scheduleId;
    }

    /** registry hash name to id; {@code kind} is {@code queue} or {@code type} */
    public String registry(String kind) {
        return prefix + "reg:" + kind;
    }
}

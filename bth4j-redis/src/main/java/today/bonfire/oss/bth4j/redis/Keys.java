package today.bonfire.oss.bth4j.redis;

/**
 * Redis key names and task hash fields of one namespace. Every key carries the namespace as a hash tag
 * ({@code {ns}:}) so that all of them live in the same Redis Cluster slot, which the scripts rely on: they
 * receive the prefix as an argument and derive every other key from it.
 * <p>
 * Keys, with {@code qid} a queue id and {@code state} a {@link today.bonfire.oss.bth4j.spi.TaskState} code:
 * <ul>
 * <li>{@code meta} hash (field {@code schema}); {@code seq:id}, {@code seq:worker}, {@code seq:queue},
 * {@code seq:event} counters</li>
 * <li>{@code reg:queue} name to id, {@code reg:queue:names} id to name, the same for {@code reg:event}</li>
 * <li>{@code w:<id>} hash (label, started)</li>
 * <li>{@code t:<id>} task hash, only non-null fields; never expires, only purge removes it</li>
 * <li>{@code q:<qid>} zset of QUEUED ids scored by due ms; {@code scheduled} zset scored by due ms;
 * {@code leased} zset scored by lease expiry ms</li>
 * <li>{@code idx:<state>:<qid>} zset scored by stateChanged ms, for LEASED and the terminal states</li>
 * <li>{@code c:<qid>} hash of counters, one field per state code; {@code queues} set of queue ids</li>
 * <li>{@code k:<key>} string holding the id of the task that owns a unique key, with a PEXPIRE when the key
 * is kept for a window after the task ended; {@code keys} zset (keyUntil ms) of finished tasks whose hash
 * still names a key</li>
 * <li>{@code cron} zset of next fire ms, {@code cron:<sid>} hash, {@code cronrev} counter</li>
 * </ul>
 */
public final class Keys {
    public static final String SCHEMA = "schema";

    public static final String F_REVISION = "rev";
    public static final String F_QUEUE = "q";
    public static final String F_EVENT = "ev";
    public static final String F_PAYLOAD_VERSION = "pv";
    public static final String F_STATE = "state";
    public static final String F_ATTEMPTS = "att";
    public static final String F_MAX_ATTEMPTS = "max";
    public static final String F_CREATED = "created";
    public static final String F_DUE = "due";
    public static final String F_STATE_CHANGED = "sc";
    public static final String F_DATA = "data";
    public static final String F_WORKER = "wid";
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
    public static final String F_MAX_RUN = "mr";
    public static final String F_CANCEL_AT = "cra";
    public static final String F_CANCEL_REASON = "crs";
    public static final String F_PROGRESS = "prg";
    public static final String F_STEPS = "steps";
    public static final String F_FAILURES = "fl";

    public static final String F_CRON = "cron";

    public final String namespace;
    public final String prefix;
    public final String meta;
    public final String seqId;
    public final String seqWorker;
    public final String scheduled;
    public final String leased;
    public final String queues;
    public final String keyWindows;
    public final String cron;
    /** counter behind recurring definition revisions; it never resets, so a recreated definition cannot reuse one */
    public final String cronRevision;

    public Keys(String namespace) {
        this.namespace = namespace;
        this.prefix = "{" + namespace + "}:";
        this.meta = prefix + "meta";
        this.seqId = prefix + "seq:id";
        this.seqWorker = prefix + "seq:worker";
        this.scheduled = prefix + "scheduled";
        this.leased = prefix + "leased";
        this.queues = prefix + "queues";
        this.keyWindows = prefix + "keys";
        this.cron = prefix + "cron";
        this.cronRevision = prefix + "cronrev";
    }

    public String task(long id) {
        return prefix + "t:" + id;
    }

    public String queue(int qid) {
        return prefix + "q:" + qid;
    }

    public String index(int state, int qid) {
        return prefix + "idx:" + state + ":" + qid;
    }

    public String counters(int qid) {
        return prefix + "c:" + qid;
    }

    public String uniqueKey(String key) {
        return prefix + "k:" + key;
    }

    public String worker(int id) {
        return prefix + "w:" + id;
    }

    public String cronDefinition(String scheduleId) {
        return prefix + "cron:" + scheduleId;
    }

    /** registry hash name to id; {@code kind} is {@code queue} or {@code event} */
    public String registry(String kind) {
        return prefix + "reg:" + kind;
    }

    public String registryNames(String kind) {
        return prefix + "reg:" + kind + ":names";
    }
}

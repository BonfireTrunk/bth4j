package today.bonfire.oss.bth4j.postgres;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
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
 * {@link FullStore} on PostgreSQL (9.5 or newer for {@code SKIP LOCKED} and {@code ON CONFLICT}; tested
 * on 17): one row per task in {@code <prefix>_task} with a state column, name registries in
 * {@code <prefix>_queue} and {@code <prefix>_event}, workers in {@code <prefix>_worker}, recurring
 * definitions in {@code <prefix>_schedule}. Each transition's check and writes commit together: claims use
 * {@code FOR UPDATE SKIP LOCKED}, the fenced transitions are {@code UPDATE ... WHERE id = ? AND revision = ?
 * AND state = 2}, and {@link #fail} locks the row before it reads the failures it appends to. Runners that
 * share the tables share the work. The database clock ({@code now()}) decides due times, leases and
 * retention.
 * <p>
 * The tables are created by {@link #init()}, or by running {@link #schemaStatements()} from the
 * application's migration tool. There are no foreign keys; the registries are only written through
 * {@link #queueId} and {@link #eventId}. Only {@code java.sql} is used: the application supplies the JDBC
 * driver and a {@link DataSource}, normally pooled.
 */
public class PostgresTaskStore implements FullStore {
    private static final Pattern PREFIX = Pattern.compile("[a-z][a-z0-9_]{0,40}");
    private static final int MAX_FAILURES = 20;
    private static final int MAX_REGISTRY_ID = Short.MAX_VALUE;
    /** SQLSTATE of nextval past the sequence's maximum: the smallint registry is full */
    private static final String SEQUENCE_LIMIT = "2200H";

    /** Releases the key at the end of the task, or starts its keep window; the right-hand sides see old values. */
    private static final String KEY_RELEASE =
            " unique_key = CASE WHEN key_keep_ms IS NULL THEN NULL ELSE unique_key END,"
                    + " key_until = CASE WHEN key_keep_ms IS NULL OR unique_key IS NULL THEN NULL"
                    + " ELSE now() + key_keep_ms * interval '1 millisecond' END";

    private static final String FENCE = " WHERE id = ? AND revision = ? AND state = 2";

    private final DataSource dataSource;
    private final String prefix;
    private final String meta;
    private final String queue;
    private final String event;
    private final String worker;
    private final String task;
    private final String schedule;
    private final String taskIdSeq;
    private final String scheduleSeq;

    private final String insertSql;
    private final String claimSql;
    private final String claimKnownEventsSql;
    private final String renewSql;
    private final String ackSql;
    private final String failLockSql;
    private final String rescheduleSql;
    private final String unclaimSql;
    private final String saveStepsSql;
    private final String progressSql;
    private final String promoteSql;
    private final String nextDueSql;
    private final String reclaimSql;
    private final String purgeSql;
    private final String releaseKeysSql;
    private final String putScheduleSql;
    private final String dueScheduleSql;
    private final String commitScheduleSql;

    /**
     * @param dataSource connections to the database; the store does not close it
     * @param prefix     table name prefix, lower case letters, digits and underscores, at most 41 characters.
     *                   Runners with the same prefix on the same database share the work.
     */
    public PostgresTaskStore(DataSource dataSource, String prefix) {
        if (dataSource == null) throw new TaskConfigurationError("DataSource is required");
        if (prefix == null || !PREFIX.matcher(prefix).matches()) {
            throw new TaskConfigurationError("prefix must match " + PREFIX.pattern());
        }
        this.dataSource = dataSource;
        this.prefix = prefix;
        this.meta = prefix + "_meta";
        this.queue = prefix + "_queue";
        this.event = prefix + "_event";
        this.worker = prefix + "_worker";
        this.task = prefix + "_task";
        this.schedule = prefix + "_schedule";
        this.taskIdSeq = prefix + "_task_id";
        this.scheduleSeq = prefix + "_schedule_rev";

        // one statement for any number of tasks; ON CONFLICT without a target covers both the id and the
        // unique key, and enqueueAll tells the two apart afterwards
        insertSql = "INSERT INTO " + task
                + " (id, revision, queue_id, event_id, payload_version, state, attempts, max_attempts, created,"
                + " due, state_changed, data, account, headers, unique_key, key_keep_ms, retry_policy, schedule_id,"
                + " retry_of, start_by, max_run_ms, cancel_requested_at, cancel_reason, progress, steps, failures)"
                + " SELECT u.id, u.revision, u.queue_id, u.event_id, u.payload_version,"
                + " CASE WHEN u.due > now() THEN 1 ELSE 0 END, u.attempts, u.max_attempts, u.created,"
                + " COALESCE(u.due, now()), now(), u.data, u.account, u.headers::jsonb, u.unique_key, u.key_keep_ms,"
                + " u.retry_policy, u.schedule_id, u.retry_of, u.start_by, u.max_run_ms, u.cancel_requested_at,"
                + " u.cancel_reason, u.progress,"
                + " u.steps::jsonb, u.failures::jsonb"
                + " FROM unnest(?::bigint[], ?::int[], ?::int[], ?::int[], ?::int[], ?::int[], ?::int[],"
                + " ?::timestamptz[], ?::timestamptz[], ?::text[], ?::text[], ?::text[], ?::text[], ?::bigint[],"
                + " ?::text[], ?::text[], ?::bigint[], ?::timestamptz[], ?::int[], ?::timestamptz[], ?::text[],"
                + " ?::text[], ?::text[], ?::text[])"
                + " WITH ORDINALITY AS u(id, revision, queue_id, event_id, payload_version, attempts, max_attempts,"
                + " created, due, data, account, headers, unique_key, key_keep_ms, retry_policy, schedule_id,"
                + " retry_of, start_by, max_run_ms, cancel_requested_at, cancel_reason, progress, steps, failures, ord)"
                + " ORDER BY u.ord"
                + " ON CONFLICT DO NOTHING RETURNING id";
        claimSql = claimSql(false);
        claimKnownEventsSql = claimSql(true);
        renewSql = "UPDATE " + task + " t SET lease_until = now() + ? * interval '1 millisecond'"
                + " FROM unnest(?::bigint[], ?::int[]) AS u(id, revision)"
                + " WHERE t.id = u.id AND t.revision = u.revision AND t.state = 2"
                + " RETURNING t.id, t.revision, t.cancel_requested_at IS NOT NULL";
        // keep = false removes the row, unless it still holds its key for a keep window; the two branches
        // exclude each other, so at most one touches the row
        ackSql = "WITH d AS (DELETE FROM " + task + FENCE
                + "   AND NOT ? AND (key_keep_ms IS NULL OR unique_key IS NULL) RETURNING id),"
                + " u AS (UPDATE " + task + " SET state = 3, revision = revision + 1, state_changed = now(),"
                + "   lease_until = NULL, data = CASE WHEN ? AND ? THEN data ELSE NULL END," + KEY_RELEASE
                + FENCE + " AND (? OR (key_keep_ms IS NOT NULL AND unique_key IS NOT NULL)) RETURNING id)"
                + " SELECT (SELECT count(*) FROM d) + (SELECT count(*) FROM u)";
        failLockSql = "SELECT attempts, max_attempts, failures::text,"
                + " key_keep_ms IS NOT NULL AND unique_key IS NOT NULL, lease_until <= now()"
                + " FROM " + task + FENCE + " FOR UPDATE";
        rescheduleSql = "UPDATE " + task + " SET state = 1, due = ?, attempts = attempts - 1,"
                + " revision = revision + 1, state_changed = now(), lease_until = NULL, worker_id = NULL" + FENCE;
        unclaimSql = "UPDATE " + task + " SET state = 0, attempts = attempts - 1,"
                + " revision = revision + 1, state_changed = now(), lease_until = NULL, worker_id = NULL" + FENCE;
        saveStepsSql = "UPDATE " + task + " SET steps = ?::jsonb" + FENCE;
        progressSql = "UPDATE " + task + " SET progress = ?" + FENCE;
        promoteSql = "WITH due AS (SELECT id FROM " + task + " WHERE state = 1 AND due <= now() ORDER BY due LIMIT ?"
                + "   FOR UPDATE SKIP LOCKED)"
                + " UPDATE " + task + " t SET state = 0, revision = t.revision + 1, state_changed = now()"
                + " FROM due WHERE t.id = due.id";
        nextDueSql = "SELECT min(due) FROM " + task + " WHERE state = 1";
        reclaimSql = "SELECT id, revision, attempts, max_attempts, event_id, cancel_requested_at IS NOT NULL" + " FROM "
                + task + " WHERE state = 2 AND lease_until <= now() ORDER BY lease_until LIMIT ?";
        purgeSql = "DELETE FROM " + task + " WHERE id IN (SELECT id FROM " + task
                + "   WHERE state = ? AND state_changed <= now() - ? * interval '1 millisecond'"
                + "   AND (key_until IS NULL OR key_until <= now())"
                + "   ORDER BY state_changed LIMIT ? FOR UPDATE SKIP LOCKED)";
        releaseKeysSql = "UPDATE " + task + " SET unique_key = NULL, key_until = NULL WHERE id IN (SELECT id FROM "
                + task + "   WHERE key_until <= now() ORDER BY key_until LIMIT ? FOR UPDATE SKIP LOCKED)";
        // the stored next fire time survives a re-put that keeps the timing (cron, zone, fixed delay) unchanged;
        // paused is only taken from a new definition, so a runner re-putting at start cannot undo an operator's pause
        putScheduleSql = "INSERT INTO " + schedule
                + " (id, event_id, payload_version, queue_id, account, cron, zone, misfire, catch_up_max,"
                + " fixed_delay_ms, overlap_skip, data, max_attempts, paused, rev, next_fire)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, nextval('" + scheduleSeq + "'), ?)"
                + " ON CONFLICT (id) DO UPDATE SET event_id = EXCLUDED.event_id,"
                + " payload_version = EXCLUDED.payload_version, queue_id = EXCLUDED.queue_id,"
                + " account = EXCLUDED.account, cron = EXCLUDED.cron, zone = EXCLUDED.zone,"
                + " misfire = EXCLUDED.misfire, catch_up_max = EXCLUDED.catch_up_max,"
                + " fixed_delay_ms = EXCLUDED.fixed_delay_ms, overlap_skip = EXCLUDED.overlap_skip,"
                + " data = EXCLUDED.data, max_attempts = EXCLUDED.max_attempts,"
                + " rev = nextval('" + scheduleSeq + "'),"
                + " next_fire = CASE WHEN " + schedule + ".cron IS NOT DISTINCT FROM EXCLUDED.cron"
                + " AND " + schedule + ".zone IS NOT DISTINCT FROM EXCLUDED.zone"
                + " AND " + schedule + ".fixed_delay_ms IS NOT DISTINCT FROM EXCLUDED.fixed_delay_ms"
                + " THEN " + schedule + ".next_fire ELSE EXCLUDED.next_fire END";
        dueScheduleSql =
                "SELECT * FROM " + schedule + " WHERE NOT paused AND next_fire <= ? ORDER BY next_fire LIMIT ?";
        // a definition paused between dueRecurring and the commit does not fire
        commitScheduleSql = "WITH adv AS (UPDATE " + schedule + " SET next_fire = ?"
                + "   WHERE id = ? AND rev = ? AND next_fire = ? AND NOT paused RETURNING id),"
                + " ins AS (INSERT INTO " + task
                + "   (id, revision, queue_id, event_id, payload_version, state, attempts, max_attempts, created,"
                + "   due, state_changed, data, account, headers, retry_policy, schedule_id, unique_key, key_keep_ms,"
                + "   start_by, max_run_ms)"
                + "   SELECT ?, 0, ?, ?, ?, 1, 0, ?, ?, ?, now(), ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ? FROM adv"
                + "   ON CONFLICT DO NOTHING RETURNING id)"
                + " SELECT count(*) FROM adv";
    }

    /**
     * One bounded, index-ordered scan per queue, then the caller's queue order decides between them; a single
     * ORDER BY array_position(...) would sort every queued row on each claim.
     */
    private String claimSql(boolean filterEvents) {
        return "WITH q AS (SELECT u.queue_id, u.prio FROM unnest(?::int[]) WITH ORDINALITY AS u(queue_id, prio)"
                + "   WHERE NOT EXISTS (SELECT 1 FROM " + queue + " p WHERE p.id = u.queue_id AND p.paused)),"
                + " locked AS (SELECT c.id, q.prio, c.due FROM q, LATERAL (SELECT id, due FROM " + task
                + "   WHERE state = 0 AND queue_id = q.queue_id AND (start_by IS NULL OR start_by > now())"
                + (filterEvents ? " AND event_id = ANY(?::int[])" : "")
                + "   ORDER BY due, id LIMIT ? FOR UPDATE SKIP LOCKED) c),"
                + " picked AS (SELECT id, row_number() OVER (ORDER BY prio, due, id) AS ord"
                + "   FROM (SELECT * FROM locked ORDER BY prio, due, id LIMIT ?) l),"
                + " upd AS (UPDATE " + task + " t SET state = 2, revision = t.revision + 1, attempts = t.attempts + 1,"
                + "   worker_id = ?, claimed_at = now(), state_changed = now(),"
                + "   lease_until = now() + ? * interval '1 millisecond'"
                + "   FROM picked WHERE t.id = picked.id RETURNING t.*, picked.ord)"
                + " SELECT * FROM upd ORDER BY ord";
    }

    // ---- schema --------------------------------------------------------------------------------------

    @Override
    public StoreInfo info() {
        return new StoreInfo("postgresql", StoreInfo.SCHEMA_VERSION);
    }

    /** The DDL for this prefix, idempotent; for migration tools that manage the schema themselves. */
    public List<String> schemaStatements() {
        return List.of(
                "CREATE TABLE IF NOT EXISTS " + meta + " (key text PRIMARY KEY, value text NOT NULL)",
                "CREATE TABLE IF NOT EXISTS " + queue + " ("
                        + " id smallserial PRIMARY KEY,"
                        + " name text NOT NULL UNIQUE,"
                        + " paused boolean NOT NULL DEFAULT false)",
                "CREATE TABLE IF NOT EXISTS " + event + " (id smallserial PRIMARY KEY, name text NOT NULL UNIQUE)",
                "CREATE TABLE IF NOT EXISTS " + worker + " ("
                        + " id serial PRIMARY KEY,"
                        + " label text NOT NULL,"
                        + " started_at timestamptz NOT NULL,"
                        + " last_seen timestamptz NOT NULL,"
                        + " running integer NOT NULL DEFAULT 0)",
                "CREATE TABLE IF NOT EXISTS " + task + " ("
                        + " id bigint PRIMARY KEY,"
                        + " revision integer NOT NULL,"
                        + " queue_id smallint NOT NULL,"
                        + " event_id smallint NOT NULL,"
                        + " payload_version smallint NOT NULL,"
                        + " state smallint NOT NULL,"
                        + " attempts smallint NOT NULL,"
                        + " max_attempts smallint NOT NULL,"
                        + " created timestamptz NOT NULL,"
                        + " due timestamptz NOT NULL,"
                        + " state_changed timestamptz NOT NULL,"
                        + " data text,"
                        + " worker_id integer,"
                        + " lease_until timestamptz,"
                        + " claimed_at timestamptz,"
                        + " account text,"
                        + " headers jsonb,"
                        + " unique_key text,"
                        + " key_keep_ms bigint,"
                        + " key_until timestamptz,"
                        + " retry_policy text,"
                        + " schedule_id text,"
                        + " retry_of bigint,"
                        + " start_by timestamptz,"
                        + " max_run_ms integer,"
                        + " cancel_requested_at timestamptz,"
                        + " cancel_reason text,"
                        + " progress text,"
                        + " steps jsonb,"
                        + " failures jsonb)",
                "CREATE SEQUENCE IF NOT EXISTS " + taskIdSeq,
                "CREATE INDEX IF NOT EXISTS " + task + "_queued ON " + task + " (queue_id, due, id) WHERE state = 0",
                "CREATE INDEX IF NOT EXISTS " + task + "_scheduled ON " + task + " (due) WHERE state = 1",
                "CREATE INDEX IF NOT EXISTS " + task + "_leased ON " + task + " (lease_until) WHERE state = 2",
                "CREATE INDEX IF NOT EXISTS " + task + "_finished ON " + task
                        + " (state, state_changed, id) WHERE state >= 3",
                "CREATE UNIQUE INDEX IF NOT EXISTS " + task + "_unique_key ON " + task
                        + " (unique_key) WHERE unique_key IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS " + task + "_key_until ON " + task
                        + " (key_until) WHERE key_until IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS " + task + "_start_by ON " + task
                        + " (start_by) WHERE start_by IS NOT NULL AND state < 2",
                "CREATE TABLE IF NOT EXISTS " + schedule + " ("
                        + " id text PRIMARY KEY,"
                        + " event_id smallint NOT NULL,"
                        + " payload_version smallint NOT NULL,"
                        + " queue_id smallint NOT NULL,"
                        + " account text,"
                        + " cron text,"
                        + " zone text,"
                        + " misfire text,"
                        + " catch_up_max smallint,"
                        + " fixed_delay_ms bigint,"
                        + " overlap_skip boolean NOT NULL DEFAULT false,"
                        + " data text,"
                        + " max_attempts smallint NOT NULL,"
                        + " paused boolean NOT NULL DEFAULT false,"
                        + " rev bigint NOT NULL,"
                        + " next_fire timestamptz NOT NULL)",
                "CREATE SEQUENCE IF NOT EXISTS " + scheduleSeq,
                "CREATE INDEX IF NOT EXISTS " + schedule + "_due ON " + schedule + " (next_fire) WHERE NOT paused",
                "INSERT INTO " + meta + " (key, value) VALUES ('schema_version', '" + StoreInfo.SCHEMA_VERSION + "')"
                        + " ON CONFLICT (key) DO NOTHING");
    }

    /**
     * Creates the tables, indexes and sequences when they are missing, then checks the schema version. Safe to
     * call from several runners at once: they serialize on a transaction-level advisory lock for the prefix.
     *
     * @throws TaskStoreException when the tables carry another schema version, or a task table exists
     *                            without a version (an older layout)
     */
    @Override
    public void init() {
        inTransaction(c -> {
            try (var ps = c.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
                ps.setString(1, "bth4j:" + prefix);
                ps.executeQuery().close();
            }
            try (var ps = c.prepareStatement("SELECT to_regclass(?) IS NOT NULL, to_regclass(?) IS NOT NULL")) {
                ps.setString(1, meta);
                ps.setString(2, task);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    if (!rs.getBoolean(1) && rs.getBoolean(2)) {
                        throw new TaskStoreException("table " + task + " exists without " + meta
                                + ": an older bth4j layout, this library needs schema version "
                                + StoreInfo.SCHEMA_VERSION);
                    }
                }
            }
            try (var st = c.createStatement()) {
                for (var sql : schemaStatements()) {
                    st.execute(sql);
                }
            }
            try (var ps = c.prepareStatement("SELECT value FROM " + meta + " WHERE key = 'schema_version'");
                    var rs = ps.executeQuery()) {
                rs.next();
                var version = rs.getString(1);
                if (!String.valueOf(StoreInfo.SCHEMA_VERSION).equals(version)) {
                    throw new TaskStoreException(
                            "schema version " + version + ", this library needs " + StoreInfo.SCHEMA_VERSION);
                }
            }
            return null;
        });
    }

    // ---- identity and registries ---------------------------------------------------------------------

    /**
     * The ids come from one sequence; holding the meta row lock keeps a concurrent allocation from
     * interleaving its values with this one, so the range is contiguous.
     */
    @Override
    public long allocateIds(int count) {
        if (count < 1) throw new IllegalArgumentException("count must be positive: " + count);
        if (count == 1) {
            return withConnection(c -> {
                try (var ps = c.prepareStatement("SELECT nextval('" + taskIdSeq + "')")) {
                    return single(ps);
                }
            });
        }
        return inTransaction(c -> {
            try (var ps = c.prepareStatement("SELECT 1 FROM " + meta + " WHERE key = 'schema_version' FOR UPDATE")) {
                ps.executeQuery().close();
            }
            try (var ps = c.prepareStatement(
                    "SELECT min(v) FROM (SELECT nextval('" + taskIdSeq + "') AS v FROM generate_series(1, ?)) s")) {
                ps.setInt(1, count);
                return single(ps);
            }
        });
    }

    @Override
    public int allocateWorkerId(String label) {
        if (label == null) throw new IllegalArgumentException("label is required");
        return withConnection(c -> {
            try (var ps = c.prepareStatement("INSERT INTO " + worker
                    + " (label, started_at, last_seen) VALUES (?, now(), now()) RETURNING id")) {
                ps.setString(1, label);
                return (int) single(ps);
            }
        });
    }

    @Override
    public int queueId(String name) {
        return registryId(queue, "queue", name);
    }

    @Override
    public int eventId(String name) {
        return registryId(event, "event", name);
    }

    /**
     * Reads first and inserts only for a new name: an insert that loses on conflict still consumes a value of
     * the smallint sequence, so upserting on every lookup would exhaust it.
     */
    private int registryId(String table, String kind, String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException(kind + " name is required");
        return withConnection(c -> {
            var found = registryLookup(c, table, name);
            if (found != null) return found;
            try (var ps = c.prepareStatement(
                    "INSERT INTO " + table + " (name) VALUES (?) ON CONFLICT (name) DO NOTHING RETURNING id")) {
                ps.setString(1, name);
                try (var rs = ps.executeQuery()) {
                    if (rs.next()) return rs.getInt(1);
                }
            } catch (SQLException e) {
                if (SEQUENCE_LIMIT.equals(e.getSQLState())) {
                    throw new TaskStoreException(
                            "the " + kind + " registry is full (" + MAX_REGISTRY_ID + " names)", e);
                }
                throw e;
            }
            found = registryLookup(c, table, name);
            if (found == null) throw new TaskStoreException("could not register " + kind + " " + name);
            return found;
        });
    }

    private static Integer registryLookup(Connection c, String table, String name) throws SQLException {
        try (var ps = c.prepareStatement("SELECT id FROM " + table + " WHERE name = ?")) {
            ps.setString(1, name);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : null;
            }
        }
    }

    @Override
    public Map<Integer, String> queueNames() {
        return names(queue);
    }

    @Override
    public Map<Integer, String> eventNames() {
        return names(event);
    }

    private Map<Integer, String> names(String table) {
        return withConnection(c -> {
            var result = new TreeMap<Integer, String>();
            try (var ps = c.prepareStatement("SELECT id, name FROM " + table);
                    var rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getInt(1), rs.getString(2));
                }
            }
            return result;
        });
    }

    // ---- producers -----------------------------------------------------------------------------------

    @Override
    public EnqueueOutcome enqueue(TaskRecord task) {
        return enqueueAll(List.of(task)).getFirst();
    }

    /**
     * Inserts every task in one statement, then classifies the ones that were not stored. A task whose
     * conflicting row went away between the insert and the classification (its key holder was purged or
     * released its key) is inserted again.
     */
    @Override
    public List<EnqueueOutcome> enqueueAll(List<TaskRecord> tasks) {
        if (tasks.isEmpty()) return List.of();
        return withConnection(c -> enqueueAll(c, tasks));
    }

    /**
     * Stores a task on the caller's connection, inside the caller's transaction: the task becomes visible to
     * workers only when the caller commits, and disappears with a rollback. The connection is neither
     * committed nor closed. The task should come from {@code TaskClient.prepare}.
     *
     * @throws TaskStoreException when a statement fails; the caller's transaction is then aborted
     */
    public EnqueueOutcome enqueue(Connection connection, TaskRecord task) {
        if (connection == null) throw new IllegalArgumentException("connection is required");
        try {
            return enqueueAll(connection, List.of(task)).getFirst();
        } catch (SQLException e) {
            throw new TaskStoreException("PostgreSQL task store operation failed", e);
        }
    }

    private List<EnqueueOutcome> enqueueAll(Connection c, List<TaskRecord> tasks) throws SQLException {
        {
            var outcomes = new EnqueueOutcome[tasks.size()];
            var pending = new ArrayList<Integer>();
            for (int i = 0; i < tasks.size(); i++) pending.add(i);
            for (int round = 0; round < 3 && !pending.isEmpty(); round++) {
                var batch = pending.stream().map(tasks::get).toList();
                releaseExpiredKeys(c, batch);
                var stored = insert(c, batch);
                var unresolved = new ArrayList<Integer>();
                // the same id twice in one call is stored once, for its first occurrence
                for (var i : pending) {
                    if (stored.remove(tasks.get(i).id())) outcomes[i] = EnqueueOutcome.STORED;
                    else unresolved.add(i);
                }
                pending = classify(c, tasks, unresolved, outcomes);
            }
            if (!pending.isEmpty()) {
                throw new TaskStoreException("enqueue did not settle for task "
                        + tasks.get(pending.getFirst()).id());
            }
            return List.of(outcomes);
        }
    }

    /** A key whose keep window has passed is free; release it now rather than wait for {@link #releaseKeys}. */
    private void releaseExpiredKeys(Connection c, List<TaskRecord> batch) throws SQLException {
        var keys = batch.stream()
                .map(TaskRecord::uniqueKey)
                .filter(Objects::nonNull)
                .distinct()
                .toArray(String[]::new);
        if (keys.length == 0) return;
        var array = c.createArrayOf("text", keys);
        try (var ps = c.prepareStatement("UPDATE " + task + " SET unique_key = NULL, key_until = NULL"
                + " WHERE unique_key = ANY(?) AND key_until <= now()")) {
            ps.setArray(1, array);
            ps.executeUpdate();
        } finally {
            array.free();
        }
    }

    private Set<Long> insert(Connection c, List<TaskRecord> batch) throws SQLException {
        int n = batch.size();
        var ids = new Long[n];
        var revisions = new Integer[n];
        var queueIds = new Integer[n];
        var eventIds = new Integer[n];
        var payloadVersions = new Integer[n];
        var attempts = new Integer[n];
        var maxAttempts = new Integer[n];
        var created = new String[n];
        var due = new String[n];
        var data = new String[n];
        var account = new String[n];
        var headers = new String[n];
        var uniqueKey = new String[n];
        var keyKeep = new Long[n];
        var retryPolicy = new String[n];
        var scheduleId = new String[n];
        var retryOf = new Long[n];
        var startBy = new String[n];
        var maxRun = new Integer[n];
        var cancelRequestedAt = new String[n];
        var cancelReason = new String[n];
        var progress = new String[n];
        var steps = new String[n];
        var failures = new String[n];
        for (int i = 0; i < n; i++) {
            var t = batch.get(i);
            ids[i] = t.id();
            revisions[i] = t.revision();
            queueIds[i] = t.queueId();
            eventIds[i] = t.eventId();
            payloadVersions[i] = t.payloadVersion();
            attempts[i] = t.attempts();
            maxAttempts[i] = t.maxAttempts();
            created[i] = text(t.created() == null ? Instant.now() : t.created());
            due[i] = text(t.due());
            data[i] = t.data();
            account[i] = t.account();
            headers[i] = RecordJson.encodeHeaders(t.headers());
            uniqueKey[i] = t.uniqueKey();
            keyKeep[i] = t.uniqueKey() == null || t.keyKeep() == null
                    ? null
                    : t.keyKeep().toMillis();
            retryPolicy[i] = t.retryPolicy();
            scheduleId[i] = t.scheduleId();
            retryOf[i] = t.retryOf();
            startBy[i] = text(t.startBy());
            maxRun[i] = t.maxRun() == null ? null : Math.toIntExact(t.maxRun().toMillis());
            cancelRequestedAt[i] = text(t.cancelRequestedAt());
            cancelReason[i] = t.cancelReason();
            progress[i] = t.progress();
            steps[i] = RecordJson.encodeSteps(t.steps());
            failures[i] = RecordJson.encodeFailures(t.failures());
        }
        Object[][] columns = {
            ids,
            revisions,
            queueIds,
            eventIds,
            payloadVersions,
            attempts,
            maxAttempts,
            created,
            due,
            data,
            account,
            headers,
            uniqueKey,
            keyKeep,
            retryPolicy,
            scheduleId,
            retryOf,
            startBy,
            maxRun,
            cancelRequestedAt,
            cancelReason,
            progress,
            steps,
            failures
        };
        var arrays = new ArrayList<Array>();
        try (var ps = c.prepareStatement(insertSql)) {
            for (int i = 0; i < columns.length; i++) {
                var column = columns[i];
                var type = column instanceof Long[] ? "int8" : column instanceof Integer[] ? "int4" : "text";
                var array = c.createArrayOf(type, column);
                arrays.add(array);
                ps.setArray(i + 1, array);
            }
            var stored = new HashSet<Long>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) stored.add(rs.getLong(1));
            }
            return stored;
        } finally {
            for (var a : arrays) a.free();
        }
    }

    /**
     * Fills the outcome of each task that was not stored: its id exists, or its key is held by another task.
     * Returns the tasks that are neither, because the conflicting row went away meanwhile.
     */
    private ArrayList<Integer> classify(
            Connection c, List<TaskRecord> tasks, List<Integer> unresolved, EnqueueOutcome[] outcomes)
            throws SQLException {
        var retry = new ArrayList<Integer>();
        if (unresolved.isEmpty()) return retry;
        var existingIds = new HashSet<Long>();
        var idArray = c.createArrayOf(
                "int8", unresolved.stream().map(i -> tasks.get(i).id()).toArray(Long[]::new));
        try (var ps = c.prepareStatement("SELECT id FROM " + task + " WHERE id = ANY(?)")) {
            ps.setArray(1, idArray);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) existingIds.add(rs.getLong(1));
            }
        } finally {
            idArray.free();
        }
        var holders = new HashMap<String, Long>();
        var keys = unresolved.stream()
                .map(i -> tasks.get(i).uniqueKey())
                .filter(Objects::nonNull)
                .distinct()
                .toArray(String[]::new);
        if (keys.length > 0) {
            var keyArray = c.createArrayOf("text", keys);
            try (var ps = c.prepareStatement("SELECT unique_key, id FROM " + task + " WHERE unique_key = ANY(?)")) {
                ps.setArray(1, keyArray);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) holders.put(rs.getString(1), rs.getLong(2));
                }
            } finally {
                keyArray.free();
            }
        }
        for (var i : unresolved) {
            var t = tasks.get(i);
            if (existingIds.contains(t.id())) {
                outcomes[i] = EnqueueOutcome.EXISTS;
            } else if (t.uniqueKey() != null && holders.containsKey(t.uniqueKey())) {
                outcomes[i] = new EnqueueOutcome.DuplicateKey(holders.get(t.uniqueKey()));
            } else {
                retry.add(i);
            }
        }
        return retry;
    }

    // ---- workers -------------------------------------------------------------------------------------

    @Override
    public List<TaskRecord> claim(ClaimRequest r) {
        if (r.count() <= 0 || r.queueIds().isEmpty()) return List.of();
        boolean filter = r.knownEventIds() != null;
        if (filter && r.knownEventIds().isEmpty()) return List.of();
        var distinct = new LinkedHashSet<>(r.queueIds()).toArray(Integer[]::new);
        return withConnection(c -> {
            Array queueArray = c.createArrayOf("int4", distinct);
            Array eventArray =
                    filter ? c.createArrayOf("int4", r.knownEventIds().toArray(Integer[]::new)) : null;
            try (var ps = c.prepareStatement(filter ? claimKnownEventsSql : claimSql)) {
                int i = 1;
                ps.setArray(i++, queueArray);
                if (filter) ps.setArray(i++, eventArray);
                ps.setInt(i++, r.count());
                ps.setInt(i++, r.count());
                ps.setInt(i++, r.workerId());
                ps.setLong(i, r.lease().toMillis());
                return readAll(ps);
            } finally {
                queueArray.free();
                if (eventArray != null) eventArray.free();
            }
        });
    }

    @Override
    public List<LeaseStatus> renew(List<LeaseRef> leases, Duration lease) {
        if (leases.isEmpty()) return List.of();
        var ids = leases.stream().map(LeaseRef::id).toArray(Long[]::new);
        var revisions = leases.stream().map(LeaseRef::revision).toArray(Integer[]::new);
        var cancelByLease = withConnection(c -> {
            Array idArray = c.createArrayOf("int8", ids);
            Array revisionArray = c.createArrayOf("int4", revisions);
            try (var ps = c.prepareStatement(renewSql)) {
                ps.setLong(1, lease.toMillis());
                ps.setArray(2, idArray);
                ps.setArray(3, revisionArray);
                var result = new HashMap<LeaseRef, Boolean>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) result.put(new LeaseRef(rs.getLong(1), rs.getInt(2)), rs.getBoolean(3));
                }
                return result;
            } finally {
                idArray.free();
                revisionArray.free();
            }
        });
        var statuses = new ArrayList<LeaseStatus>(leases.size());
        for (var ref : leases) {
            var cancel = cancelByLease.get(ref);
            statuses.add(new LeaseStatus(ref.id(), cancel != null, Boolean.TRUE.equals(cancel)));
        }
        return statuses;
    }

    @Override
    public boolean ack(long id, int revision, boolean keep, boolean keepPayload) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(ackSql)) {
                ps.setLong(1, id);
                ps.setInt(2, revision);
                ps.setBoolean(3, keep);
                ps.setBoolean(4, keep);
                ps.setBoolean(5, keepPayload);
                ps.setLong(6, id);
                ps.setInt(7, revision);
                ps.setBoolean(8, keep);
                return single(ps) == 1;
            }
        });
    }

    @Override
    public FailOutcome fail(long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead) {
        return fail(false, id, revision, failure, retryAt, keepDead);
    }

    @Override
    public FailOutcome failExpired(long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead) {
        return fail(true, id, revision, failure, retryAt, keepDead);
    }

    /**
     * Locks the row under the fence, appends the failure in Java and writes the outcome in the same
     * transaction, so no other transition can slip in between the read of {@code failures} and the write.
     */
    private FailOutcome fail(
            boolean onlyIfExpired, long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead) {
        return inTransaction(c -> {
            int attempts;
            int maxAttempts;
            String failuresJson;
            boolean holdsKey;
            try (var ps = c.prepareStatement(failLockSql)) {
                ps.setLong(1, id);
                ps.setInt(2, revision);
                try (var rs = ps.executeQuery()) {
                    if (!rs.next()) return FailOutcome.LOST;
                    if (onlyIfExpired && !rs.getBoolean(5)) return FailOutcome.LOST;
                    attempts = rs.getInt(1);
                    maxAttempts = rs.getInt(2);
                    failuresJson = rs.getString(3);
                    holdsKey = rs.getBoolean(4);
                }
            }
            boolean dead = retryAt == null || attempts >= maxAttempts;
            if (dead && !keepDead && !holdsKey) {
                try (var ps = c.prepareStatement("DELETE FROM " + task + " WHERE id = ?")) {
                    ps.setLong(1, id);
                    ps.executeUpdate();
                }
                return FailOutcome.DEAD;
            }
            var failures = new ArrayList<FailureInfo>();
            var old = RecordJson.decodeFailures(failuresJson);
            if (old != null) failures.addAll(old);
            failures.add(failure);
            var json = RecordJson.encodeFailures(
                    failures.subList(Math.max(0, failures.size() - MAX_FAILURES), failures.size()));
            if (dead) {
                try (var ps = c.prepareStatement("UPDATE " + task + " SET state = 4, revision = revision + 1,"
                        + " state_changed = now(), lease_until = NULL, failures = ?::jsonb,"
                        + " data = CASE WHEN ? THEN data ELSE NULL END," + KEY_RELEASE + " WHERE id = ?")) {
                    ps.setString(1, json);
                    ps.setBoolean(2, keepDead);
                    ps.setLong(3, id);
                    ps.executeUpdate();
                }
                return FailOutcome.DEAD;
            }
            try (var ps = c.prepareStatement(
                    "UPDATE " + task + " SET state = 1, due = ?, revision = revision + 1,"
                            + " state_changed = now(), lease_until = NULL, worker_id = NULL, failures = ?::jsonb WHERE id = ?")) {
                setInstant(ps, 1, retryAt);
                ps.setString(2, json);
                ps.setLong(3, id);
                ps.executeUpdate();
            }
            return FailOutcome.RETRY;
        });
    }

    @Override
    public boolean reschedule(long id, int revision, Instant due) {
        if (due == null) throw new IllegalArgumentException("due is required");
        return update(rescheduleSql, ps -> {
            setInstant(ps, 1, due);
            ps.setLong(2, id);
            ps.setInt(3, revision);
        });
    }

    @Override
    public boolean unclaim(long id, int revision) {
        return update(unclaimSql, ps -> {
            ps.setLong(1, id);
            ps.setInt(2, revision);
        });
    }

    @Override
    public boolean saveSteps(long id, int revision, Map<String, StepResult> steps) {
        var json = steps == null || steps.isEmpty() ? null : RecordJson.encodeSteps(steps);
        return update(saveStepsSql, ps -> {
            ps.setString(1, json);
            ps.setLong(2, id);
            ps.setInt(3, revision);
        });
    }

    @Override
    public boolean progress(long id, int revision, String progress) {
        return update(progressSql, ps -> {
            ps.setString(1, progress);
            ps.setLong(2, id);
            ps.setInt(3, revision);
        });
    }

    // ---- maintenance ---------------------------------------------------------------------------------

    @Override
    public PromoteResult promote(int limit) {
        return withConnection(c -> {
            int moved = 0;
            if (limit > 0) {
                try (var ps = c.prepareStatement(promoteSql)) {
                    ps.setInt(1, limit);
                    moved = ps.executeUpdate();
                }
            }
            try (var ps = c.prepareStatement(nextDueSql);
                    var rs = ps.executeQuery()) {
                rs.next();
                return new PromoteResult(moved, getInstant(rs, 1));
            }
        });
    }

    @Override
    public List<ExpiredLease> reclaim(int limit) {
        if (limit <= 0) return List.of();
        return withConnection(c -> {
            try (var ps = c.prepareStatement(reclaimSql)) {
                ps.setInt(1, limit);
                var result = new ArrayList<ExpiredLease>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(new ExpiredLease(
                                rs.getLong(1),
                                rs.getInt(2),
                                rs.getInt(3),
                                rs.getInt(4),
                                rs.getInt(5),
                                rs.getBoolean(6)));
                    }
                }
                return result;
            }
        });
    }

    @Override
    public long purge(TaskState state, Duration retention, int limit) {
        if (!state.isTerminal()) throw new IllegalArgumentException("only terminal states are purged: " + state);
        if (limit <= 0) return 0;
        return withConnection(c -> {
            try (var ps = c.prepareStatement(purgeSql)) {
                ps.setInt(1, state.code());
                ps.setLong(2, retention.toMillis());
                ps.setInt(3, limit);
                return (long) ps.executeUpdate();
            }
        });
    }

    @Override
    public int releaseKeys(int limit) {
        if (limit <= 0) return 0;
        return withConnection(c -> {
            try (var ps = c.prepareStatement(releaseKeysSql)) {
                ps.setInt(1, limit);
                return ps.executeUpdate();
            }
        });
    }

    @Override
    public Instant nextDue() {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(nextDueSql);
                    var rs = ps.executeQuery()) {
                rs.next();
                return getInstant(rs, 1);
            }
        });
    }

    // ---- recurring definitions -----------------------------------------------------------------------

    @Override
    public void putRecurring(RecurringDefinition d, Instant nextFire) {
        withConnection(c -> {
            try (var ps = c.prepareStatement(putScheduleSql)) {
                ps.setString(1, d.scheduleId());
                ps.setInt(2, d.eventId());
                ps.setInt(3, d.payloadVersion());
                ps.setInt(4, d.queueId());
                ps.setString(5, d.account());
                ps.setString(6, d.cron());
                ps.setString(7, d.zone());
                ps.setString(8, d.misfire());
                ps.setInt(9, d.catchUpMax());
                if (d.fixedDelayMs() == null) ps.setNull(10, Types.BIGINT);
                else ps.setLong(10, d.fixedDelayMs());
                ps.setBoolean(11, d.overlapSkip());
                ps.setString(12, d.data());
                ps.setInt(13, d.maxAttempts());
                ps.setBoolean(14, d.paused());
                setInstant(ps, 15, nextFire);
                ps.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public boolean deleteRecurring(String scheduleId) {
        return update("DELETE FROM " + schedule + " WHERE id = ?", ps -> ps.setString(1, scheduleId));
    }

    @Override
    public Map<String, Instant> recurringSchedules() {
        return withConnection(c -> {
            var result = new HashMap<String, Instant>();
            try (var ps = c.prepareStatement("SELECT id, next_fire FROM " + schedule);
                    var rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString(1), getInstant(rs, 2));
                }
            }
            return result;
        });
    }

    @Override
    public List<DueSchedule> dueRecurring(Instant upTo, int limit) {
        if (limit <= 0) return List.of();
        return withConnection(c -> {
            try (var ps = c.prepareStatement(dueScheduleSql)) {
                setInstant(ps, 1, upTo);
                ps.setInt(2, limit);
                var result = new ArrayList<DueSchedule>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        var definition = readDefinition(rs);
                        result.add(new DueSchedule(definition.scheduleId(), getInstant(rs, "next_fire"), definition));
                    }
                }
                return result;
            }
        });
    }

    /**
     * An occurrence whose unique key is held by another task is not stored, but the definition still advances
     * and the commit counts: that is how overlap protection skips a fire time.
     */
    @Override
    public boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord o) {
        return withConnection(c -> {
            if (o.uniqueKey() != null) releaseExpiredKeys(c, List.of(o));
            try (var ps = c.prepareStatement(commitScheduleSql)) {
                setInstant(ps, 1, nextFire);
                ps.setString(2, due.scheduleId());
                ps.setLong(3, due.definition().revision());
                setInstant(ps, 4, due.fireAt());
                ps.setLong(5, o.id());
                ps.setInt(6, o.queueId());
                ps.setInt(7, o.eventId());
                ps.setInt(8, o.payloadVersion());
                ps.setInt(9, o.maxAttempts());
                setInstant(ps, 10, o.created() == null ? Instant.now() : o.created());
                setInstant(ps, 11, due.fireAt());
                ps.setString(12, o.data());
                ps.setString(13, o.account());
                ps.setString(14, RecordJson.encodeHeaders(o.headers()));
                ps.setString(15, o.retryPolicy());
                ps.setString(16, o.scheduleId() == null ? due.scheduleId() : o.scheduleId());
                ps.setString(17, o.uniqueKey());
                if (o.uniqueKey() == null || o.keyKeep() == null) ps.setNull(18, Types.BIGINT);
                else ps.setLong(18, o.keyKeep().toMillis());
                setInstant(ps, 19, o.startBy());
                if (o.maxRun() == null) ps.setNull(20, Types.INTEGER);
                else ps.setInt(20, Math.toIntExact(o.maxRun().toMillis()));
                return single(ps) == 1;
            }
        });
    }

    // ---- inspection ----------------------------------------------------------------------------------

    @Override
    public Optional<TaskRecord> getTask(long id) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement("SELECT * FROM " + task + " WHERE id = ?")) {
                ps.setLong(1, id);
                try (var rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(read(rs)) : Optional.empty();
                }
            }
        });
    }

    /** Every registered queue appears, with zeros when it has no tasks. Counts by a scan of the task table. */
    @Override
    public Map<Integer, StateCounts> counts() {
        int states = TaskState.values().length;
        return withConnection(c -> {
            var raw = new TreeMap<Integer, long[]>();
            try (var ps = c.prepareStatement("SELECT id FROM " + queue);
                    var rs = ps.executeQuery()) {
                while (rs.next()) raw.put(rs.getInt(1), new long[states]);
            }
            try (var ps = c.prepareStatement(
                            "SELECT queue_id, state, count(*) FROM " + task + " GROUP BY queue_id, state");
                    var rs = ps.executeQuery()) {
                while (rs.next()) {
                    raw.computeIfAbsent(rs.getInt(1), k -> new long[states])[rs.getInt(2)] = rs.getLong(3);
                }
            }
            var result = new LinkedHashMap<Integer, StateCounts>();
            raw.forEach((q, n) -> result.put(q, new StateCounts(n[0], n[1], n[2], n[3], n[4], n[5], n[6])));
            return result;
        });
    }

    /**
     * The cursor is the {@code (state_changed, id)} of the {@code afterId} task when it still exists, else
     * {@code id > afterId}. A cursor task that moved to another state since continues from its new state
     * change time.
     */
    @Override
    public List<TaskRecord> tasks(TaskState state, Integer queueId, long afterId, int limit) {
        if (limit <= 0) return List.of();
        var sql = new StringBuilder("SELECT * FROM " + task + " WHERE state = ?");
        if (queueId != null) sql.append(" AND queue_id = ?");
        if (afterId != 0) {
            sql.append(" AND ((state_changed, id) > (SELECT state_changed, id FROM ")
                    .append(task)
                    .append(" WHERE id = ?) OR (NOT EXISTS (SELECT 1 FROM ")
                    .append(task)
                    .append(" WHERE id = ?) AND id > ?))");
        }
        sql.append(" ORDER BY state_changed, id LIMIT ?");
        return withConnection(c -> {
            try (var ps = c.prepareStatement(sql.toString())) {
                int i = 1;
                ps.setInt(i++, state.code());
                if (queueId != null) ps.setInt(i++, queueId);
                if (afterId != 0) {
                    ps.setLong(i++, afterId);
                    ps.setLong(i++, afterId);
                    ps.setLong(i++, afterId);
                }
                ps.setInt(i, limit);
                return readAll(ps);
            }
        });
    }

    // ---- JDBC helpers --------------------------------------------------------------------------------

    @FunctionalInterface
    private interface SqlFunction<T> {
        T apply(Connection c) throws SQLException;
    }

    @FunctionalInterface
    private interface SqlBinder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    private boolean update(String sql, SqlBinder binder) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(sql)) {
                binder.bind(ps);
                return ps.executeUpdate() == 1;
            }
        });
    }

    private <T> T withConnection(SqlFunction<T> fn) {
        try (var c = dataSource.getConnection()) {
            boolean autoCommit = c.getAutoCommit();
            try {
                var result = fn.apply(c);
                if (!autoCommit) c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                if (!autoCommit) c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new TaskStoreException("PostgreSQL task store operation failed", e);
        }
    }

    private <T> T inTransaction(SqlFunction<T> fn) {
        try (var c = dataSource.getConnection()) {
            boolean autoCommit = c.getAutoCommit();
            if (autoCommit) c.setAutoCommit(false);
            try {
                var result = fn.apply(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            } finally {
                if (autoCommit) c.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new TaskStoreException("PostgreSQL task store operation failed", e);
        }
    }

    private static long single(PreparedStatement ps) throws SQLException {
        try (var rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static List<TaskRecord> readAll(PreparedStatement ps) throws SQLException {
        var result = new ArrayList<TaskRecord>();
        try (var rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(read(rs));
            }
        }
        return result;
    }

    private static TaskRecord read(ResultSet rs) throws SQLException {
        return TaskRecord.builder()
                .id(rs.getLong("id"))
                .revision(rs.getInt("revision"))
                .queueId(rs.getInt("queue_id"))
                .eventId(rs.getInt("event_id"))
                .payloadVersion(rs.getInt("payload_version"))
                .state(TaskState.fromCode(rs.getInt("state")))
                .attempts(rs.getInt("attempts"))
                .maxAttempts(rs.getInt("max_attempts"))
                .created(getInstant(rs, "created"))
                .due(getInstant(rs, "due"))
                .stateChanged(getInstant(rs, "state_changed"))
                .data(rs.getString("data"))
                .workerId(getInteger(rs, "worker_id"))
                .leaseUntil(getInstant(rs, "lease_until"))
                .claimedAt(getInstant(rs, "claimed_at"))
                .account(rs.getString("account"))
                .headers(RecordJson.decodeHeaders(rs.getString("headers")))
                .uniqueKey(rs.getString("unique_key"))
                .keyKeep(getMillis(rs, "key_keep_ms"))
                .keyUntil(getInstant(rs, "key_until"))
                .retryPolicy(rs.getString("retry_policy"))
                .scheduleId(rs.getString("schedule_id"))
                .retryOf(getLong(rs, "retry_of"))
                .startBy(getInstant(rs, "start_by"))
                .maxRun(getMillis(rs, "max_run_ms"))
                .cancelRequestedAt(getInstant(rs, "cancel_requested_at"))
                .cancelReason(rs.getString("cancel_reason"))
                .progress(rs.getString("progress"))
                .steps(RecordJson.decodeSteps(rs.getString("steps")))
                .failures(RecordJson.decodeFailures(rs.getString("failures")))
                .build();
    }

    /** Microseconds, the resolution of timestamptz, so a value read back equals what was written. */
    private static Instant micros(Instant instant) {
        return instant.truncatedTo(ChronoUnit.MICROS);
    }

    private static String text(Instant instant) {
        return instant == null ? null : micros(instant).toString();
    }

    private static void setInstant(PreparedStatement ps, int index, Instant instant) throws SQLException {
        if (instant == null) {
            ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            ps.setObject(index, OffsetDateTime.ofInstant(micros(instant), ZoneOffset.UTC));
        }
    }

    private static Instant getInstant(ResultSet rs, int index) throws SQLException {
        var value = rs.getObject(index, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static Instant getInstant(ResultSet rs, String column) throws SQLException {
        var value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static Integer getInteger(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static Long getLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static Duration getMillis(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : Duration.ofMillis(value);
    }

    @Override
    public String toString() {
        return "PostgresTaskStore[" + prefix + "]";
    }

    // ---- wave 2: cancellation, expiry, admin, workers, schedule controls -------------------------------

    @Override
    public boolean finishCancelled(long id, int revision, String reason, boolean keepPayload) {
        return update(
                "UPDATE " + task + " SET state = 5, revision = revision + 1, state_changed = now(),"
                        + " cancel_reason = ?, cancel_requested_at = COALESCE(cancel_requested_at, now()),"
                        + " lease_until = NULL, data = CASE WHEN ? THEN data ELSE NULL END," + KEY_RELEASE + FENCE,
                ps -> {
                    ps.setString(1, reason);
                    ps.setBoolean(2, keepPayload);
                    ps.setLong(3, id);
                    ps.setInt(4, revision);
                });
    }

    /** Oldest deadline first, through the {@code _start_by} partial index. */
    @Override
    public List<Long> expire(int limit) {
        if (limit <= 0) return List.of();
        return withConnection(c -> {
            try (var ps = c.prepareStatement("WITH x AS (SELECT id FROM " + task
                    + "   WHERE start_by <= now() AND state < 2 ORDER BY start_by, id LIMIT ? FOR UPDATE SKIP LOCKED)"
                    + " UPDATE " + task + " t SET state = 6, revision = t.revision + 1, state_changed = now(),"
                    + KEY_RELEASE + " FROM x WHERE t.id = x.id RETURNING t.id")) {
                ps.setInt(1, limit);
                var ids = new ArrayList<Long>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) ids.add(rs.getLong(1));
                }
                return ids;
            }
        });
    }

    @Override
    public int purgeWorkers(Duration staleAfter) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(
                    "DELETE FROM " + worker + " WHERE last_seen < now() - ? * interval '1 millisecond'")) {
                ps.setLong(1, staleAfter.toMillis());
                return ps.executeUpdate();
            }
        });
    }

    /**
     * Admin cancel keeps the payload, so that a cancelled task can be retried as it was. The two branches
     * exclude each other by state, so at most one touches the row.
     */
    @Override
    public CancelOutcome cancel(long id, int revision, String reason) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement("WITH c AS (UPDATE " + task
                    + "   SET state = 5, revision = revision + 1, state_changed = now(), cancel_requested_at = now(),"
                    + "   cancel_reason = ?," + KEY_RELEASE
                    + "   WHERE id = ? AND revision = ? AND state IN (0, 1) RETURNING 1),"
                    + " r AS (UPDATE " + task + " SET cancel_requested_at = now(), cancel_reason = ?" + FENCE
                    + "   RETURNING 2)"
                    + " SELECT COALESCE((SELECT 1 FROM c), (SELECT 2 FROM r), 0)")) {
                ps.setString(1, reason);
                ps.setLong(2, id);
                ps.setInt(3, revision);
                ps.setString(4, reason);
                ps.setLong(5, id);
                ps.setInt(6, revision);
                return switch ((int) single(ps)) {
                    case 1 -> CancelOutcome.CANCELLED;
                    case 2 -> CancelOutcome.REQUESTED;
                    default -> CancelOutcome.STALE;
                };
            }
        });
    }

    /**
     * Also clears {@code start_by}: an expired task keeps a deadline in the past, which would expire the retry
     * at once.
     */
    @Override
    public boolean retry(long id, int revision) {
        return update(
                "UPDATE " + task + " SET state = 0, due = now(), attempts = 0, revision = revision + 1,"
                        + " state_changed = now(), worker_id = NULL, lease_until = NULL, cancel_requested_at = NULL,"
                        + " cancel_reason = NULL, unique_key = NULL, key_keep_ms = NULL, key_until = NULL,"
                        + " start_by = NULL WHERE id = ? AND revision = ? AND state IN (4, 5, 6)",
                ps -> {
                    ps.setLong(1, id);
                    ps.setInt(2, revision);
                });
    }

    @Override
    public boolean delete(long id, int revision) {
        return update("DELETE FROM " + task + " WHERE id = ? AND revision = ? AND state <> 2", ps -> {
            ps.setLong(1, id);
            ps.setInt(2, revision);
        });
    }

    @Override
    public void pauseQueue(int queueId, boolean paused) {
        update("UPDATE " + queue + " SET paused = ? WHERE id = ?", ps -> {
            ps.setBoolean(1, paused);
            ps.setInt(2, queueId);
        });
    }

    @Override
    public List<QueueInfo> queues() {
        return withConnection(c -> {
            var result = new ArrayList<QueueInfo>();
            try (var ps = c.prepareStatement("SELECT id, name, paused FROM " + queue + " ORDER BY id");
                    var rs = ps.executeQuery()) {
                while (rs.next()) result.add(new QueueInfo(rs.getInt(1), rs.getString(2), rs.getBoolean(3)));
            }
            return result;
        });
    }

    @Override
    public void heartbeat(int workerId, int running) {
        update("UPDATE " + worker + " SET last_seen = now(), running = ? WHERE id = ?", ps -> {
            ps.setInt(1, running);
            ps.setInt(2, workerId);
        });
    }

    @Override
    public List<WorkerInfo> workers() {
        return withConnection(c -> {
            var result = new ArrayList<WorkerInfo>();
            try (var ps = c.prepareStatement(
                            "SELECT id, label, started_at, last_seen, running FROM " + worker + " ORDER BY id");
                    var rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new WorkerInfo(
                            rs.getInt(1), rs.getString(2), getInstant(rs, 3), getInstant(rs, 4), rs.getInt(5)));
                }
            }
            return result;
        });
    }

    @Override
    public List<RecurringSchedule> recurringDefinitions() {
        return withConnection(c -> {
            var result = new ArrayList<RecurringSchedule>();
            try (var ps = c.prepareStatement("SELECT * FROM " + schedule + " ORDER BY id");
                    var rs = ps.executeQuery()) {
                while (rs.next()) result.add(new RecurringSchedule(readDefinition(rs), getInstant(rs, "next_fire")));
            }
            return result;
        });
    }

    @Override
    public boolean pauseRecurring(String scheduleId, boolean paused) {
        return update("UPDATE " + schedule + " SET paused = ? WHERE id = ?", ps -> {
            ps.setBoolean(1, paused);
            ps.setString(2, scheduleId);
        });
    }

    /** A commit already in flight for the old fire time fails, because it checks the next fire time it read. */
    @Override
    public boolean setNextFire(String scheduleId, Instant nextFire) {
        return update("UPDATE " + schedule + " SET next_fire = ? WHERE id = ?", ps -> {
            setInstant(ps, 1, nextFire);
            ps.setString(2, scheduleId);
        });
    }

    private static RecurringDefinition readDefinition(ResultSet rs) throws SQLException {
        return new RecurringDefinition(
                rs.getString("id"),
                rs.getInt("event_id"),
                rs.getInt("payload_version"),
                rs.getInt("queue_id"),
                rs.getString("account"),
                rs.getString("cron"),
                rs.getString("zone"),
                rs.getString("misfire"),
                rs.getInt("catch_up_max"),
                getLong(rs, "fixed_delay_ms"),
                rs.getBoolean("overlap_skip"),
                rs.getString("data"),
                rs.getInt("max_attempts"),
                rs.getBoolean("paused"),
                rs.getLong("rev"));
    }
}

package today.bonfire.oss.bth4j.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import today.bonfire.oss.bth4j.common.Json;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
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
import today.bonfire.oss.bth4j.spi.PutRecurringOutcome;
import today.bonfire.oss.bth4j.spi.PutRecurringResult;
import today.bonfire.oss.bth4j.spi.QueueGrant;
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
import today.bonfire.oss.bth4j.spi.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.WorkerInfo;

/**
 * {@link FullStore} on SQLite (3.45 or newer in WAL mode, tested on 3.54): one row per task in
 * {@code <prefix>_task} with a state column, name registries in {@code <prefix>_queue} and
 * {@code <prefix>_type}, workers in {@code <prefix>_worker}, recurring definitions in
 * {@code <prefix>_schedule}, counters in {@code <prefix>_meta}. All tables are STRICT; times are epoch
 * milliseconds from the {@link Clock} given to the store, never from an SQL function. Runners that share the
 * database file share the work, in one process or in several processes on one machine.
 * <p>
 * SQLite allows one writer at a time, so every write runs in a {@code BEGIN IMMEDIATE} transaction that the
 * store issues itself, whatever the pool does to the connection: a transaction that reads first and writes
 * later can fail with SQLITE_BUSY instead of waiting for the lock. A claim is several small statements in one
 * such transaction. The fenced transitions are {@code UPDATE ... WHERE id = ? AND revision = ? AND state = 2},
 * and {@link #fail} reads the failures it appends to inside the same transaction. A task removed while its
 * unique key's keep window is open leaves a key-only row, which holds the key until the window ends and is
 * invisible to every read.
 * <p>
 * {@link #init()} refuses connections that are not in WAL mode or whose busy timeout is below
 * {@value #MIN_BUSY_TIMEOUT_MS} ms, because without them concurrent runners fail with SQLITE_BUSY. The
 * {@code synchronous} setting is the application's choice: FULL keeps every confirmed commit through a power
 * cut, NORMAL survives an application crash but can lose the last commits on power loss. The database file must
 * be on a local disk; WAL does not work over network file systems.
 * <p>
 * The tables are created by {@link #init()}, or by running {@link #schemaStatements()} from the application's
 * migration tool. There are no foreign keys; the registries are only written through {@link #queueId} and
 * {@link #typeId}. Only {@code java.sql} is used: the application supplies the JDBC driver and a
 * {@link DataSource}, normally pooled.
 */
public class SqliteTaskStore implements FullStore {
    /** The schema this class writes, stored in the {@code meta} table. {@link #init} refuses tables written with another one. */
    public static final int SCHEMA_VERSION = 1;

    /** The smallest {@code busy_timeout}, in milliseconds, that {@link #init} accepts. */
    public static final int MIN_BUSY_TIMEOUT_MS = 1000;

    private static final Pattern PREFIX = Pattern.compile("[a-z][a-z0-9_]{0,40}");
    private static final int MAX_FAILURES = 20;
    private static final int MAX_REGISTRY_ID = Short.MAX_VALUE;
    private static final String FAILED = "SQLite task store operation failed";

    private static final String NEXT_TASK_ID = "next_task_id";
    private static final String NEXT_WORKER_ID = "next_worker_id";
    private static final String NEXT_QUEUE_ID = "next_queue_id";
    private static final String NEXT_TYPE_ID = "next_type_id";
    private static final String NEXT_SCHEDULE_REV = "next_schedule_rev";

    /**
     * Releases the key at the end of the task, or starts its keep window; the right-hand sides see old values.
     * The statements that use it bind the current time as {@code ?1}.
     */
    private static final String KEY_RELEASE =
            " unique_key = CASE WHEN key_keep_ms IS NULL THEN NULL ELSE unique_key END,"
                    + " key_until = CASE WHEN key_keep_ms IS NULL OR unique_key IS NULL THEN NULL"
                    + " ELSE ?1 + key_keep_ms END";

    /**
     * Store-internal state code of a key-only row: a task removed while its unique key's keep window is open.
     * The row keeps its id, queue, type, key and window, so the unique index still answers an enqueue of that key
     * with this id; nothing else reads it, and the window's end deletes it.
     */
    private static final int KEY_ONLY = 7;

    /** Turns a row into a key-only row; each statement sets the revision and, when the task ends here, key_until. */
    private static final String TO_KEY_ONLY = " state = " + KEY_ONLY + ", state_changed = ?1, data = NULL,"
            + " account = NULL, headers = NULL, retry_policy = NULL, schedule_id = NULL, retry_of = NULL,"
            + " start_by = NULL, max_run_ms = NULL, cancel_requested_at = NULL, cancel_reason = NULL,"
            + " progress = NULL, steps = NULL, failures = NULL, worker_id = NULL, lease_until = NULL,"
            + " claimed_at = NULL";

    /** Starts the keep window of a task that ends here as a key-only row. */
    private static final String KEY_WINDOW = " key_until = ?1 + key_keep_ms";

    /** Parameters ?2 and ?3 are the id and the revision. */
    private static final String FENCE = " WHERE id = ?2 AND revision = ?3 AND state = 2";

    private final DataSource dataSource;
    private final Clock clock;
    private final String prefix;
    private final String meta;
    private final String queue;
    private final String type;
    private final String worker;
    private final String task;
    private final String schedule;

    private final String insertSql;
    private final String renewSql;
    private final String finishCancelledSql;
    private final String finishCancelledExpiredSql;
    private final String ackDeleteSql;
    private final String ackKeyOnlySql;
    private final String ackKeepSql;
    private final String failLockSql;
    private final String rescheduleSql;
    private final String unclaimSql;
    private final String saveStepsSql;
    private final String progressSql;
    private final String nextDueSql;
    private final String reclaimSql;
    private final String endLeasesSql;
    private final String commitInsertSql;

    /**
     * @param dataSource connections to the database; the store does not close it
     * @param prefix     table name prefix, lower case letters, digits and underscores, at most 41 characters.
     *                   Runners with the same prefix on the same database file share the work.
     */
    public SqliteTaskStore(DataSource dataSource, String prefix) {
        this(dataSource, prefix, Clock.systemUTC());
    }

    /**
     * @param clock the time source of every due time, lease, retention and key window; tests pass a clock they
     *              control. Runners that share a database file should agree on the time, which they do when they run
     *              on one machine
     */
    public SqliteTaskStore(DataSource dataSource, String prefix, Clock clock) {
        if (dataSource == null) throw new TaskConfigurationException("DataSource is required");
        if (prefix == null || !PREFIX.matcher(prefix).matches()) {
            throw new TaskConfigurationException("prefix must match " + PREFIX.pattern());
        }
        if (prefix.equals("sqlite") || prefix.startsWith("sqlite_")) {
            throw new TaskConfigurationException("prefix " + prefix + " would make table names starting with sqlite_,"
                    + " which SQLite reserves for itself");
        }
        if (clock == null) throw new TaskConfigurationException("clock is required");
        this.dataSource = dataSource;
        this.clock = clock;
        this.prefix = prefix;
        this.meta = prefix + "_meta";
        this.queue = prefix + "_queue";
        this.type = prefix + "_type";
        this.worker = prefix + "_worker";
        this.task = prefix + "_task";
        this.schedule = prefix + "_schedule";

        // ON CONFLICT without a target covers both the id and the unique key; the caller tells the two apart
        insertSql = "INSERT INTO " + task
                + " (id, revision, queue_id, type_id, payload_version, state, attempts, max_attempts, created,"
                + " due, state_changed, data, account, headers, unique_key, key_keep_ms, retry_policy, schedule_id,"
                + " retry_of, start_by, max_run_ms, cancel_requested_at, cancel_reason, progress, steps, failures)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT DO NOTHING";
        commitInsertSql = "INSERT INTO " + task
                + " (id, revision, queue_id, type_id, payload_version, state, attempts, max_attempts, created,"
                + " due, state_changed, data, account, headers, retry_policy, schedule_id, unique_key, key_keep_ms,"
                + " start_by, max_run_ms)"
                + " VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT DO NOTHING";
        renewSql = "UPDATE " + task + " SET lease_until = ?1 + ?2 WHERE id = ?3 AND revision = ?4 AND state = 2"
                + " AND lease_until > ?1 RETURNING cancel_requested_at IS NOT NULL";
        finishCancelledSql = "UPDATE " + task + " SET state = 5, revision = revision + 1, state_changed = ?1,"
                + " cancel_reason = ?4, cancel_requested_at = COALESCE(cancel_requested_at, ?1),"
                + " lease_until = NULL, data = CASE WHEN ?5 THEN data ELSE NULL END," + KEY_RELEASE + FENCE;
        finishCancelledExpiredSql = finishCancelledSql + " AND lease_until <= ?1";
        // keep = false removes the row, or leaves only its key while the key has a keep window
        ackDeleteSql = "DELETE FROM " + task + " WHERE id = ?1 AND revision = ?2 AND state = 2"
                + " AND (key_keep_ms IS NULL OR unique_key IS NULL)";
        ackKeyOnlySql = "UPDATE " + task + " SET" + TO_KEY_ONLY + ", revision = revision + 1," + KEY_WINDOW + FENCE
                + " AND key_keep_ms IS NOT NULL AND unique_key IS NOT NULL";
        ackKeepSql = "UPDATE " + task + " SET state = 3, revision = revision + 1, state_changed = ?1,"
                + " lease_until = NULL, data = CASE WHEN ?4 THEN data ELSE NULL END," + KEY_RELEASE + FENCE;
        failLockSql = "SELECT attempts, max_attempts, failures,"
                + " key_keep_ms IS NOT NULL AND unique_key IS NOT NULL, lease_until <= ?1"
                + " FROM " + task + FENCE;
        rescheduleSql = "UPDATE " + task + " SET state = 1, due = ?4, attempts = attempts - 1,"
                + " revision = revision + 1, state_changed = ?1, lease_until = NULL, worker_id = NULL" + FENCE;
        unclaimSql = "UPDATE " + task + " SET state = 0, attempts = attempts - 1,"
                + " revision = revision + 1, state_changed = ?1, lease_until = NULL, worker_id = NULL" + FENCE;
        saveStepsSql = "UPDATE " + task + " SET steps = ?1" + FENCE;
        progressSql = "UPDATE " + task + " SET progress = ?1" + FENCE;
        nextDueSql = "SELECT min(due) FROM " + task + " WHERE state = 1";
        reclaimSql = "SELECT id, revision, attempts, max_attempts, type_id, cancel_requested_at IS NOT NULL FROM "
                + task + " WHERE state = 2 AND lease_until <= ? ORDER BY lease_until, id LIMIT ?";
        endLeasesSql = "UPDATE " + task + " SET lease_until = claimed_at"
                + " WHERE id IN (SELECT value FROM json_each(?)) AND state = 2 AND worker_id = ?";
    }

    // ---- schema --------------------------------------------------------------------------------------

    @Override
    public StoreInfo info() {
        return new StoreInfo("sqlite", SCHEMA_VERSION);
    }

    /** The DDL for this prefix, idempotent; for migration tools that manage the schema themselves. */
    public List<String> schemaStatements() {
        return List.of(
                "CREATE TABLE IF NOT EXISTS " + meta
                        + " (key TEXT NOT NULL PRIMARY KEY, value INTEGER NOT NULL) STRICT",
                "CREATE TABLE IF NOT EXISTS " + queue + " ("
                        + " id INTEGER PRIMARY KEY,"
                        + " name TEXT NOT NULL UNIQUE,"
                        + " paused INTEGER NOT NULL) STRICT",
                "CREATE TABLE IF NOT EXISTS " + type + " (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE) STRICT",
                "CREATE TABLE IF NOT EXISTS " + worker + " ("
                        + " id INTEGER PRIMARY KEY,"
                        + " label TEXT NOT NULL,"
                        + " started_at INTEGER NOT NULL,"
                        + " last_seen INTEGER NOT NULL,"
                        + " running INTEGER NOT NULL,"
                        + " accepting INTEGER NOT NULL,"
                        + " queue_ids TEXT NOT NULL,"
                        + " type_ids TEXT NOT NULL) STRICT",
                "CREATE TABLE IF NOT EXISTS " + task + " ("
                        + " id INTEGER PRIMARY KEY,"
                        + " revision INTEGER NOT NULL,"
                        + " queue_id INTEGER NOT NULL,"
                        + " type_id INTEGER NOT NULL,"
                        + " payload_version INTEGER NOT NULL,"
                        + " state INTEGER NOT NULL,"
                        + " attempts INTEGER NOT NULL,"
                        + " max_attempts INTEGER NOT NULL,"
                        + " created INTEGER NOT NULL,"
                        + " due INTEGER NOT NULL,"
                        + " state_changed INTEGER NOT NULL,"
                        + " data TEXT,"
                        + " worker_id INTEGER,"
                        + " lease_until INTEGER,"
                        + " claimed_at INTEGER,"
                        + " account TEXT,"
                        + " headers TEXT,"
                        + " unique_key TEXT,"
                        + " key_keep_ms INTEGER,"
                        + " key_until INTEGER,"
                        + " retry_policy TEXT,"
                        + " schedule_id TEXT,"
                        + " retry_of INTEGER,"
                        + " start_by INTEGER,"
                        + " max_run_ms INTEGER,"
                        + " cancel_requested_at INTEGER,"
                        + " cancel_reason TEXT,"
                        + " progress TEXT,"
                        + " steps TEXT,"
                        + " failures TEXT) STRICT",
                "CREATE INDEX IF NOT EXISTS " + task + "_queued ON " + task
                        + " (queue_id, due, id, type_id) WHERE state = 0",
                "CREATE INDEX IF NOT EXISTS " + task + "_scheduled ON " + task + " (due) WHERE state = 1",
                "CREATE INDEX IF NOT EXISTS " + task + "_leased ON " + task + " (lease_until) WHERE state = 2",
                // an OR of the four codes: SQLite uses a partial index only when the query repeats a term of
                // its condition, and a query for one state repeats one of these
                "CREATE INDEX IF NOT EXISTS " + task + "_finished ON " + task
                        + " (state, state_changed, id) WHERE state = 3 OR state = 4 OR state = 5 OR state = 6",
                "CREATE INDEX IF NOT EXISTS " + task + "_finished_queue ON " + task
                        + " (queue_id, state, state_changed, id) WHERE state = 3 OR state = 4 OR state = 5 OR state = 6",
                "CREATE INDEX IF NOT EXISTS " + task + "_counts ON " + task + " (queue_id, state) WHERE state < "
                        + KEY_ONLY,
                "CREATE UNIQUE INDEX IF NOT EXISTS " + task + "_unique_key ON " + task
                        + " (unique_key) WHERE unique_key IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS " + task + "_key_until ON " + task
                        + " (key_until) WHERE key_until IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS " + task + "_start_by ON " + task
                        + " (start_by) WHERE start_by IS NOT NULL AND state < 2",
                "CREATE TABLE IF NOT EXISTS " + schedule + " ("
                        + " id TEXT NOT NULL PRIMARY KEY,"
                        + " type_id INTEGER NOT NULL,"
                        + " payload_version INTEGER NOT NULL,"
                        + " queue_id INTEGER NOT NULL,"
                        + " account TEXT,"
                        + " cron TEXT,"
                        + " zone TEXT,"
                        + " misfire TEXT,"
                        + " catch_up_max INTEGER,"
                        + " fixed_delay_ms INTEGER,"
                        + " overlap_skip INTEGER NOT NULL,"
                        + " data TEXT,"
                        + " max_attempts INTEGER NOT NULL,"
                        + " max_run_ms INTEGER,"
                        + " paused INTEGER NOT NULL,"
                        + " rev INTEGER NOT NULL,"
                        + " occurrence_id INTEGER,"
                        + " next_fire INTEGER NOT NULL) STRICT",
                "CREATE INDEX IF NOT EXISTS " + schedule + "_due ON " + schedule + " (next_fire) WHERE paused = 0",
                "INSERT INTO " + meta + " (key, value) VALUES ('schema_version', " + SCHEMA_VERSION + "),"
                        + " ('" + NEXT_TASK_ID + "', 1), ('" + NEXT_WORKER_ID + "', 1), ('" + NEXT_QUEUE_ID
                        + "', 1), ('" + NEXT_TYPE_ID + "', 1), ('" + NEXT_SCHEDULE_REV + "', 1)"
                        + " ON CONFLICT (key) DO NOTHING");
    }

    /**
     * Checks the connection settings and the schema version, and creates the tables and indexes when there is no
     * schema yet. A matching schema runs no DDL and takes no write lock. Safe to call from several runners at
     * once: they serialize on the write lock, and the one that finds the schema made by another one only checks it.
     *
     * @throws TaskConfigurationException when the connection is not in WAL mode, or its busy timeout is below
     *                                    {@value #MIN_BUSY_TIMEOUT_MS} ms
     * @throws TaskStoreException         when the tables carry another schema version, or a task table exists
     *                                    without a version
     */
    @Override
    public void init() {
        withConnection(c -> {
            checkConnectionSettings(c);
            if (schemaPresent(c)) return null;
            return immediate(c, tx -> {
                if (schemaPresent(tx)) return null;
                try (var st = tx.createStatement()) {
                    for (var sql : schemaStatements()) {
                        st.execute(sql);
                    }
                }
                return null;
            });
        });
    }

    private static void checkConnectionSettings(Connection c) throws SQLException {
        var mode = pragma(c, "journal_mode");
        if (!"wal".equalsIgnoreCase(mode)) {
            throw new TaskConfigurationException("the SQLite journal_mode is " + mode + " and this store needs wal."
                    + " Set it on every connection, for example with sqlite-jdbc"
                    + " SQLiteConfig.setJournalMode(SQLiteConfig.JournalMode.WAL) or journal_mode=WAL in the JDBC URL");
        }
        var timeout = pragma(c, "busy_timeout");
        long millis;
        try {
            millis = Long.parseLong(timeout);
        } catch (NumberFormatException e) {
            millis = 0;
        }
        if (millis < MIN_BUSY_TIMEOUT_MS) {
            throw new TaskConfigurationException("the SQLite busy_timeout is " + timeout + " ms and this store needs"
                    + " at least " + MIN_BUSY_TIMEOUT_MS + " ms, so that a writer waits for the write lock."
                    + " Set it on every connection, for example with sqlite-jdbc SQLiteConfig.setBusyTimeout(5000)"
                    + " or busy_timeout=5000 in the JDBC URL");
        }
    }

    private static String pragma(Connection c, String name) throws SQLException {
        try (var st = c.createStatement();
                var rs = st.executeQuery("PRAGMA " + name)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /** @return whether the schema of this version is there; refuses another version and a layout without a version */
    private boolean schemaPresent(Connection c) throws SQLException {
        boolean hasMeta = tableExists(c, meta);
        if (!hasMeta) {
            if (tableExists(c, task)) {
                throw new TaskStoreException("table " + task + " exists without " + meta
                        + ": an older bth4j layout, this library needs schema version " + SCHEMA_VERSION);
            }
            return false;
        }
        var version = queryOne(c, "SELECT value FROM " + meta + " WHERE key = 'schema_version'", rs -> rs.getLong(1));
        if (version == null || version != SCHEMA_VERSION) {
            throw new TaskStoreException("schema version " + version + ", this library needs " + SCHEMA_VERSION);
        }
        return true;
    }

    private static boolean tableExists(Connection c, String name) throws SQLException {
        return queryOne(c, "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", rs -> true, name) != null;
    }

    // ---- identity and registries ---------------------------------------------------------------------

    /**
     * The ids come from a counter row in the meta table, advanced in the write transaction, so the range is
     * contiguous and never handed out twice.
     */
    @Override
    public long allocateIds(int count) {
        if (count < 1) throw new IllegalArgumentException("count must be positive: " + count);
        return inTransaction(c -> nextValue(c, NEXT_TASK_ID, count));
    }

    /** The first of {@code count} values from the counter {@code key}; the counter row holds the next unused one. */
    private long nextValue(Connection c, String key, int count) throws SQLException {
        var first = queryOne(
                c,
                "UPDATE " + meta + " SET value = value + ?1 WHERE key = ?2 RETURNING value - ?1",
                rs -> rs.getLong(1),
                (long) count,
                key);
        if (first == null) throw new TaskStoreException("counter " + key + " is missing from " + meta);
        return first;
    }

    @Override
    public long idHighWaterMark() {
        return withConnection(c -> {
            var next = queryOne(c, "SELECT value FROM " + meta + " WHERE key = ?", rs -> rs.getLong(1), NEXT_TASK_ID);
            if (next == null) throw new TaskStoreException("counter " + NEXT_TASK_ID + " is missing from " + meta);
            return next;
        });
    }

    @Override
    public int allocateWorkerId(String label, Set<Integer> queueIds, Set<Integer> typeIds) {
        if (label == null) throw new IllegalArgumentException("label is required");
        var queues = idsJson(queueIds);
        var types = idsJson(typeIds);
        return inTransaction(c -> {
            long now = clock.millis();
            long id = nextValue(c, NEXT_WORKER_ID, 1);
            update(
                    c,
                    "INSERT INTO " + worker + " (id, label, started_at, last_seen, running, accepting, queue_ids,"
                            + " type_ids) VALUES (?1, ?2, ?3, ?3, 0, 1, ?4, ?5)",
                    id,
                    label,
                    now,
                    queues,
                    types);
            return (int) id;
        });
    }

    @Override
    public int queueId(String name) {
        return registryId(queue, NEXT_QUEUE_ID, "queue", name);
    }

    @Override
    public int typeId(String name) {
        return registryId(type, NEXT_TYPE_ID, "type", name);
    }

    /**
     * Reads first and writes only for a new name, so that the lookups every runner repeats take no write lock.
     * A full registry leaves its counter where it was: the transaction that found it full rolls back.
     */
    private int registryId(String table, String counter, String kind, String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException(kind + " name is required");
        var found = withConnection(c -> registryLookup(c, table, name));
        if (found != null) return found;
        return inTransaction(c -> {
            var again = registryLookup(c, table, name);
            if (again != null) return again;
            long id = nextValue(c, counter, 1);
            if (id > MAX_REGISTRY_ID) {
                throw new TaskStoreException("the " + kind + " registry is full (" + MAX_REGISTRY_ID + " names)");
            }
            if (table.equals(queue))
                update(c, "INSERT INTO " + table + " (id, name, paused) VALUES (?, ?, 0)", id, name);
            else update(c, "INSERT INTO " + table + " (id, name) VALUES (?, ?)", id, name);
            return (int) id;
        });
    }

    private static Integer registryLookup(Connection c, String table, String name) throws SQLException {
        return queryOne(c, "SELECT id FROM " + table + " WHERE name = ?", rs -> rs.getInt(1), name);
    }

    @Override
    public Map<Integer, String> queueNames() {
        return names(queue);
    }

    @Override
    public Map<Integer, String> typeNames() {
        return names(type);
    }

    private Map<Integer, String> names(String table) {
        return withConnection(c -> {
            var result = new TreeMap<Integer, String>();
            queryEach(c, "SELECT id, name FROM " + table, rs -> result.put(rs.getInt(1), rs.getString(2)));
            return result;
        });
    }

    // ---- producers -----------------------------------------------------------------------------------

    @Override
    public EnqueueOutcome enqueue(TaskRecord task) {
        return enqueueAll(List.of(task)).getFirst();
    }

    /** Inserts the tasks one after the other in one transaction, so a batch is stored whole or not at all. */
    @Override
    public List<EnqueueOutcome> enqueueAll(List<TaskRecord> tasks) {
        if (tasks.isEmpty()) return List.of();
        return inTransaction(c -> enqueueAll(c, tasks));
    }

    /**
     * Stores a task on the caller's connection, inside the caller's transaction: the task becomes visible to
     * workers only when the caller commits, and disappears with a rollback. The connection is neither
     * committed nor closed. The task should come from {@code TaskClient.prepare}.
     * <p>
     * A write in SQLite that follows a read in the same transaction can fail with SQLITE_BUSY when another
     * writer committed in between. Start the caller's transaction as IMMEDIATE (with sqlite-jdbc,
     * {@code SQLiteConfig.setTransactionMode(SQLiteConfig.TransactionMode.IMMEDIATE)} on a pool whose connections
     * have auto-commit on), or make this call before the transaction reads. Call {@code TaskClient.prepare}
     * before the transaction begins: it can allocate ids and register queue and type names, writes that run on
     * another connection and would wait for this transaction's write lock until the busy timeout.
     *
     * @throws TaskStoreException when a statement fails; the caller's transaction is then aborted
     */
    public EnqueueOutcome enqueue(Connection connection, TaskRecord task) {
        if (connection == null) throw new IllegalArgumentException("connection is required");
        try {
            return enqueueAll(connection, List.of(task)).getFirst();
        } catch (SQLException e) {
            throw new TaskStoreException(FAILED, e);
        }
    }

    /**
     * On a caller's connection without an open transaction each statement commits on its own, so the row that
     * blocked an insert can be gone when {@link #classify} looks for it; the insert is then tried again, up to three
     * rounds, as in PostgreSQL.
     */
    private List<EnqueueOutcome> enqueueAll(Connection c, List<TaskRecord> tasks) throws SQLException {
        long now = clock.millis();
        releaseExpiredKeys(c, now, tasks.stream().map(TaskRecord::uniqueKey).toList());
        var outcomes = new ArrayList<EnqueueOutcome>(tasks.size());
        try (var ps = c.prepareStatement(insertSql)) {
            for (var t : tasks) {
                EnqueueOutcome outcome = null;
                for (int round = 0; round < 3 && outcome == null; round++) {
                    if (round > 0) releaseExpiredKeys(c, now, Collections.singletonList(t.uniqueKey()));
                    bind(ps, insertValues(t, now));
                    outcome = ps.executeUpdate() == 1 ? EnqueueOutcome.STORED : classify(c, t);
                }
                if (outcome == null) throw new TaskStoreException("enqueue did not settle for task " + t.id());
                outcomes.add(outcome);
            }
        }
        return outcomes;
    }

    private Object[] insertValues(TaskRecord t, long now) {
        var due = t.due() == null ? now : t.due().toEpochMilli();
        return new Object[] {
            t.id(),
            t.revision(),
            t.queueId(),
            t.typeId(),
            t.payloadVersion(),
            due > now ? TaskState.SCHEDULED.code() : TaskState.QUEUED.code(),
            t.attempts(),
            t.maxAttempts(),
            t.createdAt() == null ? now : t.createdAt().toEpochMilli(),
            due,
            now,
            t.encodedPayload(),
            t.account(),
            RecordJson.encodeHeaders(t.headers()),
            t.uniqueKey(),
            t.uniqueKey() == null || t.keyKeep() == null ? null : t.keyKeep().toMillis(),
            t.retryPolicy(),
            t.scheduleId(),
            t.retryOf(),
            t.startBy(),
            t.maxRunTime() == null ? null : t.maxRunTime().toMillis(),
            t.cancelRequestedAt(),
            t.cancelReason(),
            t.progress(),
            RecordJson.encodeSteps(t.steps()),
            RecordJson.encodeFailures(t.failures())
        };
    }

    /** @return why a task was not stored (its id exists, or its key is held by another task), or null when neither */
    private EnqueueOutcome classify(Connection c, TaskRecord t) throws SQLException {
        if (queryOne(c, "SELECT 1 FROM " + task + " WHERE id = ?", rs -> true, t.id()) != null) {
            return EnqueueOutcome.EXISTS;
        }
        if (t.uniqueKey() != null) {
            var holder =
                    queryOne(c, "SELECT id FROM " + task + " WHERE unique_key = ?", rs -> rs.getLong(1), t.uniqueKey());
            if (holder != null) return new EnqueueOutcome.DuplicateKey(holder);
        }
        return null;
    }

    /**
     * A key whose keep window has passed is free; release it now rather than wait for {@link #releaseKeys}. Both
     * statements write, with no read before them, so that on a caller's deferred transaction the write lock is taken
     * by the first statement and waits up to the busy timeout instead of failing on a stale read.
     */
    private void releaseExpiredKeys(Connection c, long now, Collection<String> keys) throws SQLException {
        var distinct = keys.stream().filter(Objects::nonNull).distinct().toList();
        if (distinct.isEmpty()) return;
        var json = Json.write(distinct);
        update(
                c,
                "DELETE FROM " + task + " WHERE unique_key IN (SELECT value FROM json_each(?2)) AND key_until <= ?1"
                        + " AND state = " + KEY_ONLY,
                now,
                json);
        update(
                c,
                "UPDATE " + task + " SET unique_key = NULL, key_until = NULL"
                        + " WHERE unique_key IN (SELECT value FROM json_each(?2)) AND key_until <= ?1"
                        + " AND state <> " + KEY_ONLY,
                now,
                json);
    }

    /**
     * Ends the key windows that the selection picks: a key-only row goes, any other row keeps its task and frees
     * the key. The selection binds {@code now} as ?1.
     *
     * @return how many keys were released
     */
    private int releaseSelected(Connection c, long now, String selection, Object... more) throws SQLException {
        var args = new Object[more.length + 1];
        args[0] = now;
        System.arraycopy(more, 0, args, 1, more.length);
        var keyOnly = new ArrayList<Long>();
        var finished = new ArrayList<Long>();
        queryEach(c, selection, rs -> (rs.getInt(2) == KEY_ONLY ? keyOnly : finished).add(rs.getLong(1)), args);
        int released = 0;
        if (!keyOnly.isEmpty()) {
            released += update(
                    c,
                    "DELETE FROM " + task + " WHERE id IN (SELECT value FROM json_each(?)) AND state = " + KEY_ONLY,
                    idsJson(keyOnly));
        }
        if (!finished.isEmpty()) {
            released += update(
                    c,
                    "UPDATE " + task + " SET unique_key = NULL, key_until = NULL"
                            + " WHERE id IN (SELECT value FROM json_each(?)) AND state <> " + KEY_ONLY,
                    idsJson(finished));
        }
        return released;
    }

    // ---- workers -------------------------------------------------------------------------------------

    /**
     * Both claim passes in one write transaction. Pass 1 takes each grant's share in grant order; pass 2 lets the
     * grants that took their share in full take up to their cap, skipping what pass 1 took of that queue. Each pick
     * is one index-ordered statement per queue and stops at the count left. Nothing is written until both passes
     * have picked, so the tasks a queue's pass 2 skips are exactly the first ones of that queue in the same order.
     * A claim that finds nothing to take does not touch the write lock, which keeps idle runners from queueing
     * behind real writes.
     */
    @Override
    public List<TaskRecord> claim(ClaimRequest r) {
        if (r.count() <= 0 || (r.knownTypeIds() != null && r.knownTypeIds().isEmpty())) return List.of();
        var types = r.knownTypeIds() == null ? null : idsJson(r.knownTypeIds());
        if (!withConnection(c -> anyClaimable(c, r, types))) return List.of();
        return inTransaction(c -> {
            long now = clock.millis();
            var picked = pickForClaim(c, r, types, now);
            if (picked.isEmpty()) return List.<TaskRecord>of();
            var byId = new HashMap<Long, TaskRecord>();
            var unreadable = new ArrayList<Long>();
            queryEach(
                    c,
                    "UPDATE " + task + " SET state = 2, revision = revision + 1, attempts = attempts + 1,"
                            + " worker_id = ?1, claimed_at = ?2, state_changed = ?2, lease_until = ?3"
                            + " WHERE id IN (SELECT value FROM json_each(?4)) RETURNING *",
                    rs -> {
                        try {
                            var t = read(rs);
                            byId.put(t.id(), t);
                        } catch (UnreadableTaskException e) {
                            unreadable.add(e.taskId());
                        }
                    },
                    r.workerId(),
                    now,
                    now + r.lease().toMillis(),
                    idsJson(picked));
            // a task it cannot read gets no lease, so that maintenance takes it over
            if (!unreadable.isEmpty()) update(c, endLeasesSql, idsJson(unreadable), r.workerId());
            var result = new ArrayList<TaskRecord>(byId.size());
            for (var id : picked) {
                var t = byId.get(id);
                if (t != null) result.add(t);
            }
            return result;
        });
    }

    private boolean anyClaimable(Connection c, ClaimRequest r, String types) throws SQLException {
        var queues = r.grants().stream()
                .filter(g -> g.share() > 0)
                .map(QueueGrant::queueId)
                .toList();
        if (queues.isEmpty()) return false;
        return queryOne(
                        c,
                        "SELECT 1 FROM " + task + " WHERE state = 0 AND queue_id IN (SELECT value FROM json_each(?1))"
                                + " AND (start_by IS NULL OR start_by > ?2)" + typeFilter(types, 3)
                                + " AND NOT EXISTS (SELECT 1 FROM " + queue + " p WHERE p.id = " + task
                                + ".queue_id AND p.paused <> 0) LIMIT 1",
                        rs -> true,
                        types == null
                                ? new Object[] {idsJson(queues), clock.millis()}
                                : new Object[] {idsJson(queues), clock.millis(), types})
                != null;
    }

    private static String typeFilter(String types, int parameter) {
        return types == null ? "" : " AND type_id IN (SELECT value FROM json_each(?" + parameter + "))";
    }

    /** @return the ids to claim, in the order the claim returns them */
    private List<Long> pickForClaim(Connection c, ClaimRequest r, String types, long now) throws SQLException {
        var paused = new HashSet<Integer>();
        queryEach(
                c,
                "SELECT id FROM " + queue + " WHERE paused <> 0 AND id IN (SELECT value FROM json_each(?))",
                rs -> paused.add(rs.getInt(1)),
                idsJson(r.grants().stream().map(QueueGrant::queueId).toList()));
        var pick =
                "SELECT id FROM " + task + " WHERE state = 0 AND queue_id = ?1 AND (start_by IS NULL OR start_by > ?2)"
                        + typeFilter(types, 5) + " ORDER BY due, id LIMIT ?3 OFFSET ?4";
        var ids = new ArrayList<Long>();
        var filled = new ArrayList<QueueGrant>();
        int remaining = r.count();
        for (var g : r.grants()) {
            int take = Math.min(g.share(), r.count());
            if (take <= 0 || remaining <= 0 || paused.contains(g.queueId())) continue;
            int got = pickQueue(c, pick, g.queueId(), now, Math.min(take, remaining), 0, types, ids);
            remaining -= got;
            if (got == take && g.cap() > take) filled.add(g);
        }
        for (var g : filled) {
            if (remaining <= 0) break;
            int take = Math.min(g.share(), r.count());
            remaining -= pickQueue(c, pick, g.queueId(), now, Math.min(g.cap() - take, remaining), take, types, ids);
        }
        return ids;
    }

    private static int pickQueue(
            Connection c, String sql, int queueId, long now, int limit, int offset, String types, List<Long> into)
            throws SQLException {
        int before = into.size();
        var args = types == null
                ? new Object[] {queueId, now, limit, offset}
                : new Object[] {queueId, now, limit, offset, types};
        queryEach(c, sql, rs -> into.add(rs.getLong(1)), args);
        return into.size() - before;
    }

    @Override
    public List<LeaseStatus> renew(List<LeaseRef> leases, Duration lease) {
        if (leases.isEmpty()) return List.of();
        return inTransaction(c -> {
            long now = clock.millis();
            var statuses = new ArrayList<LeaseStatus>(leases.size());
            try (var ps = c.prepareStatement(renewSql)) {
                for (var ref : leases) {
                    bind(ps, now, lease.toMillis(), ref.id(), ref.revision());
                    try (var rs = ps.executeQuery()) {
                        statuses.add(
                                rs.next()
                                        ? new LeaseStatus(ref.id(), true, rs.getInt(1) == 1)
                                        : new LeaseStatus(ref.id(), false, false));
                    }
                }
            }
            return statuses;
        });
    }

    @Override
    public boolean ack(long id, int revision, boolean keep, boolean keepPayload, ScheduleEffect effect) {
        return inTransaction(c -> {
            long now = clock.millis();
            var acked = ackStatement(c, now, id, revision, keep, keepPayload);
            if (acked && effect != null) applyEffect(c, effect);
            return acked;
        });
    }

    /** The acks run one after the other in one transaction; a lease listed twice answers true only the first time. */
    @Override
    public List<Boolean> ackAll(List<LeaseRef> leases, boolean keep, boolean keepPayload) {
        if (leases.isEmpty()) return List.of();
        return inTransaction(c -> {
            long now = clock.millis();
            var out = new ArrayList<Boolean>(leases.size());
            for (var ref : leases) out.add(ackStatement(c, now, ref.id(), ref.revision(), keep, keepPayload));
            return out;
        });
    }

    private boolean ackStatement(Connection c, long now, long id, int revision, boolean keep, boolean keepPayload)
            throws SQLException {
        if (keep) return update(c, ackKeepSql, now, id, revision, keepPayload) == 1;
        if (update(c, ackDeleteSql, id, revision) == 1) return true;
        return update(c, ackKeyOnlySql, now, id, revision) == 1;
    }

    /**
     * Does nothing unless the definition's occurrence_id names the task that ends. A paused definition gets the
     * new next fire time too and stays out of the due scan until it is resumed.
     */
    private void applyEffect(Connection c, ScheduleEffect effect) throws SQLException {
        int changed = effect.pauses()
                ? update(
                        c,
                        "UPDATE " + schedule + " SET paused = 1, occurrence_id = NULL"
                                + " WHERE id = ? AND occurrence_id = ?",
                        effect.scheduleId(),
                        effect.occurrenceId())
                : update(
                        c,
                        "UPDATE " + schedule + " SET next_fire = ?, occurrence_id = NULL"
                                + " WHERE id = ? AND occurrence_id = ?",
                        effect.nextFire(),
                        effect.scheduleId(),
                        effect.occurrenceId());
        if (changed == 1) effect.markApplied();
    }

    @Override
    public FailOutcome fail(
            long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead, ScheduleEffect effect) {
        return fail(false, id, revision, failure, retryAt, keepDead, effect);
    }

    @Override
    public FailOutcome failExpired(
            long id, int revision, FailureInfo failure, Instant retryAt, boolean keepDead, ScheduleEffect effect) {
        return fail(true, id, revision, failure, retryAt, keepDead, effect);
    }

    /**
     * Reads the row under the fence, appends the failure in Java and writes the outcome in the same write
     * transaction, so no other transition can slip in between the read of {@code failures} and the write.
     */
    private FailOutcome fail(
            boolean onlyIfExpired,
            long id,
            int revision,
            FailureInfo failure,
            Instant retryAt,
            boolean keepDead,
            ScheduleEffect effect) {
        return inTransaction(c -> {
            var outcome = failLocked(c, clock.millis(), onlyIfExpired, id, revision, failure, retryAt, keepDead);
            if (outcome == FailOutcome.DEAD && effect != null) applyEffect(c, effect);
            return outcome;
        });
    }

    private record Attempt(int attempts, int maxAttempts, String failures, boolean holdsKey, boolean expired) {}

    private FailOutcome failLocked(
            Connection c,
            long now,
            boolean onlyIfExpired,
            long id,
            int revision,
            FailureInfo failure,
            Instant retryAt,
            boolean keepDead)
            throws SQLException {
        var row = queryOne(
                c,
                failLockSql,
                rs -> new Attempt(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getInt(4) == 1, rs.getInt(5) == 1),
                now,
                id,
                revision);
        if (row == null || (onlyIfExpired && !row.expired())) return FailOutcome.LOST;
        boolean dead = retryAt == null || row.attempts() >= row.maxAttempts();
        if (dead && !keepDead) {
            if (row.holdsKey()) {
                update(
                        c,
                        "UPDATE " + task + " SET" + TO_KEY_ONLY + ", revision = revision + 1," + KEY_WINDOW
                                + " WHERE id = ?2",
                        now,
                        id);
            } else {
                update(c, "DELETE FROM " + task + " WHERE id = ?", id);
            }
            return FailOutcome.DEAD;
        }
        var failures = new ArrayList<FailureInfo>();
        try {
            var old = RecordJson.decodeFailures(row.failures());
            if (old != null) failures.addAll(old);
        } catch (IllegalArgumentException | DateTimeException unreadable) {
            // dropped, so that the task can still end
        }
        failures.add(failure);
        var json = RecordJson.encodeFailures(
                failures.subList(Math.max(0, failures.size() - MAX_FAILURES), failures.size()));
        if (dead) {
            update(
                    c,
                    "UPDATE " + task + " SET state = 4, revision = revision + 1, state_changed = ?1,"
                            + " lease_until = NULL, failures = ?3," + KEY_RELEASE + " WHERE id = ?2",
                    now,
                    id,
                    json);
            return FailOutcome.DEAD;
        }
        update(
                c,
                "UPDATE " + task + " SET state = 1, due = ?3, revision = revision + 1, state_changed = ?1,"
                        + " lease_until = NULL, worker_id = NULL, failures = ?4 WHERE id = ?2",
                now,
                id,
                retryAt,
                json);
        return FailOutcome.RETRY;
    }

    @Override
    public boolean reschedule(long id, int revision, Instant due) {
        if (due == null) throw new IllegalArgumentException("due is required");
        return inTransaction(c -> update(c, rescheduleSql, clock.millis(), id, revision, due) == 1);
    }

    @Override
    public boolean unclaim(long id, int revision) {
        return inTransaction(c -> update(c, unclaimSql, clock.millis(), id, revision) == 1);
    }

    @Override
    public boolean saveSteps(long id, int revision, Map<String, StepResult> steps) {
        var json = steps == null || steps.isEmpty() ? null : RecordJson.encodeSteps(steps);
        return inTransaction(c -> update(c, saveStepsSql, json, id, revision) == 1);
    }

    @Override
    public boolean progress(long id, int revision, String progress) {
        return inTransaction(c -> update(c, progressSql, progress, id, revision) == 1);
    }

    // ---- maintenance ---------------------------------------------------------------------------------

    /** Reads the due tasks first, so that a poll that finds none does not take the write lock. */
    @Override
    public PromoteResult promote(int limit) {
        var select = "SELECT id FROM " + task + " WHERE state = 1 AND due <= ? ORDER BY due, id LIMIT ?";
        long moved = 0;
        if (limit > 0 && withConnection(c -> queryOne(c, select, rs -> true, clock.millis(), limit) != null)) {
            moved = inTransaction(c -> {
                long now = clock.millis();
                var ids = new ArrayList<Long>();
                queryEach(c, select, rs -> ids.add(rs.getLong(1)), now, limit);
                if (ids.isEmpty()) return 0;
                return update(
                        c,
                        "UPDATE " + task + " SET state = 0, revision = revision + 1, state_changed = ?1"
                                + " WHERE id IN (SELECT value FROM json_each(?2)) AND state = 1",
                        now,
                        idsJson(ids));
            });
        }
        return new PromoteResult((int) moved, nextDue());
    }

    @Override
    public List<ExpiredLease> reclaim(int limit) {
        if (limit <= 0) return List.of();
        return withConnection(c -> {
            var result = new ArrayList<ExpiredLease>();
            queryEach(
                    c,
                    reclaimSql,
                    rs -> result.add(new ExpiredLease(
                            rs.getLong(1), rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getInt(5), rs.getInt(6) == 1)),
                    clock.millis(),
                    limit);
            return result;
        });
    }

    /**
     * A task past its retention whose key is still in its keep window becomes a key-only row; the others are
     * deleted. Either way the task is gone, so the next batch does not walk it again. The state is written into
     * the statement, so that the plan can use the {@code _finished} partial index.
     */
    @Override
    public long purge(TaskState state, Duration retention, int limit) {
        if (!state.isTerminal()) throw new IllegalArgumentException("only terminal states are purged: " + state);
        if (limit <= 0) return 0;
        var select = "SELECT id, COALESCE(key_until > ?1, 0) FROM " + task + " WHERE state = " + state.code()
                + " AND state_changed <= ?1 - ?2 ORDER BY state_changed, id LIMIT ?3";
        if (!withConnection(
                c -> queryOne(c, select, rs -> true, clock.millis(), retention.toMillis(), limit) != null)) {
            return 0;
        }
        return inTransaction(c -> {
            long now = clock.millis();
            var gone = new ArrayList<Long>();
            var held = new ArrayList<Long>();
            queryEach(
                    c,
                    select,
                    rs -> (rs.getInt(2) == 1 ? held : gone).add(rs.getLong(1)),
                    now,
                    retention.toMillis(),
                    limit);
            long purged = 0;
            if (!gone.isEmpty()) {
                purged += update(
                        c, "DELETE FROM " + task + " WHERE id IN (SELECT value FROM json_each(?))", idsJson(gone));
            }
            if (!held.isEmpty()) {
                purged += update(
                        c,
                        "UPDATE " + task + " SET" + TO_KEY_ONLY + ", revision = revision + 1"
                                + " WHERE id IN (SELECT value FROM json_each(?2))",
                        now,
                        idsJson(held));
            }
            return purged;
        });
    }

    @Override
    public int releaseKeys(int limit) {
        if (limit <= 0) return 0;
        var select = "SELECT id, state FROM " + task + " WHERE key_until <= ?1 ORDER BY key_until, id LIMIT ?2";
        if (!withConnection(c -> queryOne(c, select, rs -> true, clock.millis(), limit) != null)) return 0;
        return inTransaction(c -> releaseSelected(c, clock.millis(), select, (long) limit));
    }

    @Override
    public Instant nextDue() {
        return withConnection(c -> queryOne(c, nextDueSql, rs -> instant(rs, 1)));
    }

    // ---- recurring definitions -----------------------------------------------------------------------

    /**
     * Compares the stored definition with the new one in Java, inside the write transaction. The stored next fire
     * time survives a re-put that keeps the timing (cron, zone, fixed delay) unchanged and either has its
     * occurrence still running or is not past the caller's keep-until bound. {@code paused} is only taken from a
     * new definition, so a runner re-putting at start cannot undo an operator's pause. A re-put that would change
     * nothing writes nothing, so it keeps the revision.
     */
    @Override
    public PutRecurringResult putRecurring(RecurringDefinition d, Instant fireAt, Instant keepUntil) {
        var nextFire = Instant.ofEpochMilli(fireAt.toEpochMilli());
        return inTransaction(c -> {
            var old = queryOne(
                    c,
                    "SELECT " + SCHEDULE_COLUMNS + " FROM " + schedule + " WHERE id = ?",
                    SqliteTaskStore::readSchedule,
                    d.scheduleId());
            if (old == null) {
                long rev = nextValue(c, NEXT_SCHEDULE_REV, 1);
                update(
                        c,
                        "INSERT INTO " + schedule
                                + " (id, type_id, payload_version, queue_id, account, cron, zone, misfire,"
                                + " catch_up_max, fixed_delay_ms, overlap_skip, data, max_attempts, max_run_ms,"
                                + " paused, rev, occurrence_id, next_fire)"
                                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)",
                        d.scheduleId(),
                        d.typeId(),
                        d.payloadVersion(),
                        d.queueId(),
                        d.account(),
                        d.cron(),
                        d.zone(),
                        d.misfire(),
                        d.catchUpMaxFires(),
                        d.fixedDelayMs(),
                        d.overlapSkip(),
                        d.encodedPayload(),
                        d.maxAttempts(),
                        d.maxRunTimeMs(),
                        d.paused(),
                        rev,
                        nextFire);
                return new PutRecurringResult(PutRecurringOutcome.CREATED, nextFire);
            }
            var stored = old.definition();
            boolean sameTiming = Objects.equals(stored.cron(), d.cron())
                    && Objects.equals(stored.zone(), d.zone())
                    && Objects.equals(stored.fixedDelayMs(), d.fixedDelayMs());
            boolean occurrenceRunning = old.occurrenceId() != null
                    && queryOne(
                                    c,
                                    "SELECT 1 FROM " + task + " WHERE id = ? AND state < 3",
                                    rs -> true,
                                    old.occurrenceId())
                            != null;
            boolean keepFire =
                    sameTiming && (occurrenceRunning || !old.nextFire().isAfter(keepUntil));
            boolean sameDefinition = sameTiming
                    && stored.typeId() == d.typeId()
                    && stored.payloadVersion() == d.payloadVersion()
                    && stored.queueId() == d.queueId()
                    && Objects.equals(stored.account(), d.account())
                    && Objects.equals(stored.misfire(), d.misfire())
                    && stored.catchUpMaxFires() == d.catchUpMaxFires()
                    && stored.overlapSkip() == d.overlapSkip()
                    && Objects.equals(stored.encodedPayload(), d.encodedPayload())
                    && stored.maxAttempts() == d.maxAttempts()
                    && Objects.equals(stored.maxRunTimeMs(), d.maxRunTimeMs());
            if (sameDefinition && keepFire && (old.occurrenceId() == null || occurrenceRunning)) {
                return new PutRecurringResult(PutRecurringOutcome.UNCHANGED, old.nextFire());
            }
            var fire = keepFire ? old.nextFire() : nextFire;
            update(
                    c,
                    "UPDATE " + schedule + " SET type_id = ?, payload_version = ?, queue_id = ?, account = ?,"
                            + " cron = ?, zone = ?, misfire = ?, catch_up_max = ?, fixed_delay_ms = ?,"
                            + " overlap_skip = ?, data = ?, max_attempts = ?, max_run_ms = ?, rev = ?,"
                            + " occurrence_id = ?, next_fire = ? WHERE id = ?",
                    d.typeId(),
                    d.payloadVersion(),
                    d.queueId(),
                    d.account(),
                    d.cron(),
                    d.zone(),
                    d.misfire(),
                    d.catchUpMaxFires(),
                    d.fixedDelayMs(),
                    d.overlapSkip(),
                    d.encodedPayload(),
                    d.maxAttempts(),
                    d.maxRunTimeMs(),
                    nextValue(c, NEXT_SCHEDULE_REV, 1),
                    sameTiming && occurrenceRunning ? old.occurrenceId() : null,
                    fire,
                    d.scheduleId());
            return new PutRecurringResult(PutRecurringOutcome.CHANGED, fire);
        });
    }

    @Override
    public boolean deleteRecurring(String scheduleId) {
        return inTransaction(c -> update(c, "DELETE FROM " + schedule + " WHERE id = ?", scheduleId) == 1);
    }

    @Override
    public Map<String, Instant> recurringSchedules() {
        return withConnection(c -> {
            var result = new HashMap<String, Instant>();
            queryEach(c, "SELECT id, next_fire FROM " + schedule, rs -> result.put(rs.getString(1), instant(rs, 2)));
            return result;
        });
    }

    @Override
    public List<DueSchedule> dueRecurring(Instant upTo, int limit) {
        if (limit <= 0) return List.of();
        return withConnection(c -> {
            var result = new ArrayList<DueSchedule>();
            queryEach(
                    c,
                    "SELECT " + SCHEDULE_COLUMNS + " FROM " + schedule
                            + " WHERE paused = 0 AND next_fire <= ? ORDER BY next_fire, id LIMIT ?",
                    rs -> {
                        var row = readSchedule(rs);
                        result.add(new DueSchedule(row.definition().scheduleId(), row.nextFire(), row.definition()));
                    },
                    upTo,
                    limit);
            return result;
        });
    }

    /**
     * An occurrence whose unique key is held by another task is not stored, but the definition still advances
     * and the commit counts: that is how overlap protection skips a fire time. An occurrence whose id is taken
     * writes nothing. A stored occurrence of a fixed-delay definition becomes its occurrence_id, in the same
     * transaction; when the key is held, occurrence_id is empty and the holder belongs to the schedule, the
     * holder becomes it.
     */
    @Override
    public boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord o) {
        var fixedDelay = due.definition().isFixedDelay();
        return inTransaction(c -> {
            long now = clock.millis();
            if (o.uniqueKey() != null) releaseExpiredKeys(c, now, List.of(o.uniqueKey()));
            if (queryOne(c, "SELECT 1 FROM " + task + " WHERE id = ?", rs -> true, o.id()) != null) return false;
            if (update(
                            c,
                            "UPDATE " + schedule + " SET next_fire = ? WHERE id = ? AND rev = ? AND next_fire = ?"
                                    + " AND paused = 0",
                            nextFire,
                            due.scheduleId(),
                            due.definition().revision(),
                            due.fireAt())
                    != 1) {
                return false;
            }
            update(
                    c,
                    commitInsertSql,
                    o.id(),
                    o.revision(),
                    o.queueId(),
                    o.typeId(),
                    o.payloadVersion(),
                    o.maxAttempts(),
                    o.createdAt() == null ? now : o.createdAt().toEpochMilli(),
                    due.fireAt(),
                    now,
                    o.encodedPayload(),
                    o.account(),
                    RecordJson.encodeHeaders(o.headers()),
                    o.retryPolicy(),
                    o.scheduleId() == null ? due.scheduleId() : o.scheduleId(),
                    o.uniqueKey(),
                    o.uniqueKey() == null || o.keyKeep() == null
                            ? null
                            : o.keyKeep().toMillis(),
                    o.startBy(),
                    o.maxRunTime() == null ? null : o.maxRunTime().toMillis());
            if (fixedDelay) {
                if (update(
                                c,
                                "UPDATE " + schedule + " SET occurrence_id = ? WHERE id = ?"
                                        + " AND EXISTS (SELECT 1 FROM " + task + " WHERE id = ?)",
                                o.id(),
                                due.scheduleId(),
                                o.id())
                        == 1) {
                    return true;
                }
                if (o.uniqueKey() != null) {
                    update(
                            c,
                            "UPDATE " + schedule + " SET occurrence_id = (SELECT t.id FROM " + task
                                    + " t WHERE t.unique_key = ? AND t.schedule_id = ? AND t.state < 3 LIMIT 1)"
                                    + " WHERE id = ? AND occurrence_id IS NULL",
                            o.uniqueKey(),
                            due.scheduleId(),
                            due.scheduleId());
                }
            }
            return true;
        });
    }

    // ---- inspection ----------------------------------------------------------------------------------

    @Override
    public Optional<TaskRecord> getTask(long id) {
        return withConnection(c -> Optional.ofNullable(queryOne(
                c, "SELECT * FROM " + task + " WHERE id = ? AND state < " + KEY_ONLY, SqliteTaskStore::read, id)));
    }

    /** The unique index covers the key column; a key whose window has passed is free even before the release job ran. */
    @Override
    public Optional<TaskRecord> taskByKey(String key) {
        return withConnection(c -> Optional.ofNullable(queryOne(
                c,
                "SELECT * FROM " + task + " WHERE unique_key = ? AND (key_until IS NULL OR key_until > ?)"
                        + " AND state < " + KEY_ONLY,
                SqliteTaskStore::read,
                key,
                clock.millis())));
    }

    /** Every registered queue appears, with zeros when it has no tasks. Counts by a scan of the {@code _counts} index. */
    @Override
    public Map<Integer, StateCounts> counts() {
        int states = TaskState.values().length;
        return snapshot(c -> {
            var raw = new TreeMap<Integer, long[]>();
            queryEach(c, "SELECT id FROM " + queue, rs -> raw.put(rs.getInt(1), new long[states]));
            queryEach(
                    c,
                    "SELECT queue_id, state, count(*) FROM " + task + " WHERE state < " + KEY_ONLY
                            + " GROUP BY queue_id, state",
                    rs -> raw.computeIfAbsent(rs.getInt(1), k -> new long[states])[rs.getInt(2)] = rs.getLong(3));
            var result = new LinkedHashMap<Integer, StateCounts>();
            raw.forEach((q, n) -> result.put(q, new StateCounts(n[0], n[1], n[2], n[3], n[4], n[5], n[6])));
            return result;
        });
    }

    /**
     * The cursor is {@code <sort time in epoch milliseconds>:<id>} of the last task of the page; the sort time is
     * due for QUEUED and SCHEDULED, state_changed otherwise. The state is written into the statement, so that the
     * plan can use the partial indexes. QUEUED across every queue is one bounded scan of {@code _queued} per queue,
     * merged; that index leads with the queue, so a single scan would sort every queued row.
     */
    @Override
    public TaskPage tasks(TaskState state, Integer queueId, String cursor, int limit) {
        var position = cursor == null ? null : decodeCursor(cursor);
        if (limit <= 0) return new TaskPage(List.of(), null);
        var column = listedByDue(state) ? "due" : "state_changed";
        var perQueue = queueId == null && state == TaskState.QUEUED;
        var sql = "SELECT * FROM " + task + " WHERE state = " + state.code()
                + (queueId != null || perQueue ? " AND queue_id = ?" : "")
                + (position != null ? " AND (" + column + ", id) > (?, ?)" : "")
                + " ORDER BY " + column + ", id LIMIT ?";
        var rows = snapshot(c -> {
            var queues = new ArrayList<Integer>();
            if (perQueue) queryEach(c, "SELECT id FROM " + queue + " ORDER BY id", rs -> queues.add(rs.getInt(1)));
            else if (queueId != null) queues.add(queueId);
            else queues.add(null);
            var out = new ArrayList<Listed>();
            for (var q : queues) {
                var args = new ArrayList<Object>();
                if (q != null) args.add(q);
                if (position != null) {
                    args.add(position.at());
                    args.add(position.id());
                }
                args.add((long) limit + 1);
                queryEach(c, sql, rs -> out.add(listed(rs, column)), args.toArray());
            }
            if (queues.size() > 1) out.sort(Comparator.comparing(Listed::at).thenComparingLong(Listed::id));
            return out;
        });
        var page = rows.subList(0, Math.min(limit, rows.size()));
        var items = page.stream().map(Listed::task).filter(Objects::nonNull).toList();
        if (rows.size() <= limit) return new TaskPage(items, null);
        var last = page.getLast();
        return new TaskPage(items, last.at().toEpochMilli() + ":" + last.id());
    }

    /** A listed row; an unreadable task is left out of the page (null) but still positions the cursor. */
    private record Listed(Instant at, long id, TaskRecord task) {}

    private static Listed listed(ResultSet rs, String column) throws SQLException {
        var at = getInstant(rs, column);
        long id = rs.getLong("id");
        try {
            return new Listed(at, id, read(rs));
        } catch (UnreadableTaskException e) {
            return new Listed(at, id, null);
        }
    }

    private static boolean listedByDue(TaskState state) {
        return state == TaskState.QUEUED || state == TaskState.SCHEDULED;
    }

    private record Position(Instant at, long id) {}

    private static Position decodeCursor(String cursor) {
        var colon = cursor.indexOf(':');
        try {
            if (colon > 0) {
                return new Position(
                        Instant.ofEpochMilli(Long.parseLong(cursor.substring(0, colon))),
                        Long.parseLong(cursor.substring(colon + 1)));
            }
        } catch (NumberFormatException | DateTimeException e) {
            throw new IllegalArgumentException("malformed cursor");
        }
        throw new IllegalArgumentException("malformed cursor");
    }

    // ---- JDBC helpers --------------------------------------------------------------------------------

    @FunctionalInterface
    private interface SqlFunction<T> {
        T apply(Connection c) throws SQLException;
    }

    @FunctionalInterface
    private interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    @FunctionalInterface
    private interface RowConsumer {
        void accept(ResultSet rs) throws SQLException;
    }

    /**
     * Runs {@code fn} on a connection whose auto-commit is on, whatever the pool handed out: a connection with
     * auto-commit off has an open transaction in the driver, which this ends first and opens again at the end,
     * so that the connection goes back in the state it was received. The statements of {@code fn} that are not
     * inside {@link #immediate} or {@link #snapshot} each run on their own.
     */
    private <T> T withConnection(SqlFunction<T> fn) {
        try (var c = dataSource.getConnection()) {
            boolean autoCommit = c.getAutoCommit();
            if (!autoCommit) c.setAutoCommit(true);
            T result;
            try {
                result = fn.apply(c);
            } catch (Throwable e) {
                restoreAutoCommit(c, autoCommit, e);
                throw e;
            }
            restoreAutoCommit(c, autoCommit, null);
            return result;
        } catch (SQLException e) {
            throw new TaskStoreException(FAILED, e);
        }
    }

    /** A connection that cannot even switch modes is broken, and the pool drops it when it is closed. */
    private static void restoreAutoCommit(Connection c, boolean autoCommit, Throwable failure) {
        if (autoCommit) return;
        try {
            c.setAutoCommit(false);
        } catch (SQLException | RuntimeException e) {
            if (failure != null) failure.addSuppressed(e);
        }
    }

    /** A write transaction: the write lock is taken at the start, waiting for it up to the busy timeout. */
    private <T> T inTransaction(SqlFunction<T> fn) {
        return withConnection(c -> immediate(c, fn));
    }

    /** One read transaction, so that the statements of {@code fn} see one snapshot of the database. */
    private <T> T snapshot(SqlFunction<T> fn) {
        return withConnection(c -> inside(c, "BEGIN", fn));
    }

    private static <T> T immediate(Connection c, SqlFunction<T> fn) throws SQLException {
        return inside(c, "BEGIN IMMEDIATE", fn);
    }

    /** Any failure, an Error too, rolls back; a failed rollback is added to the failure that caused it. */
    private static <T> T inside(Connection c, String begin, SqlFunction<T> fn) throws SQLException {
        execute(c, begin);
        try {
            T result = fn.apply(c);
            execute(c, "COMMIT");
            return result;
        } catch (Throwable e) {
            try {
                execute(c, "ROLLBACK");
            } catch (SQLException | RuntimeException rollback) {
                e.addSuppressed(rollback);
            }
            throw e;
        }
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    /**
     * Binds the values in order: a number or a boolean as an integer, an instant as epoch milliseconds, text as
     * text, null as null.
     */
    private static void bind(PreparedStatement ps, Object... values) throws SQLException {
        for (int i = 0; i < values.length; i++) {
            var position = i + 1;
            switch (values[i]) {
                case null -> ps.setNull(position, Types.NULL);
                case Long v -> ps.setLong(position, v);
                case Integer v -> ps.setLong(position, v);
                case Boolean v -> ps.setLong(position, v ? 1 : 0);
                case Instant v -> ps.setLong(position, v.toEpochMilli());
                case String v -> ps.setString(position, v);
                default ->
                    throw new IllegalArgumentException(
                            "cannot bind " + values[i].getClass().getName());
            }
        }
    }

    /** @return the number of rows the statement changed */
    private static int update(Connection c, String sql, Object... values) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            bind(ps, values);
            return ps.executeUpdate();
        }
    }

    /** @return the mapped first row, or null when there is none */
    private static <T> T queryOne(Connection c, String sql, RowMapper<T> mapper, Object... values) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            bind(ps, values);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? mapper.map(rs) : null;
            }
        }
    }

    private static void queryEach(Connection c, String sql, RowConsumer consumer, Object... values)
            throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            bind(ps, values);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) consumer.accept(rs);
            }
        }
    }

    private static String idsJson(Collection<? extends Number> ids) {
        return Json.write(
                ids.stream().map(Number::longValue).sorted().distinct().toList());
    }

    private static Set<Integer> parseIds(String json) {
        var ids = new HashSet<Integer>();
        for (var id : (List<?>) Json.parse(json)) ids.add(((Number) id).intValue());
        return Set.copyOf(ids);
    }

    private static TaskRecord read(ResultSet rs) throws SQLException {
        long id = rs.getLong("id");
        try {
            return readFields(rs, id);
        } catch (IllegalArgumentException | DateTimeException e) {
            // the cause's message may quote the stored value, so it stays out of issues and stored failures
            throw new UnreadableTaskException(id, "task " + id + " has a stored value that cannot be read", e);
        }
    }

    private static TaskRecord readFields(ResultSet rs, long id) throws SQLException {
        return TaskRecord.builder()
                .id(id)
                .revision(rs.getInt("revision"))
                .queueId(rs.getInt("queue_id"))
                .typeId(rs.getInt("type_id"))
                .payloadVersion(rs.getInt("payload_version"))
                .state(TaskState.fromCode(rs.getInt("state")))
                .attempts(rs.getInt("attempts"))
                .maxAttempts(rs.getInt("max_attempts"))
                .createdAt(getInstant(rs, "created"))
                .due(getInstant(rs, "due"))
                .stateChangedAt(getInstant(rs, "state_changed"))
                .encodedPayload(rs.getString("data"))
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
                .maxRunTime(getMillis(rs, "max_run_ms"))
                .cancelRequestedAt(getInstant(rs, "cancel_requested_at"))
                .cancelReason(rs.getString("cancel_reason"))
                .progress(rs.getString("progress"))
                .steps(RecordJson.decodeSteps(rs.getString("steps")))
                .failures(RecordJson.decodeFailures(rs.getString("failures")))
                .build();
    }

    private static Instant instant(ResultSet rs, int index) throws SQLException {
        long value = rs.getLong(index);
        return rs.wasNull() ? null : Instant.ofEpochMilli(value);
    }

    private static Instant getInstant(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : Instant.ofEpochMilli(value);
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
        return "SqliteTaskStore[" + prefix + "]";
    }

    // ---- cancellation, expiry, admin, workers, schedule controls ---------------------------------------

    @Override
    public boolean finishCancelled(long id, int revision, String reason, boolean keepPayload, ScheduleEffect effect) {
        return finishCancelled(finishCancelledSql, id, revision, reason, keepPayload, effect);
    }

    @Override
    public boolean finishCancelledExpired(
            long id, int revision, String reason, boolean keepPayload, ScheduleEffect effect) {
        return finishCancelled(finishCancelledExpiredSql, id, revision, reason, keepPayload, effect);
    }

    private boolean finishCancelled(
            String sql, long id, int revision, String reason, boolean keepPayload, ScheduleEffect effect) {
        return inTransaction(c -> {
            if (update(c, sql, clock.millis(), id, revision, reason, keepPayload) != 1) return false;
            if (effect != null) applyEffect(c, effect);
            return true;
        });
    }

    /** Oldest deadline first, through the {@code _start_by} partial index. */
    @Override
    public List<Long> expire(int limit) {
        if (limit <= 0) return List.of();
        var select = "SELECT id FROM " + task + " WHERE start_by <= ? AND state < 2 ORDER BY start_by, id LIMIT ?";
        if (!withConnection(c -> queryOne(c, select, rs -> true, clock.millis(), limit) != null)) return List.of();
        return inTransaction(c -> {
            long now = clock.millis();
            var ids = new ArrayList<Long>();
            queryEach(c, select, rs -> ids.add(rs.getLong(1)), now, limit);
            if (!ids.isEmpty()) {
                update(
                        c,
                        "UPDATE " + task + " SET state = 6, revision = revision + 1, state_changed = ?1," + KEY_RELEASE
                                + " WHERE id IN (SELECT value FROM json_each(?2))",
                        now,
                        idsJson(ids));
            }
            return ids;
        });
    }

    @Override
    public int purgeWorkers(Duration staleAfter) {
        var stale = "SELECT 1 FROM " + worker + " WHERE last_seen < ? LIMIT 1";
        if (!withConnection(c -> queryOne(c, stale, rs -> true, clock.millis() - staleAfter.toMillis()) != null)) {
            return 0;
        }
        return inTransaction(c ->
                update(c, "DELETE FROM " + worker + " WHERE last_seen < ?", clock.millis() - staleAfter.toMillis()));
    }

    /**
     * Admin cancel keeps the payload, so that a cancelled task can be retried as it was. The two branches
     * exclude each other by state, so at most one touches the row.
     */
    @Override
    public CancelOutcome cancel(long id, int revision, String reason, ScheduleEffect effect) {
        return inTransaction(c -> {
            long now = clock.millis();
            if (update(
                            c,
                            "UPDATE " + task
                                    + " SET state = 5, revision = revision + 1, state_changed = ?1,"
                                    + " cancel_requested_at = ?1, cancel_reason = ?4," + KEY_RELEASE
                                    + " WHERE id = ?2 AND revision = ?3 AND state IN (0, 1)",
                            now,
                            id,
                            revision,
                            reason)
                    == 1) {
                if (effect != null) applyEffect(c, effect);
                return CancelOutcome.CANCELLED;
            }
            if (update(
                            c,
                            "UPDATE " + task + " SET cancel_requested_at = ?1, cancel_reason = ?4" + FENCE,
                            now,
                            id,
                            revision,
                            reason)
                    == 1) {
                return CancelOutcome.REQUESTED;
            }
            return CancelOutcome.STALE;
        });
    }

    /**
     * Also clears {@code start_by}: an expired task keeps a deadline in the past, which would expire the retry
     * at once. A row that still has its {@code unique_key} already owns that key in the partial unique index, so
     * keeping the column cannot collide; only {@code key_until} is cleared, which makes the task the open holder
     * again. A key whose window has passed is dropped, as enqueue would drop it, so the outcome does not depend on
     * whether the sweep has run.
     */
    @Override
    public RetryOutcome retry(long id, int revision) {
        return inTransaction(c -> update(
                                c,
                                "UPDATE " + task
                                        + " SET state = 0, due = ?1, attempts = 0, revision = revision + 1,"
                                        + " state_changed = ?1, worker_id = NULL, lease_until = NULL,"
                                        + " cancel_requested_at = NULL, cancel_reason = NULL,"
                                        + " key_keep_ms = CASE WHEN unique_key IS NULL OR key_until <= ?1 THEN NULL"
                                        + " ELSE key_keep_ms END,"
                                        + " unique_key = CASE WHEN key_until <= ?1 THEN NULL ELSE unique_key END,"
                                        + " key_until = NULL, start_by = NULL"
                                        + " WHERE id = ?2 AND revision = ?3 AND state IN (4, 5, 6)",
                                clock.millis(),
                                id,
                                revision)
                        == 1
                ? RetryOutcome.RETRIED
                : RetryOutcome.STALE);
    }

    @Override
    public boolean delete(long id, int revision, ScheduleEffect effect) {
        return inTransaction(c -> {
            if (update(
                            c,
                            "DELETE FROM " + task + " WHERE id = ? AND revision = ? AND state NOT IN (2, " + KEY_ONLY
                                    + ")",
                            id,
                            revision)
                    != 1) {
                return false;
            }
            if (effect != null) applyEffect(c, effect);
            return true;
        });
    }

    @Override
    public void pauseQueue(int queueId, boolean paused) {
        inTransaction(c -> update(c, "UPDATE " + queue + " SET paused = ? WHERE id = ?", paused, queueId));
    }

    @Override
    public List<QueueInfo> queues() {
        return withConnection(c -> {
            var result = new ArrayList<QueueInfo>();
            queryEach(
                    c,
                    "SELECT id, name, paused FROM " + queue + " ORDER BY id",
                    rs -> result.add(new QueueInfo(rs.getInt(1), rs.getString(2), rs.getInt(3) != 0)));
            return result;
        });
    }

    @Override
    public void heartbeat(
            int workerId, String label, Set<Integer> queueIds, Set<Integer> typeIds, int running, boolean accepting) {
        if (label == null) throw new IllegalArgumentException("label is required");
        var queues = idsJson(queueIds);
        var types = idsJson(typeIds);
        inTransaction(c -> update(
                c,
                "INSERT INTO " + worker
                        + " (id, label, started_at, last_seen, running, accepting, queue_ids, type_ids)"
                        + " VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?6, ?7) ON CONFLICT (id) DO UPDATE"
                        + " SET last_seen = excluded.last_seen, running = excluded.running,"
                        + " accepting = excluded.accepting",
                workerId,
                label,
                clock.millis(),
                running,
                accepting,
                queues,
                types));
    }

    @Override
    public Map<Integer, Map<Integer, Long>> queuedByType() {
        return withConnection(c -> {
            var result = new TreeMap<Integer, Map<Integer, Long>>();
            queryEach(
                    c,
                    "SELECT queue_id, type_id, count(*) FROM " + task + " WHERE state = 0 GROUP BY queue_id, type_id",
                    rs -> result.computeIfAbsent(rs.getInt(1), id -> new TreeMap<>())
                            .put(rs.getInt(2), rs.getLong(3)));
            return result;
        });
    }

    @Override
    public List<WorkerInfo> workers() {
        return withConnection(c -> {
            var result = new ArrayList<WorkerInfo>();
            queryEach(
                    c,
                    "SELECT id, label, started_at, last_seen, running, accepting, queue_ids, type_ids FROM " + worker
                            + " ORDER BY id",
                    rs -> result.add(new WorkerInfo(
                            rs.getInt(1),
                            rs.getString(2),
                            instant(rs, 3),
                            instant(rs, 4),
                            rs.getInt(5),
                            rs.getInt(6) != 0,
                            parseIds(rs.getString(7)),
                            parseIds(rs.getString(8)))));
            return result;
        });
    }

    @Override
    public List<RecurringSchedule> recurringDefinitions() {
        return withConnection(c -> {
            var result = new ArrayList<RecurringSchedule>();
            queryEach(c, "SELECT " + SCHEDULE_COLUMNS + " FROM " + schedule + " ORDER BY id", rs -> {
                var row = readSchedule(rs);
                result.add(new RecurringSchedule(row.definition(), row.nextFire()));
            });
            return result;
        });
    }

    @Override
    public boolean pauseRecurring(String scheduleId) {
        return inTransaction(c -> update(c, "UPDATE " + schedule + " SET paused = 1 WHERE id = ?", scheduleId) == 1);
    }

    /** Only a paused row is written; a definition that is not paused is left as it is. */
    @Override
    public boolean resumeRecurring(String scheduleId, Instant nextFire) {
        return inTransaction(c -> {
            if (nextFire == null) {
                update(c, "UPDATE " + schedule + " SET paused = 0 WHERE id = ? AND paused <> 0", scheduleId);
            } else if (queryOne(
                            c, "SELECT 1 FROM " + schedule + " WHERE id = ? AND paused <> 0", rs -> true, scheduleId)
                    != null) {
                update(
                        c,
                        "UPDATE " + schedule + " SET paused = 0, next_fire = ?, rev = ? WHERE id = ?",
                        nextFire,
                        nextValue(c, NEXT_SCHEDULE_REV, 1),
                        scheduleId);
            }
            return queryOne(c, "SELECT 1 FROM " + schedule + " WHERE id = ?", rs -> true, scheduleId) != null;
        });
    }

    /** A commit already in flight for the old definition fails, because it checks the revision and fire time it read. */
    @Override
    public boolean setNextFire(String scheduleId, Instant nextFire) {
        return inTransaction(c -> {
            if (queryOne(c, "SELECT 1 FROM " + schedule + " WHERE id = ?", rs -> true, scheduleId) == null) {
                return false;
            }
            update(
                    c,
                    "UPDATE " + schedule + " SET next_fire = ?, rev = ?, occurrence_id = NULL WHERE id = ?",
                    nextFire,
                    nextValue(c, NEXT_SCHEDULE_REV, 1),
                    scheduleId);
            return true;
        });
    }

    @Override
    public boolean setNextFire(String scheduleId, long revision, Instant expectedFire, Instant nextFire) {
        return inTransaction(c -> update(
                        c,
                        "UPDATE " + schedule
                                + " SET next_fire = ? WHERE id = ? AND rev = ? AND next_fire = ? AND paused = 0",
                        nextFire,
                        scheduleId,
                        revision,
                        expectedFire)
                == 1);
    }

    private static final String SCHEDULE_COLUMNS = "id, type_id, payload_version, queue_id, account, cron, zone,"
            + " misfire, catch_up_max, fixed_delay_ms, overlap_skip, data, max_attempts, max_run_ms, paused, rev,"
            + " next_fire, occurrence_id";

    /** A stored definition with the next fire time and the occurrence in flight. */
    private record ScheduleRow(RecurringDefinition definition, Instant nextFire, Long occurrenceId) {}

    private static ScheduleRow readSchedule(ResultSet rs) throws SQLException {
        var definition = new RecurringDefinition(
                rs.getString("id"),
                rs.getInt("type_id"),
                rs.getInt("payload_version"),
                rs.getInt("queue_id"),
                rs.getString("account"),
                rs.getString("cron"),
                rs.getString("zone"),
                rs.getString("misfire"),
                rs.getInt("catch_up_max"),
                getLong(rs, "fixed_delay_ms"),
                rs.getInt("overlap_skip") != 0,
                rs.getString("data"),
                rs.getInt("max_attempts"),
                getLong(rs, "max_run_ms"),
                rs.getInt("paused") != 0,
                rs.getLong("rev"));
        return new ScheduleRow(definition, getInstant(rs, "next_fire"), getLong(rs, "occurrence_id"));
    }
}

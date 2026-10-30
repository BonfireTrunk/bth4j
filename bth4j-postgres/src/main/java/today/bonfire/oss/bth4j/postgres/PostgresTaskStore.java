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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.store.DueSchedule;
import today.bonfire.oss.bth4j.store.ExpiredLease;
import today.bonfire.oss.bth4j.store.FailOutcome;
import today.bonfire.oss.bth4j.store.PromoteResult;
import today.bonfire.oss.bth4j.store.RecurringDefinition;
import today.bonfire.oss.bth4j.store.TaskRecord;
import today.bonfire.oss.bth4j.store.TaskStore;
import today.bonfire.oss.bth4j.store.TaskStoreException;

/**
 * {@link TaskStore} on PostgreSQL (9.5 or newer for {@code SKIP LOCKED} and {@code ON CONFLICT}; tested
 * on 17): one row per task in {@code <prefix>_task} with a state column, recurring definitions in
 * {@code <prefix>_schedule}. Every transition is one SQL statement, so its check and its writes commit
 * together: claims use {@code FOR UPDATE SKIP LOCKED}, the token-checked transitions are
 * {@code UPDATE ... WHERE id = ? AND token = ?}. Runners that share the tables share the work.
 * <p>
 * The tables are created by {@link #createSchema()}, or by running {@link #schemaStatements()} from
 * the application's migration tool. Only {@code java.sql} is used: the application supplies the JDBC
 * driver and a {@link DataSource}, normally pooled.
 */
public class PostgresTaskStore implements TaskStore {
    private static final Pattern PREFIX = Pattern.compile("[a-z][a-z0-9_]{0,40}");

    static final int QUEUED = 0;
    static final int SCHEDULED = 1;
    static final int LEASED = 2;
    static final int DEAD = 3;

    private final DataSource dataSource;
    private final String prefix;
    private final String task;
    private final String schedule;
    private final String seq;

    private final String enqueueSql;
    private final String claimSql;
    private final String renewSql;
    private final String ackSql;
    private final String failSql;
    private final String rescheduleSql;
    private final String unclaimSql;
    private final String promoteSql;
    private final String nextDueSql;
    private final String reclaimSql;
    private final String getSql;
    private final String deadSql;
    private final String retryDeadSql;
    private final String purgeSql;
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
        this.task = prefix + "_task";
        this.schedule = prefix + "_schedule";
        this.seq = prefix + "_task_pos";

        enqueueSql = "INSERT INTO " + task
                + " (id, state, queue, event, account, data, attempts, max_attempts, created, due, pos)"
                + " VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, CASE WHEN ? = 0 THEN nextval('" + seq + "') ELSE 0 END)"
                + " ON CONFLICT (id) DO NOTHING";
        claimSql = "WITH locked AS (SELECT id, queue, pos FROM " + task
                + "   WHERE state = 0 AND queue = ANY(?) ORDER BY array_position(?, queue), pos LIMIT ?"
                + "   FOR UPDATE SKIP LOCKED),"
                + " picked AS (SELECT id, row_number() OVER (ORDER BY array_position(?, queue), pos) AS ord FROM locked),"
                + " upd AS (UPDATE " + task + " t SET state = 2, attempts = t.attempts + 1,"
                + "   token = ? || ':' || picked.ord::text, lease_until = ?, due = NULL"
                + "   FROM picked WHERE t.id = picked.id RETURNING t.*, picked.ord)"
                + " SELECT * FROM upd ORDER BY ord";
        renewSql = "UPDATE " + task + " t SET lease_until = ? FROM unnest(?, ?) AS u(id, token)"
                + " WHERE t.id = u.id AND t.token = u.token AND t.state = 2";
        ackSql = "DELETE FROM " + task + " WHERE id = ? AND token = ? AND state = 2";
        failSql = "UPDATE " + task + " SET"
                + " state = CASE WHEN ? OR attempts >= max_attempts THEN 3 ELSE 1 END,"
                + " due = CASE WHEN ? OR attempts >= max_attempts THEN NULL ELSE ? END,"
                + " died = CASE WHEN ? OR attempts >= max_attempts THEN ? ELSE NULL END,"
                + " error = ?, token = NULL, lease_until = NULL"
                + " WHERE id = ? AND token = ? AND state = 2 RETURNING state";
        rescheduleSql = "UPDATE " + task + " SET state = 1, due = ?, attempts = attempts - 1,"
                + " token = NULL, lease_until = NULL WHERE id = ? AND token = ? AND state = 2";
        unclaimSql = "UPDATE " + task + " SET state = 0, pos = -nextval('" + seq + "'), attempts = attempts - 1,"
                + " token = NULL, lease_until = NULL WHERE id = ? AND token = ? AND state = 2";
        promoteSql = "WITH due AS (SELECT id FROM " + task + " WHERE state = 1 AND due <= ? ORDER BY due LIMIT ?"
                + "   FOR UPDATE SKIP LOCKED)"
                + " UPDATE " + task + " t SET state = 0, pos = nextval('" + seq + "'), due = NULL"
                + " FROM due WHERE t.id = due.id";
        nextDueSql = "SELECT min(due) FROM " + task + " WHERE state = 1";
        reclaimSql = "SELECT id, token, attempts, max_attempts, event FROM " + task
                + " WHERE state = 2 AND lease_until <= ? ORDER BY lease_until LIMIT ?";
        getSql = "SELECT * FROM " + task + " WHERE id = ?";
        deadSql = "SELECT * FROM " + task + " WHERE state = 3 ORDER BY died LIMIT ?";
        retryDeadSql = "UPDATE " + task + " SET state = 0, pos = nextval('" + seq + "'), attempts = 0,"
                + " died = NULL, error = NULL WHERE id = ? AND state = 3";
        purgeSql = "DELETE FROM " + task + " WHERE state = 3 AND died <= ?";
        putScheduleSql = "INSERT INTO " + schedule
                + " (id, event, account, queue, cron, data, max_attempts, rev, next_fire)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)"
                + " ON CONFLICT (id) DO UPDATE SET event = EXCLUDED.event, account = EXCLUDED.account,"
                + " queue = EXCLUDED.queue, cron = EXCLUDED.cron, data = EXCLUDED.data,"
                + " max_attempts = EXCLUDED.max_attempts, rev = " + schedule + ".rev + 1,"
                + " next_fire = CASE WHEN " + schedule + ".cron = EXCLUDED.cron THEN " + schedule
                + ".next_fire ELSE EXCLUDED.next_fire END";
        dueScheduleSql = "SELECT * FROM " + schedule + " WHERE next_fire <= ? ORDER BY next_fire LIMIT ?";
        commitScheduleSql = "WITH adv AS (UPDATE " + schedule + " SET next_fire = ?"
                + "   WHERE id = ? AND rev = ? AND next_fire = ? RETURNING id),"
                + " ins AS (INSERT INTO " + task
                + "   (id, state, queue, event, account, data, attempts, max_attempts, created, due, schedule_id, pos)"
                + "   SELECT ?, 1, ?, ?, ?, ?, 0, ?, ?, ?, ?, 0 FROM adv ON CONFLICT (id) DO NOTHING RETURNING id)"
                + " SELECT count(*) FROM adv";
    }

    // ---- schema --------------------------------------------------------------------------------------

    /** The DDL for this prefix, idempotent; for migration tools that manage the schema themselves. */
    public List<String> schemaStatements() {
        return List.of(
                "CREATE TABLE IF NOT EXISTS " + task + " ("
                        + " id text PRIMARY KEY,"
                        + " state smallint NOT NULL,"
                        + " queue text NOT NULL,"
                        + " event text NOT NULL,"
                        + " account text NOT NULL,"
                        + " data text,"
                        + " attempts integer NOT NULL DEFAULT 0,"
                        + " max_attempts integer NOT NULL,"
                        + " created timestamptz NOT NULL,"
                        + " due timestamptz,"
                        + " token text,"
                        + " lease_until timestamptz,"
                        + " schedule_id text,"
                        + " error text,"
                        + " died timestamptz,"
                        + " pos bigint NOT NULL DEFAULT 0)",
                "CREATE SEQUENCE IF NOT EXISTS " + seq,
                "CREATE INDEX IF NOT EXISTS " + task + "_queued ON " + task + " (queue, pos) WHERE state = 0",
                "CREATE INDEX IF NOT EXISTS " + task + "_scheduled ON " + task + " (due) WHERE state = 1",
                "CREATE INDEX IF NOT EXISTS " + task + "_leased ON " + task + " (lease_until) WHERE state = 2",
                "CREATE INDEX IF NOT EXISTS " + task + "_dead ON " + task + " (died) WHERE state = 3",
                "CREATE TABLE IF NOT EXISTS " + schedule + " ("
                        + " id text PRIMARY KEY,"
                        + " event text NOT NULL,"
                        + " account text NOT NULL,"
                        + " queue text NOT NULL,"
                        + " cron text NOT NULL,"
                        + " data text,"
                        + " max_attempts integer NOT NULL,"
                        + " rev integer NOT NULL,"
                        + " next_fire timestamptz NOT NULL)",
                "CREATE INDEX IF NOT EXISTS " + schedule + "_due ON " + schedule + " (next_fire)");
    }

    /**
     * Creates the tables, indexes and sequence if they do not exist. Run it once before
     * the first runner starts, not from several runners at the same time.
     */
    public void createSchema() {
        withConnection(c -> {
            try (var st = c.createStatement()) {
                for (var sql : schemaStatements()) {
                    st.execute(sql);
                }
            }
            return null;
        });
    }

    // ---- tasks and definitions ----------------------------------------------------------------------

    @Override
    public boolean enqueue(TaskRecord t) {
        int state = t.due() == null ? QUEUED : SCHEDULED;
        return withConnection(c -> {
            try (var ps = c.prepareStatement(enqueueSql)) {
                ps.setString(1, t.id());
                ps.setInt(2, state);
                ps.setString(3, t.queue());
                ps.setString(4, t.event());
                ps.setString(5, t.account());
                ps.setString(6, t.data());
                ps.setInt(7, t.maxAttempts());
                setInstant(ps, 8, t.created());
                setInstant(ps, 9, t.due());
                ps.setInt(10, state);
                return ps.executeUpdate() == 1;
            }
        });
    }

    @Override
    public void putRecurring(RecurringDefinition d, Instant nextFire) {
        withConnection(c -> {
            try (var ps = c.prepareStatement(putScheduleSql)) {
                ps.setString(1, d.scheduleId());
                ps.setString(2, d.event());
                ps.setString(3, d.account());
                ps.setString(4, d.queue());
                ps.setString(5, d.cron());
                ps.setString(6, d.data());
                ps.setInt(7, d.maxAttempts());
                setInstant(ps, 8, nextFire);
                ps.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public boolean deleteRecurring(String scheduleId) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement("DELETE FROM " + schedule + " WHERE id = ?")) {
                ps.setString(1, scheduleId);
                return ps.executeUpdate() == 1;
            }
        });
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
    public Optional<TaskRecord> getTask(String id) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(getSql)) {
                ps.setString(1, id);
                try (var rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(read(rs)) : Optional.empty();
                }
            }
        });
    }

    @Override
    public long queueSize(String queue) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement("SELECT count(*) FROM " + task + " WHERE state = 0 AND queue = ?")) {
                ps.setString(1, queue);
                return single(ps);
            }
        });
    }

    @Override
    public long leasedCount() {
        return count(LEASED);
    }

    @Override
    public long scheduledCount() {
        return count(SCHEDULED);
    }

    @Override
    public long deadCount() {
        return count(DEAD);
    }

    private long count(int state) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement("SELECT count(*) FROM " + task + " WHERE state = ?")) {
                ps.setInt(1, state);
                return single(ps);
            }
        });
    }

    @Override
    public List<TaskRecord> deadTasks(int limit) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(deadSql)) {
                ps.setInt(1, limit);
                return readAll(ps);
            }
        });
    }

    @Override
    public boolean retryDeadTask(String id) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(retryDeadSql)) {
                ps.setString(1, id);
                return ps.executeUpdate() == 1;
            }
        });
    }

    @Override
    public long purgeDeadTasks(Instant diedBefore) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(purgeSql)) {
                setInstant(ps, 1, diedBefore);
                return (long) ps.executeUpdate();
            }
        });
    }

    // ---- transitions ---------------------------------------------------------------------------------

    @Override
    public List<TaskRecord> claim(List<String> queues, int count, String tokenBase, Duration lease) {
        return withConnection(c -> {
            Array queueArray = c.createArrayOf("text", queues.toArray(String[]::new));
            try (var ps = c.prepareStatement(claimSql)) {
                ps.setArray(1, queueArray);
                ps.setArray(2, queueArray);
                ps.setInt(3, count);
                ps.setArray(4, queueArray);
                ps.setString(5, tokenBase);
                setInstant(ps, 6, Instant.now().plus(lease));
                return readAll(ps);
            } finally {
                queueArray.free();
            }
        });
    }

    @Override
    public int renew(Map<String, String> tokensById, Duration lease) {
        if (tokensById.isEmpty()) return 0;
        var ids = new String[tokensById.size()];
        var tokens = new String[tokensById.size()];
        int i = 0;
        for (var e : tokensById.entrySet()) {
            ids[i] = e.getKey();
            tokens[i++] = e.getValue();
        }
        return withConnection(c -> {
            Array idArray = c.createArrayOf("text", ids);
            Array tokenArray = c.createArrayOf("text", tokens);
            try (var ps = c.prepareStatement(renewSql)) {
                setInstant(ps, 1, Instant.now().plus(lease));
                ps.setArray(2, idArray);
                ps.setArray(3, tokenArray);
                return ps.executeUpdate();
            } finally {
                idArray.free();
                tokenArray.free();
            }
        });
    }

    @Override
    public boolean ack(String id, String token) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(ackSql)) {
                ps.setString(1, id);
                ps.setString(2, token);
                return ps.executeUpdate() == 1;
            }
        });
    }

    /** {@code keepDeadFor} is not needed here: dead rows stay until {@link #purgeDeadTasks} removes them. */
    @Override
    public FailOutcome fail(String id, String token, Duration retryDelay, String error, Duration keepDeadFor) {
        var now = Instant.now();
        boolean forceDead = retryDelay == null;
        return withConnection(c -> {
            try (var ps = c.prepareStatement(failSql)) {
                ps.setBoolean(1, forceDead);
                ps.setBoolean(2, forceDead);
                setInstant(ps, 3, forceDead ? null : now.plus(retryDelay));
                ps.setBoolean(4, forceDead);
                setInstant(ps, 5, now);
                ps.setString(6, error);
                ps.setString(7, id);
                ps.setString(8, token);
                try (var rs = ps.executeQuery()) {
                    if (!rs.next()) return FailOutcome.LOST;
                    return rs.getInt(1) == DEAD ? FailOutcome.DEAD : FailOutcome.RETRY;
                }
            }
        });
    }

    @Override
    public boolean reschedule(String id, String token, Instant due) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(rescheduleSql)) {
                setInstant(ps, 1, due);
                ps.setString(2, id);
                ps.setString(3, token);
                return ps.executeUpdate() == 1;
            }
        });
    }

    @Override
    public boolean unclaim(String id, String token) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(unclaimSql)) {
                ps.setString(1, id);
                ps.setString(2, token);
                return ps.executeUpdate() == 1;
            }
        });
    }

    @Override
    public PromoteResult promote(int limit) {
        return withConnection(c -> {
            int moved;
            try (var ps = c.prepareStatement(promoteSql)) {
                setInstant(ps, 1, Instant.now());
                ps.setInt(2, limit);
                moved = ps.executeUpdate();
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
        return withConnection(c -> {
            try (var ps = c.prepareStatement(reclaimSql)) {
                setInstant(ps, 1, Instant.now());
                ps.setInt(2, limit);
                var result = new ArrayList<ExpiredLease>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(new ExpiredLease(
                                rs.getString(1), rs.getString(2), rs.getInt(3), rs.getInt(4), rs.getString(5)));
                    }
                }
                return result;
            }
        });
    }

    @Override
    public List<DueSchedule> dueRecurring(Instant upTo, int limit) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(dueScheduleSql)) {
                setInstant(ps, 1, upTo);
                ps.setInt(2, limit);
                var result = new ArrayList<DueSchedule>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        var definition = new RecurringDefinition(
                                rs.getString("id"),
                                rs.getString("event"),
                                rs.getString("account"),
                                rs.getString("queue"),
                                rs.getString("cron"),
                                rs.getString("data"),
                                rs.getInt("max_attempts"),
                                rs.getInt("rev"));
                        result.add(new DueSchedule(definition.scheduleId(), getInstant(rs, "next_fire"), definition));
                    }
                }
                return result;
            }
        });
    }

    @Override
    public boolean commitRecurring(DueSchedule due, Instant nextFire, TaskRecord o) {
        return withConnection(c -> {
            try (var ps = c.prepareStatement(commitScheduleSql)) {
                setInstant(ps, 1, nextFire);
                ps.setString(2, due.scheduleId());
                ps.setInt(3, due.definition().revision());
                setInstant(ps, 4, due.fireAt());
                ps.setString(5, o.id());
                ps.setString(6, o.queue());
                ps.setString(7, o.event());
                ps.setString(8, o.account());
                ps.setString(9, o.data());
                ps.setInt(10, o.maxAttempts());
                setInstant(ps, 11, o.created());
                setInstant(ps, 12, due.fireAt());
                ps.setString(13, o.scheduleId());
                return single(ps) == 1;
            }
        });
    }

    // ---- JDBC helpers --------------------------------------------------------------------------------

    @FunctionalInterface
    private interface SqlFunction<T> {
        T apply(Connection c) throws SQLException;
    }

    private <T> T withConnection(SqlFunction<T> fn) {
        try (var c = dataSource.getConnection()) {
            boolean autoCommit = c.getAutoCommit();
            try {
                var result = fn.apply(c);
                if (!autoCommit) c.commit();
                return result;
            } catch (SQLException e) {
                if (!autoCommit) c.rollback();
                throw e;
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
        return new TaskRecord(
                rs.getString("id"),
                rs.getString("event"),
                rs.getString("account"),
                rs.getString("queue"),
                rs.getString("data"),
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                getInstant(rs, "created"),
                getInstant(rs, "due"),
                rs.getString("token"),
                getInstant(rs, "lease_until"),
                rs.getString("schedule_id"),
                rs.getString("error"),
                getInstant(rs, "died"));
    }

    /** Millisecond precision on the way in, so a value read back compares equal to what was written. */
    private static void setInstant(PreparedStatement ps, int index, Instant instant) throws SQLException {
        if (instant == null) {
            ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            ps.setObject(index, OffsetDateTime.ofInstant(instant.truncatedTo(ChronoUnit.MILLIS), ZoneOffset.UTC));
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

    @Override
    public String toString() {
        return "PostgresTaskStore[" + prefix + "]";
    }
}

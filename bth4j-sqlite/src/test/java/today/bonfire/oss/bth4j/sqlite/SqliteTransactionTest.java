package today.bonfire.oss.bth4j.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.RetryOutcome;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.StateCounts;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * What the store does with transactions and connections, on a real database file: the write lock, the state a
 * pooled connection is returned in, a caller's own transaction, the times the store writes and the atomicity of
 * multi-statement operations, which the tests break with a trigger that the test itself installs.
 */
@Timeout(60)
class SqliteTransactionTest {
    private static final AtomicInteger NAMESPACES = new AtomicInteger();
    private static final Duration LEASE = Duration.ofSeconds(30);

    private static SqliteTestSupport database;

    private final MutableClock clock = new MutableClock(Instant.parse("2100-06-15T12:00:00.123Z"));
    private SqliteTaskStore store;
    private String prefix;
    private int queue;
    private int type;
    private int worker;

    @BeforeAll
    static void openDatabase() {
        database = SqliteTestSupport.open();
    }

    @AfterAll
    static void closeDatabase() {
        database.close();
    }

    @BeforeEach
    void setUp() {
        var namespace = "tx" + NAMESPACES.incrementAndGet();
        prefix = SqliteTestSupport.prefix(namespace);
        store = database.store(namespace, clock);
        queue = store.queueId("default");
        type = store.typeId("send-mail");
        worker = store.allocateWorkerId("host:1", Set.of(), Set.of());
    }

    private TaskRecord.Builder task() {
        return SqliteTestSupport.task(store.allocateIds(1), queue, type, clock.instant());
    }

    private TaskRecord get(long id) {
        return store.getTask(id).orElseThrow();
    }

    private List<TaskRecord> claim(int count) {
        return store.claim(new ClaimRequest(List.of(new QueueGrant(queue, count, count)), count, LEASE, worker, null));
    }

    private TaskRecord enqueueAndClaim(TaskRecord.Builder builder) {
        var t = builder.build();
        assertThat(store.enqueue(t)).isEqualTo(EnqueueOutcome.STORED);
        var claimed = claim(1);
        assertThat(claimed).extracting(TaskRecord::id).containsExactly(t.id());
        return claimed.getFirst();
    }

    private FailureInfo failure(int attempt) {
        return new FailureInfo(attempt, clock.instant(), "java.io.IOException", "boom " + attempt, null);
    }

    private void sql(String statement) throws SQLException {
        try (var c = database.dataSource().getConnection();
                var st = c.createStatement()) {
            st.execute(statement);
        }
    }

    /** The first column of the first row of a query on the shared database. */
    private long scalar(String query) throws SQLException {
        try (var c = database.dataSource().getConnection();
                var st = c.createStatement();
                var rs = st.executeQuery(query)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private record Row(int state, int attempts, int revision) {}

    /** A task row as stored, readable even when its values are not. */
    private Row row(long id) throws SQLException {
        try (var c = database.dataSource().getConnection();
                var st = c.createStatement();
                var rs =
                        st.executeQuery("SELECT state, attempts, revision FROM " + prefix + "_task WHERE id = " + id)) {
            assertThat(rs.next()).isTrue();
            return new Row(rs.getInt(1), rs.getInt(2), rs.getInt(3));
        }
    }

    private void failWrites(String event, String table, String condition) throws SQLException {
        sql("CREATE TRIGGER " + prefix + "_boom BEFORE " + event + " ON " + prefix + "_" + table + " WHEN " + condition
                + " BEGIN SELECT RAISE(ABORT, 'injected failure'); END");
    }

    private void healWrites() throws SQLException {
        sql("DROP TRIGGER IF EXISTS " + prefix + "_boom");
    }

    private RecurringDefinition fixedDelay(String id, long delayMs) {
        return new RecurringDefinition(
                id, type, 1, queue, null, null, null, null, 0, delayMs, false, null, 1, null, false, 0);
    }

    // ---- the state a connection is returned in ----------------------------------------------------------

    private static final String WAL = "journal_mode=WAL&busy_timeout=5000";

    /** Runs a representative round trip of the store on its data source. */
    private void roundTrip(SqliteTaskStore pooled) {
        int pooledQueue = pooled.queueId("pooled");
        int pooledType = pooled.typeId("pooled-type");
        int pooledWorker = pooled.allocateWorkerId("pooled", Set.of(pooledQueue), Set.of(pooledType));
        long succeededBefore =
                pooled.counts().getOrDefault(pooledQueue, StateCounts.ZERO).succeeded();
        int deadBefore =
                pooled.tasks(TaskState.DEAD, pooledQueue, null, 100).items().size();
        var first = SqliteTestSupport.task(pooled.allocateIds(1), pooledQueue, pooledType, clock.instant());
        var second = SqliteTestSupport.task(pooled.allocateIds(1), pooledQueue, pooledType, clock.instant());
        assertThat(pooled.enqueueAll(List.of(first.build(), second.build())))
                .containsExactly(EnqueueOutcome.STORED, EnqueueOutcome.STORED);
        var claimed = pooled.claim(
                new ClaimRequest(List.of(new QueueGrant(pooledQueue, 2, 2)), 2, LEASE, pooledWorker, null));
        assertThat(claimed).hasSize(2);
        assertThat(pooled.renew(
                        claimed.stream()
                                .map(t -> new LeaseRef(t.id(), t.revision()))
                                .toList(),
                        LEASE))
                .allSatisfy(s -> assertThat(s.owned()).isTrue());
        assertThat(pooled.ack(claimed.get(0).id(), claimed.get(0).revision(), true, true, null))
                .isTrue();
        assertThat(pooled.fail(claimed.get(1).id(), claimed.get(1).revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(pooled.counts().get(pooledQueue).succeeded()).isEqualTo(succeededBefore + 1);
        assertThat(pooled.tasks(TaskState.DEAD, pooledQueue, null, 100).items()).hasSize(deadBefore + 1);
    }

    /** A second connection that can still take the write lock: the first left none behind. */
    private void assertNoWriteLockHeld() throws SQLException {
        try (var c = DriverManager.getConnection(SqliteTestSupport.url(database.file(), "busy_timeout=100"));
                var st = c.createStatement()) {
            assertThatCode(() -> {
                        st.execute("BEGIN IMMEDIATE");
                        st.execute("COMMIT");
                    })
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void aPoolWithoutAutoCommitGetsItsConnectionBackAsItWasAndHoldsNoLock() throws SQLException {
        try (var pool = SqliteTestSupport.pool(SqliteTestSupport.url(database.file(), WAL), false, 1)) {
            var pooled = new SqliteTaskStore(pool, prefix, clock);

            roundTrip(pooled);
            roundTrip(pooled);

            try (Connection c = pool.getConnection()) {
                assertThat(c.getAutoCommit()).isFalse();
            }
            assertNoWriteLockHeld();
        }
    }

    @Test
    void aPoolWithAutoCommitGetsItsConnectionBackAsItWasAndHoldsNoLock() throws SQLException {
        try (var pool = SqliteTestSupport.pool(SqliteTestSupport.url(database.file(), WAL), true, 1)) {
            var pooled = new SqliteTaskStore(pool, prefix, clock);

            roundTrip(pooled);
            roundTrip(pooled);

            try (Connection c = pool.getConnection()) {
                assertThat(c.getAutoCommit()).isTrue();
            }
            assertNoWriteLockHeld();
        }
    }

    @Test
    void aPooledConnectionWithoutAutoCommitDoesNotKeepAReadSnapshot() throws SQLException {
        try (var pool = SqliteTestSupport.pool(SqliteTestSupport.url(database.file(), WAL), false, 1)) {
            var pooled = new SqliteTaskStore(pool, prefix, clock);
            assertThat(pooled.counts().get(queue).queued()).isZero();

            var stored = task().build();
            assertThat(store.enqueue(stored)).isEqualTo(EnqueueOutcome.STORED);

            assertThat(pooled.getTask(stored.id())).isPresent();
            assertThat(pooled.counts().get(queue).queued()).isEqualTo(1);
        }
    }

    @Test
    void aPoolInImmediateTransactionModeWithAutoCommitHoldsNoLock() throws SQLException {
        var url = SqliteTestSupport.url(database.file(), WAL + "&transaction_mode=IMMEDIATE");
        try (var pool = SqliteTestSupport.pool(url, true, 1)) {
            roundTrip(new SqliteTaskStore(pool, prefix, clock));

            try (Connection c = pool.getConnection()) {
                assertThat(c.getAutoCommit()).isTrue();
            }
            assertNoWriteLockHeld();
        }
    }

    @Test
    void aPoolInImmediateTransactionModeWithoutAutoCommitWorksButTheDriverHoldsTheLockOnIdleConnections()
            throws SQLException {
        var url = SqliteTestSupport.url(database.file(), WAL + "&transaction_mode=IMMEDIATE");
        try (var pool = SqliteTestSupport.pool(url, false, 1)) {
            assertThatThrownBy(this::assertNoWriteLockHeld)
                    .as("the driver begins an IMMEDIATE transaction as soon as the pool turns auto-commit off")
                    .isInstanceOf(AssertionError.class);

            roundTrip(new SqliteTaskStore(pool, prefix, clock));

            try (Connection c = pool.getConnection()) {
                assertThat(c.getAutoCommit()).isFalse();
            }
        }
    }

    @Test
    void aFailedWriteRollsBackAndLeavesTheConnectionUsable() throws SQLException {
        for (var autoCommit : new boolean[] {true, false}) {
            try (var pool = SqliteTestSupport.pool(SqliteTestSupport.url(database.file(), WAL), autoCommit, 1)) {
                var pooled = new SqliteTaskStore(pool, prefix, clock);
                var a = task().build();
                var b = task().build();
                assertThat(pooled.enqueueAll(List.of(a, b)))
                        .containsExactly(EnqueueOutcome.STORED, EnqueueOutcome.STORED);
                var request = new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, worker, null);

                failWrites("UPDATE", "task", "NEW.id = " + b.id());
                try {
                    assertThatThrownBy(() -> pooled.claim(request)).isInstanceOf(TaskStoreException.class);
                } finally {
                    healWrites();
                }

                assertThat(row(a.id())).isEqualTo(new Row(0, 0, a.revision()));
                assertThat(row(b.id())).isEqualTo(new Row(0, 0, b.revision()));
                assertThat(pooled.claim(request)).extracting(TaskRecord::id).containsExactlyInAnyOrder(a.id(), b.id());
                assertNoWriteLockHeld();
            }
        }
    }

    // ---- atomicity of multi-statement operations -----------------------------------------------------

    @Test
    void aBatchOfEnqueuesIsStoredWholeOrNotAtAll() throws SQLException {
        var batch = IntStream.range(0, 5).mapToObj(i -> task().build()).toList();

        failWrites("INSERT", "task", "NEW.id = " + batch.get(3).id());
        try {
            assertThatThrownBy(() -> store.enqueueAll(batch)).isInstanceOf(TaskStoreException.class);
        } finally {
            healWrites();
        }

        batch.forEach(t -> assertThat(store.getTask(t.id())).isEmpty());
        assertThat(store.enqueueAll(batch)).containsOnly(EnqueueOutcome.STORED);
    }

    @Test
    void aClaimThatFailsAfterItsFirstStatementLeavesNothingLeased() throws SQLException {
        var bad = task().build();
        var good = task().build();
        assertThat(store.enqueueAll(List.of(bad, good))).containsOnly(EnqueueOutcome.STORED);
        sql("UPDATE " + prefix + "_task SET headers = '{\"a\":null}' WHERE id = " + bad.id());

        // the unreadable task makes the claim end its lease, which is the statement that fails here
        failWrites("UPDATE", "task", "NEW.lease_until = NEW.claimed_at");
        try {
            assertThatThrownBy(() -> claim(10)).isInstanceOf(TaskStoreException.class);
        } finally {
            healWrites();
        }

        assertThat(row(good.id())).isEqualTo(new Row(0, 0, good.revision()));
        assertThat(row(bad.id())).isEqualTo(new Row(0, 0, bad.revision()));
        assertThat(claim(10)).extracting(TaskRecord::id).containsExactly(good.id());
        assertThat(store.reclaim(10)).extracting(l -> l.id()).containsExactly(bad.id());
    }

    private record Occurrence(Instant fireAt, DueSchedule due, TaskRecord leased) {}

    private Occurrence leasedOccurrenceOfFixedDelay(String scheduleId, String key) {
        var fireAt = clock.instant().minusSeconds(10);
        store.putRecurring(fixedDelay(scheduleId, 60_000), fireAt, fireAt);
        var due = store.dueRecurring(clock.instant(), 10).getFirst();
        var builder = task();
        if (key != null) builder.uniqueKey(key);
        assertThat(store.commitRecurring(due, fireAt.plusSeconds(1), builder.build()))
                .isTrue();
        assertThat(store.promote(10).moved()).isEqualTo(1);
        return new Occurrence(fireAt, due, claim(1).getFirst());
    }

    private Long occurrenceOf(String scheduleId) throws SQLException {
        try (var c = database.dataSource().getConnection();
                var st = c.createStatement();
                var rs = st.executeQuery(
                        "SELECT occurrence_id FROM " + prefix + "_schedule WHERE id = '" + scheduleId + "'")) {
            assertThat(rs.next()).isTrue();
            var id = rs.getLong(1);
            return rs.wasNull() ? null : id;
        }
    }

    @Test
    void anAckThatFailsAfterTheTaskIsEndedLeavesTaskAndScheduleUntouched() throws SQLException {
        var o = leasedOccurrenceOfFixedDelay("ack", null);
        var effect = ScheduleEffect.rearm("ack", o.leased().id(), o.fireAt().plusSeconds(500));

        failWrites("UPDATE", "schedule", "OLD.occurrence_id IS NOT NULL AND NEW.occurrence_id IS NULL");
        try {
            assertThatThrownBy(() -> store.ack(o.leased().id(), o.leased().revision(), true, true, effect))
                    .isInstanceOf(TaskStoreException.class);
        } finally {
            healWrites();
        }

        assertThat(get(o.leased().id()).state()).isEqualTo(TaskState.LEASED);
        assertThat(get(o.leased().id()).revision()).isEqualTo(o.leased().revision());
        assertThat(occurrenceOf("ack")).isEqualTo(o.leased().id());
        assertThat(store.recurringSchedules()).containsEntry("ack", o.fireAt().plusSeconds(1));
        assertThat(effect.applied()).isFalse();

        assertThat(store.ack(o.leased().id(), o.leased().revision(), true, true, effect))
                .isTrue();
        assertThat(effect.applied()).isTrue();
        assertThat(store.recurringSchedules()).containsEntry("ack", o.fireAt().plusSeconds(500));
        assertThat(occurrenceOf("ack")).isNull();
    }

    @Test
    void aFailThatFailsAfterTheTaskDiesLeavesTaskAndScheduleUntouched() throws SQLException {
        var o = leasedOccurrenceOfFixedDelay("dies", null);
        var effect = ScheduleEffect.pause("dies", o.leased().id());

        failWrites("UPDATE", "schedule", "OLD.occurrence_id IS NOT NULL AND NEW.occurrence_id IS NULL");
        try {
            assertThatThrownBy(() -> store.fail(o.leased().id(), o.leased().revision(), failure(1), null, true, effect))
                    .isInstanceOf(TaskStoreException.class);
        } finally {
            healWrites();
        }

        var after = get(o.leased().id());
        assertThat(after.state()).isEqualTo(TaskState.LEASED);
        assertThat(after.revision()).isEqualTo(o.leased().revision());
        assertThat(after.failures()).isNull();
        assertThat(occurrenceOf("dies")).isEqualTo(o.leased().id());
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isFalse();
        assertThat(effect.applied()).isFalse();

        assertThat(store.fail(o.leased().id(), o.leased().revision(), failure(1), null, true, effect))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(effect.applied()).isTrue();
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isTrue();
        assertThat(occurrenceOf("dies")).isNull();
    }

    @Test
    void aRecurringCommitThatFailsAfterTheOccurrenceIsInsertedStoresNothing() throws SQLException {
        var fireAt = clock.instant().minusSeconds(10);
        store.putRecurring(fixedDelay("commit", 60_000), fireAt, fireAt);
        var due = store.dueRecurring(clock.instant(), 10).getFirst();
        var occurrence = task().build();

        failWrites(
                "UPDATE", "schedule", "NEW.occurrence_id IS NOT NULL AND OLD.occurrence_id IS NOT NEW.occurrence_id");
        try {
            assertThatThrownBy(() -> store.commitRecurring(due, fireAt.plusSeconds(1), occurrence))
                    .isInstanceOf(TaskStoreException.class);
        } finally {
            healWrites();
        }

        assertThat(store.getTask(occurrence.id())).isEmpty();
        assertThat(occurrenceOf("commit")).isNull();
        assertThat(store.recurringSchedules()).containsEntry("commit", fireAt);
        assertThat(store.dueRecurring(clock.instant(), 10)).hasSize(1);

        assertThat(store.commitRecurring(due, fireAt.plusSeconds(1), occurrence))
                .isTrue();
        assertThat(occurrenceOf("commit")).isEqualTo(occurrence.id());
        assertThat(get(occurrence.id()).scheduleId()).isEqualTo("commit");
        assertThat(get(occurrence.id()).revision()).isEqualTo(occurrence.revision());
    }

    // ---- the caller's own transaction ----------------------------------------------------------------

    @Test
    void enqueueOnTheCallersConnectionFollowsItsTransaction() throws SQLException {
        var rolledBack = task().build();
        var committed = task().uniqueKey("tx").build();
        var other = task().uniqueKey("tx").build();
        try (var c = database.dataSource().getConnection()) {
            c.setAutoCommit(false);
            assertThat(store.enqueue(c, rolledBack)).isEqualTo(EnqueueOutcome.STORED);
            assertThat(store.getTask(rolledBack.id())).isEmpty();
            c.rollback();

            assertThat(store.enqueue(c, committed)).isEqualTo(EnqueueOutcome.STORED);
            assertThat(store.enqueue(c, committed)).isEqualTo(EnqueueOutcome.EXISTS);
            assertThat(store.enqueue(c, other)).isEqualTo(new EnqueueOutcome.DuplicateKey(committed.id()));
            assertThat(store.getTask(committed.id())).isEmpty();
            c.commit();
            assertThat(c.isClosed()).isFalse();
            c.setAutoCommit(true);
        }

        assertThat(store.getTask(rolledBack.id())).isEmpty();
        assertThat(get(committed.id()).state()).isEqualTo(TaskState.QUEUED);
        var unused = task().build();
        assertThatThrownBy(() -> store.enqueue(null, unused)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void enqueueOnTheCallersConnectionInImmediateModeLeavesNoTaskAfterARollback() throws SQLException {
        var url = SqliteTestSupport.url(database.file(), WAL + "&transaction_mode=IMMEDIATE");
        var rolledBack = task().build();
        try (var pool = SqliteTestSupport.pool(url, true, 2);
                var c = pool.getConnection()) {
            c.setAutoCommit(false);
            assertThat(store.enqueue(c, rolledBack)).isEqualTo(EnqueueOutcome.STORED);
            assertThat(scalar("SELECT count(*) FROM " + prefix + "_task WHERE id = " + rolledBack.id()))
                    .isZero();
            c.rollback();
            c.setAutoCommit(true);
        }

        assertThat(store.getTask(rolledBack.id())).isEmpty();
        assertThat(store.enqueue(rolledBack)).isEqualTo(EnqueueOutcome.STORED);
    }

    // ---- the write lock ------------------------------------------------------------------------------

    @Test
    void emptyClaimsAndIdleMaintenanceDoNotWaitForTheWriteLock() throws SQLException {
        try (var quickPool = SqliteTestSupport.pool(
                        SqliteTestSupport.url(database.file(), "journal_mode=WAL&busy_timeout=1000"), true, 2);
                var holder = database.dataSource().getConnection();
                var hold = holder.createStatement()) {
            var quick = new SqliteTaskStore(quickPool, prefix, clock);
            hold.execute("BEGIN IMMEDIATE");
            try {
                assertThat(claim(5)).isEmpty();
                assertThat(quick.claim(new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, worker, null)))
                        .isEmpty();
                assertThat(quick.promote(10).moved()).isZero();
                assertThat(quick.reclaim(10)).isEmpty();
                assertThat(quick.purge(TaskState.DEAD, Duration.ZERO, 10)).isZero();
                assertThat(quick.purge(TaskState.CANCELLED, Duration.ZERO, 10)).isZero();
                assertThat(quick.releaseKeys(10)).isZero();
                assertThat(quick.expire(10)).isEmpty();
                assertThat(quick.purgeWorkers(Duration.ofDays(1))).isZero();
                assertThat(quick.nextDue()).isNull();
                assertThat(quick.queueId("default")).isEqualTo(queue);
                assertThat(quick.typeId("send-mail")).isEqualTo(type);
                assertThat(quick.counts()).containsKey(queue);

                // the lock is really held: a write waits for the busy timeout and then fails
                assertThatThrownBy(() -> quick.allocateIds(1)).isInstanceOf(TaskStoreException.class);
            } finally {
                hold.execute("ROLLBACK");
            }
        }
    }

    // ---- times and counters --------------------------------------------------------------------------

    @Test
    void everyTimeComesFromTheStoreClock() {
        var now = clock.instant();
        var t = enqueueAndClaim(task().createdAt(null).due(null).uniqueKey("k").keyKeep(Duration.ofMinutes(1)));

        assertThat(t.createdAt()).isEqualTo(now);
        assertThat(t.due()).isEqualTo(now);
        assertThat(t.stateChangedAt()).isEqualTo(now);
        assertThat(t.claimedAt()).isEqualTo(now);
        assertThat(t.leaseUntil()).isEqualTo(now.plus(LEASE));
        var worker = store.workers().stream()
                .filter(w -> w.id() == this.worker)
                .findFirst()
                .orElseThrow();
        assertThat(worker.startedAt()).isEqualTo(now);
        assertThat(worker.lastSeen()).isEqualTo(now);

        clock.advance(Duration.ofSeconds(5));
        var later = clock.instant();
        store.heartbeat(this.worker, "host:1", Set.of(), Set.of(), 1, true);
        assertThat(store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE))
                .singleElement()
                .satisfies(s -> assertThat(s.owned()).isTrue());
        assertThat(get(t.id()).leaseUntil()).isEqualTo(later.plus(LEASE));
        assertThat(store.workers().getFirst().lastSeen()).isEqualTo(later);
        assertThat(store.workers().getFirst().startedAt()).isEqualTo(now);

        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        var done = get(t.id());
        assertThat(done.stateChangedAt()).isEqualTo(later);
        assertThat(done.keyUntil()).isEqualTo(later.plus(Duration.ofMinutes(1)));

        var retried = enqueueAndClaim(task());
        var retryAt = later.plusSeconds(60);
        assertThat(store.fail(retried.id(), retried.revision(), failure(1), retryAt, true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(retried.id()).due()).isEqualTo(retryAt);
        assertThat(get(retried.id()).stateChangedAt()).isEqualTo(later);
        clock.advance(Duration.ofSeconds(61));
        assertThat(store.promote(10).moved()).isEqualTo(1);
        assertThat(get(retried.id()).stateChangedAt()).isEqualTo(clock.instant());
    }

    @Test
    void workerIdsAreNeverReusedAfterAPurge() {
        int first = store.allocateWorkerId("host:2", Set.of(), Set.of());
        clock.advance(Duration.ofHours(1));

        assertThat(store.purgeWorkers(Duration.ofMinutes(30))).isEqualTo(2);
        int next = store.allocateWorkerId("host:3", Set.of(), Set.of());

        assertThat(next).isGreaterThan(first);
        assertThat(store.workers()).extracting(w -> w.id()).containsExactly(next);
    }

    @Test
    void registryLookupsResolveNamesOnceWithoutBurningIds() {
        for (int i = 0; i < 20; i++) assertThat(store.queueId("default")).isEqualTo(queue);
        int other = store.queueId("other");

        assertThat(other).isEqualTo(queue + 1);
        assertThat(store.typeId("send-mail")).isEqualTo(type);
        int report = store.typeId("report");
        assertThat(store.queueNames()).containsExactly(Map.entry(queue, "default"), Map.entry(other, "other"));
        assertThat(store.typeNames()).containsExactly(Map.entry(type, "send-mail"), Map.entry(report, "report"));
        assertThatThrownBy(() -> store.queueId(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.typeId(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aFullRegistryIsReportedAndKeepsItsCounter() throws SQLException {
        sql("UPDATE " + prefix + "_meta SET value = 32768 WHERE key = 'next_type_id'");

        assertThatThrownBy(() -> store.typeId("one-too-many"))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("type registry is full");
        assertThat(scalar("SELECT value FROM " + prefix + "_meta WHERE key = 'next_type_id'"))
                .isEqualTo(32768);
        assertThat(store.typeNames()).doesNotContainValue("one-too-many");
    }

    // ---- values that go through JSON text ------------------------------------------------------------

    @Test
    void uniqueKeysWithQuotesAndUnicodeAreHeldAndReleased() {
        var key = "a\"b\\c\né😀'; DROP TABLE x; --";
        var first = enqueueAndClaim(task().uniqueKey(key).keyKeep(Duration.ofSeconds(1)));
        assertThat(store.ack(first.id(), first.revision(), false, false, null)).isTrue();
        assertThat(store.enqueue(task().uniqueKey(key).build())).isEqualTo(new EnqueueOutcome.DuplicateKey(first.id()));

        clock.advance(Duration.ofMillis(1100));
        var second = task().uniqueKey(key).build();

        assertThat(store.enqueue(second)).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.taskByKey(key)).map(TaskRecord::id).contains(second.id());
        assertThat(store.releaseKeys(10)).isZero();
        assertThat(store.tasks(TaskState.QUEUED, queue, null, 10).items()).hasSize(1);
    }

    @Test
    void aClaimFiltersByALongListOfKnownTypes() {
        var known = task().build();
        var unknown = task().typeId(9000).build();
        assertThat(store.enqueueAll(List.of(unknown, known))).containsOnly(EnqueueOutcome.STORED);
        var types = new java.util.HashSet<Integer>();
        IntStream.rangeClosed(1, 5000).forEach(types::add);

        var claimed = store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 10, 10)), 10, LEASE, worker, types));

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(known.id());
        assertThat(get(unknown.id()).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void aKeyOnlyRowIsOutOfReachOfAdminWrites() throws SQLException {
        var t = enqueueAndClaim(task().uniqueKey("gone").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(t.id(), t.revision(), false, false, null)).isTrue();
        var row = row(t.id());
        assertThat(row.state()).isEqualTo(7);

        assertThat(store.getTask(t.id())).isEmpty();
        assertThat(store.taskByKey("gone")).isEmpty();
        assertThat(store.counts().get(queue).succeeded()).isZero();
        assertThat(store.delete(t.id(), row.revision(), null)).isFalse();
        assertThat(store.cancel(t.id(), row.revision(), "no", null)).isEqualTo(CancelOutcome.STALE);
        assertThat(store.retry(t.id(), row.revision())).isEqualTo(RetryOutcome.STALE);
        assertThat(store.enqueue(task().id(t.id()).build())).isEqualTo(EnqueueOutcome.EXISTS);
        assertThat(store.enqueue(task().uniqueKey("gone").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));

        clock.advance(Duration.ofHours(2));
        assertThat(store.releaseKeys(10)).isEqualTo(1);
        assertThat(scalar("SELECT count(*) FROM " + prefix + "_task WHERE id = " + t.id()))
                .isZero();
    }
}

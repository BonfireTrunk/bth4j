package today.bonfire.oss.bth4j.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import today.bonfire.oss.bth4j.spi.CancelOutcome;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.LeaseStatus;
import today.bonfire.oss.bth4j.spi.PutRecurringOutcome;
import today.bonfire.oss.bth4j.spi.QueueInfo;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
import today.bonfire.oss.bth4j.spi.RetryOutcome;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;

/** Every {@link PostgresTaskStore} operation against a real PostgreSQL, each test on its own tables. Needs Docker. */
class PostgresTaskStoreIT {
    private static final AtomicInteger NAMESPACES = new AtomicInteger();
    private static final Duration LEASE = Duration.ofSeconds(30);

    private PostgresTaskStore store;
    private String prefix;
    private int queue;
    private int type;
    private int worker;

    @BeforeEach
    void setUp() {
        var namespace = "it" + NAMESPACES.incrementAndGet() + "_" + System.nanoTime();
        prefix = PostgresTestSupport.prefix(namespace);
        store = PostgresTestSupport.store(namespace);
        queue = store.queueId("default");
        type = store.typeId("send-mail");
        worker = store.allocateWorkerId("host:1", Set.of(), Set.of());
    }

    private TaskRecord.Builder task(long id) {
        return TaskRecord.builder()
                .id(id)
                .queueId(queue)
                .typeId(type)
                .maxAttempts(3)
                .createdAt(Instant.now())
                .encodedPayload("payload-" + id);
    }

    private long newId() {
        return store.allocateIds(1);
    }

    private TaskRecord enqueueAndClaim(TaskRecord.Builder b) {
        var t = b.build();
        assertThat(store.enqueue(t)).isEqualTo(EnqueueOutcome.STORED);
        var claimed = claim(1);
        assertThat(claimed).extracting(TaskRecord::id).containsExactly(t.id());
        return claimed.getFirst();
    }

    private List<TaskRecord> claim(int count) {
        return store.claim(new ClaimRequest(List.of(queue), count, LEASE, worker, null));
    }

    private TaskRecord get(long id) {
        return store.getTask(id).orElseThrow();
    }

    private static FailureInfo failure(int attempt) {
        return new FailureInfo(
                attempt,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                "java.io.IOException",
                "boom " + attempt,
                "stack");
    }

    private void sql(String statement) throws SQLException {
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var st = c.createStatement()) {
            st.execute(statement);
        }
    }

    private static boolean tableExists(String name) throws SQLException {
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var ps = c.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
            ps.setString(1, name);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    // ---- schema and identity -------------------------------------------------------------------------

    @Test
    void initIsIdempotentAndReportsTheEngine() {
        store.init();
        store.init();
        assertThat(store.info().engine()).isEqualTo("postgresql");
        assertThat(store.info().schemaVersion()).isEqualTo(PostgresTaskStore.SCHEMA_VERSION);
    }

    @Test
    void initAddsCoverageToAnExistingWorkerTableWithoutChangingTheSchemaVersion() throws SQLException {
        sql("ALTER TABLE " + prefix + "_worker DROP COLUMN queue_ids, DROP COLUMN type_ids");
        store.init();
        store.init();

        var legacy = store.workers().getFirst();
        assertThat(legacy.id()).isEqualTo(worker);
        assertThat(legacy.queueIds()).isNull();
        assertThat(legacy.typeIds()).isNull();
        sql("INSERT INTO " + prefix + "_worker (label, started_at, last_seen) VALUES ('old', now(), now())");
        int registered = store.allocateWorkerId("new", Set.of(queue), Set.of(type));
        store.heartbeat(registered, 2);
        assertThat(store.workers())
                .filteredOn(w -> w.id() == registered)
                .singleElement()
                .satisfies(w -> {
                    assertThat(w.queueIds()).containsExactly(queue);
                    assertThat(w.typeIds()).containsExactly(type);
                    assertThat(w.running()).isEqualTo(2);
                });
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var st = c.createStatement();
                var rs = st.executeQuery("SELECT value FROM " + prefix + "_meta WHERE key = 'schema_version'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("2");
        }
    }

    @Test
    void initRefusesAnotherSchemaVersion() throws SQLException {
        sql("UPDATE " + prefix + "_meta SET value = '3' WHERE key = 'schema_version'");
        assertThatThrownBy(store::init)
                .isInstanceOf(TaskStoreException.class)
                .hasMessage("schema version 3, this library needs 2");
    }

    @Test
    void initRefusesThe30SchemaBeforeChangingIt() throws SQLException {
        var old = new PostgresTaskStore(PostgresTestSupport.dataSource(), prefix + "_v1");
        sql("CREATE TABLE " + prefix + "_v1_meta (key text PRIMARY KEY, value text NOT NULL)");
        sql("INSERT INTO " + prefix + "_v1_meta (key, value) VALUES ('schema_version', '1')");
        assertThatThrownBy(old::init)
                .isInstanceOf(TaskStoreException.class)
                .hasMessage("schema version 1, this library needs 2");
        assertThat(tableExists(prefix + "_v1_task")).isFalse();
    }

    @Test
    void initRefusesATaskTableWithoutMeta() throws SQLException {
        var old = new PostgresTaskStore(PostgresTestSupport.dataSource(), prefix + "_old");
        sql("CREATE TABLE " + prefix + "_old_task (id text PRIMARY KEY)");
        assertThatThrownBy(old::init).isInstanceOf(TaskStoreException.class).hasMessageContaining("older bth4j layout");
    }

    @Test
    void concurrentInitsDoNotFail() throws Exception {
        var fresh = new PostgresTaskStore(PostgresTestSupport.dataSource(), prefix + "_c");
        try (var pool = Executors.newFixedThreadPool(4)) {
            var calls = new ArrayList<Callable<Object>>();
            for (int i = 0; i < 4; i++) calls.add(Executors.callable(fresh::init));
            for (var f : pool.invokeAll(calls)) f.get();
        }
    }

    @Test
    void idBlocksAreContiguousAndDisjointAcrossThreads() throws Exception {
        var ids = new HashSet<Long>();
        try (var pool = Executors.newFixedThreadPool(8)) {
            var calls = new ArrayList<Callable<Long>>();
            for (int i = 0; i < 40; i++) calls.add(() -> store.allocateIds(64));
            for (var f : pool.invokeAll(calls)) {
                long first = f.get();
                for (long id = first; id < first + 64; id++)
                    assertThat(ids.add(id)).as("id %d", id).isTrue();
            }
        }
        assertThat(ids).hasSize(40 * 64);
        assertThatThrownBy(() -> store.allocateIds(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void idHighWaterMarkNeedsOnlyUsageOnTheSequence() throws SQLException {
        String role = prefix + "_producer";
        sql("CREATE ROLE " + role + " LOGIN PASSWORD 'test-producer'");
        try {
            sql("GRANT USAGE ON SCHEMA public TO " + role);
            sql("GRANT SELECT, UPDATE ON TABLE " + prefix + "_meta TO " + role);
            sql("GRANT USAGE ON SEQUENCE " + prefix + "_task_id TO " + role);
            var restricted = new PGSimpleDataSource();
            try (var c = PostgresTestSupport.dataSource().getConnection()) {
                restricted.setUrl(c.getMetaData().getURL());
            }
            restricted.setUser(role);
            restricted.setPassword("test-producer");
            var producer = new PostgresTaskStore(restricted, prefix);
            try (var c = restricted.getConnection();
                    var st = c.createStatement();
                    var rs = st.executeQuery("SELECT current_user, has_sequence_privilege('" + prefix
                            + "_task_id', 'USAGE'), has_sequence_privilege('" + prefix + "_task_id', 'SELECT')")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo(role);
                assertThat(rs.getBoolean(2)).isTrue();
                assertThat(rs.getBoolean(3)).isFalse();
            }
            assertThat(producer.idHighWaterMark()).isEqualTo(1);
            assertThat(producer.allocateIds(3)).isEqualTo(1);
            assertThat(producer.idHighWaterMark()).isEqualTo(4);
            assertThat(store.allocateIds(2)).isEqualTo(4);
            assertThat(producer.idHighWaterMark()).isEqualTo(6);
        } finally {
            sql("DROP OWNED BY " + role);
            sql("DROP ROLE " + role);
        }
    }

    @Test
    void workerIdsAreNeverReused() {
        int second = store.allocateWorkerId("host:2", Set.of(), Set.of());
        assertThat(second).isGreaterThan(worker);
    }

    @Test
    void registryResolvesNamesOnceWithoutBurningIds() {
        for (int i = 0; i < 20; i++) assertThat(store.queueId("default")).isEqualTo(queue);
        int other = store.queueId("other");
        assertThat(other).isEqualTo(queue + 1);
        assertThat(store.typeId("send-mail")).isEqualTo(type);
        int report = store.typeId("report");
        assertThat(store.queueNames()).containsExactly(Map.entry(queue, "default"), Map.entry(other, "other"));
        assertThat(store.typeNames()).containsExactly(Map.entry(type, "send-mail"), Map.entry(report, "report"));
        assertThatThrownBy(() -> store.queueId(" ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fullRegistryIsReported() throws SQLException {
        sql("SELECT setval('" + prefix + "_event_id_seq', 32767)");
        assertThatThrownBy(() -> store.typeId("one-too-many"))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("type registry is full");
    }

    // ---- enqueue -------------------------------------------------------------------------------------

    @Test
    void enqueueStoresEveryFieldAndUsesStoreTimeForDue() {
        long id = newId();
        var before = Instant.now().minusSeconds(5);
        var steps = Map.of("charge", new StepResult(Instant.now().truncatedTo(ChronoUnit.MILLIS), "{\"ok\":true}"));
        var t = task(id).payloadVersion(2)
                .account("acme")
                .headers(Map.of("trace", "abc"))
                .uniqueKey("k1")
                .keyKeep(Duration.ofMinutes(5))
                .retryPolicy("slow")
                .scheduleId("nightly")
                .retryOf(7L)
                .startBy(Instant.now().plusSeconds(60))
                .maxRunTime(Duration.ofSeconds(9))
                .progress("10%")
                .steps(steps)
                .build();

        assertThat(store.enqueue(t)).isEqualTo(EnqueueOutcome.STORED);

        var r = get(id);
        assertThat(r.state()).isEqualTo(TaskState.QUEUED);
        assertThat(r.revision()).isZero();
        assertThat(r.attempts()).isZero();
        assertThat(r.payloadVersion()).isEqualTo(2);
        assertThat(r.due()).isAfter(before).isEqualTo(r.stateChangedAt());
        assertThat(r.createdAt()).isEqualTo(t.createdAt().truncatedTo(ChronoUnit.MICROS));
        assertThat(r.encodedPayload()).isEqualTo("payload-" + id);
        assertThat(r.account()).isEqualTo("acme");
        assertThat(r.headers()).isEqualTo(Map.of("trace", "abc"));
        assertThat(r.uniqueKey()).isEqualTo("k1");
        assertThat(r.keyKeep()).isEqualTo(Duration.ofMinutes(5));
        assertThat(r.keyUntil()).isNull();
        assertThat(r.retryPolicy()).isEqualTo("slow");
        assertThat(r.scheduleId()).isEqualTo("nightly");
        assertThat(r.retryOf()).isEqualTo(7L);
        assertThat(r.startBy()).isEqualTo(t.startBy().truncatedTo(ChronoUnit.MICROS));
        assertThat(r.maxRunTime()).isEqualTo(Duration.ofSeconds(9));
        assertThat(r.progress()).isEqualTo("10%");
        assertThat(r.steps()).isEqualTo(steps);
        assertThat(r.workerId()).isNull();
        assertThat(r.leaseUntil()).isNull();
        assertThat(r.failures()).isNull();
    }

    @Test
    void futureDueIsScheduledAndPastDueIsQueued() {
        long later = newId();
        long earlier = newId();
        var at = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MICROS);
        store.enqueue(task(later).due(at).build());
        store.enqueue(task(earlier).due(Instant.now().minusSeconds(60)).build());

        assertThat(get(later).state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(get(later).due()).isEqualTo(at);
        assertThat(get(earlier).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void enqueueAllReportsEachTaskInOrder() {
        long held = newId();
        store.enqueue(task(held).uniqueKey("dup").build());
        long base = store.allocateIds(5);
        var outcomes = store.enqueueAll(List.of(
                task(base).build(),
                task(held).build(),
                task(base + 1).uniqueKey("dup").build(),
                task(base + 2).uniqueKey("fresh").build(),
                task(base + 3).uniqueKey("fresh").build(),
                task(base).build()));

        assertThat(outcomes)
                .containsExactly(
                        EnqueueOutcome.STORED,
                        EnqueueOutcome.EXISTS,
                        new EnqueueOutcome.DuplicateKey(held),
                        EnqueueOutcome.STORED,
                        new EnqueueOutcome.DuplicateKey(base + 2),
                        EnqueueOutcome.EXISTS);
        assertThat(store.enqueueAll(List.of())).isEmpty();
    }

    @Test
    void theKeyIsReleasedWhenTheTaskEnds() {
        var t = enqueueAndClaim(task(newId()).uniqueKey("once"));
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();

        assertThat(get(t.id()).uniqueKey()).isNull();
        assertThat(store.enqueue(task(newId()).uniqueKey("once").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void aKeptKeyIsHeldForItsWindowThenReleasedInline() throws Exception {
        var t = enqueueAndClaim(task(newId()).uniqueKey("window").keyKeep(Duration.ofHours(1)));
        store.ack(t.id(), t.revision(), true, true, null);

        var done = get(t.id());
        assertThat(done.uniqueKey()).isEqualTo("window");
        assertThat(done.keyUntil()).isAfter(done.stateChangedAt());
        assertThat(store.enqueue(task(newId()).uniqueKey("window").build()))
                .isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));

        sql("UPDATE " + prefix + "_task SET key_until = now() - interval '1 second' WHERE id = " + t.id());
        long next = newId();
        assertThat(store.enqueue(task(next).uniqueKey("window").build())).isEqualTo(EnqueueOutcome.STORED);
        assertThat(get(t.id()).uniqueKey()).isNull();
        assertThat(get(t.id()).keyUntil()).isNull();
    }

    @Test
    void releaseKeysFreesExpiredWindows() throws Exception {
        var a = enqueueAndClaim(task(newId()).uniqueKey("a").keyKeep(Duration.ofHours(1)));
        store.ack(a.id(), a.revision(), true, true, null);
        var b = enqueueAndClaim(task(newId()).uniqueKey("b").keyKeep(Duration.ofHours(1)));
        store.ack(b.id(), b.revision(), true, true, null);
        assertThat(store.releaseKeys(10)).isZero();

        sql("UPDATE " + prefix + "_task SET key_until = now() - interval '1 second' WHERE id = " + a.id());
        assertThat(store.releaseKeys(10)).isEqualTo(1);
        assertThat(get(a.id()).uniqueKey()).isNull();
        assertThat(get(b.id()).uniqueKey()).isEqualTo("b");
        assertThat(store.releaseKeys(10)).isZero();
    }

    // ---- claim ---------------------------------------------------------------------------------------

    @Test
    void claimLeasesByDueThenIdAndRecordsTheWorker() {
        long base = store.allocateIds(3);
        var now = Instant.now();
        store.enqueue(task(base).due(now.minusSeconds(10)).build());
        store.enqueue(task(base + 1).due(now.minusSeconds(30)).build());
        store.enqueue(task(base + 2).due(now.minusSeconds(30)).build());

        var claimed = claim(2);

        assertThat(claimed).extracting(TaskRecord::id).containsExactly(base + 1, base + 2);
        var t = claimed.getFirst();
        assertThat(t.state()).isEqualTo(TaskState.LEASED);
        assertThat(t.revision()).isEqualTo(1);
        assertThat(t.attempts()).isEqualTo(1);
        assertThat(t.workerId()).isEqualTo(worker);
        assertThat(t.claimedAt()).isNotNull().isEqualTo(t.stateChangedAt());
        assertThat(t.leaseUntil()).isAfter(t.claimedAt().plus(LEASE).minusSeconds(1));
        assertThat(t.due()).isEqualTo(now.minusSeconds(30).truncatedTo(ChronoUnit.MICROS));
        assertThat(claim(5)).extracting(TaskRecord::id).containsExactly(base);
        assertThat(claim(5)).isEmpty();
    }

    @Test
    void claimDrainsQueuesInTheGivenOrder() {
        int urgent = store.queueId("urgent");
        long base = store.allocateIds(3);
        store.enqueue(task(base).due(Instant.now().minusSeconds(60)).build());
        store.enqueue(task(base + 1).queueId(urgent).build());
        store.enqueue(task(base + 2).queueId(urgent).build());

        var claimed = store.claim(new ClaimRequest(List.of(urgent, queue, urgent), 2, LEASE, worker, null));
        assertThat(claimed).extracting(TaskRecord::id).containsExactly(base + 1, base + 2);
        claimed = store.claim(new ClaimRequest(List.of(urgent, queue), 2, LEASE, worker, null));
        assertThat(claimed).extracting(TaskRecord::id).containsExactly(base);
    }

    @Test
    void claimSkipsUnknownTypes() {
        int unknown = store.typeId("from-a-newer-runner");
        long known = newId();
        long other = newId();
        store.enqueue(
                task(other).typeId(unknown).due(Instant.now().minusSeconds(60)).build());
        store.enqueue(task(known).build());

        assertThat(store.claim(new ClaimRequest(List.of(queue), 5, LEASE, worker, Set.of())))
                .isEmpty();
        assertThat(store.claim(new ClaimRequest(List.of(queue), 5, LEASE, worker, Set.of(type))))
                .extracting(TaskRecord::id)
                .containsExactly(known);
        assertThat(get(other).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(other).attempts()).isZero();
        assertThat(store.claim(new ClaimRequest(List.of(queue), 5, LEASE, worker, null)))
                .extracting(TaskRecord::id)
                .containsExactly(other);
    }

    @Test
    void concurrentClaimsNeverShareATask() throws Exception {
        long base = store.allocateIds(200);
        var batch = new ArrayList<TaskRecord>();
        for (int i = 0; i < 200; i++) batch.add(task(base + i).build());
        store.enqueueAll(batch);

        var seen = new HashSet<Long>();
        try (var pool = Executors.newFixedThreadPool(8)) {
            var calls = new ArrayList<Callable<List<TaskRecord>>>();
            for (int i = 0; i < 40; i++) calls.add(() -> claim(7));
            for (var f : pool.invokeAll(calls)) {
                for (var t : f.get())
                    assertThat(seen.add(t.id())).as("task %d", t.id()).isTrue();
            }
        }
        assertThat(seen).hasSize(200);
    }

    // ---- fenced transitions --------------------------------------------------------------------------

    @Test
    void renewExtendsOwnedLeasesAndReportsTheOthers() throws SQLException {
        var a = enqueueAndClaim(task(newId()));
        var b = enqueueAndClaim(task(newId()));
        sql("UPDATE " + prefix + "_task SET cancel_requested_at = now() WHERE id = " + b.id());

        var statuses = store.renew(
                List.of(
                        new LeaseRef(a.id(), a.revision()),
                        new LeaseRef(b.id(), b.revision()),
                        new LeaseRef(a.id(), a.revision() + 1)),
                Duration.ofMinutes(10));

        assertThat(statuses)
                .containsExactly(
                        new LeaseStatus(a.id(), true, false),
                        new LeaseStatus(b.id(), true, true),
                        new LeaseStatus(a.id(), false, false));
        var renewed = get(a.id());
        assertThat(renewed.leaseUntil()).isAfter(a.leaseUntil());
        assertThat(renewed.revision()).isEqualTo(a.revision());
        assertThat(store.renew(List.of(), LEASE)).isEmpty();
    }

    @Test
    void everyFencedTransitionRefusesAStaleRevision() {
        var t = enqueueAndClaim(task(newId()));
        int stale = t.revision() - 1;
        long id = t.id();

        assertThat(store.ack(id, stale, true, true, null)).isFalse();
        assertThat(store.fail(id, stale, failure(1), Instant.now(), true, null)).isEqualTo(FailOutcome.LOST);
        assertThat(store.failExpired(id, stale, failure(1), Instant.now(), true, null))
                .isEqualTo(FailOutcome.LOST);
        assertThat(store.reschedule(id, stale, Instant.now())).isFalse();
        assertThat(store.unclaim(id, stale)).isFalse();
        assertThat(store.saveSteps(id, stale, Map.of("s", new StepResult(Instant.now(), null))))
                .isFalse();
        assertThat(store.progress(id, stale, "x")).isFalse();
        assertThat(store.renew(List.of(new LeaseRef(id, stale)), LEASE)
                        .getFirst()
                        .owned())
                .isFalse();

        var unchanged = get(id);
        assertThat(unchanged.state()).isEqualTo(TaskState.LEASED);
        assertThat(unchanged.revision()).isEqualTo(t.revision());
        assertThat(unchanged.steps()).isNull();
        assertThat(unchanged.progress()).isNull();
    }

    @Test
    void fencedTransitionsRefuseATaskThatIsNotLeased() {
        long id = newId();
        store.enqueue(task(id).build());
        assertThat(store.ack(id, 0, true, true, null)).isFalse();
        assertThat(store.unclaim(id, 0)).isFalse();
        assertThat(store.fail(id, 0, failure(1), Instant.now(), true, null)).isEqualTo(FailOutcome.LOST);
    }

    @Test
    void ackKeepsTheRowAndOptionallyThePayload() {
        var steps = Map.of("one", new StepResult(Instant.now().truncatedTo(ChronoUnit.MILLIS), "1"));
        var kept = enqueueAndClaim(task(newId()));
        store.saveSteps(kept.id(), kept.revision(), steps);
        assertThat(store.ack(kept.id(), kept.revision(), true, true, null)).isTrue();
        var dropped = enqueueAndClaim(task(newId()));
        store.saveSteps(dropped.id(), dropped.revision(), steps);
        assertThat(store.ack(dropped.id(), dropped.revision(), true, false, null))
                .isTrue();

        var a = get(kept.id());
        assertThat(a.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(a.revision()).isEqualTo(kept.revision() + 1);
        assertThat(a.encodedPayload()).isEqualTo(kept.encodedPayload());
        assertThat(a.leaseUntil()).isNull();
        assertThat(a.workerId()).isEqualTo(worker);
        var b = get(dropped.id());
        assertThat(b.encodedPayload()).isNull();
        assertThat(b.steps()).isEqualTo(steps);
        assertThat(store.ack(kept.id(), a.revision(), true, true, null)).isFalse();
    }

    @Test
    void ackWithoutKeepRemovesTheTaskUnlessItHoldsItsKey() {
        var plain = enqueueAndClaim(task(newId()));
        assertThat(store.ack(plain.id(), plain.revision(), false, true, null)).isTrue();
        assertThat(store.getTask(plain.id())).isEmpty();

        var keyed = enqueueAndClaim(task(newId()).uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(keyed.id(), keyed.revision(), false, true, null)).isTrue();
        var row = get(keyed.id());
        assertThat(row.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(row.encodedPayload()).isNull();
        assertThat(row.uniqueKey()).isEqualTo("k");
    }

    @Test
    void failSchedulesARetryAndRecordsTheFailure() {
        var t = enqueueAndClaim(task(newId()));
        var retryAt = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MICROS);

        var f = failure(1);
        assertThat(store.fail(t.id(), t.revision(), f, retryAt, true, null)).isEqualTo(FailOutcome.RETRY);

        var r = get(t.id());
        assertThat(r.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(r.revision()).isEqualTo(t.revision() + 1);
        assertThat(r.due()).isEqualTo(retryAt);
        assertThat(r.attempts()).isEqualTo(1);
        assertThat(r.workerId()).isNull();
        assertThat(r.leaseUntil()).isNull();
        assertThat(r.failures()).containsExactly(f);
        assertThat(store.nextDue()).isEqualTo(retryAt);
    }

    @Test
    void failKillsWhenAttemptsAreExhaustedOrNoRetryIsAllowed() {
        var exhausted = enqueueAndClaim(task(newId()).maxAttempts(1));
        assertThat(store.fail(exhausted.id(), exhausted.revision(), failure(1), Instant.now(), true, null))
                .isEqualTo(FailOutcome.DEAD);
        var dead = get(exhausted.id());
        assertThat(dead.state()).isEqualTo(TaskState.DEAD);
        assertThat(dead.encodedPayload()).isEqualTo(exhausted.encodedPayload());
        assertThat(dead.workerId()).isEqualTo(worker);

        var refused = enqueueAndClaim(task(newId()));
        assertThat(store.fail(refused.id(), refused.revision(), failure(1), null, false, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(store.getTask(refused.id())).isEmpty();
    }

    @Test
    void failExpiredOnlyActsOnAnExpiredLease() throws Exception {
        long id = newId();
        store.enqueue(task(id).build());
        var t = store.claim(new ClaimRequest(List.of(queue), 1, Duration.ofMillis(1), worker, null))
                .getFirst();
        Thread.sleep(20);

        var expired = store.reclaim(10);
        assertThat(expired).hasSize(1);
        var e = expired.getFirst();
        assertThat(e.id()).isEqualTo(id);
        assertThat(e.revision()).isEqualTo(t.revision());
        assertThat(e.attempts()).isEqualTo(1);
        assertThat(e.maxAttempts()).isEqualTo(3);
        assertThat(e.typeId()).isEqualTo(type);
        assertThat(e.cancelRequested()).isFalse();

        assertThat(store.renew(List.of(new LeaseRef(id, t.revision())), Duration.ofMinutes(1)))
                .extracting(LeaseStatus::owned)
                .containsExactly(false);
        assertThat(store.failExpired(id, t.revision(), failure(1), Instant.now(), true, null))
                .isEqualTo(FailOutcome.RETRY);
    }

    @Test
    void rescheduleGivesTheAttemptBack() {
        var t = enqueueAndClaim(task(newId()));
        var due = Instant.now().plusSeconds(30).truncatedTo(ChronoUnit.MICROS);
        assertThat(store.reschedule(t.id(), t.revision(), due)).isTrue();

        var r = get(t.id());
        assertThat(r.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(r.attempts()).isZero();
        assertThat(r.due()).isEqualTo(due);
        assertThat(r.revision()).isEqualTo(t.revision() + 1);
        assertThat(r.workerId()).isNull();
        assertThat(r.leaseUntil()).isNull();
    }

    @Test
    void unclaimReturnsTheTaskAtItsOldDue() {
        long first = newId();
        store.enqueue(task(first).due(Instant.now().minusSeconds(60)).build());
        var t = claim(1).getFirst();
        long later = newId();
        store.enqueue(task(later).build());

        assertThat(store.unclaim(first, t.revision())).isTrue();

        var r = get(first);
        assertThat(r.state()).isEqualTo(TaskState.QUEUED);
        assertThat(r.attempts()).isZero();
        assertThat(r.due()).isEqualTo(t.due());
        assertThat(r.revision()).isEqualTo(t.revision() + 1);
        assertThat(r.workerId()).isNull();
        assertThat(claim(1)).extracting(TaskRecord::id).containsExactly(first);
    }

    @Test
    void stepsAndProgressDoNotBumpTheRevisionAndSurviveARetry() {
        var t = enqueueAndClaim(task(newId()));
        var steps = Map.of(
                "reserve", new StepResult(Instant.now().truncatedTo(ChronoUnit.MILLIS), "[]"),
                "notify", new StepResult(Instant.now().truncatedTo(ChronoUnit.MILLIS), null));

        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isTrue();
        assertThat(store.progress(t.id(), t.revision(), "half")).isTrue();
        assertThat(get(t.id()).revision()).isEqualTo(t.revision());

        store.fail(t.id(), t.revision(), failure(1), Instant.now().minusSeconds(1), true, null);
        store.promote(10);
        var retry = claim(1).getFirst();
        assertThat(retry.steps()).isEqualTo(steps);
        assertThat(retry.progress()).isEqualTo("half");

        assertThat(store.saveSteps(retry.id(), retry.revision(), null)).isTrue();
        assertThat(get(t.id()).steps()).isNull();
    }

    // ---- maintenance ---------------------------------------------------------------------------------

    @Test
    void promoteMovesDueTasksKeepingTheirDue() {
        long due = newId();
        long notYet = newId();
        var past = Instant.now().minusSeconds(5).truncatedTo(ChronoUnit.MICROS);
        var future = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MICROS);
        store.enqueue(task(due).build());
        var claimed = claim(1).getFirst();
        store.reschedule(due, claimed.revision(), past);
        store.enqueue(task(notYet).due(future).build());

        var result = store.promote(10);

        assertThat(result.moved()).isEqualTo(1);
        assertThat(result.nextDue()).isEqualTo(future);
        var r = get(due);
        assertThat(r.state()).isEqualTo(TaskState.QUEUED);
        assertThat(r.due()).isEqualTo(past);
        assertThat(r.revision()).isEqualTo(claimed.revision() + 2);
        assertThat(store.promote(10).moved()).isZero();
        assertThat(store.nextDue()).isEqualTo(future);
    }

    @Test
    void nextDueIsNullWithoutScheduledTasks() {
        assertThat(store.nextDue()).isNull();
        assertThat(store.promote(10).nextDue()).isNull();
    }

    @Test
    void purgeRemovesOldTerminalTasksOfOneState() throws Exception {
        var ok = enqueueAndClaim(task(newId()));
        store.ack(ok.id(), ok.revision(), true, true, null);
        var bad = enqueueAndClaim(task(newId()).maxAttempts(1));
        store.fail(bad.id(), bad.revision(), failure(1), null, true, null);
        var queued = newId();
        store.enqueue(task(queued).build());

        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofHours(1), 100)).isZero();
        Thread.sleep(5);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 100)).isEqualTo(1);
        assertThat(store.getTask(ok.id())).isEmpty();
        assertThat(store.getTask(bad.id())).isPresent();
        assertThat(store.purge(TaskState.DEAD, Duration.ZERO, 100)).isEqualTo(1);
        assertThat(store.getTask(queued)).isPresent();
        assertThatThrownBy(() -> store.purge(TaskState.QUEUED, Duration.ZERO, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void purgeWorksInBatches() throws Exception {
        for (int i = 0; i < 5; i++) {
            var t = enqueueAndClaim(task(newId()));
            store.ack(t.id(), t.revision(), true, true, null);
        }
        Thread.sleep(5);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 2)).isEqualTo(2);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 2)).isEqualTo(2);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 2)).isEqualTo(1);
    }

    @Test
    void purgeKeepsTasksThatStillHoldTheirKey() throws Exception {
        var t = enqueueAndClaim(task(newId()).uniqueKey("held").keyKeep(Duration.ofMillis(300)));
        store.ack(t.id(), t.revision(), false, true, null);

        Thread.sleep(5);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 100)).isZero();
        Thread.sleep(400);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 100)).isEqualTo(1);
        assertThat(store.enqueue(task(newId()).uniqueKey("held").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    // ---- inspection ----------------------------------------------------------------------------------

    @Test
    void countsCoverEveryRegisteredQueueAndState() {
        int idle = store.queueId("idle");
        long base = store.allocateIds(4);
        store.enqueue(task(base).build());
        store.enqueue(task(base + 1).due(Instant.now().plusSeconds(60)).build());
        store.enqueue(task(base + 2).build());
        var leased = claim(1).getFirst();
        store.ack(leased.id(), leased.revision(), true, true, null);
        store.enqueue(task(base + 3).build());
        claim(1);

        var counts = store.counts();
        assertThat(counts.get(queue).of(TaskState.QUEUED)).isEqualTo(1);
        assertThat(counts.get(queue).of(TaskState.SCHEDULED)).isEqualTo(1);
        assertThat(counts.get(queue).of(TaskState.LEASED)).isEqualTo(1);
        assertThat(counts.get(queue).of(TaskState.SUCCEEDED)).isEqualTo(1);
        assertThat(counts.get(idle).queued()).isZero();
        store.enqueue(task(store.allocateIds(1)).build());
        assertThat(store.counts().get(queue).queued()).isEqualTo(2);
    }

    @Test
    void tasksAreListedByStateChangeWithKeysetPaging() throws Exception {
        int other = store.queueId("other");
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) {
            long id = newId();
            store.enqueue(task(id).queueId(i == 2 ? other : queue).build());
            ids.add(id);
            Thread.sleep(2);
        }

        var page1 = store.tasks(TaskState.QUEUED, null, null, 2);
        assertThat(page1.items()).extracting(TaskRecord::id).containsExactly(ids.get(0), ids.get(1));
        var page2 = store.tasks(TaskState.QUEUED, null, page1.next(), 2);
        assertThat(page2.items()).extracting(TaskRecord::id).containsExactly(ids.get(2), ids.get(3));
        var page3 = store.tasks(TaskState.QUEUED, null, page2.next(), 2);
        assertThat(page3.items()).extracting(TaskRecord::id).containsExactly(ids.get(4));
        assertThat(page3.next()).isNull();

        assertThat(store.tasks(TaskState.QUEUED, other, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(ids.get(2));
        var afterFirst = store.tasks(TaskState.QUEUED, queue, null, 1).next();
        assertThat(store.tasks(TaskState.QUEUED, queue, afterFirst, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(ids.get(1), ids.get(3), ids.get(4));
        assertThat(store.tasks(TaskState.SUCCEEDED, null, null, 10).items()).isEmpty();
        assertThat(store.tasks(TaskState.QUEUED, null, null, 0).items()).isEmpty();
    }

    // ---- recurring definitions -----------------------------------------------------------------------

    private RecurringDefinition definition(String id, String cron) {
        return new RecurringDefinition(id, type, 1, queue, "acme", cron, "{\"x\":1}", 4, 0);
    }

    @Test
    void recurringOccurrenceIsCommittedOncePerFireTime() {
        var fireAt = Instant.now().minusSeconds(1).truncatedTo(ChronoUnit.MICROS);
        var nextFire = fireAt.plusSeconds(3600);
        store.putRecurring(definition("hourly", "0 * * * *"), fireAt, fireAt);

        assertThat(store.recurringSchedules()).containsExactly(Map.entry("hourly", fireAt));
        var due = store.dueRecurring(Instant.now(), 10);
        assertThat(due).hasSize(1);
        DueSchedule d = due.getFirst();
        assertThat(d.fireAt()).isEqualTo(fireAt);
        assertThat(d.definition().typeId()).isEqualTo(type);
        assertThat(d.definition().queueId()).isEqualTo(queue);
        assertThat(d.definition().account()).isEqualTo("acme");
        assertThat(d.definition().maxAttempts()).isEqualTo(4);
        assertThat(d.definition().revision()).isPositive();

        long id = newId();
        var occurrence = task(id).scheduleId("hourly")
                .encodedPayload(d.definition().encodedPayload())
                .maxAttempts(4)
                .build();
        assertThat(store.commitRecurring(d, nextFire, occurrence)).isTrue();
        assertThat(store.commitRecurring(d, nextFire, task(newId()).build())).isFalse();

        var r = get(id);
        assertThat(r.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(r.due()).isEqualTo(fireAt);
        assertThat(r.scheduleId()).isEqualTo("hourly");
        assertThat(r.revision()).isZero();
        assertThat(store.recurringSchedules()).containsExactly(Map.entry("hourly", nextFire));
        assertThat(store.dueRecurring(Instant.now(), 10)).isEmpty();
    }

    @Test
    void putRecurringKeepsTheFireTimeForAnUnchangedCronAndTheRevisionForAnUnchangedDefinition() {
        var first = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MICROS);
        assertThat(store.putRecurring(definition("job", "0 * * * *"), first, first))
                .isEqualTo(PutRecurringOutcome.CREATED);
        long revision = store.dueRecurring(first, 10).getFirst().definition().revision();

        assertThat(store.putRecurring(definition("job", "0 * * * *"), first.plusSeconds(600), first.plusSeconds(600)))
                .isEqualTo(PutRecurringOutcome.UNCHANGED);
        assertThat(store.recurringSchedules()).containsEntry("job", first);
        assertThat(store.dueRecurring(first, 10).getFirst().definition().revision())
                .isEqualTo(revision);

        var otherData = new RecurringDefinition("job", type, 1, queue, "acme", "0 * * * *", "{\"x\":2}", 4, 0);
        assertThat(store.putRecurring(otherData, first.plusSeconds(600), first.plusSeconds(600)))
                .isEqualTo(PutRecurringOutcome.CHANGED);
        assertThat(store.recurringSchedules()).containsEntry("job", first);
        var d = store.dueRecurring(first, 10).getFirst();
        assertThat(d.definition().revision()).isGreaterThan(revision);

        store.putRecurring(definition("job", "*/5 * * * *"), first.plusSeconds(600), first.plusSeconds(600));
        assertThat(store.recurringSchedules()).containsEntry("job", first.plusSeconds(600));
        assertThat(store.commitRecurring(
                        d, first.plusSeconds(1200), task(newId()).build()))
                .isFalse();
    }

    // ---- wave 2: cancellation and expiry -------------------------------------------------------------

    @Test
    void finishCancelledEndsALeasedTaskAndReleasesItsKey() {
        var t = enqueueAndClaim(task(newId()).uniqueKey("c1"));
        assertThat(store.finishCancelled(t.id(), t.revision() - 1, "stop", false, null))
                .isFalse();
        assertThat(store.finishCancelled(t.id(), t.revision(), "stop", false, null))
                .isTrue();

        var r = get(t.id());
        assertThat(r.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(r.revision()).isEqualTo(t.revision() + 1);
        assertThat(r.cancelReason()).isEqualTo("stop");
        assertThat(r.cancelRequestedAt()).isNotNull();
        assertThat(r.stateChangedAt()).isEqualTo(r.cancelRequestedAt());
        assertThat(r.encodedPayload()).isNull();
        assertThat(r.workerId()).isEqualTo(worker);
        assertThat(r.leaseUntil()).isNull();
        assertThat(r.uniqueKey()).isNull();
        assertThat(store.finishCancelled(t.id(), r.revision(), "again", false, null))
                .isFalse();
        assertThat(store.enqueue(task(newId()).uniqueKey("c1").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void finishCancelledKeepsThePayloadAndTheRequestTimeAndStartsAKeyWindow() throws Exception {
        var t = enqueueAndClaim(task(newId()).uniqueKey("c2").keyKeep(Duration.ofHours(1)));
        assertThat(store.cancel(t.id(), t.revision(), "operator", null)).isEqualTo(CancelOutcome.REQUESTED);
        var requested = get(t.id()).cancelRequestedAt();
        Thread.sleep(5);

        assertThat(store.finishCancelled(t.id(), t.revision(), "operator", true, null))
                .isTrue();

        var r = get(t.id());
        assertThat(r.cancelRequestedAt()).isEqualTo(requested);
        assertThat(r.encodedPayload()).isEqualTo(t.encodedPayload());
        assertThat(r.uniqueKey()).isEqualTo("c2");
        assertThat(r.keyUntil()).isAfter(r.stateChangedAt());
    }

    @Test
    void cancelEndsWaitingTasksAndOnlyRequestsItForLeasedOnes() {
        long queued = newId();
        store.enqueue(task(queued).uniqueKey("q").build());
        long scheduled = newId();
        store.enqueue(task(scheduled).due(Instant.now().plusSeconds(60)).build());

        assertThat(store.cancel(queued, 1, "x", null)).isEqualTo(CancelOutcome.STALE);
        assertThat(store.cancel(queued, 0, "not needed", null)).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(store.cancel(scheduled, 0, "not needed", null)).isEqualTo(CancelOutcome.CANCELLED);
        var c = get(queued);
        assertThat(c.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(c.revision()).isEqualTo(1);
        assertThat(c.cancelReason()).isEqualTo("not needed");
        assertThat(c.cancelRequestedAt()).isEqualTo(c.stateChangedAt());
        assertThat(c.encodedPayload()).isEqualTo("payload-" + queued);
        assertThat(c.uniqueKey()).isNull();
        assertThat(get(scheduled).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(store.cancel(queued, 1, "again", null)).isEqualTo(CancelOutcome.STALE);
        assertThat(store.cancel(newId(), 0, "missing", null)).isEqualTo(CancelOutcome.STALE);

        var leased = enqueueAndClaim(task(newId()));
        assertThat(store.cancel(leased.id(), leased.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
        var r = get(leased.id());
        assertThat(r.state()).isEqualTo(TaskState.LEASED);
        assertThat(r.revision()).isEqualTo(leased.revision());
        assertThat(r.cancelReason()).isEqualTo("stop");
        assertThat(store.renew(List.of(new LeaseRef(leased.id(), leased.revision())), LEASE))
                .containsExactly(new LeaseStatus(leased.id(), true, true));
        assertThat(store.reclaim(10)).isEmpty();
    }

    @Test
    void claimSkipsPassedDeadlinesAndExpireMovesThem() throws Exception {
        var leased = enqueueAndClaim(task(newId()));
        long late = newId();
        store.enqueue(task(late)
                .uniqueKey("late")
                .startBy(Instant.now().plusMillis(100))
                .build());
        long lateScheduled = newId();
        store.enqueue(task(lateScheduled)
                .due(Instant.now().plusSeconds(60))
                .startBy(Instant.now().plusMillis(100))
                .build());
        long fine = newId();
        store.enqueue(task(fine).startBy(Instant.now().plusSeconds(60)).build());
        Thread.sleep(150);
        sql("UPDATE " + prefix + "_task SET start_by = now() - interval '1 second' WHERE id = " + leased.id());

        assertThat(claim(10)).extracting(TaskRecord::id).containsExactly(fine);
        assertThat(store.expire(10)).containsExactlyInAnyOrder(late, lateScheduled);

        var r = get(late);
        assertThat(r.state()).isEqualTo(TaskState.EXPIRED);
        assertThat(r.revision()).isEqualTo(1);
        assertThat(r.uniqueKey()).isNull();
        assertThat(get(lateScheduled).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(get(leased.id()).state()).isEqualTo(TaskState.LEASED);
        assertThat(store.expire(10)).isEmpty();
        assertThat(store.expire(0)).isEmpty();
        assertThat(store.enqueue(task(newId()).uniqueKey("late").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void expireWorksInBatchesOldestDeadlineFirst() throws Exception {
        long base = store.allocateIds(3);
        var now = Instant.now();
        store.enqueue(task(base).startBy(now.plusMillis(60)).build());
        store.enqueue(task(base + 1).startBy(now.plusMillis(20)).build());
        store.enqueue(task(base + 2).startBy(now.plusMillis(40)).build());
        Thread.sleep(100);

        assertThat(store.expire(2)).containsExactlyInAnyOrder(base + 1, base + 2);
        assertThat(store.expire(2)).containsExactly(base);
    }

    // ---- wave 2: admin -------------------------------------------------------------------------------

    @Test
    void retryReturnsATerminalTaskToItsQueue() {
        var steps = Map.of("one", new StepResult(Instant.now().truncatedTo(ChronoUnit.MILLIS), "1"));
        var t = enqueueAndClaim(task(newId()).maxAttempts(1).uniqueKey("r").keyKeep(Duration.ofHours(1)));
        store.saveSteps(t.id(), t.revision(), steps);
        store.fail(t.id(), t.revision(), failure(1), null, true, null);
        var dead = get(t.id());

        assertThat(store.retry(t.id(), dead.revision() - 1)).isEqualTo(RetryOutcome.STALE);
        assertThat(store.retry(t.id(), dead.revision())).isEqualTo(RetryOutcome.RETRIED);

        var r = get(t.id());
        assertThat(r.state()).isEqualTo(TaskState.QUEUED);
        assertThat(r.revision()).isEqualTo(dead.revision() + 1);
        assertThat(r.attempts()).isZero();
        assertThat(r.due()).isAfterOrEqualTo(dead.stateChangedAt());
        assertThat(r.workerId()).isNull();
        assertThat(r.uniqueKey()).isEqualTo("r");
        assertThat(r.keyKeep()).isEqualTo(Duration.ofHours(1));
        assertThat(r.keyUntil()).isNull();
        assertThat(r.failures()).hasSize(1);
        assertThat(r.steps()).isEqualTo(steps);
        assertThat(r.encodedPayload()).isEqualTo(t.encodedPayload());
        assertThat(store.retry(t.id(), r.revision())).isEqualTo(RetryOutcome.STALE);
        assertThat(claim(1)).extracting(TaskRecord::id).containsExactly(t.id());
    }

    @Test
    void retryWorksForCancelledAndExpiredTasks() throws Exception {
        long cancelled = newId();
        store.enqueue(task(cancelled).build());
        store.cancel(cancelled, 0, "oops", null);
        long expired = newId();
        store.enqueue(task(expired).startBy(Instant.now().plusMillis(20)).build());
        Thread.sleep(50);
        store.expire(10);

        assertThat(store.retry(cancelled, 1)).isEqualTo(RetryOutcome.RETRIED);
        assertThat(store.retry(expired, 1)).isEqualTo(RetryOutcome.RETRIED);
        var c = get(cancelled);
        assertThat(c.cancelRequestedAt()).isNull();
        assertThat(c.cancelReason()).isNull();
        assertThat(get(expired).startBy()).isNull();
        assertThat(claim(5)).extracting(TaskRecord::id).containsExactlyInAnyOrder(cancelled, expired);
    }

    @Test
    void deleteRemovesAnyTaskButALeasedOne() {
        var leased = enqueueAndClaim(task(newId()));
        var done = enqueueAndClaim(task(newId()));
        store.ack(done.id(), done.revision(), true, true, null);
        long queued = newId();
        store.enqueue(task(queued).uniqueKey("d").build());

        assertThat(store.delete(queued, 1, null)).isFalse();
        assertThat(store.delete(queued, 0, null)).isTrue();
        assertThat(store.getTask(queued)).isEmpty();
        assertThat(store.enqueue(task(newId()).uniqueKey("d").build())).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.delete(leased.id(), leased.revision(), null)).isFalse();
        assertThat(get(leased.id()).state()).isEqualTo(TaskState.LEASED);
        assertThat(store.delete(done.id(), done.revision() + 1, null)).isTrue();
        assertThat(store.delete(done.id(), done.revision() + 1, null)).isFalse();
    }

    @Test
    void aPausedQueueIsNotClaimedFrom() {
        int other = store.queueId("other");
        long a = newId();
        long b = newId();
        store.enqueue(task(a).build());
        store.enqueue(task(b).queueId(other).build());

        store.pauseQueue(queue, true);
        assertThat(store.queues())
                .containsExactly(new QueueInfo(queue, "default", true), new QueueInfo(other, "other", false));
        assertThat(store.claim(new ClaimRequest(List.of(queue, other), 5, LEASE, worker, null)))
                .extracting(TaskRecord::id)
                .containsExactly(b);

        store.pauseQueue(queue, false);
        assertThat(store.queues().getFirst().paused()).isFalse();
        assertThat(claim(5)).extracting(TaskRecord::id).containsExactly(a);
    }

    @Test
    void heartbeatsAreListedAndStaleWorkersPurged() throws Exception {
        int second = store.allocateWorkerId("host:2", Set.of(), Set.of());
        store.heartbeat(worker, 5);

        var workers = store.workers();
        assertThat(workers).extracting(w -> w.id()).containsExactly(worker, second);
        var w = workers.getFirst();
        assertThat(w.label()).isEqualTo("host:1");
        assertThat(w.running()).isEqualTo(5);
        assertThat(w.lastSeen()).isAfterOrEqualTo(w.startedAt());
        assertThat(workers.get(1).running()).isZero();

        assertThat(store.purgeWorkers(Duration.ofHours(1))).isZero();
        sql("UPDATE " + prefix + "_worker SET last_seen = now() - interval '2 hours' WHERE id = " + second);
        assertThat(store.purgeWorkers(Duration.ofHours(1))).isEqualTo(1);
        assertThat(store.workers()).extracting(x -> x.id()).containsExactly(worker);
    }

    // ---- wave 2: schedule controls -------------------------------------------------------------------

    private RecurringDefinition fullDefinition(String id) {
        return new RecurringDefinition(
                id,
                type,
                2,
                queue,
                "acme",
                "0 9 * * *",
                "Europe/Berlin",
                RecurringDefinition.MISFIRE_CATCH_UP,
                3,
                null,
                true,
                "{}",
                5,
                null,
                false,
                0);
    }

    private RecurringDefinition fixedDelay(String id, long delayMs) {
        return new RecurringDefinition(
                id, type, 1, queue, null, null, null, null, 0, delayMs, false, null, 1, null, false, 0);
    }

    @Test
    void recurringDefinitionsRoundTripEveryField() {
        var at = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MICROS);
        store.putRecurring(fullDefinition("b-cron"), at, at);
        store.putRecurring(fixedDelay("a-delay", 60_000), at.plusSeconds(1), at.plusSeconds(1));

        var all = store.recurringDefinitions();
        assertThat(all).extracting(x -> x.definition().scheduleId()).containsExactly("a-delay", "b-cron");
        var delay = all.getFirst();
        assertThat(delay.definition().withRevision(0)).isEqualTo(fixedDelay("a-delay", 60_000));
        assertThat(delay.definition().revision()).isPositive();
        assertThat(delay.nextFire()).isEqualTo(at.plusSeconds(1));
        assertThat(all.get(1).definition().withRevision(0)).isEqualTo(fullDefinition("b-cron"));

        store.putRecurring(fixedDelay("a-delay", 60_000), at.plusSeconds(500), at.plusSeconds(500));
        assertThat(store.recurringSchedules()).containsEntry("a-delay", at.plusSeconds(1));
        store.putRecurring(fixedDelay("a-delay", 30_000), at.plusSeconds(500), at.plusSeconds(500));
        assertThat(store.recurringSchedules()).containsEntry("a-delay", at.plusSeconds(500));
        var zoneChanged = new RecurringDefinition(
                "b-cron", type, 2, queue, "acme", "0 9 * * *", "UTC", null, 0, null, false, "{}", 5, null, false, 0);
        store.putRecurring(zoneChanged, at.plusSeconds(700), at.plusSeconds(700));
        assertThat(store.recurringSchedules()).containsEntry("b-cron", at.plusSeconds(700));
    }

    @Test
    void aPausedDefinitionIsNotDueAndCannotCommit() {
        var fireAt = Instant.now().minusSeconds(1).truncatedTo(ChronoUnit.MICROS);
        store.putRecurring(definition("p", "0 * * * *"), fireAt, fireAt);
        var due = store.dueRecurring(Instant.now(), 10).getFirst();

        assertThat(store.pauseRecurring("p", true)).isTrue();
        assertThat(store.dueRecurring(Instant.now(), 10)).isEmpty();
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isTrue();
        assertThat(store.recurringDefinitions().getFirst().nextFire()).isEqualTo(fireAt);
        assertThat(store.commitRecurring(
                        due, fireAt.plusSeconds(3600), task(newId()).build()))
                .isFalse();

        assertThat(store.pauseRecurring("p", false)).isTrue();
        assertThat(store.dueRecurring(Instant.now(), 10)).hasSize(1);
        assertThat(store.pauseRecurring("missing", true)).isFalse();
    }

    @Test
    void reputtingADefinitionKeepsItsPausedFlag() {
        var fireAt = Instant.now().minusSeconds(1).truncatedTo(ChronoUnit.MICROS);
        store.putRecurring(definition("kept", "0 * * * *"), fireAt, fireAt);
        store.pauseRecurring("kept", true);

        store.putRecurring(definition("kept", "0 * * * *"), fireAt, fireAt);
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isTrue();
        assertThat(store.dueRecurring(Instant.now(), 10)).isEmpty();

        store.pauseRecurring("kept", false);
        store.putRecurring(definition("kept", "0 * * * *").withPaused(true), fireAt, fireAt);
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isFalse();
        assertThat(store.dueRecurring(Instant.now(), 10)).hasSize(1);

        store.putRecurring(definition("new", "0 * * * *").withPaused(true), fireAt, fireAt);
        assertThat(store.recurringDefinitions())
                .filteredOn(s -> s.definition().scheduleId().equals("new"))
                .singleElement()
                .satisfies(s -> assertThat(s.definition().paused()).isTrue());
    }

    @Test
    void setNextFireReplacesTheFireTimeAndFencesAnOlderCommit() {
        var fireAt = Instant.now().minusSeconds(1).truncatedTo(ChronoUnit.MICROS);
        store.putRecurring(definition("n", "0 * * * *"), fireAt, fireAt);
        var due = store.dueRecurring(Instant.now(), 10).getFirst();
        var later = fireAt.plusSeconds(120);

        assertThat(store.setNextFire("n", later)).isTrue();
        assertThat(store.recurringSchedules()).containsEntry("n", later);
        assertThat(store.commitRecurring(
                        due, fireAt.plusSeconds(3600), task(newId()).build()))
                .isFalse();
        assertThat(store.setNextFire("missing", later)).isFalse();
    }

    @Test
    void anOccurrenceWhoseKeyIsHeldIsSkippedButTheDefinitionAdvances() {
        var fireAt = Instant.now().minusSeconds(10).truncatedTo(ChronoUnit.MICROS);
        store.putRecurring(definition("o", "* * * * *"), fireAt, fireAt);
        long first = newId();
        var d1 = store.dueRecurring(Instant.now(), 10).getFirst();
        assertThat(store.commitRecurring(
                        d1,
                        fireAt.plusSeconds(1),
                        task(first).uniqueKey("sched:o").build()))
                .isTrue();
        assertThat(store.promote(10).moved()).isEqualTo(1);

        long second = newId();
        var d2 = store.dueRecurring(Instant.now(), 10).getFirst();
        assertThat(d2.fireAt()).isEqualTo(fireAt.plusSeconds(1));
        assertThat(store.commitRecurring(
                        d2,
                        fireAt.plusSeconds(2),
                        task(second).uniqueKey("sched:o").build()))
                .isTrue();

        assertThat(store.getTask(second)).isEmpty();
        assertThat(get(first).state()).isEqualTo(TaskState.QUEUED);
        assertThat(store.recurringSchedules()).containsEntry("o", fireAt.plusSeconds(2));

        var t = claim(1).getFirst();
        store.ack(t.id(), t.revision(), true, true, null);
        long third = newId();
        var d3 = store.dueRecurring(Instant.now(), 10).getFirst();
        assertThat(store.commitRecurring(
                        d3,
                        fireAt.plusSeconds(3),
                        task(third).uniqueKey("sched:o").build()))
                .isTrue();
        assertThat(get(third).uniqueKey()).isEqualTo("sched:o");
        assertThat(get(third).scheduleId()).isEqualTo("o");
    }

    // ---- wave 2: transactional enqueue ---------------------------------------------------------------

    @Test
    void enqueueOnTheCallersConnectionFollowsItsTransaction() throws SQLException {
        long rolledBack = newId();
        long committed = newId();
        try (var c = PostgresTestSupport.dataSource().getConnection()) {
            c.setAutoCommit(false);
            assertThat(store.enqueue(c, task(rolledBack).build())).isEqualTo(EnqueueOutcome.STORED);
            assertThat(store.getTask(rolledBack)).isEmpty();
            c.rollback();

            assertThat(store.enqueue(c, task(committed).uniqueKey("tx").build()))
                    .isEqualTo(EnqueueOutcome.STORED);
            assertThat(store.enqueue(c, task(committed).build())).isEqualTo(EnqueueOutcome.EXISTS);
            assertThat(store.enqueue(c, task(newId()).uniqueKey("tx").build()))
                    .isEqualTo(new EnqueueOutcome.DuplicateKey(committed));
            assertThat(store.getTask(committed)).isEmpty();
            c.commit();
            assertThat(c.isClosed()).isFalse();
            c.setAutoCommit(true);
        }
        assertThat(store.getTask(rolledBack)).isEmpty();
        assertThat(get(committed).state()).isEqualTo(TaskState.QUEUED);
        assertThatThrownBy(() -> store.enqueue(null, task(newId()).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---- fault injection: statement-level failures and contention ------------------------------------

    private long occurrenceOf(String scheduleId) throws SQLException {
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var ps = c.prepareStatement("SELECT occurrence_id FROM " + prefix + "_schedule WHERE id = ?")) {
            ps.setString(1, scheduleId);
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                var id = rs.getLong(1);
                return rs.wasNull() ? 0 : id;
            }
        }
    }

    /** A trigger on the schedule table that fails the update the test wants to break. */
    private void failScheduleUpdatesWhen(String condition) throws SQLException {
        sql("CREATE FUNCTION " + prefix + "_boom() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " RAISE EXCEPTION 'injected failure'; END $$");
        sql("CREATE TRIGGER " + prefix + "_boom BEFORE UPDATE ON " + prefix + "_schedule FOR EACH ROW WHEN ("
                + condition + ") EXECUTE FUNCTION " + prefix + "_boom()");
    }

    private void healScheduleUpdates() throws SQLException {
        sql("DROP TRIGGER IF EXISTS " + prefix + "_boom ON " + prefix + "_schedule");
        sql("DROP FUNCTION IF EXISTS " + prefix + "_boom()");
    }

    private record Occurrence(Instant fireAt, DueSchedule due, TaskRecord leased) {}

    private Occurrence leasedOccurrenceOfFixedDelay(String scheduleId, String key) {
        var fireAt = Instant.now().minusSeconds(10).truncatedTo(ChronoUnit.MICROS);
        store.putRecurring(fixedDelay(scheduleId, 60_000), fireAt, fireAt);
        var due = store.dueRecurring(Instant.now(), 10).getFirst();
        var builder = task(newId());
        if (key != null) builder.uniqueKey(key);
        assertThat(store.commitRecurring(due, fireAt.plusSeconds(1), builder.build()))
                .isTrue();
        assertThat(store.promote(10).moved()).isEqualTo(1);
        return new Occurrence(fireAt, due, claim(1).getFirst());
    }

    @Test
    void anAckThatFailsAfterTheTaskIsEndedLeavesTaskAndScheduleUntouched() throws SQLException {
        var o = leasedOccurrenceOfFixedDelay("ack", null);
        var effect = ScheduleEffect.rearm("ack", o.leased().id(), o.fireAt().plusSeconds(500));

        failScheduleUpdatesWhen("OLD.occurrence_id IS NOT NULL AND NEW.occurrence_id IS NULL");
        try {
            assertThatThrownBy(() -> store.ack(o.leased().id(), o.leased().revision(), true, true, effect))
                    .isInstanceOf(TaskStoreException.class);
        } finally {
            healScheduleUpdates();
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
        assertThat(occurrenceOf("ack")).isZero();
    }

    @Test
    void aFailThatFailsAfterTheTaskDiesLeavesTaskAndScheduleUntouched() throws SQLException {
        var o = leasedOccurrenceOfFixedDelay("dies", null);
        var effect = ScheduleEffect.pause("dies", o.leased().id());

        failScheduleUpdatesWhen("OLD.occurrence_id IS NOT NULL AND NEW.occurrence_id IS NULL");
        try {
            assertThatThrownBy(() -> store.fail(o.leased().id(), o.leased().revision(), failure(1), null, true, effect))
                    .isInstanceOf(TaskStoreException.class);
        } finally {
            healScheduleUpdates();
        }

        var task = get(o.leased().id());
        assertThat(task.state()).isEqualTo(TaskState.LEASED);
        assertThat(task.revision()).isEqualTo(o.leased().revision());
        assertThat(task.failures()).isNullOrEmpty();
        assertThat(occurrenceOf("dies")).isEqualTo(o.leased().id());
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isFalse();
        assertThat(effect.applied()).isFalse();

        assertThat(store.fail(o.leased().id(), o.leased().revision(), failure(1), null, true, effect))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(effect.applied()).isTrue();
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isTrue();
        assertThat(occurrenceOf("dies")).isZero();
    }

    @Test
    void aRecurringCommitThatFailsAfterTheOccurrenceIsInsertedStoresNothing() throws SQLException {
        var fireAt = Instant.now().minusSeconds(10).truncatedTo(ChronoUnit.MICROS);
        store.putRecurring(fixedDelay("commit", 60_000), fireAt, fireAt);
        var due = store.dueRecurring(Instant.now(), 10).getFirst();
        long occurrence = newId();

        failScheduleUpdatesWhen(
                "NEW.occurrence_id IS NOT NULL AND OLD.occurrence_id IS DISTINCT FROM NEW.occurrence_id");
        try {
            assertThatThrownBy(() -> store.commitRecurring(
                            due, fireAt.plusSeconds(1), task(occurrence).build()))
                    .isInstanceOf(TaskStoreException.class);
        } finally {
            healScheduleUpdates();
        }

        assertThat(store.getTask(occurrence)).isEmpty();
        assertThat(occurrenceOf("commit")).isZero();
        assertThat(store.recurringSchedules()).containsEntry("commit", fireAt);
        assertThat(store.dueRecurring(Instant.now(), 10)).hasSize(1);

        assertThat(store.commitRecurring(
                        due, fireAt.plusSeconds(1), task(occurrence).build()))
                .isTrue();
        assertThat(occurrenceOf("commit")).isEqualTo(occurrence);
        assertThat(get(occurrence).scheduleId()).isEqualTo("commit");
    }

    private int lockWaiters() throws SQLException {
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var ps = c.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                        + " AND wait_event_type = 'Lock' AND query LIKE ?")) {
            ps.setString(1, "%" + prefix + "\\_%");
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private void awaitLockWaiters(int expected) throws Exception {
        var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (lockWaiters() != expected) {
            assertThat(System.nanoTime()).as("lock waiters").isLessThan(deadline);
            Thread.onSpinWait();
        }
    }

    @Test
    void anAckContendingWithARecurringCommitFinishesWithCorrectOwnership() throws Exception {
        var o = leasedOccurrenceOfFixedDelay("busy", "sched:busy");
        var nextDue = store.dueRecurring(Instant.now(), 10).getFirst();
        assertThat(nextDue.fireAt()).isEqualTo(o.fireAt().plusSeconds(1));
        var effect = ScheduleEffect.rearm("busy", o.leased().id(), o.fireAt().plusSeconds(500));
        long skipped = newId();

        var pool = Executors.newFixedThreadPool(2);
        try (var holder = PostgresTestSupport.dataSource().getConnection()) {
            holder.setAutoCommit(false);
            try (var st = holder.createStatement()) {
                st.execute("DELETE FROM " + prefix + "_task WHERE id = "
                        + o.leased().id());
            }

            var commit = pool.submit(() -> store.commitRecurring(
                    nextDue,
                    o.fireAt().plusSeconds(2),
                    task(skipped).uniqueKey("sched:busy").build()));
            awaitLockWaiters(1);
            var ack = pool.submit(() -> store.ack(o.leased().id(), o.leased().revision(), false, false, effect));
            awaitLockWaiters(2);
            holder.rollback();

            assertThat(commit.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(ack.get(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(store.getTask(skipped)).isEmpty();
        assertThat(store.getTask(o.leased().id())).isEmpty();
        assertThat(effect.applied()).isTrue();
        assertThat(occurrenceOf("busy")).isZero();
        assertThat(store.recurringSchedules()).containsEntry("busy", o.fireAt().plusSeconds(500));
    }

    // ---- fault injection: a key holder that disappears during enqueue --------------------------------

    /**
     * Makes the first {@code rounds} insert statements on the task table meet a holder of the key that the
     * same statement's trigger removes again before the store classifies the conflict.
     */
    private void keyHolderAppearsAndVanishes(String key, int rounds) throws SQLException {
        sql("CREATE TABLE " + prefix + "_chaos (rounds_left int NOT NULL)");
        sql("INSERT INTO " + prefix + "_chaos VALUES (" + rounds + ")");
        long holder = newId();
        sql("CREATE FUNCTION " + prefix + "_appear() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;"
                + " IF (SELECT rounds_left FROM " + prefix + "_chaos) > 0 THEN"
                + " UPDATE " + prefix + "_chaos SET rounds_left = rounds_left - 1;"
                + " INSERT INTO " + prefix + "_task (id, revision, queue_id, event_id, payload_version, state,"
                + " attempts, max_attempts, created, due, state_changed, unique_key)"
                + " VALUES (" + holder + ", 0, " + queue + ", " + type + ", 1, 0, 0, 1, now(), now(), now(), '"
                + key + "');"
                + " END IF; RETURN NULL; END $$");
        sql("CREATE FUNCTION " + prefix + "_vanish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " IF pg_trigger_depth() = 1 THEN DELETE FROM " + prefix + "_task WHERE id = " + holder
                + "; END IF; RETURN NULL; END $$");
        sql("CREATE TRIGGER " + prefix + "_appear BEFORE INSERT ON " + prefix
                + "_task FOR EACH STATEMENT EXECUTE FUNCTION " + prefix + "_appear()");
        sql("CREATE TRIGGER " + prefix + "_vanish AFTER INSERT ON " + prefix
                + "_task FOR EACH STATEMENT EXECUTE FUNCTION " + prefix + "_vanish()");
    }

    private int chaosRoundsLeft() throws SQLException {
        try (var c = PostgresTestSupport.dataSource().getConnection();
                var st = c.createStatement();
                var rs = st.executeQuery("SELECT rounds_left FROM " + prefix + "_chaos")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private void removeKeyHolderChaos() throws SQLException {
        sql("DROP TRIGGER IF EXISTS " + prefix + "_appear ON " + prefix + "_task");
        sql("DROP TRIGGER IF EXISTS " + prefix + "_vanish ON " + prefix + "_task");
        sql("DROP FUNCTION IF EXISTS " + prefix + "_appear()");
        sql("DROP FUNCTION IF EXISTS " + prefix + "_vanish()");
        sql("DROP TABLE IF EXISTS " + prefix + "_chaos");
    }

    @Test
    void aKeyHolderThatDisappearsBeforeClassificationIsRetriedAndOutcomesStayInOrder() throws SQLException {
        keyHolderAppearsAndVanishes("race", 2);
        long base = store.allocateIds(5);
        List<EnqueueOutcome> outcomes;
        try {
            outcomes = store.enqueueAll(List.of(
                    task(base).build(),
                    task(base + 1).uniqueKey("race").build(),
                    task(base + 2).uniqueKey("race").build(),
                    task(base + 3).uniqueKey("other").build(),
                    task(base).build()));
            assertThat(chaosRoundsLeft()).isZero();
        } finally {
            removeKeyHolderChaos();
        }

        assertThat(outcomes)
                .containsExactly(
                        EnqueueOutcome.STORED,
                        EnqueueOutcome.STORED,
                        new EnqueueOutcome.DuplicateKey(base + 1),
                        EnqueueOutcome.STORED,
                        EnqueueOutcome.EXISTS);
        assertThat(get(base + 1).uniqueKey()).isEqualTo("race");
        assertThat(store.getTask(base + 2)).isEmpty();
        assertThat(get(base + 3).uniqueKey()).isEqualTo("other");
    }

    @Test
    void enqueueGivesUpAfterThreeRoundsOfAKeyHolderThatKeepsDisappearing() throws SQLException {
        keyHolderAppearsAndVanishes("race", 3);
        long id = newId();
        try {
            assertThatThrownBy(() -> store.enqueue(task(id).uniqueKey("race").build()))
                    .isInstanceOf(TaskStoreException.class)
                    .hasMessageContaining("did not settle for task " + id);
            assertThat(chaosRoundsLeft()).isZero();
        } finally {
            removeKeyHolderChaos();
        }

        assertThat(store.getTask(id)).isEmpty();
        assertThat(store.enqueue(task(id).uniqueKey("race").build())).isEqualTo(EnqueueOutcome.STORED);
    }
}

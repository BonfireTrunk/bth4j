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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.DueSchedule;
import today.bonfire.oss.bth4j.spi.EnqueueOutcome;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.LeaseStatus;
import today.bonfire.oss.bth4j.spi.RecurringDefinition;
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
    private int event;
    private int worker;

    @BeforeEach
    void setUp() {
        var namespace = "it" + NAMESPACES.incrementAndGet() + "_" + System.nanoTime();
        prefix = PostgresTestSupport.prefix(namespace);
        store = PostgresTestSupport.store(namespace);
        queue = store.queueId("default");
        event = store.eventId("send-mail");
        worker = store.allocateWorkerId("host:1");
    }

    private TaskRecord.Builder task(long id) {
        return TaskRecord.builder()
                .id(id)
                .queueId(queue)
                .eventId(event)
                .maxAttempts(3)
                .created(Instant.now())
                .data("payload-" + id);
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

    // ---- schema and identity -------------------------------------------------------------------------

    @Test
    void initIsIdempotentAndReportsTheEngine() {
        store.init();
        store.init();
        assertThat(store.info().engine()).isEqualTo("postgresql");
        assertThat(store.info().schemaVersion()).isEqualTo(1);
    }

    @Test
    void initRefusesAnotherSchemaVersion() throws SQLException {
        sql("UPDATE " + prefix + "_meta SET value = '2' WHERE key = 'schema_version'");
        assertThatThrownBy(store::init)
                .isInstanceOf(TaskStoreException.class)
                .hasMessage("schema version 2, this library needs 1");
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
    void workerIdsAreNeverReused() {
        int second = store.allocateWorkerId("host:2");
        assertThat(second).isGreaterThan(worker);
    }

    @Test
    void registryResolvesNamesOnceWithoutBurningIds() {
        for (int i = 0; i < 20; i++) assertThat(store.queueId("default")).isEqualTo(queue);
        int other = store.queueId("other");
        assertThat(other).isEqualTo(queue + 1);
        assertThat(store.eventId("send-mail")).isEqualTo(event);
        int report = store.eventId("report");
        assertThat(store.queueNames()).containsExactly(Map.entry(queue, "default"), Map.entry(other, "other"));
        assertThat(store.eventNames()).containsExactly(Map.entry(event, "send-mail"), Map.entry(report, "report"));
        assertThatThrownBy(() -> store.queueId(" ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fullRegistryIsReported() throws SQLException {
        sql("SELECT setval('" + prefix + "_event_id_seq', 32767)");
        assertThatThrownBy(() -> store.eventId("one-too-many"))
                .isInstanceOf(TaskStoreException.class)
                .hasMessageContaining("event registry is full");
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
                .maxRun(Duration.ofSeconds(9))
                .progress("10%")
                .steps(steps)
                .build();

        assertThat(store.enqueue(t)).isEqualTo(EnqueueOutcome.STORED);

        var r = get(id);
        assertThat(r.state()).isEqualTo(TaskState.QUEUED);
        assertThat(r.revision()).isZero();
        assertThat(r.attempts()).isZero();
        assertThat(r.payloadVersion()).isEqualTo(2);
        assertThat(r.due()).isAfter(before).isEqualTo(r.stateChanged());
        assertThat(r.created()).isEqualTo(t.created().truncatedTo(ChronoUnit.MICROS));
        assertThat(r.data()).isEqualTo("payload-" + id);
        assertThat(r.account()).isEqualTo("acme");
        assertThat(r.headers()).isEqualTo(Map.of("trace", "abc"));
        assertThat(r.uniqueKey()).isEqualTo("k1");
        assertThat(r.keyKeep()).isEqualTo(Duration.ofMinutes(5));
        assertThat(r.keyUntil()).isNull();
        assertThat(r.retryPolicy()).isEqualTo("slow");
        assertThat(r.scheduleId()).isEqualTo("nightly");
        assertThat(r.retryOf()).isEqualTo(7L);
        assertThat(r.startBy()).isEqualTo(t.startBy().truncatedTo(ChronoUnit.MICROS));
        assertThat(r.maxRun()).isEqualTo(Duration.ofSeconds(9));
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
    void reinsertingAnExistingIdReportsExistsAndChangesNothing() {
        long id = newId();
        store.enqueue(task(id).build());
        assertThat(store.enqueue(task(id).data("other").build())).isEqualTo(EnqueueOutcome.EXISTS);
        assertThat(get(id).data()).isEqualTo("payload-" + id);
    }

    @Test
    void aHeldKeyReportsItsHolder() {
        long first = newId();
        store.enqueue(task(first).uniqueKey("order-1").build());
        long second = newId();

        assertThat(store.enqueue(task(second).uniqueKey("order-1").build()))
                .isEqualTo(new EnqueueOutcome.DuplicateKey(first));
        assertThat(store.getTask(second)).isEmpty();
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
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();

        assertThat(get(t.id()).uniqueKey()).isNull();
        assertThat(store.enqueue(task(newId()).uniqueKey("once").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void theKeyIsReleasedWhenTheTaskDies() {
        var t = enqueueAndClaim(task(newId()).uniqueKey("once").maxAttempts(1));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);

        assertThat(store.enqueue(task(newId()).uniqueKey("once").build())).isEqualTo(EnqueueOutcome.STORED);
    }

    @Test
    void aKeptKeyIsHeldForItsWindowThenReleasedInline() throws Exception {
        var t = enqueueAndClaim(task(newId()).uniqueKey("window").keyKeep(Duration.ofMillis(300)));
        store.ack(t.id(), t.revision(), true, true);

        var done = get(t.id());
        assertThat(done.uniqueKey()).isEqualTo("window");
        assertThat(done.keyUntil()).isAfter(done.stateChanged());
        assertThat(store.enqueue(task(newId()).uniqueKey("window").build()))
                .isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));

        Thread.sleep(400);
        long next = newId();
        assertThat(store.enqueue(task(next).uniqueKey("window").build())).isEqualTo(EnqueueOutcome.STORED);
        assertThat(get(t.id()).uniqueKey()).isNull();
        assertThat(get(t.id()).keyUntil()).isNull();
    }

    @Test
    void releaseKeysFreesExpiredWindows() throws Exception {
        var a = enqueueAndClaim(task(newId()).uniqueKey("a").keyKeep(Duration.ofMillis(100)));
        store.ack(a.id(), a.revision(), true, true);
        var b = enqueueAndClaim(task(newId()).uniqueKey("b").keyKeep(Duration.ofHours(1)));
        store.ack(b.id(), b.revision(), true, true);

        Thread.sleep(200);
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
        assertThat(t.claimedAt()).isNotNull().isEqualTo(t.stateChanged());
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
    void claimSkipsUnknownEvents() {
        int unknown = store.eventId("from-a-newer-runner");
        long known = newId();
        long other = newId();
        store.enqueue(
                task(other).eventId(unknown).due(Instant.now().minusSeconds(60)).build());
        store.enqueue(task(known).build());

        assertThat(store.claim(new ClaimRequest(List.of(queue), 5, LEASE, worker, Set.of())))
                .isEmpty();
        assertThat(store.claim(new ClaimRequest(List.of(queue), 5, LEASE, worker, Set.of(event))))
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

        assertThat(store.ack(id, stale, true, true)).isFalse();
        assertThat(store.fail(id, stale, failure(1), Instant.now(), true)).isEqualTo(FailOutcome.LOST);
        assertThat(store.failExpired(id, stale, failure(1), Instant.now(), true))
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
        assertThat(store.ack(id, 0, true, true)).isFalse();
        assertThat(store.unclaim(id, 0)).isFalse();
        assertThat(store.fail(id, 0, failure(1), Instant.now(), true)).isEqualTo(FailOutcome.LOST);
    }

    @Test
    void ackKeepsTheRowAndOptionallyThePayload() {
        var steps = Map.of("one", new StepResult(Instant.now().truncatedTo(ChronoUnit.MILLIS), "1"));
        var kept = enqueueAndClaim(task(newId()));
        store.saveSteps(kept.id(), kept.revision(), steps);
        assertThat(store.ack(kept.id(), kept.revision(), true, true)).isTrue();
        var dropped = enqueueAndClaim(task(newId()));
        store.saveSteps(dropped.id(), dropped.revision(), steps);
        assertThat(store.ack(dropped.id(), dropped.revision(), true, false)).isTrue();

        var a = get(kept.id());
        assertThat(a.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(a.revision()).isEqualTo(kept.revision() + 1);
        assertThat(a.data()).isEqualTo(kept.data());
        assertThat(a.leaseUntil()).isNull();
        assertThat(a.workerId()).isEqualTo(worker);
        var b = get(dropped.id());
        assertThat(b.data()).isNull();
        assertThat(b.steps()).isEqualTo(steps);
        assertThat(store.ack(kept.id(), a.revision(), true, true)).isFalse();
    }

    @Test
    void ackWithoutKeepRemovesTheTaskUnlessItHoldsItsKey() {
        var plain = enqueueAndClaim(task(newId()));
        assertThat(store.ack(plain.id(), plain.revision(), false, true)).isTrue();
        assertThat(store.getTask(plain.id())).isEmpty();

        var keyed = enqueueAndClaim(task(newId()).uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(keyed.id(), keyed.revision(), false, true)).isTrue();
        var row = get(keyed.id());
        assertThat(row.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(row.data()).isNull();
        assertThat(row.uniqueKey()).isEqualTo("k");
    }

    @Test
    void failSchedulesARetryAndRecordsTheFailure() {
        var t = enqueueAndClaim(task(newId()));
        var retryAt = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MICROS);

        var f = failure(1);
        assertThat(store.fail(t.id(), t.revision(), f, retryAt, true)).isEqualTo(FailOutcome.RETRY);

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
        assertThat(store.fail(exhausted.id(), exhausted.revision(), failure(1), Instant.now(), true))
                .isEqualTo(FailOutcome.DEAD);
        var dead = get(exhausted.id());
        assertThat(dead.state()).isEqualTo(TaskState.DEAD);
        assertThat(dead.data()).isEqualTo(exhausted.data());
        assertThat(dead.workerId()).isEqualTo(worker);

        var refused = enqueueAndClaim(task(newId()));
        assertThat(store.fail(refused.id(), refused.revision(), failure(1), null, false))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(store.getTask(refused.id())).isEmpty();
    }

    @Test
    void failuresAreBoundedToTheLastTwenty() {
        long id = newId();
        store.enqueue(task(id).maxAttempts(100).build());
        for (int attempt = 1; attempt <= 22; attempt++) {
            var t = claim(1).getFirst();
            assertThat(store.fail(
                            id, t.revision(), failure(attempt), Instant.now().minusSeconds(1), true))
                    .isEqualTo(FailOutcome.RETRY);
            assertThat(store.promote(10).moved()).isEqualTo(1);
        }

        var failures = get(id).failures();
        assertThat(failures).hasSize(20);
        assertThat(failures.getFirst().attempt()).isEqualTo(3);
        assertThat(get(id).lastFailure().attempt()).isEqualTo(22);
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
        assertThat(e.eventId()).isEqualTo(event);
        assertThat(e.cancelRequested()).isFalse();

        store.renew(List.of(new LeaseRef(id, t.revision())), Duration.ofMinutes(1));
        assertThat(store.failExpired(id, t.revision(), failure(1), Instant.now(), true))
                .isEqualTo(FailOutcome.LOST);
        assertThat(store.reclaim(10)).isEmpty();

        store.renew(List.of(new LeaseRef(id, t.revision())), Duration.ofMillis(1));
        Thread.sleep(20);
        assertThat(store.failExpired(id, t.revision(), failure(1), Instant.now(), true))
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

        store.fail(t.id(), t.revision(), failure(1), Instant.now().minusSeconds(1), true);
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
        store.ack(ok.id(), ok.revision(), true, true);
        var bad = enqueueAndClaim(task(newId()).maxAttempts(1));
        store.fail(bad.id(), bad.revision(), failure(1), null, true);
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
            store.ack(t.id(), t.revision(), true, true);
        }
        Thread.sleep(5);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 2)).isEqualTo(2);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 2)).isEqualTo(2);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 2)).isEqualTo(1);
    }

    @Test
    void purgeKeepsTasksThatStillHoldTheirKey() throws Exception {
        var t = enqueueAndClaim(task(newId()).uniqueKey("held").keyKeep(Duration.ofMillis(300)));
        store.ack(t.id(), t.revision(), false, true);

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
        store.ack(leased.id(), leased.revision(), true, true);
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

        var page1 = store.tasks(TaskState.QUEUED, null, 0, 2);
        assertThat(page1).extracting(TaskRecord::id).containsExactly(ids.get(0), ids.get(1));
        var page2 = store.tasks(TaskState.QUEUED, null, page1.getLast().id(), 2);
        assertThat(page2).extracting(TaskRecord::id).containsExactly(ids.get(2), ids.get(3));
        var page3 = store.tasks(TaskState.QUEUED, null, page2.getLast().id(), 2);
        assertThat(page3).extracting(TaskRecord::id).containsExactly(ids.get(4));

        assertThat(store.tasks(TaskState.QUEUED, other, 0, 10))
                .extracting(TaskRecord::id)
                .containsExactly(ids.get(2));
        assertThat(store.tasks(TaskState.QUEUED, queue, ids.get(0), 10))
                .extracting(TaskRecord::id)
                .containsExactly(ids.get(1), ids.get(3), ids.get(4));
        assertThat(store.tasks(TaskState.SUCCEEDED, null, 0, 10)).isEmpty();
        assertThat(store.tasks(TaskState.QUEUED, null, 0, 0)).isEmpty();
    }

    @Test
    void listingAfterAMissingTaskContinuesById() {
        long gone = newId();
        long kept = newId();
        store.enqueue(task(kept).build());

        assertThat(store.tasks(TaskState.QUEUED, null, gone, 10))
                .extracting(TaskRecord::id)
                .containsExactly(kept);
        assertThat(store.tasks(TaskState.QUEUED, null, kept + 1, 10)).isEmpty();
    }

    // ---- recurring definitions -----------------------------------------------------------------------

    private RecurringDefinition definition(String id, String cron) {
        return new RecurringDefinition(id, event, 1, queue, "acme", cron, "{\"x\":1}", 4, 0);
    }

    @Test
    void recurringOccurrenceIsCommittedOncePerFireTime() {
        var fireAt = Instant.now().minusSeconds(1).truncatedTo(ChronoUnit.MICROS);
        var nextFire = fireAt.plusSeconds(3600);
        store.putRecurring(definition("hourly", "0 * * * *"), fireAt);

        assertThat(store.recurringSchedules()).containsExactly(Map.entry("hourly", fireAt));
        var due = store.dueRecurring(Instant.now(), 10);
        assertThat(due).hasSize(1);
        DueSchedule d = due.getFirst();
        assertThat(d.fireAt()).isEqualTo(fireAt);
        assertThat(d.definition().eventId()).isEqualTo(event);
        assertThat(d.definition().queueId()).isEqualTo(queue);
        assertThat(d.definition().account()).isEqualTo("acme");
        assertThat(d.definition().maxAttempts()).isEqualTo(4);
        assertThat(d.definition().revision()).isPositive();

        long id = newId();
        var occurrence = task(id).scheduleId("hourly")
                .data(d.definition().data())
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
    void putRecurringKeepsTheFireTimeForAnUnchangedCronAndBumpsTheRevision() {
        var first = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MICROS);
        store.putRecurring(definition("job", "0 * * * *"), first);
        long revision = store.dueRecurring(first, 10).getFirst().definition().revision();

        store.putRecurring(definition("job", "0 * * * *"), first.plusSeconds(600));
        assertThat(store.recurringSchedules()).containsEntry("job", first);
        var d = store.dueRecurring(first, 10).getFirst();
        assertThat(d.definition().revision()).isGreaterThan(revision);

        store.putRecurring(definition("job", "*/5 * * * *"), first.plusSeconds(600));
        assertThat(store.recurringSchedules()).containsEntry("job", first.plusSeconds(600));
        assertThat(store.commitRecurring(
                        d, first.plusSeconds(1200), task(newId()).build()))
                .isFalse();
    }

    @Test
    void deleteRecurringReportsWhetherItExisted() {
        store.putRecurring(definition("gone", "0 * * * *"), Instant.now());
        assertThat(store.deleteRecurring("gone")).isTrue();
        assertThat(store.deleteRecurring("gone")).isFalse();
        assertThat(store.recurringSchedules()).isEmpty();
    }
}

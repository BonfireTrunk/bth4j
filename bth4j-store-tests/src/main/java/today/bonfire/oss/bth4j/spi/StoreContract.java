package today.bonfire.oss.bth4j.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.CancelOutcome;
import today.bonfire.oss.bth4j.DeleteOutcome;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.PutScheduleOutcome;
import today.bonfire.oss.bth4j.PutScheduleResult;
import today.bonfire.oss.bth4j.QueueInfo;
import today.bonfire.oss.bth4j.RetryOutcome;
import today.bonfire.oss.bth4j.ScheduleOutcome;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.TaskRequest;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.Uniqueness;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;

/**
 * Contract of every {@link FullStore}, checked through SPI reads only. A store module subclasses it and
 * supplies a fresh, isolated store per test through {@link #newStore(String)}. Time based tests use short
 * durations and move time through {@link #advance(Duration)}.
 */
public abstract class StoreContract {

    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration SHORT = Duration.ofMillis(200);
    private static final Duration PAST_SHORT = Duration.ofMillis(300);
    private static final Duration MID = Duration.ofMillis(600);
    private static final Duration PAST_MID = Duration.ofMillis(700);

    protected FullStore store;
    private int runner;
    /** A registered queue no test enqueues to; with it the ids of queue, type and runner all differ. */
    private int idle;

    private int queue;
    private String namespace;
    private int type;

    /** @return a store with no state, isolated from every other namespace */
    protected abstract FullStore newStore(String namespace);

    /** @return the clock the store reads, when the test can control it; otherwise the system clock */
    protected Clock clock() {
        return Clock.systemUTC();
    }

    /** @return how a listing orders the ids of tasks that share its time: id order, unless the store documents another */
    protected Comparator<Long> orderOfIdsThatShareATime() {
        return Comparator.naturalOrder();
    }

    /** @return whether finished keys are released by the store itself, so that {@link MaintenanceStore#releaseKeys} has nothing to count */
    protected boolean releasesKeysOnItsOwn() {
        return false;
    }

    /**
     * Overwrites a stored value of a task in the store made with {@code namespace} by one the store cannot read,
     * for example headers JSON with a null value.
     *
     * @return false when the store cannot hold an unreadable value; the tests that need one are skipped then
     */
    protected boolean corruptStoredValue(String namespace, long taskId) {
        return false;
    }

    /**
     * Replaces the schema version that the store made with {@code namespace} keeps by one this library does not
     * write.
     *
     * @return false when the store keeps no schema version; the test that needs one is skipped then
     */
    protected boolean writeOtherSchemaVersion(String namespace) {
        return false;
    }

    /**
     * Removes the schema version that the store made with {@code namespace} keeps and leaves its tasks in place.
     *
     * @return false when the store keeps no schema version; the test that needs one is skipped then
     */
    protected boolean removeSchemaVersion(String namespace) {
        return false;
    }

    /** Lets store time pass. Real stores sleep; an in-memory store moves its clock. */
    protected void advance(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** @return the largest queue or type id the store under test accepts */
    protected int registryBound() {
        return 32767;
    }

    /** The two name registries of a store. */
    protected enum Registry {
        QUEUE,
        TYPE
    }

    /**
     * Brings a registry of {@code fresh}, made by {@link #newStore(String)} with {@code namespace} and holding
     * ids 1 up to the first one only, to the state where {@code lastId} ids are allocated, so that the next new
     * name gets {@code lastId + 1}. The default registers filler names, which suits a store with a small bound;
     * a store with a large bound overrides it to move its id counter directly.
     */
    protected void fillRegistry(TaskStore fresh, String namespace, Registry registry, int lastId) {
        for (int id = 2; id <= lastId; id++) {
            register(fresh, registry, "filler" + id);
        }
    }

    private static int register(TaskStore target, Registry registry, String name) {
        return registry == Registry.QUEUE ? target.queueId(name) : target.typeId(name);
    }

    @BeforeEach
    void setUpStore() {
        namespace = "contract" + COUNTER.incrementAndGet() + "x" + System.nanoTime() % 100_000;
        store = newStore(namespace);
        store.init();
        // a store that writes one id where another belongs fails when queue, type and runner ids differ
        idle = store.queueId("idle");
        queue = store.queueId("main");
        store.typeId("filler-1");
        store.typeId("filler-2");
        type = store.typeId("work");
        runner = store.allocateRunnerId("contract-test", Set.of(queue), Set.of(type));
    }

    /**
     * Every task a listing returns, after every test, is the task {@link InspectionStore#task} reads, in the
     * listed state, with a lease only while LEASED and no key-until while open.
     */
    @AfterEach
    void listingsMatchReads() {
        for (TaskState state : TaskState.values()) {
            String cursor = null;
            int pages = 0;
            do {
                TaskPage page = store.tasks(state, null, cursor, 100);
                for (TaskRecord item : page.items()) {
                    assertThat(item.state()).as("state of listed %s", item.id()).isEqualTo(state);
                    assertThat(item)
                            .as("listed %s %s against task", state, item.id())
                            .isEqualTo(get(item.id()));
                    assertThat(item.leaseUntil() != null)
                            .as("lease of %s in %s", item.id(), state)
                            .isEqualTo(state == TaskState.LEASED);
                    if (!state.isTerminal()) {
                        assertThat(item.keyUntil())
                                .as("keyUntil of open %s", item.id())
                                .isNull();
                    }
                }
                cursor = page.next();
            } while (cursor != null && ++pages < 1000);
            assertThat(cursor).as("walk of %s ended", state).isNull();
        }
    }

    private Instant now() {
        return clock().instant();
    }

    /**
     * Ends the test as inconclusive when {@code deadline} has passed. A check that only holds before a deadline
     * proves nothing after it, and on a store with a real clock a busy machine can take longer than the window.
     * The deadline is a time on the test's clock, or a stored time that the test checked against that clock first:
     * a store that wrote a wrong time then fails the test instead of ending it as inconclusive.
     */
    private void assumeBefore(Instant deadline) {
        assumeTrue(now().isBefore(deadline), "the machine took longer than the test's time window");
    }

    private TaskRecord.Builder task() {
        return task(queue, type);
    }

    private TaskRecord.Builder task(int queueId, int typeId) {
        return TaskRecord.builder()
                .id(store.allocateIds(1))
                .queueId(queueId)
                .typeId(typeId)
                .maxAttempts(3)
                .createdAt(now())
                .encodedPayload("payload");
    }

    private long put(TaskRecord.Builder builder) {
        TaskRecord record = builder.build();
        assertThat(store.enqueue(record)).isEqualTo(EnqueueOutcome.STORED);
        return record.id();
    }

    private long putNew() {
        return put(task());
    }

    private TaskRecord get(long id) {
        return store.task(id).orElseThrow(() -> new AssertionError("task " + id + " is missing"));
    }

    private List<TaskRecord> claim(int count, int... queueIds) {
        return claim(count, LEASE, null, queueIds);
    }

    private List<TaskRecord> claim(int count, Duration lease, Set<Integer> typeIds, int... queueIds) {
        List<Integer> queues = new ArrayList<>();
        for (int q : queueIds) {
            queues.add(q);
        }
        if (queues.isEmpty()) {
            queues.add(queue);
        }
        return store.claim(new ClaimRequest(
                queues.stream().map(q -> new QueueGrant(q, count, count)).toList(), count, lease, runner, typeIds));
    }

    private TaskRecord claimOne() {
        List<TaskRecord> claimed = claim(1);
        assertThat(claimed).hasSize(1);
        return claimed.get(0);
    }

    private TaskRecord leased(TaskRecord.Builder builder) {
        put(builder);
        return claimOne();
    }

    private FailureInfo failure(int attempt) {
        return new FailureInfo(attempt, now(), "java.lang.IllegalStateException", "boom " + attempt, null);
    }

    private static List<Long> ids(List<ExpiredTask> expired) {
        return expired.stream().map(ExpiredTask::id).toList();
    }

    private static Instant millis(Instant instant) {
        return instant.truncatedTo(ChronoUnit.MILLIS);
    }

    private static <T> List<T> runConcurrently(int threads, Callable<T> body) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return body.call();
                }));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) {
                results.add(f.get(30, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    // ids and registries

    @Test
    void infoReportsTheSchemaVersion() {
        assertThat(store.info().schemaVersion()).isPositive();
        assertThat(store.info().engine()).isNotBlank();
    }

    @Test
    void initIsRepeatable() {
        var version = store.info().schemaVersion();
        TaskRecord running = leased(task().uniqueKey("l"));
        TaskRecord done = leased(task().uniqueKey("d").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(done.id(), done.revision(), true, true, null)).isTrue();
        TaskRecord dead = leased(task().uniqueKey("x").keyKeep(Duration.ofHours(1)));
        assertThat(store.fail(dead.id(), dead.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        long queued = put(task().uniqueKey("q"));
        long scheduled = put(task().uniqueKey("s").dueAt(now().plus(Duration.ofHours(1))));
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(fullDefinition("fixed"), fire, fire);
        store.putSchedule(definition("cron", "* * * * *"), fire.plusSeconds(5), fire.plusSeconds(5));
        List<Long> ids = List.of(queued, scheduled, running.id(), done.id(), dead.id());
        List<Object> before = snapshot(ids, List.of("q", "s", "l", "d", "x"));

        store.init();
        store.init();

        assertThat(store.info().schemaVersion()).isEqualTo(version);
        assertThat(snapshot(ids, List.of("q", "s", "l", "d", "x"))).isEqualTo(before);
        assertThat(store.allocateIds(1))
                .isGreaterThanOrEqualTo(ids.stream().max(Long::compare).orElseThrow() + 1);
        assertThat(store.enqueue(task().uniqueKey("l").build()))
                .isEqualTo(new EnqueueOutcome.DuplicateKey(running.id()));
        assertThat(claimOne().id()).isEqualTo(queued);
    }

    private List<Object> snapshot(List<Long> ids, List<String> keys) {
        List<Object> all = new ArrayList<>();
        ids.forEach(id -> all.add(store.task(id)));
        keys.forEach(key -> all.add(store.taskByKey(key)));
        all.add(store.counts());
        all.add(allSchedules());
        all.add(store.nextFireTimes());
        all.add(store.idHighWaterMark());
        all.add(store.queueNames());
        all.add(store.typeNames());
        return all;
    }

    @Test
    void allocateIdsReturnsDisjointBlocks() {
        long first = store.allocateIds(10);
        long second = store.allocateIds(5);
        long third = store.allocateIds(1);
        assertThat(second).isGreaterThanOrEqualTo(first + 10);
        assertThat(third).isGreaterThanOrEqualTo(second + 5);
    }

    @Test
    void idHighWaterMarkIsTheFirstIdNotYetAllocated() {
        long fresh = store.idHighWaterMark();
        assertThat(fresh).isEqualTo(1);
        assertThat(store.idHighWaterMark()).isEqualTo(fresh);

        long first = store.allocateIds(10);
        assertThat(first).isEqualTo(fresh);
        assertThat(store.idHighWaterMark()).isEqualTo(first + 10);

        long single = store.allocateIds(1);
        assertThat(store.idHighWaterMark()).isEqualTo(single + 1);
    }

    @Test
    void allocateRunnerIdIsUniqueAcrossCalls() {
        int a = store.allocateRunnerId("a", Set.of(), Set.of());
        int b = store.allocateRunnerId("b", Set.of(), Set.of());
        assertThat(a).isNotEqualTo(b).isNotEqualTo(runner);
        assertThat(b).isNotEqualTo(runner);
    }

    @Test
    void queueIdIsStablePermanentAndStartsAtOne() {
        TaskStore fresh = newStore("fresh" + COUNTER.incrementAndGet());
        assertThat(fresh.queueId("a")).isEqualTo(1);
        assertThat(fresh.queueId("b")).isEqualTo(2);
        assertThat(fresh.queueId("a")).isEqualTo(1);
    }

    @Test
    void typeIdIsStablePermanentAndStartsAtOne() {
        TaskStore fresh = newStore("fresh" + COUNTER.incrementAndGet());
        assertThat(fresh.typeId("a")).isEqualTo(1);
        assertThat(fresh.typeId("b")).isEqualTo(2);
        assertThat(fresh.typeId("a")).isEqualTo(1);
    }

    @Test
    void queueAndTypeRegistriesAreIndependent() {
        TaskStore fresh = newStore("fresh" + COUNTER.incrementAndGet());
        assertThat(fresh.queueId("x")).isEqualTo(1);
        assertThat(fresh.typeId("y")).isEqualTo(1);
        assertThat(fresh.queueNames()).containsOnly(Map.entry(1, "x"));
        assertThat(fresh.typeNames()).containsOnly(Map.entry(1, "y"));
    }

    @Test
    void registryNamesMapIdToName() {
        int other = store.queueId("other");
        int ev = store.typeId("other-type");
        assertThat(store.queueNames()).containsEntry(queue, "main").containsEntry(other, "other");
        assertThat(store.typeNames()).containsEntry(type, "work").containsEntry(ev, "other-type");
    }

    @Test
    void registryRefusesMoreNamesThanItsBound() {
        int bound = registryBound();
        for (Registry registry : Registry.values()) {
            String namespace = "fresh" + COUNTER.incrementAndGet();
            TaskStore fresh = newStore(namespace);
            assertThat(register(fresh, registry, "first")).isEqualTo(1);
            fillRegistry(fresh, namespace, registry, bound - 1);

            assertThat(register(fresh, registry, "last"))
                    .as("%s last id", registry)
                    .isEqualTo(bound);
            assertThatThrownBy(() -> register(fresh, registry, "one-too-many"))
                    .as("%s overflow", registry)
                    .isInstanceOf(TaskStoreException.class);
            assertThat(register(fresh, registry, "first")).isEqualTo(1);
            assertThat(register(fresh, registry, "last")).isEqualTo(bound);
            Map<Integer, String> names = registry == Registry.QUEUE ? fresh.queueNames() : fresh.typeNames();
            assertThat(names).containsEntry(1, "first").containsEntry(bound, "last");
            assertThat(names.values()).doesNotContain("one-too-many");
        }
    }

    @Test
    void argumentsThatBreakAnSpiRuleAreRefused() {
        Map<Integer, String> queues = store.queueNames();
        Map<Integer, String> types = store.typeNames();
        for (String bad : new String[] {null, "", " "}) {
            assertThatThrownBy(() -> store.queueId(bad))
                    .as("queue name '%s'", bad)
                    .isInstanceOf(TaskValidationException.class);
            assertThatThrownBy(() -> store.typeId(bad))
                    .as("type name '%s'", bad)
                    .isInstanceOf(TaskValidationException.class);
        }
        assertThat(store.queueNames()).isEqualTo(queues);
        assertThat(store.typeNames()).isEqualTo(types);

        assertThatThrownBy(() -> store.allocateIds(0)).isInstanceOf(TaskValidationException.class);
        int runners = store.runners().size();
        assertThatThrownBy(() -> store.allocateRunnerId(null, Set.of(queue), Set.of(type)))
                .isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> store.heartbeat(runner, null, Set.of(queue), Set.of(type), 0, true))
                .isInstanceOf(TaskValidationException.class);
        assertThat(store.runners()).hasSize(runners);

        TaskRecord t = leased(task());
        assertThatThrownBy(() -> store.reschedule(t.id(), t.revision(), null))
                .isInstanceOf(TaskValidationException.class);
        assertThat(get(t.id())).isEqualTo(t);
    }

    @Test
    void aLimitBelowOneIsRefusedByEveryCallThatTakesOneAndChangesNothing() {
        TaskRecord done = leased(task());
        assertThat(store.ack(done.id(), done.revision(), true, false, null)).isTrue();
        long overdue = put(task().startBy(now().plus(SHORT)));
        Instant fire = millis(now());
        store.putSchedule(definition("due", "* * * * *"), fire, fire);
        advance(PAST_SHORT);
        StateCounts before = counts();

        for (int limit : new int[] {0, -1, Integer.MIN_VALUE}) {
            Map<String, ThrowingCallable> calls = new LinkedHashMap<>();
            calls.put("promote", () -> store.promote(limit));
            calls.put("reclaim", () -> store.reclaim(limit));
            calls.put("expire", () -> store.expire(limit));
            calls.put("purge", () -> store.purge(TaskState.SUCCEEDED, Duration.ZERO, limit));
            calls.put("releaseKeys", () -> store.releaseKeys(limit));
            calls.put("dueSchedules", () -> store.dueSchedules(fire, limit));
            calls.put("tasks", () -> store.tasks(TaskState.QUEUED, null, (String) null, limit));
            calls.forEach((name, call) ->
                    assertThatThrownBy(call).as("%s(%d)", name, limit).isInstanceOf(TaskValidationException.class));
        }

        assertThat(counts()).isEqualTo(before);
        assertThat(store.dueSchedules(fire, 10)).hasSize(1);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(overdue);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isEqualTo(1);
    }

    @Test
    void expireReportsTheQueueAndTypeOfEachTaskItMoves() {
        int otherQueue = store.queueId("expire-queue");
        int otherType = store.typeId("expire-type");
        Instant startBy = now().plus(SHORT);
        long queued = put(task().startBy(startBy));
        long scheduled = put(task(otherQueue, otherType)
                .dueAt(now().plus(Duration.ofHours(1)))
                .startBy(startBy));
        assumeBefore(startBy);
        advance(PAST_SHORT);

        assertThat(store.expire(10))
                .containsExactlyInAnyOrder(
                        new ExpiredTask(queued, queue, type), new ExpiredTask(scheduled, otherQueue, otherType));
    }

    @Test
    void expireHandsBackTheQueueAndTypeOfATaskThatIsPurgedOrDeletedRightAfter() {
        int otherQueue = store.queueId("expire-queue");
        int otherType = store.typeId("expire-type");
        Instant startBy = now().plus(SHORT);
        long purged = put(task().startBy(startBy));
        long deleted = put(task(otherQueue, otherType).startBy(startBy));
        assumeBefore(startBy);
        advance(PAST_SHORT);

        List<ExpiredTask> expired = store.expire(10);
        assertThat(store.delete(deleted, get(deleted).revision(), null)).isEqualTo(DeleteOutcome.DELETED);
        assertThat(store.purge(TaskState.EXPIRED, Duration.ZERO, 10)).isEqualTo(1);

        assertThat(store.task(purged)).isEmpty();
        assertThat(store.task(deleted)).isEmpty();
        assertThat(expired)
                .containsExactlyInAnyOrder(
                        new ExpiredTask(purged, queue, type), new ExpiredTask(deleted, otherQueue, otherType));
    }

    @Test
    void pausingAQueueIdThatWasNeverRegisteredChangesNothing() {
        List<QueueInfo> before = store.queues();
        int unknown = before.stream().mapToInt(QueueInfo::id).max().orElse(0) + 1;
        store.pauseQueue(unknown, true);
        assertThat(store.queues()).isEqualTo(before);

        int registered = store.queueId("registered-after-a-pause");
        assertThat(store.queues())
                .filteredOn(q -> q.id() == registered)
                .singleElement()
                .satisfies(q -> assertThat(q.paused())
                        .as("queue %d, registered after id %d was paused", registered, unknown)
                        .isFalse());
    }

    // enqueue

    @Test
    void enqueueStoresAnImmediateTaskAsQueuedWithStoreTimes() {
        Instant before = now();
        long id = put(task().dueAt(null));
        Instant after = now();
        TaskRecord t = get(id);
        assertThat(t.state()).isEqualTo(TaskState.QUEUED);
        assertThat(t.dueAt()).isBetween(millis(before), after.plusMillis(1));
        assertThat(t.stateChangedAt()).isBetween(millis(before), after.plusMillis(1));
        assertThat(t.attempts()).isZero();
        assertThat(t.queueId()).isEqualTo(queue);
        assertThat(t.typeId()).isEqualTo(type);
        assertThat(t.encodedPayload()).isEqualTo("payload");
    }

    @Test
    void enqueueWithFutureDueIsScheduledAndKeepsDue() {
        Instant due = millis(now().plus(Duration.ofHours(1)));
        long id = put(task().dueAt(due));
        TaskRecord t = get(id);
        assertThat(t.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(t.dueAt()).isEqualTo(due);
    }

    @Test
    void enqueueRoundTripsOptionalFields() {
        Map<String, String> headers = Map.of("k", "v");
        long id = put(task().account("acme")
                .headers(headers)
                .scheduleId("nightly")
                .retryPolicy("slow")
                .retryOf(7L)
                .payloadVersion(4)
                .maxRunTime(Duration.ofSeconds(9)));
        TaskRecord t = get(id);
        assertThat(t.account()).isEqualTo("acme");
        assertThat(t.headers()).isEqualTo(headers);
        assertThat(t.scheduleId()).isEqualTo("nightly");
        assertThat(t.retryPolicy()).isEqualTo("slow");
        assertThat(t.retryOf()).isEqualTo(7L);
        assertThat(t.payloadVersion()).isEqualTo(4);
        assertThat(t.maxRunTime()).isEqualTo(Duration.ofSeconds(9));
        assertThat(t.maxAttempts()).isEqualTo(3);
    }

    /** Fields a producer sets that no transition changes, except that {@link AdminStore#retry} clears startBy. */
    private static final Map<String, Function<TaskRecord, Object>> CARRIED = carriedFields();

    private static Map<String, Function<TaskRecord, Object>> carriedFields() {
        Map<String, Function<TaskRecord, Object>> fields = new LinkedHashMap<>();
        fields.put("id", TaskRecord::id);
        fields.put("queueId", TaskRecord::queueId);
        fields.put("typeId", TaskRecord::typeId);
        fields.put("payloadVersion", TaskRecord::payloadVersion);
        fields.put("maxAttempts", TaskRecord::maxAttempts);
        fields.put("createdAt", TaskRecord::createdAt);
        fields.put("account", TaskRecord::account);
        fields.put("headers", TaskRecord::headers);
        fields.put("keyKeep", TaskRecord::keyKeep);
        fields.put("retryPolicy", TaskRecord::retryPolicy);
        fields.put("scheduleId", TaskRecord::scheduleId);
        fields.put("retryOf", TaskRecord::retryOf);
        fields.put("startBy", TaskRecord::startBy);
        fields.put("maxRunTime", TaskRecord::maxRunTime);
        return Collections.unmodifiableMap(fields);
    }

    /** A task with every field a producer may set, each to a value that no default and no other field has. */
    private TaskRecord.Builder everyField(String key) {
        return task().revision(424_242)
                .payloadVersion(7)
                .maxAttempts(9)
                .createdAt(millis(now().minusSeconds(17)))
                .encodedPayload("{\"every\":\"field\"}")
                .account("account-é")
                .headers(Map.of("h1", "v1", "h\"2", "v\"2"))
                .uniqueKey(key)
                .keyKeep(Duration.ofMinutes(13))
                .retryPolicy("policy-x")
                .scheduleId("every")
                .retryOf(31L)
                .startBy(millis(now().plus(Duration.ofHours(5))))
                .maxRunTime(Duration.ofSeconds(41));
    }

    private static void assertCarried(TaskRecord expected, TaskRecord actual, String step) {
        CARRIED.forEach((name, field) ->
                assertThat(field.apply(actual)).as("%s after %s", name, step).isEqualTo(field.apply(expected)));
    }

    /** A new task as stored: the producer's fields as given, nothing from a run. */
    private void assertStoredAsGiven(TaskRecord given, TaskState state, String path) {
        TaskRecord stored = get(given.id());
        assertCarried(given, stored, path);
        assertThat(stored.revision()).as("revision after %s", path).isEqualTo(given.revision());
        assertThat(stored.state()).as("state after %s", path).isEqualTo(state);
        assertThat(stored.dueAt()).as("due after %s", path).isEqualTo(given.dueAt());
        assertThat(stored.encodedPayload()).as("payload after %s", path).isEqualTo(given.encodedPayload());
        assertThat(stored.uniqueKey()).as("unique key after %s", path).isEqualTo(given.uniqueKey());
        assertThat(stored.attempts()).as("attempts after %s", path).isZero();
        assertThat(stored.stateChangedAt())
                .as("state change time after %s", path)
                .isNotNull();
        assertThat(Stream.of(
                        stored.runnerId(),
                        stored.leaseUntil(),
                        stored.claimedAt(),
                        stored.keyUntil(),
                        stored.cancelRequestedAt(),
                        stored.cancelReason(),
                        stored.progress(),
                        stored.steps(),
                        stored.failures()))
                .as("run fields after %s", path)
                .containsOnlyNulls();
    }

    /** A definition due at {@code fire} and an occurrence for it that the builder may still change. */
    private TaskRecord.Builder occurrenceOfNewSchedule(String scheduleId, Instant fire) {
        store.putSchedule(definition(scheduleId, "* * * * *"), fire, fire);
        return task().dueAt(fire).state(TaskState.SCHEDULED);
    }

    @Test
    void aNewTaskWithoutACreatedTimeIsCreatedWhenItIsStoredOnEachWayIn() {
        Instant fire = millis(now().plusSeconds(1));
        TaskRecord single = task().createdAt(null).build();
        TaskRecord batched = task().createdAt(null)
                .dueAt(millis(now().plus(Duration.ofHours(1))))
                .build();
        TaskRecord occurrence =
                occurrenceOfNewSchedule("nightly", fire).createdAt(null).build();

        assertThat(store.enqueue(single)).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.enqueueAll(List.of(batched))).containsExactly(EnqueueOutcome.STORED);
        assertThat(store.commitSchedule(dueOf("nightly", fire), fire.plusSeconds(60), occurrence)
                        .isCommitted())
                .isTrue();

        for (TaskRecord given : List.of(single, batched, occurrence)) {
            TaskRecord stored = get(given.id());
            assertThat(stored.createdAt())
                    .as("created time of task %s", given.id())
                    .isEqualTo(stored.stateChangedAt());
        }
    }

    @Test
    void aKeyKeepWithoutAUniqueKeyIsDroppedOnEachWayIn() {
        Instant fire = millis(now().plusSeconds(1));
        Duration keep = Duration.ofMinutes(5);
        TaskRecord single = task().keyKeep(keep).build();
        TaskRecord batched = task().keyKeep(keep).build();
        TaskRecord occurrence =
                occurrenceOfNewSchedule("nightly", fire).keyKeep(keep).build();

        assertThat(store.enqueue(single)).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.enqueueAll(List.of(batched))).containsExactly(EnqueueOutcome.STORED);
        assertThat(store.commitSchedule(dueOf("nightly", fire), fire.plusSeconds(60), occurrence)
                        .isCommitted())
                .isTrue();

        for (TaskRecord given : List.of(single, batched, occurrence)) {
            assertThat(get(given.id()).keyKeep())
                    .as("key keep of task %s", given.id())
                    .isNull();
        }
    }

    @Test
    void anOccurrenceThatNamesNoScheduleBelongsToTheOneThatFiredIt() {
        Instant fire = millis(now().plusSeconds(1));
        TaskRecord unnamed = occurrenceOfNewSchedule("nightly", fire).build();
        TaskRecord named =
                occurrenceOfNewSchedule("weekly", fire).scheduleId("another").build();

        assertThat(store.commitSchedule(dueOf("nightly", fire), fire.plusSeconds(60), unnamed)
                        .isCommitted())
                .isTrue();
        assertThat(store.commitSchedule(dueOf("weekly", fire), fire.plusSeconds(60), named)
                        .isCommitted())
                .isTrue();

        assertThat(get(unnamed.id()).scheduleId()).isEqualTo("nightly");
        assertThat(get(named.id()).scheduleId()).isEqualTo("another");
    }

    @Test
    void anEmptyUniqueKeyIsRefusedOnEachWayInAndNothingIsStored() {
        Instant fire = millis(now().plusSeconds(1));
        TaskRecord single = task().uniqueKey("").build();
        TaskRecord sound = task().build();
        TaskRecord batched = task().uniqueKey("").build();
        TaskRecord occurrence =
                occurrenceOfNewSchedule("nightly", fire).uniqueKey("").build();
        DueSchedule due = dueOf("nightly", fire);

        assertThatThrownBy(() -> store.enqueue(single)).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> store.enqueueAll(List.of(sound, batched))).isInstanceOf(TaskValidationException.class);
        assertThatThrownBy(() -> store.commitSchedule(due, fire.plusSeconds(60), occurrence)
                        .isCommitted())
                .isInstanceOf(TaskValidationException.class);

        for (TaskRecord given : List.of(single, sound, batched, occurrence)) {
            assertThat(store.task(given.id())).as("task %s", given.id()).isEmpty();
        }
        assertThat(nextFireOf("nightly")).isEqualTo(fire);
    }

    @Test
    void everyFieldOfANewTaskIsStoredAsGivenOnEachWayIn() {
        Instant later = millis(now().plus(Duration.ofHours(1)));
        Instant past = millis(now().minusSeconds(1));
        TaskRecord scheduled = everyField("one").dueAt(later).build();
        TaskRecord queued = everyField("two").dueAt(past).revision(515_151).build();
        assertThat(store.enqueue(scheduled)).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.enqueue(queued)).isEqualTo(EnqueueOutcome.STORED);
        assertStoredAsGiven(scheduled, TaskState.SCHEDULED, "enqueue");
        assertStoredAsGiven(queued, TaskState.QUEUED, "enqueue");

        TaskRecord first = everyField("three").dueAt(later).revision(626_262).build();
        TaskRecord second = everyField("four").dueAt(past).revision(737_373).build();
        assertThat(store.enqueueAll(List.of(first, second)))
                .containsExactly(EnqueueOutcome.STORED, EnqueueOutcome.STORED);
        assertStoredAsGiven(first, TaskState.SCHEDULED, "enqueueAll");
        assertStoredAsGiven(second, TaskState.QUEUED, "enqueueAll");

        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("every", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        TaskRecord occurrence = everyField("five")
                .dueAt(fire)
                .state(TaskState.SCHEDULED)
                .revision(848_484)
                .build();
        assertThat(store.commitSchedule(due, millis(now().plusSeconds(3600)), occurrence)
                        .isCommitted())
                .isTrue();
        assertStoredAsGiven(occurrence, TaskState.SCHEDULED, "commitSchedule");
    }

    /** The task as stored after {@code step}: carried fields, unique key and payload as given, at {@code revision}. */
    private TaskRecord walked(TaskRecord expected, String step, int revision) {
        TaskRecord t = get(expected.id());
        assertCarried(expected, t, step);
        assertThat(t.revision()).as("revision after %s", step).isEqualTo(revision);
        assertThat(t.uniqueKey()).as("unique key after %s", step).isEqualTo(expected.uniqueKey());
        assertThat(t.encodedPayload()).as("payload after %s", step).isEqualTo(expected.encodedPayload());
        return t;
    }

    /**
     * One task through every transition a store implements. The key keep window outlasts the test and every
     * finish keeps the payload, so only the revision, as the SPI counts it, and the run fields may change.
     */
    @Test
    void theFieldsAProducerSetSurviveEveryTransition() {
        TaskRecord given = everyField("walk").build();
        long id = given.id();
        assertThat(store.enqueue(given)).isEqualTo(EnqueueOutcome.STORED);

        assertThat(claimOne().id()).isEqualTo(id);
        TaskRecord t = walked(given, "claim", given.revision() + 1);
        assertThat(store.renew(List.of(new LeaseRef(id, t.revision())), LEASE))
                .singleElement()
                .satisfies(s -> assertThat(s.owned()).isTrue());
        walked(given, "renew", t.revision());
        assertThat(store.saveSteps(id, t.revision(), Map.of("first", new StepResult(millis(now()), "ok"))))
                .isTrue();
        walked(given, "saveSteps", t.revision());
        assertThat(store.progress(id, t.revision(), "half")).isTrue();
        walked(given, "progress", t.revision());

        assertThat(store.fail(id, t.revision(), failure(1), millis(now().minusSeconds(1)), true, null))
                .isEqualTo(FailOutcome.RETRY);
        t = walked(given, "fail with a retry", t.revision() + 1);
        assertThat(store.promote(10).moved()).isEqualTo(1);
        t = walked(given, "promote", t.revision() + 1);

        assertThat(claimOne().id()).isEqualTo(id);
        t = walked(given, "second claim", t.revision() + 1);
        assertThat(store.reschedule(id, t.revision(), millis(now().minusSeconds(1))))
                .isTrue();
        t = walked(given, "reschedule", t.revision() + 1);
        assertThat(store.promote(10).moved()).isEqualTo(1);
        t = walked(given, "second promote", t.revision() + 1);

        assertThat(claimOne().id()).isEqualTo(id);
        t = walked(given, "third claim", t.revision() + 1);
        assertThat(store.unclaim(id, t.revision())).isTrue();
        t = walked(given, "unclaim", t.revision() + 1);

        assertThat(claimOne().id()).isEqualTo(id);
        t = walked(given, "fourth claim", t.revision() + 1);
        assertThat(store.cancel(id, t.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
        walked(given, "cancel request", t.revision());
        assertThat(store.finishCancelled(id, t.revision(), "stop", true, null)).isTrue();
        t = walked(given, "finishCancelled", t.revision() + 1);

        TaskRecord retried = given.toBuilder().startBy(null).build();
        assertThat(store.retry(id, t.revision())).isEqualTo(RetryOutcome.RETRIED);
        t = walked(retried, "retry", t.revision() + 1);

        assertThat(claim(1, SHORT, null)).extracting(TaskRecord::id).containsExactly(id);
        t = walked(retried, "short claim", t.revision() + 1);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(id);
        walked(retried, "reclaim", t.revision());
        assertThat(store.failExpired(id, t.revision(), failure(2), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        t = walked(retried, "failExpired", t.revision() + 1);

        assertThat(store.retry(id, t.revision())).isEqualTo(RetryOutcome.RETRIED);
        t = walked(retried, "second retry", t.revision() + 1);
        assertThat(claimOne().id()).isEqualTo(id);
        t = walked(retried, "fifth claim", t.revision() + 1);
        assertThat(store.ack(id, t.revision(), true, true, null)).isTrue();
        t = walked(retried, "ack", t.revision() + 1);
        assertThat(t.state()).isEqualTo(TaskState.SUCCEEDED);
    }

    /** Every run field set, as a copy of a claimed, failed and progressing task carries them. */
    private TaskRecord.Builder withRunFields(TaskRecord.Builder builder) {
        Instant at = millis(now().minusSeconds(5));
        return builder.state(TaskState.LEASED)
                .attempts(3)
                .runnerId(runner)
                .leaseUntil(millis(now().plusSeconds(60)))
                .claimedAt(at)
                .keyUntil(millis(now().plusSeconds(90)))
                .cancelRequestedAt(at)
                .cancelReason("copied")
                .progress("copied")
                .steps(Map.of("s", new StepResult(at, "r")))
                .failures(List.of(failure(2)));
    }

    @Test
    void aNewTaskStartsWithoutRunFieldsOnEveryWayIn() {
        Instant later = millis(now().plus(Duration.ofHours(1)));
        Instant past = millis(now().minusSeconds(1));
        TaskRecord scheduled = withRunFields(everyField("one").dueAt(later)).build();
        TaskRecord queued = withRunFields(everyField("two").dueAt(past)).build();
        assertThat(store.enqueue(scheduled)).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.enqueue(queued)).isEqualTo(EnqueueOutcome.STORED);
        assertStoredAsGiven(scheduled, TaskState.SCHEDULED, "enqueue");
        assertStoredAsGiven(queued, TaskState.QUEUED, "enqueue");

        TaskRecord first = withRunFields(everyField("three").dueAt(later)).build();
        TaskRecord second = withRunFields(everyField("four").dueAt(past)).build();
        assertThat(store.enqueueAll(List.of(first, second)))
                .containsExactly(EnqueueOutcome.STORED, EnqueueOutcome.STORED);
        assertStoredAsGiven(first, TaskState.SCHEDULED, "enqueueAll");
        assertStoredAsGiven(second, TaskState.QUEUED, "enqueueAll");

        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("every", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        TaskRecord occurrence = withRunFields(everyField("five").dueAt(fire)).build();
        assertThat(store.commitSchedule(due, millis(now().plusSeconds(3600)), occurrence)
                        .isCommitted())
                .isTrue();
        assertStoredAsGiven(occurrence, TaskState.SCHEDULED, "commitSchedule");
    }

    @Test
    void aTaskThatWentBackToWaitingNamesNoRunnerOnceItEnds() {
        TaskRecord retried = leased(task());
        assertThat(store.fail(retried.id(), retried.revision(), failure(1), now().plusSeconds(60), true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(retried.id()).runnerId())
                .as("waiting after fail with a retry")
                .isNull();
        assertThat(store.cancel(retried.id(), retried.revision() + 1, "stop", null))
                .isEqualTo(CancelOutcome.CANCELLED);
        assertThat(get(retried.id()).runnerId())
                .as("cancelled after fail with a retry")
                .isNull();

        TaskRecord rescheduled = leased(task());
        assertThat(store.reschedule(rescheduled.id(), rescheduled.revision(), now().plusSeconds(60)))
                .isTrue();
        assertThat(store.cancel(rescheduled.id(), rescheduled.revision() + 1, "stop", null))
                .isEqualTo(CancelOutcome.CANCELLED);
        assertThat(get(rescheduled.id()).runnerId())
                .as("cancelled after reschedule")
                .isNull();

        TaskRecord unclaimed = leased(task());
        assertThat(store.unclaim(unclaimed.id(), unclaimed.revision())).isTrue();
        assertThat(store.cancel(unclaimed.id(), unclaimed.revision() + 1, "stop", null))
                .isEqualTo(CancelOutcome.CANCELLED);
        assertThat(get(unclaimed.id()).runnerId()).as("cancelled after unclaim").isNull();

        Instant startBy = now().plus(SHORT);
        TaskRecord expiring = leased(task().startBy(startBy));
        assumeBefore(startBy);
        assertThat(store.fail(expiring.id(), expiring.revision(), failure(1), now().plusSeconds(60), true, null))
                .isEqualTo(FailOutcome.RETRY);
        advance(PAST_SHORT);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(expiring.id());
        assertThat(get(expiring.id()).runnerId())
                .as("expired after fail with a retry")
                .isNull();
    }

    @Test
    void aListingOfTasksDueAfterTheYear2262EndsItsWalk() {
        Instant far = Instant.parse("3000-01-01T00:00:00Z");
        List<Long> ids = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            ids.add(put(task().dueAt(far.plusSeconds(i))));
        }
        List<Long> seen = new ArrayList<>();
        String cursor = null;
        for (int page = 0; page < 5; page++) {
            TaskPage p = store.tasks(TaskState.SCHEDULED, queue, cursor, 2);
            p.items().forEach(t -> seen.add(t.id()));
            cursor = p.next();
            if (cursor == null) break;
        }
        assertThat(seen).containsExactlyElementsOf(ids);
        assertThat(cursor).isNull();
    }

    @Test
    void purgeRefusesAStateThatIsNotTerminal() {
        for (TaskState state : List.of(TaskState.LEASED, TaskState.SCHEDULED, TaskState.QUEUED)) {
            long id = inState(state, task());
            assertThatThrownBy(() -> store.purge(state, Duration.ZERO, 10))
                    .as("purge of %s", state)
                    .isInstanceOf(TaskValidationException.class);
            assertThat(store.task(id)).as("task in %s", state).isPresent();
        }
    }

    @Test
    void anEmptyTextIsStoredAsGivenNotAsNull() {
        long labelled = put(task().account("").headers(Map.of("h", "")));
        assertThat(get(labelled).account()).as("account").isEmpty();
        assertThat(get(labelled).headers()).as("headers").containsExactly(Map.entry("h", ""));

        TaskRecord t = leased(task());
        assertThat(store.progress(t.id(), t.revision(), "")).isTrue();
        assertThat(get(t.id()).progress()).as("progress").isEmpty();
        assertThat(store.cancel(t.id(), t.revision(), "", null)).isEqualTo(CancelOutcome.REQUESTED);
        assertThat(get(t.id()).cancelReason()).as("reason of a cancel request").isEmpty();
        assertThat(store.finishCancelled(t.id(), t.revision(), "", true, null)).isTrue();
        assertThat(get(t.id()).cancelReason()).as("reason of finishCancelled").isEmpty();

        long waiting = putNew();
        assertThat(store.cancel(waiting, get(waiting).revision(), "", null)).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(get(waiting).cancelReason()).as("reason of a cancel").isEmpty();
    }

    @Test
    void aClaimReturnsTheTaskAsStored() {
        long id = put(everyField("claimed"));
        assertThat(claimOne()).as("first claim").isEqualTo(get(id));
        TaskRecord t = get(id);
        assertThat(store.saveSteps(id, t.revision(), Map.of("s", new StepResult(millis(now()), "r"))))
                .isTrue();
        assertThat(store.progress(id, t.revision(), "p")).isTrue();
        assertThat(store.fail(id, t.revision(), failure(1), millis(now().minusSeconds(1)), true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(store.promote(10).moved()).isEqualTo(1);
        assertThat(claimOne()).as("claim after a failure").isEqualTo(get(id));
    }

    @Test
    void theLongestKeepWindowHoldsTheKeyOnEveryWayATaskEnds() {
        Duration longest = Uniqueness.MAX_KEEP_AFTER_DONE;
        TaskRecord acked = leased(task().uniqueKey("acked").keyKeep(longest));
        assertThat(store.ack(acked.id(), acked.revision(), true, true, null)).isTrue();
        TaskRecord dead = leased(task().uniqueKey("dead").keyKeep(longest));
        assertThat(store.fail(dead.id(), dead.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        TaskRecord cancelled = leased(task().uniqueKey("cancelled").keyKeep(longest));
        assertThat(store.finishCancelled(cancelled.id(), cancelled.revision(), "stop", true, null))
                .isTrue();
        long waiting = put(task().uniqueKey("waiting").keyKeep(longest));
        assertThat(store.cancel(waiting, get(waiting).revision(), "stop", null)).isEqualTo(CancelOutcome.CANCELLED);
        TaskRecord removed = leased(task().uniqueKey("removed").keyKeep(longest));
        long expired = put(task().uniqueKey("expired").keyKeep(longest).startBy(now().plus(SHORT)));
        advance(PAST_SHORT);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(expired);
        assertThat(store.fail(removed.id(), removed.revision(), failure(1), null, false, null))
                .isEqualTo(FailOutcome.DEAD);

        Map<String, Long> holders = new LinkedHashMap<>();
        holders.put("acked", acked.id());
        holders.put("dead", dead.id());
        holders.put("cancelled", cancelled.id());
        holders.put("waiting", waiting);
        holders.put("expired", expired);
        holders.put("removed", removed.id());
        holders.forEach((key, id) -> {
            assertThat(store.enqueue(task().uniqueKey(key).build()))
                    .as("same-key enqueue after %s", key)
                    .isEqualTo(new EnqueueOutcome.DuplicateKey(id));
            store.task(id)
                    .ifPresent(t -> assertThat(t.keyUntil())
                            .as("key window of %s", key)
                            .isEqualTo(t.stateChangedAt().plus(longest)));
        });
        assertThat(store.task(removed.id())).isEmpty();
    }

    @Test
    void timesAsFarFromNowAsTheLibraryAllowsAreKept() {
        Duration longest = TaskRequest.MAX_TIME_FROM_NOW;
        Instant ahead = millis(now().plus(longest));
        Instant back = millis(now().minus(longest));

        long scheduled = put(task().state(TaskState.SCHEDULED).dueAt(ahead).startBy(ahead));
        assertThat(get(scheduled).dueAt()).isEqualTo(ahead);
        assertThat(get(scheduled).startBy()).isEqualTo(ahead);
        assertThat(store.promote(10).moved()).isZero();
        assertThat(store.expire(10)).isEmpty();
        assertThat(store.promote(10).nextDue()).isEqualTo(ahead);

        long overdue = put(task().dueAt(back));
        assertThat(get(overdue).dueAt()).isEqualTo(back);
        TaskRecord claimed = claimOne();
        assertThat(claimed.id()).isEqualTo(overdue);
        assertThat(store.fail(claimed.id(), claimed.revision(), failure(1), ahead, true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(overdue).dueAt()).isEqualTo(ahead);

        put(task());
        TaskRecord rescheduled = claimOne();
        assertThat(store.reschedule(rescheduled.id(), rescheduled.revision(), ahead))
                .isTrue();
        assertThat(get(rescheduled.id()).dueAt()).isEqualTo(ahead);

        put(task());
        Instant before = now();
        TaskRecord held = claim(1, longest, null).get(0);
        assertThat(held.leaseUntil())
                .isBetween(
                        before.plus(longest).minusSeconds(5),
                        now().plus(longest).plusSeconds(5));
        assertThat(store.renew(List.of(new LeaseRef(held.id(), held.revision())), longest))
                .containsExactly(new LeaseStatus(held.id(), true, false));
        assertThat(store.reclaim(10)).isEmpty();
        assertThat(store.ack(held.id(), held.revision(), true, true, null)).isTrue();

        assertThat(store.purge(TaskState.SUCCEEDED, longest, 10)).isZero();
        assertThat(store.task(held.id())).isPresent();
        assertThat(store.purgeRunners(longest)).isZero();
        assertThat(store.runners()).extracting(RunnerRecord::id).contains(runner);
    }

    @Test
    void aTaskDueWithinTheCurrentMillisecondIsQueued() {
        long id = put(task().dueAt(millis(now()).plusNanos(456_789)));

        assertThat(get(id).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void aPurgeRetentionIsCountedInWholeMilliseconds() {
        TaskRecord done = leased(task());
        assertThat(store.ack(done.id(), done.revision(), true, true, null)).isTrue();
        advance(Duration.ofMillis(1));

        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(1).plusNanos(456_789), 10))
                .isEqualTo(1);
    }

    @Test
    void aListingOfQueuedTasksInEveryQueueMergesTheQueuesInDueOrderPageByPage() {
        int[] queues = {queue, store.queueId("second"), store.queueId("third")};
        Instant past = millis(now().minusSeconds(60));
        var expected = new ArrayList<Long>();
        for (int i = 0; i < 9; i++) {
            expected.add(put(task(queues[i % 3], type).dueAt(past.plusMillis(i))));
        }

        var seen = new ArrayList<Long>();
        String cursor = null;
        for (int page = 0; page < 10; page++) {
            TaskPage got = store.tasks(TaskState.QUEUED, null, cursor, 2);
            assertThat(got.items()).as("page %d", page).hasSizeLessThanOrEqualTo(2);
            got.items().forEach(t -> seen.add(t.id()));
            cursor = got.next();
            if (cursor == null) break;
        }
        assertThat(cursor).isNull();
        assertThat(seen).containsExactlyElementsOf(expected);
    }

    @Test
    void timesAndDurationsAreKeptInWholeMilliseconds() {
        Instant odd = millis(now()).plusNanos(456_789);
        Duration oddWindow = Duration.ofMinutes(10).plusNanos(456_789);

        long waiting = put(task().createdAt(odd)
                .dueAt(odd.plusSeconds(3600))
                .startBy(odd.plusSeconds(7200))
                .uniqueKey("ms-waiting")
                .keyKeep(oddWindow)
                .maxRunTime(oddWindow));
        TaskRecord scheduled = get(waiting);
        assertWholeMillis(scheduled, "enqueue");
        assertThat(scheduled.createdAt()).isEqualTo(millis(odd));
        assertThat(scheduled.dueAt()).isEqualTo(millis(odd.plusSeconds(3600)));
        assertThat(scheduled.keyKeep()).isEqualTo(Duration.ofMinutes(10));
        assertThat(scheduled.maxRunTime()).isEqualTo(Duration.ofMinutes(10));

        put(task().createdAt(odd)
                .dueAt(odd.minusSeconds(60))
                .uniqueKey("ms-done")
                .keyKeep(oddWindow));
        TaskRecord held = claim(1, LEASE.plusNanos(456_789), null).get(0);
        assertWholeMillis(held, "claim");
        assertWholeMillis(get(held.id()), "claim, read back");
        assertThat(store.cancel(held.id(), held.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
        assertWholeMillis(get(held.id()), "cancel request");
        assertThat(store.ack(held.id(), held.revision(), true, true, null)).isTrue();
        assertWholeMillis(get(held.id()), "ack");

        put(task().dueAt(odd.minusSeconds(60)));
        TaskRecord failing = claimOne();
        assertThat(store.fail(failing.id(), failing.revision(), failure(1), odd.plusSeconds(300), true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertWholeMillis(get(failing.id()), "fail with a retry");
        assertThat(get(failing.id()).dueAt()).isEqualTo(millis(odd.plusSeconds(300)));

        put(task().dueAt(odd.minusSeconds(60)));
        TaskRecord later = claimOne();
        assertThat(store.reschedule(later.id(), later.revision(), odd.plusSeconds(600)))
                .isTrue();
        assertWholeMillis(get(later.id()), "reschedule");
        assertThat(get(later.id()).dueAt()).isEqualTo(millis(odd.plusSeconds(600)));

        assertThat(store.putSchedule(definition("ms-every", "* * * * *"), odd, odd)
                        .nextFireAt())
                .isEqualTo(millis(odd));
        assertThat(store.nextFireTimes()).containsEntry("ms-every", millis(odd));
        DueSchedule due = store.dueSchedules(odd, 10).get(0);
        assertThat(due.fireAt()).isEqualTo(millis(odd));
        assertThat(store.setNextFire("ms-every", due.definition().revision(), odd, odd.plusSeconds(60)))
                .as("the expected fire time is compared to the millisecond")
                .isTrue();
        assertThat(store.nextFireTimes()).containsEntry("ms-every", millis(odd.plusSeconds(60)));

        store.heartbeat(runner, "contract-test", Set.of(queue), Set.of(type), 0, true);
        assertThat(store.runners())
                .filteredOn(w -> w.id() == runner)
                .singleElement()
                .satisfies(w -> {
                    assertThat(w.startedAt().getNano() % 1_000_000)
                            .as("runner startedAt")
                            .isZero();
                    assertThat(w.lastSeenAt().getNano() % 1_000_000)
                            .as("runner lastSeen")
                            .isZero();
                });
    }

    private static void assertWholeMillis(TaskRecord t, String step) {
        Instant[] times = {
            t.createdAt(),
            t.dueAt(),
            t.stateChangedAt(),
            t.leaseUntil(),
            t.claimedAt(),
            t.keyUntil(),
            t.startBy(),
            t.cancelRequestedAt()
        };
        String[] names = {
            "createdAt",
            "dueAt",
            "stateChangedAt",
            "leaseUntil",
            "claimedAt",
            "keyUntil",
            "startBy",
            "cancelRequestedAt"
        };
        for (int i = 0; i < times.length; i++) {
            if (times[i] != null) {
                assertThat(times[i].getNano() % 1_000_000)
                        .as("%s after %s", names[i], step)
                        .isZero();
            }
        }
    }

    // sibling paths, terminal bookkeeping, what a transition leaves behind, instants the caller supplies

    private static final Duration ENDING_KEY_WINDOW = Duration.ofHours(1);
    private static final Duration[] ENDING_KEY_WINDOWS = {null, ENDING_KEY_WINDOW};

    /** The instants the tasks of one comparison share, so that two of them differ in nothing but id and key. */
    private record EndingTimes(
            Instant created, Instant due, Instant startBy, Instant stepAt, Instant failedAt, Instant retryAt) {}

    /** Two tasks made alike, to be ended by two sibling paths and compared. */
    private record EndingTwins(String name, Object kind, Duration window, TaskRecord first, TaskRecord second) {}

    private EndingTimes endingTimes() {
        Instant base = millis(now());
        return new EndingTimes(
                base.minusSeconds(120),
                base.minusSeconds(60),
                base.plus(Duration.ofHours(5)),
                base.minus(Duration.ofHours(2)),
                base.minus(Duration.ofHours(3)),
                base.plus(Duration.ofHours(2)));
    }

    private TaskRecord.Builder twinBuilder(String key, Duration window, EndingTimes times) {
        return everyField(key)
                .createdAt(times.created())
                .dueAt(times.due())
                .startBy(times.startBy())
                .keyKeep(window);
    }

    /** Stores and claims the task, saves two steps and a progress text, and reads it back as the runner left it. */
    private TaskRecord workedTask(
            TaskRecord.Builder builder, Duration lease, EndingTimes times, boolean cancelRequested) {
        long id = put(builder);
        TaskRecord claimed = claim(1, lease, null).get(0);
        assertThat(claimed.id()).isEqualTo(id);
        Map<String, StepResult> steps = Map.of(
                "one", new StepResult(times.stepAt(), "{\"a\":1}"),
                "two", new StepResult(times.stepAt().plusSeconds(1), null));
        assertThat(store.saveSteps(id, claimed.revision(), steps)).isTrue();
        assertThat(store.progress(id, claimed.revision(), "halfway")).isTrue();
        if (cancelRequested) {
            assertThat(store.cancel(id, claimed.revision(), "asked", null)).isEqualTo(CancelOutcome.REQUESTED);
        }
        return get(id);
    }

    /** The task as it must be after an ending: as it was, in {@code state}, one revision on, without a lease. */
    private static TaskRecord.Builder endedFrom(TaskRecord before, TaskState state) {
        return before.toBuilder()
                .state(state)
                .revision(before.revision() + 1)
                .leaseUntil(null)
                .uniqueKey(before.keyKeep() == null ? null : before.uniqueKey());
    }

    /**
     * The record is the expected one in every field but {@code notChecked}, was entered into its state between
     * {@code from} and {@code to}, has no lease, and holds its key until the keep window after that when it is
     * terminal and a window was set.
     */
    private void assertEnding(
            String path,
            TaskRecord actual,
            TaskRecord.Builder expected,
            Instant from,
            Instant to,
            String... notChecked) {
        TaskRecord want = expected.build();
        assertThat(actual.stateChangedAt()).as("finish time after %s", path).isBetween(millis(from), to.plusMillis(1));
        assertThat(actual.keyUntil())
                .as("key until after %s", path)
                .isEqualTo(
                        want.state().isTerminal() && want.keyKeep() != null
                                ? actual.stateChangedAt().plus(want.keyKeep())
                                : null);
        assertThat(actual.leaseUntil()).as("lease after %s", path).isNull();
        List<String> ignored = new ArrayList<>(List.of("stateChangedAt", "keyUntil"));
        ignored.addAll(List.of(notChecked));
        assertThat(actual)
                .as("record after %s", path)
                .usingRecursiveComparison()
                .ignoringFields(ignored.toArray(String[]::new))
                .isEqualTo(want);
    }

    /** Two records the sibling paths left from alike tasks are alike in every field the ending does not time. */
    private void assertTwinsAlike(String name, TaskRecord first, TaskRecord second, String... notChecked) {
        List<String> ignored = new ArrayList<>(
                List.of("id", "uniqueKey", "claimedAt", "stateChangedAt", "keyUntil", "cancelRequestedAt"));
        ignored.addAll(List.of(notChecked));
        assertThat(second)
                .as("the sibling path against the first for %s", name)
                .usingRecursiveComparison()
                .ignoringFields(ignored.toArray(String[]::new))
                .isEqualTo(first);
        assertThat(second.cancelRequestedAt() == null)
                .as("cancel request kept by both paths for %s", name)
                .isEqualTo(first.cancelRequestedAt() == null);
    }

    /** The task is gone from every read; its key is taken until the keep window ends, or free when no window was set. */
    private void assertRemoved(String path, TaskRecord claimed) {
        assertThat(store.task(claimed.id())).as("record after %s", path).isEmpty();
        EnqueueOutcome again =
                store.enqueue(task().uniqueKey(claimed.uniqueKey()).build());
        assertThat(again)
                .as("same-key enqueue after %s", path)
                .isEqualTo(
                        claimed.keyKeep() == null
                                ? EnqueueOutcome.STORED
                                : new EnqueueOutcome.DuplicateKey(claimed.id()));
    }

    private enum FailEnding {
        DEAD,
        DEAD_REMOVED,
        DEAD_EXHAUSTED,
        DEAD_AFTER_CANCEL_REQUEST,
        RETRY
    }

    private TaskRecord assertFailEnded(
            String path,
            TaskRecord claimed,
            FailEnding ending,
            FailureInfo info,
            Instant retryAt,
            Instant from,
            Instant to) {
        if (ending == FailEnding.DEAD_REMOVED) {
            assertRemoved(path, claimed);
            return null;
        }
        boolean retry = ending == FailEnding.RETRY;
        TaskRecord.Builder expected =
                endedFrom(claimed, retry ? TaskState.SCHEDULED : TaskState.DEAD).failures(List.of(info));
        List<String> notChecked = new ArrayList<>(List.of("progress"));
        if (retry) {
            expected.dueAt(retryAt).runnerId(null).uniqueKey(claimed.uniqueKey());
        } else {
            notChecked.add("dueAt");
        }
        if (ending == FailEnding.DEAD_AFTER_CANCEL_REQUEST) {
            notChecked.addAll(List.of("cancelRequestedAt", "cancelReason"));
        }
        TaskRecord after = get(claimed.id());
        assertEnding(path, after, expected, from, to, notChecked.toArray(String[]::new));
        return after;
    }

    @Test
    void failExpiredLeavesTheRecordThatFailLeavesOnceTheLeaseExpired() {
        EndingTimes times = endingTimes();
        FailureInfo info = new FailureInfo(1, times.failedAt(), "java.io.IOException", "disk full", "at a\nat b");
        List<EndingTwins> twins = new ArrayList<>();
        for (Duration window : ENDING_KEY_WINDOWS) {
            for (FailEnding ending : FailEnding.values()) {
                String name = ending + (window == null ? "-no-window" : "-window");
                TaskRecord[] pair = new TaskRecord[2];
                for (int i = 0; i < 2; i++) {
                    TaskRecord.Builder builder = twinBuilder("fail-" + name + "-" + i, window, times);
                    if (ending == FailEnding.DEAD_EXHAUSTED) {
                        builder.maxAttempts(1);
                    }
                    pair[i] = workedTask(builder, SHORT, times, ending == FailEnding.DEAD_AFTER_CANCEL_REQUEST);
                }
                twins.add(new EndingTwins(name, ending, window, pair[0], pair[1]));
            }
        }
        advance(PAST_SHORT);

        for (EndingTwins twin : twins) {
            FailEnding ending = (FailEnding) twin.kind();
            Instant retryAt =
                    ending == FailEnding.RETRY || ending == FailEnding.DEAD_EXHAUSTED ? times.retryAt() : null;
            boolean keepDead = ending != FailEnding.DEAD_REMOVED;
            FailOutcome outcome = ending == FailEnding.RETRY ? FailOutcome.RETRY : FailOutcome.DEAD;

            Instant from = now();
            assertThat(store.fail(twin.first().id(), twin.first().revision(), info, retryAt, keepDead, null))
                    .as("fail %s", twin.name())
                    .isEqualTo(outcome);
            Instant to = now();
            TaskRecord viaFail = assertFailEnded("fail " + twin.name(), twin.first(), ending, info, retryAt, from, to);

            from = now();
            assertThat(store.failExpired(twin.second().id(), twin.second().revision(), info, retryAt, keepDead, null))
                    .as("failExpired %s", twin.name())
                    .isEqualTo(outcome);
            to = now();
            TaskRecord viaExpired =
                    assertFailEnded("failExpired " + twin.name(), twin.second(), ending, info, retryAt, from, to);

            if (keepDead) {
                assertTwinsAlike(twin.name(), viaFail, viaExpired);
            }
        }
    }

    @Test
    void finishCancelledExpiredLeavesTheRecordThatFinishCancelledLeavesOnceTheLeaseExpired() {
        EndingTimes times = endingTimes();
        List<EndingTwins> twins = new ArrayList<>();
        for (Duration window : ENDING_KEY_WINDOWS) {
            for (boolean keepPayload : new boolean[] {true, false}) {
                for (boolean requested : new boolean[] {true, false}) {
                    String name = "payload-" + keepPayload + "-requested-" + requested
                            + (window == null ? "-no-window" : "-window");
                    TaskRecord first =
                            workedTask(twinBuilder("cancel-" + name + "-0", window, times), SHORT, times, requested);
                    TaskRecord second =
                            workedTask(twinBuilder("cancel-" + name + "-1", window, times), SHORT, times, requested);
                    twins.add(new EndingTwins(name, keepPayload, window, first, second));
                }
            }
        }
        advance(PAST_SHORT);

        for (EndingTwins twin : twins) {
            boolean keepPayload = (Boolean) twin.kind();
            List<TaskRecord> after = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                TaskRecord claimed = i == 0 ? twin.first() : twin.second();
                String path = (i == 0 ? "finishCancelled " : "finishCancelledExpired ") + twin.name();
                Instant from = now();
                boolean ended = i == 0
                        ? store.finishCancelled(claimed.id(), claimed.revision(), "stopped", keepPayload, null)
                        : store.finishCancelledExpired(claimed.id(), claimed.revision(), "stopped", keepPayload, null);
                Instant to = now();
                assertThat(ended).as(path).isTrue();

                boolean requested = claimed.cancelRequestedAt() != null;
                TaskRecord done = get(claimed.id());
                TaskRecord.Builder expected = endedFrom(claimed, TaskState.CANCELLED)
                        .cancelReason("stopped")
                        .encodedPayload(keepPayload ? claimed.encodedPayload() : null);
                if (requested) {
                    assertEnding(path, done, expected, from, to, "progress");
                } else {
                    assertThat(done.cancelRequestedAt())
                            .as("cancel request time after %s", path)
                            .isBetween(millis(from), to.plusMillis(1));
                    assertEnding(path, done, expected, from, to, "progress", "cancelRequestedAt");
                }
                after.add(done);
            }
            assertTwinsAlike(twin.name(), after.get(0), after.get(1));
        }
    }

    @Test
    void ackAllLeavesTheRecordThatAckLeavesForEveryKeepAndKeepPayload() {
        EndingTimes times = endingTimes();
        for (boolean keep : new boolean[] {true, false}) {
            for (boolean keepPayload : new boolean[] {true, false}) {
                List<EndingTwins> twins = new ArrayList<>();
                for (Duration window : ENDING_KEY_WINDOWS) {
                    String name =
                            "keep-" + keep + "-payload-" + keepPayload + (window == null ? "-no-window" : "-window");
                    twins.add(new EndingTwins(
                            name,
                            null,
                            window,
                            workedTask(twinBuilder("ack-" + name + "-0", window, times), LEASE, times, false),
                            workedTask(twinBuilder("ack-" + name + "-1", window, times), LEASE, times, false)));
                }

                advance(Duration.ofMillis(50));
                Instant from = now();
                for (EndingTwins twin : twins) {
                    assertThat(store.ack(twin.first().id(), twin.first().revision(), keep, keepPayload, null))
                            .as("ack %s", twin.name())
                            .isTrue();
                }
                Instant middle = now();
                List<Boolean> applied = store.ackAll(
                        twins.stream()
                                .map(twin -> new LeaseRef(
                                        twin.second().id(), twin.second().revision()))
                                .toList(),
                        keep,
                        keepPayload);
                Instant to = now();
                assertThat(applied)
                        .as("ackAll keep %s payload %s", keep, keepPayload)
                        .containsExactly(true, true);

                for (EndingTwins twin : twins) {
                    List<TaskRecord> after = new ArrayList<>();
                    for (int i = 0; i < 2; i++) {
                        TaskRecord claimed = i == 0 ? twin.first() : twin.second();
                        String path = (i == 0 ? "ack " : "ackAll ") + twin.name();
                        if (!keep) {
                            assertRemoved(path, claimed);
                            continue;
                        }
                        TaskRecord done = get(claimed.id());
                        TaskRecord.Builder expected = endedFrom(claimed, TaskState.SUCCEEDED)
                                .encodedPayload(keepPayload ? claimed.encodedPayload() : null);
                        assertEnding(path, done, expected, i == 0 ? from : middle, i == 0 ? middle : to, "progress");
                        after.add(done);
                    }
                    if (keep) {
                        assertTwinsAlike(twin.name(), after.get(0), after.get(1));
                    }
                }
            }
        }
    }

    @Test
    void cancelAndExpireOfAWaitingTaskStampTheFinishTimeAndTheKeyWindow() {
        EndingTimes times = endingTimes();
        Instant later = millis(now().plus(Duration.ofHours(1)));
        Instant soon = millis(now().plus(SHORT));
        List<EndingTwins> cancels = new ArrayList<>();
        List<EndingTwins> expiries = new ArrayList<>();
        for (Duration window : ENDING_KEY_WINDOWS) {
            String suffix = window == null ? "-no-window" : "-window";
            for (boolean scheduled : new boolean[] {false, true}) {
                String name = (scheduled ? "scheduled" : "queued") + suffix;
                Instant due = scheduled ? later : times.due();
                long cancelId = put(twinBuilder("cancel-" + name, window, times).dueAt(due));
                cancels.add(new EndingTwins(name, null, window, get(cancelId), null));
                long expireId = put(
                        twinBuilder("expire-" + name, window, times).dueAt(due).startBy(soon));
                expiries.add(new EndingTwins(name, null, window, get(expireId), null));
            }
        }

        advance(Duration.ofMillis(50));
        for (EndingTwins twin : cancels) {
            TaskRecord before = twin.first();
            Instant from = now();
            assertThat(store.cancel(before.id(), before.revision(), "withdrawn", null))
                    .as("cancel %s", twin.name())
                    .isEqualTo(CancelOutcome.CANCELLED);
            Instant to = now();
            TaskRecord done = get(before.id());
            assertThat(done.cancelRequestedAt())
                    .as("cancel request time of %s", twin.name())
                    .isBetween(millis(from), to.plusMillis(1));
            assertEnding(
                    "cancel " + twin.name(),
                    done,
                    endedFrom(before, TaskState.CANCELLED).cancelReason("withdrawn"),
                    from,
                    to,
                    "cancelRequestedAt");
        }

        advance(PAST_SHORT);
        Instant from = now();
        List<Long> expired = ids(store.expire(100));
        Instant to = now();
        assertThat(expired)
                .containsExactlyInAnyOrderElementsOf(
                        expiries.stream().map(twin -> twin.first().id()).toList());
        for (EndingTwins twin : expiries) {
            TaskRecord before = twin.first();
            assertEnding("expire " + twin.name(), get(before.id()), endedFrom(before, TaskState.EXPIRED), from, to);
        }
    }

    @Test
    void purgeRemovesTasksOfEachTerminalStateByTheLimitAndTheRetentionCutoff() {
        List<TaskState> terminals =
                List.of(TaskState.SUCCEEDED, TaskState.DEAD, TaskState.CANCELLED, TaskState.EXPIRED);
        Map<TaskState, List<Long>> old = new LinkedHashMap<>();
        for (TaskState state : terminals) {
            old.put(state, terminalTasks(state, 3));
        }
        advance(PAST_MID);
        Instant youngFrom = now();
        Map<TaskState, Long> young = new LinkedHashMap<>();
        for (TaskState state : List.of(TaskState.EXPIRED, TaskState.SUCCEEDED, TaskState.DEAD, TaskState.CANCELLED)) {
            young.put(state, terminalTasks(state, 1).get(0));
        }
        Set<Long> present = new HashSet<>();
        old.values().forEach(present::addAll);
        present.addAll(young.values());

        for (TaskState state : terminals) {
            assertThat(store.purge(state, Duration.ofHours(1), 10))
                    .as("%s with a retention nothing has reached", state)
                    .isZero();
        }
        for (TaskState state : terminals) {
            List<Long> ids = old.get(state);
            assertThat(store.purge(state, MID, 2))
                    .as("%s purged under a limit of 2", state)
                    .isEqualTo(2);
            present.removeAll(ids.subList(0, 2));
            assertThat(store.task(ids.get(0))).as("%s oldest", state).isEmpty();
            assertThat(store.task(ids.get(1))).as("%s second oldest", state).isEmpty();
            assertThat(store.task(ids.get(2)))
                    .as("%s third kept by the limit", state)
                    .isPresent();

            long rest = store.purge(state, MID, 2);
            assumeBefore(youngFrom.plus(MID));
            assertThat(rest).as("%s purged the rest", state).isEqualTo(1);
            present.remove(ids.get(2));
            long again = store.purge(state, MID, 2);
            assumeBefore(youngFrom.plus(MID));
            assertThat(again).as("%s purged again", state).isZero();
            assertThat(get(young.get(state)).state())
                    .as("%s younger than the retention", state)
                    .isEqualTo(state);

            for (long id : present) {
                assertThat(store.task(id))
                        .as("task %s after the purge of %s", id, state)
                        .isPresent();
            }
        }
    }

    private TaskRecord claimedWithCancelRequest(Duration lease) {
        putNew();
        TaskRecord claimed = claim(1, lease, null).get(0);
        assertThat(store.cancel(claimed.id(), claimed.revision(), "ask", null)).isEqualTo(CancelOutcome.REQUESTED);
        return get(claimed.id());
    }

    @Test
    void retryOfACancelledOrDeadTaskLeavesNoCancelRequestForTheNextAttempt() {
        Map<String, Long> ids = new LinkedHashMap<>();
        long queued = putNew();
        assertThat(store.cancel(queued, get(queued).revision(), "ask", null)).isEqualTo(CancelOutcome.CANCELLED);
        ids.put("cancelled while queued", queued);
        long scheduled = put(task().dueAt(now().plus(Duration.ofHours(1))));
        assertThat(store.cancel(scheduled, get(scheduled).revision(), "ask", null))
                .isEqualTo(CancelOutcome.CANCELLED);
        ids.put("cancelled while scheduled", scheduled);

        TaskRecord finished = claimedWithCancelRequest(SHORT);
        TaskRecord finishedUnasked = shortLeased();
        TaskRecord failed = claimedWithCancelRequest(SHORT);
        TaskRecord finishedLate = claimedWithCancelRequest(SHORT);
        TaskRecord failedLate = claimedWithCancelRequest(SHORT);
        assertThat(store.finishCancelled(finished.id(), finished.revision(), "stop", true, null))
                .isTrue();
        ids.put("finishCancelled after a request", finished.id());
        assertThat(store.fail(failed.id(), failed.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        ids.put("fail to DEAD after a request", failed.id());
        advance(PAST_SHORT);
        assertThat(store.finishCancelledExpired(finishedLate.id(), finishedLate.revision(), "stop", true, null))
                .isTrue();
        ids.put("finishCancelledExpired after a request", finishedLate.id());
        assertThat(store.failExpired(failedLate.id(), failedLate.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        ids.put("failExpired to DEAD after a request", failedLate.id());
        assertThat(store.finishCancelledExpired(finishedUnasked.id(), finishedUnasked.revision(), "stop", true, null))
                .isTrue();
        ids.put("finishCancelledExpired without a request", finishedUnasked.id());

        ids.forEach((how, id) -> {
            TaskRecord ended = get(id);
            assertThat(store.retry(id, ended.revision())).as("retry of %s", how).isEqualTo(RetryOutcome.RETRIED);
            TaskRecord retried = get(id);
            assertThat(retried.state()).as("state of %s", how).isEqualTo(TaskState.QUEUED);
            assertThat(retried.cancelRequestedAt())
                    .as("cancel request of %s", how)
                    .isNull();
            assertThat(retried.cancelReason()).as("cancel reason of %s", how).isNull();
            assertThat(retried.runnerId()).as("runner of %s", how).isNull();
            assertThat(retried.leaseUntil()).as("lease of %s", how).isNull();
            assertThat(retried.attempts()).as("attempts of %s", how).isZero();
        });

        Instant claimedFrom = now();
        List<TaskRecord> claimed = claim(ids.size(), SHORT, null);
        assertThat(claimed).extracting(TaskRecord::id).containsExactlyInAnyOrderElementsOf(ids.values());
        for (TaskRecord t : claimed) {
            assertThat(t.leaseUntil())
                    .as("lease of %s", t.id())
                    .isAfterOrEqualTo(millis(claimedFrom).plus(SHORT));
            assertThat(t.cancelRequestedAt())
                    .as("cancel request of the claimed %s", t.id())
                    .isNull();
            assertThat(t.cancelReason())
                    .as("cancel reason of the claimed %s", t.id())
                    .isNull();
            assertThat(get(t.id())).isEqualTo(t);
        }
        List<LeaseRef> refs =
                claimed.stream().map(t -> new LeaseRef(t.id(), t.revision())).toList();
        List<LeaseStatus> renewed = store.renew(refs, SHORT);
        assumeBefore(claimed.stream()
                .map(TaskRecord::leaseUntil)
                .min(Comparator.naturalOrder())
                .orElseThrow());
        assertThat(renewed)
                .as("renew of the retried tasks")
                .containsExactlyElementsOf(claimed.stream()
                        .map(t -> new LeaseStatus(t.id(), true, false))
                        .toList());
        advance(PAST_SHORT);
        List<ExpiredLease> expired = store.reclaim(100);
        assertThat(expired)
                .as("reclaim of the retried tasks")
                .extracting(ExpiredLease::id)
                .containsExactlyInAnyOrderElementsOf(ids.values());
        assertThat(expired)
                .as("cancel request reported by reclaim")
                .extracting(ExpiredLease::cancelRequested)
                .containsOnly(false);
    }

    private TaskRecord shortLeased() {
        putNew();
        return claim(1, SHORT, null).get(0);
    }

    @Test
    void aLeaseThatEndsLeavesNothingForReclaimToFind() {
        Instant far = millis(now().plus(Duration.ofHours(2)));
        Instant past = millis(now().minus(Duration.ofHours(2)));
        TaskRecord unclaimed = shortLeased();
        TaskRecord rescheduled = shortLeased();
        TaskRecord rescheduledToThePast = shortLeased();
        TaskRecord retried = shortLeased();
        TaskRecord retriedInThePast = shortLeased();
        TaskRecord acked = shortLeased();
        TaskRecord dead = shortLeased();
        TaskRecord cancelled = shortLeased();
        TaskRecord expiredRetried = shortLeased();
        TaskRecord expiredDead = shortLeased();
        TaskRecord expiredCancelled = shortLeased();
        TaskRecord control = shortLeased();

        assertThat(store.unclaim(unclaimed.id(), unclaimed.revision())).isTrue();
        assertThat(store.reschedule(rescheduled.id(), rescheduled.revision(), far))
                .isTrue();
        assertThat(store.reschedule(rescheduledToThePast.id(), rescheduledToThePast.revision(), past))
                .isTrue();
        assertThat(store.fail(retried.id(), retried.revision(), failure(1), far, true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(store.fail(retriedInThePast.id(), retriedInThePast.revision(), failure(1), past, true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(store.ack(acked.id(), acked.revision(), true, true, null)).isTrue();
        assertThat(store.fail(dead.id(), dead.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(store.finishCancelled(cancelled.id(), cancelled.revision(), "stop", true, null))
                .isTrue();
        advance(PAST_SHORT);
        assertThat(store.failExpired(expiredRetried.id(), expiredRetried.revision(), failure(1), far, true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(store.failExpired(expiredDead.id(), expiredDead.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(store.finishCancelledExpired(expiredCancelled.id(), expiredCancelled.revision(), "stop", true, null))
                .isTrue();

        for (TaskRecord ended : List.of(
                unclaimed,
                rescheduled,
                rescheduledToThePast,
                retried,
                retriedInThePast,
                acked,
                dead,
                cancelled,
                expiredRetried,
                expiredDead,
                expiredCancelled)) {
            TaskRecord after = get(ended.id());
            assertThat(after.state()).as("state of %s", ended.id()).isNotEqualTo(TaskState.LEASED);
            assertThat(after.leaseUntil()).as("lease of %s", ended.id()).isNull();
        }
        assertThat(store.reclaim(100)).extracting(ExpiredLease::id).containsExactly(control.id());
        assertThat(counts().leased()).isEqualTo(1);
        assertThat(store.tasks(TaskState.LEASED, queue, null, 100).items())
                .extracting(TaskRecord::id)
                .containsExactly(control.id());

        assertThat(store.promote(100).moved()).isEqualTo(2);
        List<TaskRecord> again = claim(10, SHORT, null);
        assertThat(again)
                .extracting(TaskRecord::id)
                .containsExactlyInAnyOrder(unclaimed.id(), rescheduledToThePast.id(), retriedInThePast.id());
        advance(PAST_SHORT);
        Map<Long, Integer> revisions = new LinkedHashMap<>();
        again.forEach(t -> revisions.put(t.id(), t.revision()));
        revisions.put(control.id(), control.revision());
        List<ExpiredLease> expired = store.reclaim(100);
        assertThat(expired)
                .as("reclaim of the leases taken again")
                .extracting(ExpiredLease::id)
                .containsExactlyInAnyOrderElementsOf(revisions.keySet());
        expired.forEach(e -> assertThat(e.revision())
                .as("revision of the lease of %s", e.id())
                .isEqualTo(revisions.get(e.id())));
    }

    @Test
    void instantsTheCallerSuppliesAreStoredAsGivenOnEveryPath() {
        Instant base = millis(now());
        Instant failedAt = base.minus(Duration.ofHours(5));
        Instant stepAt = base.minus(Duration.ofHours(2));
        Instant retryAt = base.plus(Duration.ofHours(3));
        Instant earlierRetryAt = base.minus(Duration.ofHours(4));
        Instant rescheduledTo = base.plus(Duration.ofHours(6));
        Instant rescheduledEarlier = base.minus(Duration.ofHours(7));
        FailureInfo info = new FailureInfo(1, failedAt, "java.io.IOException", "disk", "trace");

        TaskRecord expiredRetried = shortLeased();
        TaskRecord expiredDead = shortLeased();
        TaskRecord stepper = leased(task());
        TaskRecord retried = leased(task());
        TaskRecord retriedEarlier = leased(task());
        TaskRecord dead = leased(task());
        TaskRecord rescheduled = leased(task());
        TaskRecord rescheduledBack = leased(task());

        Map<String, StepResult> steps = Map.of(
                "early", new StepResult(stepAt, "1"),
                "late", new StepResult(base.plus(Duration.ofHours(1)), null));
        assertThat(store.saveSteps(stepper.id(), stepper.revision(), steps)).isTrue();
        assertThat(get(stepper.id()).steps()).as("steps as saved").isEqualTo(steps);

        assertThat(store.fail(retried.id(), retried.revision(), info, retryAt, true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(retried.id()).dueAt()).as("retry time of fail").isEqualTo(retryAt);
        assertThat(get(retried.id()).failures())
                .as("failure of fail with a retry")
                .containsExactly(info);
        assertThat(store.fail(retriedEarlier.id(), retriedEarlier.revision(), info, earlierRetryAt, true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(retriedEarlier.id()).dueAt())
                .as("retry time in the past")
                .isEqualTo(earlierRetryAt);
        assertThat(store.fail(dead.id(), dead.revision(), info, null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(get(dead.id()).failures()).as("failure of fail to DEAD").containsExactly(info);

        assertThat(store.reschedule(rescheduled.id(), rescheduled.revision(), rescheduledTo))
                .isTrue();
        assertThat(get(rescheduled.id()).dueAt()).as("due of reschedule").isEqualTo(rescheduledTo);
        assertThat(store.reschedule(rescheduledBack.id(), rescheduledBack.revision(), rescheduledEarlier))
                .isTrue();
        assertThat(get(rescheduledBack.id()).dueAt())
                .as("due of reschedule to the past")
                .isEqualTo(rescheduledEarlier);

        advance(PAST_SHORT);
        assertThat(store.failExpired(expiredRetried.id(), expiredRetried.revision(), info, retryAt, true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(expiredRetried.id()).dueAt())
                .as("retry time of failExpired")
                .isEqualTo(retryAt);
        assertThat(get(expiredRetried.id()).failures())
                .as("failure of failExpired with a retry")
                .containsExactly(info);
        assertThat(store.failExpired(expiredDead.id(), expiredDead.revision(), info, null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(get(expiredDead.id()).failures())
                .as("failure of failExpired to DEAD")
                .containsExactly(info);

        assertThat(store.promote(100).moved()).isEqualTo(2);
        assertThat(get(retriedEarlier.id()).dueAt())
                .as("retry time after promote")
                .isEqualTo(earlierRetryAt);
        assertThat(get(rescheduledBack.id()).dueAt()).as("due after promote").isEqualTo(rescheduledEarlier);
    }

    @Test
    void aRetryTimeInThePastKeepsTheTaskAheadOfWorkThatWasQueuedBeforeIt() {
        Instant earlier = millis(now().minus(Duration.ofHours(4)));
        TaskRecord failing = leased(task());
        long waiting = putNew();
        assertThat(store.fail(failing.id(), failing.revision(), failure(1), earlier, true, null))
                .isEqualTo(FailOutcome.RETRY);

        assertThat(store.promote(10).moved()).isEqualTo(1);

        assertThat(claim(2)).extracting(TaskRecord::id).containsExactly(failing.id(), waiting);
    }

    @Test
    void enqueueOfAnExistingIdReturnsExistsAndChangesNothing() {
        TaskRecord original = task().encodedPayload("first").build();
        assertThat(store.enqueue(original)).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.enqueue(original.toBuilder().encodedPayload("second").build()))
                .isEqualTo(EnqueueOutcome.EXISTS);
        assertThat(get(original.id()).encodedPayload()).isEqualTo("first");
    }

    @Test
    void enqueueOfAnExistingIdReturnsExistsInEveryState() {
        List<TaskState> order = List.of(
                TaskState.LEASED,
                TaskState.SUCCEEDED,
                TaskState.DEAD,
                TaskState.CANCELLED,
                TaskState.EXPIRED,
                TaskState.QUEUED,
                TaskState.SCHEDULED);
        for (TaskState state : order) {
            String key = "k-" + state;
            long id = inState(state, task().uniqueKey(key).keyKeep(Duration.ofHours(1)));
            TaskRecord before = get(id);
            assertThat(before.state()).isEqualTo(state);
            StateCounts counts = counts();
            Optional<TaskRecord> holder = store.taskByKey(key);
            assertThat(holder).contains(before);

            TaskRecord conflicting = task().id(id)
                    .uniqueKey(key)
                    .keyKeep(Duration.ofHours(1))
                    .encodedPayload("conflicting")
                    .build();

            assertThat(store.enqueue(conflicting)).as("%s", state).isEqualTo(EnqueueOutcome.EXISTS);
            assertThat(get(id)).as("%s", state).isEqualTo(before);
            assertThat(counts()).as("%s", state).isEqualTo(counts);
            assertThat(store.taskByKey(key)).as("%s", state).isEqualTo(holder);
        }
    }

    /**
     * Stores a task built from {@code builder} and takes it to {@code state}. A task that has to be claimed is
     * the oldest queued one, so the states that need a claim come before QUEUED and SCHEDULED.
     */
    private long inState(TaskState state, TaskRecord.Builder builder) {
        switch (state) {
            case QUEUED:
                return put(builder);
            case SCHEDULED:
                return put(builder.dueAt(now().plus(Duration.ofHours(1))));
            case LEASED:
                return leased(builder).id();
            case SUCCEEDED: {
                TaskRecord t = leased(builder);
                assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
                return t.id();
            }
            case DEAD: {
                TaskRecord t = leased(builder);
                assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                        .isEqualTo(FailOutcome.DEAD);
                return t.id();
            }
            case CANCELLED: {
                long id = put(builder);
                assertThat(store.cancel(id, get(id).revision(), "stop", null)).isEqualTo(CancelOutcome.CANCELLED);
                return id;
            }
            case EXPIRED: {
                long id = put(builder.startBy(now().plus(SHORT)));
                advance(PAST_SHORT);
                assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(id);
                return id;
            }
            default:
                throw new IllegalArgumentException(state.toString());
        }
    }

    @Test
    void duplicateKeyNamesTheHolder() {
        long holder = put(task().uniqueKey("order-1"));
        TaskRecord second = task().uniqueKey("order-1").build();
        assertThat(store.enqueue(second)).isEqualTo(new EnqueueOutcome.DuplicateKey(holder));
        assertThat(store.task(second.id())).isEmpty();
    }

    @Test
    void differentKeysDoNotCollide() {
        put(task().uniqueKey("a"));
        put(task().uniqueKey("b"));
        put(task());
        put(task());
    }

    @Test
    void keyIsHeldWhileTheTaskIsLeased() {
        long holder = leased(task().uniqueKey("k")).id();
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(holder));
    }

    @Test
    void keyIsReleasedWhenTheTaskEndsWithoutKeyKeep() {
        TaskRecord t = leased(task().uniqueKey("k"));
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        put(task().uniqueKey("k"));
    }

    @Test
    void keyIsReleasedWhenTheTaskDiesWithoutKeyKeep() {
        TaskRecord t = leased(task().uniqueKey("k"));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        put(task().uniqueKey("k"));
    }

    @Test
    void keyStaysHeldAfterTerminalUntilKeyUntil() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        Instant endedFrom = now();
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.stateChangedAt()).isAfterOrEqualTo(millis(endedFrom));
        assertThat(done.keyUntil())
                .isBetween(
                        done.stateChangedAt().plus(SHORT).minusMillis(5),
                        done.stateChangedAt().plus(SHORT).plusMillis(5));
        EnqueueOutcome whileHeld = store.enqueue(task().uniqueKey("k").build());
        assumeBefore(done.keyUntil());
        assertThat(whileHeld).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
        advance(PAST_SHORT);
        put(task().uniqueKey("k"));
    }

    @Test
    void enqueueAllStoresInOrderAndReportsEachOutcome() {
        TaskRecord a = task().uniqueKey("k").encodedPayload("pa").build();
        TaskRecord b = task().uniqueKey("k").encodedPayload("pb").build();
        TaskRecord c = task().encodedPayload("pc").build();
        List<EnqueueOutcome> outcomes = store.enqueueAll(List.of(a, b, c, a));
        assertThat(outcomes)
                .containsExactly(
                        EnqueueOutcome.STORED,
                        new EnqueueOutcome.DuplicateKey(a.id()),
                        EnqueueOutcome.STORED,
                        EnqueueOutcome.EXISTS);
        assertThat(store.task(b.id())).isEmpty();
        TaskRecord storedA = get(a.id());
        assertThat(storedA.encodedPayload()).isEqualTo("pa");
        assertThat(storedA.uniqueKey()).isEqualTo("k");
        assertThat(storedA.state()).isEqualTo(TaskState.QUEUED);
        TaskRecord storedC = get(c.id());
        assertThat(storedC.encodedPayload()).isEqualTo("pc");
        assertThat(storedC.uniqueKey()).isNull();
        assertThat(storedC.state()).isEqualTo(TaskState.QUEUED);
        assertThat(store.taskByKey("k")).contains(storedA);
        assertThat(counts().queued()).isEqualTo(2);
    }

    private record Raced(TaskRecord record, EnqueueOutcome outcome) {
        boolean stored() {
            return outcome.equals(EnqueueOutcome.STORED);
        }
    }

    private List<Raced> raceEnqueues(List<TaskRecord> records) throws Exception {
        AtomicInteger turn = new AtomicInteger();
        return runConcurrently(records.size(), () -> {
            TaskRecord mine = records.get(turn.getAndIncrement());
            return new Raced(mine, store.enqueue(mine));
        });
    }

    private Raced soleWinner(List<Raced> results) {
        List<Raced> winners = results.stream().filter(Raced::stored).toList();
        assertThat(winners).hasSize(1);
        return winners.get(0);
    }

    @Test
    void concurrentEnqueuesOfOneKeyHaveExactlyOneWinnerAndEveryDuplicateNamesIt() throws Exception {
        for (int round = 0; round < 3; round++) {
            String key = "race" + round;
            List<TaskRecord> records = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                records.add(task().uniqueKey(key).build());
            }

            List<Raced> results = raceEnqueues(records);

            long winner = soleWinner(results).record().id();
            for (Raced r : results) {
                if (r.record().id() != winner) {
                    assertThat(r.outcome()).isEqualTo(new EnqueueOutcome.DuplicateKey(winner));
                    assertThat(store.task(r.record().id())).isEmpty();
                }
            }
            assertThat(store.taskByKey(key)).map(TaskRecord::id).contains(winner);
            assertThat(get(winner).uniqueKey()).isEqualTo(key);
        }
        assertThat(counts().queued()).isEqualTo(3);
    }

    @Test
    void concurrentEnqueuesRightAfterAKeptWindowExpiredHaveExactlyOneWinnerAndCleanupKeepsIt() throws Exception {
        List<Long> old = new ArrayList<>();
        for (int round = 0; round < 3; round++) {
            TaskRecord t = leased(task().uniqueKey("kept" + round).keyKeep(SHORT));
            assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
            old.add(t.id());
        }
        advance(PAST_SHORT);
        for (int round = 0; round < 3; round++) {
            String key = "kept" + round;
            List<TaskRecord> records = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                records.add(task().uniqueKey(key).build());
            }

            List<Raced> results = raceEnqueues(records);

            long winner = soleWinner(results).record().id();
            for (Raced r : results) {
                if (r.record().id() != winner) {
                    assertThat(r.outcome()).isEqualTo(new EnqueueOutcome.DuplicateKey(winner));
                }
            }
            assertThat(store.taskByKey(key)).map(TaskRecord::id).contains(winner);
        }

        store.releaseKeys(10);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isEqualTo(3);
        old.forEach(id -> assertThat(store.task(id)).isEmpty());
        for (int round = 0; round < 3; round++) {
            String key = "kept" + round;
            TaskRecord holder = store.taskByKey(key).orElseThrow();
            assertThat(holder.state()).isEqualTo(TaskState.QUEUED);
            assertThat(store.enqueue(task().uniqueKey(key).build()))
                    .isEqualTo(new EnqueueOutcome.DuplicateKey(holder.id()));
        }
    }

    @Test
    void keyCleanupRacingTheNextHoldersEnqueueNeverReleasesTheNewHoldersKey() throws Exception {
        int rounds = 5;
        List<Long> old = new ArrayList<>();
        for (int round = 0; round < rounds; round++) {
            TaskRecord t = leased(task().uniqueKey("swap" + round).keyKeep(SHORT));
            assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
            old.add(t.id());
        }
        advance(PAST_SHORT);
        for (int round = 0; round < rounds; round++) {
            String key = "swap" + round;
            List<TaskRecord> records = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                records.add(task().uniqueKey(key).build());
            }
            AtomicInteger turn = new AtomicInteger();
            List<Raced> results = runConcurrently(records.size() + 2, () -> {
                int mine = turn.getAndIncrement();
                if (mine == records.size()) {
                    store.releaseKeys(10);
                    return null;
                }
                if (mine == records.size() + 1) {
                    store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10);
                    return null;
                }
                TaskRecord record = records.get(mine);
                return new Raced(record, store.enqueue(record));
            });

            long winner = soleWinner(results.stream().filter(Objects::nonNull).toList())
                    .record()
                    .id();
            assertThat(store.taskByKey(key)).map(TaskRecord::id).contains(winner);
            assertThat(get(winner).uniqueKey()).isEqualTo(key);
            assertThat(store.enqueue(task().uniqueKey(key).build())).isEqualTo(new EnqueueOutcome.DuplicateKey(winner));
        }

        store.releaseKeys(10);
        while (store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10) > 0) {}
        old.forEach(id -> assertThat(store.task(id)).isEmpty());
        for (int round = 0; round < rounds; round++) {
            String key = "swap" + round;
            TaskRecord holder = store.taskByKey(key).orElseThrow();
            assertThat(holder.state()).isEqualTo(TaskState.QUEUED);
            assertThat(store.enqueue(task().uniqueKey(key).build()))
                    .isEqualTo(new EnqueueOutcome.DuplicateKey(holder.id()));
        }
        assertThat(counts().queued()).isEqualTo(rounds);
    }

    @Test
    void concurrentEnqueuesOfOneIdWithDifferentContentLeaveOneImmutableWinner() throws Exception {
        for (int round = 0; round < 3; round++) {
            long id = store.allocateIds(1);
            List<TaskRecord> records = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                records.add(task().id(id)
                        .uniqueKey("same-id-" + round + "-" + i)
                        .encodedPayload("p" + i)
                        .build());
            }

            List<Raced> results = raceEnqueues(records);

            TaskRecord winner = soleWinner(results).record();
            for (Raced r : results) {
                if (r.record() != winner) {
                    assertThat(r.outcome()).isEqualTo(EnqueueOutcome.EXISTS);
                    assertThat(store.taskByKey(r.record().uniqueKey())).isEmpty();
                }
            }
            TaskRecord stored = get(id);
            assertThat(stored.encodedPayload()).isEqualTo(winner.encodedPayload());
            assertThat(stored.uniqueKey()).isEqualTo(winner.uniqueKey());
            assertThat(store.taskByKey(winner.uniqueKey())).contains(stored);
            assertThat(store.enqueue(winner)).isEqualTo(EnqueueOutcome.EXISTS);
            assertThat(get(id)).isEqualTo(stored);
        }
        assertThat(counts().queued()).isEqualTo(3);
    }

    // claim

    @Test
    void claimLeasesTheTaskAndRecordsTheAttempt() {
        long id = put(task());
        Instant before = now();
        TaskRecord claimed = claimOne();
        TaskRecord t = get(id);
        assertThat(claimed.id()).isEqualTo(id);
        assertThat(t.state()).isEqualTo(TaskState.LEASED);
        assertThat(t.attempts()).isEqualTo(1);
        assertThat(t.revision()).isEqualTo(1);
        assertThat(t.runnerId()).isEqualTo(runner);
        assertThat(t.claimedAt()).isAfterOrEqualTo(millis(before));
        assertThat(t.leaseUntil()).isAfterOrEqualTo(millis(before).plus(LEASE));
        assertThat(claimed.revision()).isEqualTo(t.revision());
        assertThat(claimed.state()).isEqualTo(TaskState.LEASED);
        assertThat(claimed.attempts()).isEqualTo(1);
    }

    @Test
    void claimKeepsDue() {
        long id = put(task());
        Instant due = get(id).dueAt();
        claimOne();
        assertThat(get(id).dueAt()).isEqualTo(due);
    }

    @Test
    void claimReturnsNothingFromAnEmptyQueue() {
        assertThat(claim(5)).isEmpty();
    }

    @Test
    void claimOrdersByDueThenId() {
        Instant base = millis(now().minusSeconds(60));
        long late = put(task().dueAt(base.plusSeconds(10)));
        long earlyB = put(task().dueAt(base));
        long earlyA = put(task().dueAt(base));
        List<Long> order = claim(3).stream().map(TaskRecord::id).toList();
        List<Long> expectedTies = earlyA < earlyB ? List.of(earlyA, earlyB) : List.of(earlyB, earlyA);
        assertThat(order.subList(0, 2)).containsExactlyElementsOf(expectedTies);
        assertThat(order.get(2)).isEqualTo(late);
    }

    @Test
    void claimReturnsAtMostCountAndTheRestStaysQueued() {
        for (int i = 0; i < 5; i++) {
            putNew();
        }
        assertThat(claim(3)).hasSize(3);
        assertThat(store.tasks(TaskState.QUEUED, queue, null, 10).items()).hasSize(2);
    }

    @Test
    void claimReadsQueuesInRequestOrder() {
        int second = store.queueId("second");
        long inMain = put(task());
        long inSecond = put(task(second, type));
        List<Long> order = claim(2, second, queue).stream().map(TaskRecord::id).toList();
        assertThat(order).containsExactly(inSecond, inMain);
    }

    @Test
    void claimReadsALaterQueueOnlyToFillTheCount() {
        int second = store.queueId("second");
        long inMain = put(task());
        long inSecond = put(task(second, type));
        assertThat(claim(1, queue, second)).extracting(TaskRecord::id).containsExactly(inMain);
        assertThat(get(inSecond).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void claimGrantsUseBothPassesInOrderAndRespectCaps() {
        int second = store.queueId("second");
        int empty = store.queueId("empty");
        var firstIds = new ArrayList<Long>();
        var secondIds = new ArrayList<Long>();
        // Distinct due times: Redis orders tasks due in the same millisecond by id as text, so 10 before 8.
        Instant past = millis(now().minusSeconds(60));
        for (int i = 0; i < 5; i++) {
            firstIds.add(put(task().dueAt(past.plusMillis(2 * i))));
            secondIds.add(put(task(second, type).dueAt(past.plusMillis(2 * i + 1))));
        }
        var grants = List.of(new QueueGrant(queue, 1, 2), new QueueGrant(empty, 3, 3), new QueueGrant(second, 1, 4));

        assertThat(store.claim(new ClaimRequest(grants, 6, LEASE, runner, null)))
                .extracting(TaskRecord::id)
                .containsExactly(
                        firstIds.get(0),
                        secondIds.get(0),
                        firstIds.get(1),
                        secondIds.get(1),
                        secondIds.get(2),
                        secondIds.get(3));
        assertThat(get(firstIds.get(2)).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(secondIds.get(4)).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void claimGrantsSkipPausedAndZeroSharesAndRefillOnlyFilledGrants() {
        int paused = store.queueId("paused");
        int zero = store.queueId("zero");
        int partial = store.queueId("partial");
        long pausedId = put(task(paused, type));
        long zeroId = put(task(zero, type));
        long partialId = put(task(partial, type));
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) ids.add(put(task()));
        store.pauseQueue(paused, true);
        var grants = List.of(
                new QueueGrant(paused, 2, 5),
                new QueueGrant(zero, 0, 5),
                new QueueGrant(partial, 2, 5),
                new QueueGrant(queue, 1, 3));

        assertThat(store.claim(new ClaimRequest(grants, 5, LEASE, runner, null)))
                .extracting(TaskRecord::id)
                .containsExactly(partialId, ids.get(0), ids.get(1), ids.get(2));
        assertThat(get(pausedId).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(zeroId).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(ids.get(3)).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void claimRefillsInGrantOrderWithoutExceedingTheTotalCount() {
        int second = store.queueId("second");
        int empty = store.queueId("empty");
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) ids.add(put(task()));
        long next = put(task(second, type));
        long extra = put(task(second, type));
        var grants = List.of(new QueueGrant(queue, 1, 5), new QueueGrant(empty, 2, 2), new QueueGrant(second, 1, 5));

        assertThat(store.claim(new ClaimRequest(grants, 4, LEASE, runner, null)))
                .extracting(TaskRecord::id)
                .containsExactly(ids.get(0), next, ids.get(1), ids.get(2));
        assertThat(get(ids.get(3)).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(extra).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void claimGrantsTruncateSharesAtTheTotalCount() {
        int second = store.queueId("second");
        long first = put(task());
        long next = put(task(second, type));
        long extra = put(task(second, type));
        var grants = List.of(new QueueGrant(queue, 1, 5), new QueueGrant(second, 5, 5));

        assertThat(store.claim(new ClaimRequest(grants, 2, LEASE, runner, null)))
                .extracting(TaskRecord::id)
                .containsExactly(first, next);
        assertThat(get(extra).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void claimIgnoresScheduledTasks() {
        put(task().dueAt(now().plus(Duration.ofHours(1))));
        assertThat(claim(5)).isEmpty();
    }

    @Test
    void claimSkipsUnknownTypesWithoutCountingAnAttempt() {
        int other = store.typeId("other");
        long known = put(task());
        long unknown = put(task(queue, other));
        TaskRecord before = get(unknown);
        List<TaskRecord> claimed = claim(5, LEASE, Set.of(type));
        assertThat(claimed).extracting(TaskRecord::id).containsExactly(known);
        TaskRecord skipped = get(unknown);
        assertThat(skipped.attempts()).isZero();
        assertThat(skipped).isEqualTo(before);
        assertThat(claim(1, LEASE, Set.of(other))).extracting(TaskRecord::id).containsExactly(unknown);
    }

    @Test
    void claimWithNullKnownTypesTakesEveryType() {
        int other = store.typeId("other");
        put(task());
        put(task(queue, other));
        assertThat(claim(5, LEASE, null)).hasSize(2);
    }

    @Test
    void concurrentClaimsNeverReturnTheSameTask() throws Exception {
        int total = 200;
        for (int i = 0; i < total; i++) {
            putNew();
        }
        int threads = 8;
        List<List<Long>> perThread = runConcurrently(threads, () -> {
            int w = store.allocateRunnerId("t", Set.of(), Set.of());
            List<Long> mine = new ArrayList<>();
            while (true) {
                List<TaskRecord> got =
                        store.claim(new ClaimRequest(List.of(new QueueGrant(queue, 5, 5)), 5, LEASE, w, null));
                if (got.isEmpty()) {
                    return mine;
                }
                got.forEach(t -> mine.add(t.id()));
            }
        });
        List<Long> all = perThread.stream().flatMap(List::stream).toList();
        assertThat(all).hasSize(total).doesNotHaveDuplicates();
    }

    @Test
    void concurrentAllocateIdsNeverOverlap() throws Exception {
        int threads = 8;
        int calls = 50;
        int block = 10;
        List<List<Long>> perThread = runConcurrently(threads, () -> {
            List<Long> firsts = new ArrayList<>();
            for (int i = 0; i < calls; i++) {
                firsts.add(store.allocateIds(block));
            }
            return firsts;
        });
        List<Long> firsts =
                new ArrayList<>(perThread.stream().flatMap(List::stream).toList());
        Collections.sort(firsts);
        assertThat(firsts).hasSize(threads * calls).doesNotHaveDuplicates();
        for (int i = 1; i < firsts.size(); i++) {
            assertThat(firsts.get(i)).isGreaterThanOrEqualTo(firsts.get(i - 1) + block);
        }
    }

    @Test
    void concurrentAllocateIdsOfMixedCountsNeverOverlap() throws Exception {
        int[] sizes = {1, 64};
        int calls = 200;
        List<List<long[]>> perThread = runConcurrently(8, () -> {
            List<long[]> ranges = new ArrayList<>();
            int seed = (int) Thread.currentThread().threadId();
            for (int i = 0; i < calls; i++) {
                int count = sizes[(seed + i) % 2];
                ranges.add(new long[] {store.allocateIds(count), count});
            }
            return ranges;
        });
        List<long[]> ranges =
                new ArrayList<>(perThread.stream().flatMap(List::stream).toList());
        ranges.sort((a, b) -> Long.compare(a[0], b[0]));
        assertThat(ranges).hasSize(8 * calls);
        for (int i = 1; i < ranges.size(); i++) {
            long[] previous = ranges.get(i - 1);
            assertThat(ranges.get(i)[0])
                    .as("range at %d overlaps the one before", ranges.get(i)[0])
                    .isGreaterThanOrEqualTo(previous[0] + previous[1]);
        }
    }

    // renew

    @Test
    void renewExtendsTheLeaseAndKeepsTheRevision() {
        TaskRecord t = leased(task());
        Instant before = get(t.id()).leaseUntil();
        advance(Duration.ofMillis(50));
        List<LeaseStatus> status = store.renew(List.of(new LeaseRef(t.id(), t.revision())), Duration.ofMinutes(5));
        assertThat(status).containsExactly(new LeaseStatus(t.id(), true, false));
        TaskRecord after = get(t.id());
        assertThat(after.leaseUntil()).isAfter(before.plusSeconds(60));
        assertThat(after.revision()).isEqualTo(t.revision());
        assertThat(after.state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void renewReportsNotOwnedForAWrongRevisionOrAnotherState() {
        TaskRecord t = leased(task());
        long queued = putNew();
        long missing = store.allocateIds(1);
        List<LeaseStatus> status = store.renew(
                List.of(
                        new LeaseRef(t.id(), t.revision() + 1),
                        new LeaseRef(queued, 0),
                        new LeaseRef(missing, 0),
                        new LeaseRef(t.id(), t.revision())),
                LEASE);
        assertThat(status).extracting(LeaseStatus::id).containsExactly(t.id(), queued, missing, t.id());
        assertThat(status).extracting(LeaseStatus::owned).containsExactly(false, false, false, true);
    }

    @Test
    void renewOfAnExpiredLeaseIsNotOwnedAndDoesNotExtendIt() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        Instant until = get(t.id()).leaseUntil();
        advance(PAST_SHORT);
        List<LeaseStatus> status = store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        assertThat(status).extracting(LeaseStatus::owned).containsExactly(false);
        TaskRecord after = get(t.id());
        assertThat(after.leaseUntil()).isEqualTo(until);
        assertThat(after.revision()).isEqualTo(t.revision());
        assertThat(after.state()).isEqualTo(TaskState.LEASED);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(t.id());
    }

    @Test
    void renewBeforeTheLeaseExpiresKeepsTheTaskAwayFromTheSweeper() {
        put(task());
        Instant claimedFrom = now();
        TaskRecord t = claim(1, MID, null).get(0);
        assertThat(t.leaseUntil()).isAfterOrEqualTo(millis(claimedFrom).plus(MID));
        advance(Duration.ofMillis(100));
        List<LeaseStatus> status = store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        assumeBefore(t.leaseUntil());
        assertThat(status).containsExactly(new LeaseStatus(t.id(), true, false));
        advance(PAST_MID);
        assertThat(store.reclaim(10)).isEmpty();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void renewReportsACancelRequest() {
        TaskRecord t = leased(task());
        assertThat(store.cancel(t.id(), t.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
        List<LeaseStatus> status = store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        assertThat(status).containsExactly(new LeaseStatus(t.id(), true, true));
    }

    // ack

    @Test
    void ackKeepMovesToSucceededAndDropsTheData() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), true, false, null)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(done.revision()).isEqualTo(t.revision() + 1);
        assertThat(done.encodedPayload()).isNull();
        assertThat(done.leaseUntil()).isNull();
        assertThat(done.runnerId()).isEqualTo(runner);
        assertThat(done.stateChangedAt()).isAfterOrEqualTo(get(t.id()).claimedAt());
    }

    @Test
    void ackKeepPayloadKeepsDataAndSteps() {
        TaskRecord t = leased(task());
        Map<String, StepResult> steps = Map.of("one", new StepResult(millis(now()), "{\"a\":1}"));
        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isTrue();
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.encodedPayload()).isEqualTo("payload");
        assertThat(done.steps()).isEqualTo(steps);
    }

    @Test
    void ackDropsThePayloadButKeepsTheStepsWhenKeepPayloadIsFalse() {
        TaskRecord t = leased(task());
        Map<String, StepResult> steps = Map.of("one", new StepResult(millis(now()), null));
        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isTrue();
        assertThat(store.ack(t.id(), t.revision(), true, false, null)).isTrue();
        assertThat(get(t.id()).steps()).isEqualTo(steps);
        assertThat(get(t.id()).encodedPayload()).isNull();
    }

    @Test
    void ackWithoutKeepRemovesTheTask() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), false, false, null)).isTrue();
        assertThat(store.task(t.id())).isEmpty();
    }

    @Test
    void ackWithoutKeepRemovesTheTaskAndItsKeyStaysTakenUntilTheWindowEnds() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        Instant keptFrom = now();
        assertThat(store.ack(t.id(), t.revision(), false, false, null)).isTrue();
        assertRemovedButKeyTaken(t, "k", keptFrom.plus(SHORT));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isZero();
        advance(PAST_SHORT);
        put(task().uniqueKey("k"));
    }

    @Test
    void holdsKeyNamesTheHolderOfAKeyAlsoAfterTheHolderWasRemovedInsideItsWindow() {
        TaskRecord removed = leased(task().uniqueKey("removed").keyKeep(Duration.ofHours(1)));
        assertThat(store.holdsKey(removed.id(), "removed")).as("leased").isTrue();
        assertThat(store.ack(removed.id(), removed.revision(), false, false, null))
                .isTrue();
        TaskRecord kept = leased(task().uniqueKey("kept").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(kept.id(), kept.revision(), true, true, null)).isTrue();
        TaskRecord freed = leased(task().uniqueKey("freed"));
        assertThat(store.ack(freed.id(), freed.revision(), true, true, null)).isTrue();
        long open = put(task().uniqueKey("open"));

        assertThat(store.task(removed.id())).isEmpty();
        assertThat(store.holdsKey(removed.id(), "removed"))
                .as("removed inside its window")
                .isTrue();
        assertThat(store.holdsKey(kept.id(), "kept"))
                .as("finished inside its window")
                .isTrue();
        assertThat(store.holdsKey(freed.id(), "freed"))
                .as("finished without a window")
                .isFalse();
        assertThat(store.holdsKey(open, "open")).as("open").isTrue();
        assertThat(store.holdsKey(kept.id(), "open"))
                .as("the key of an open task")
                .isFalse();
        assertThat(store.holdsKey(open, "kept"))
                .as("the key of a finished task")
                .isFalse();
        assertThat(store.holdsKey(open, "removed"))
                .as("the key of another task")
                .isFalse();
        assertThat(store.holdsKey(kept.id(), "removed"))
                .as("the key of a removed task")
                .isFalse();
        assertThat(store.holdsKey(kept.id(), "missing"))
                .as("a key no task holds")
                .isFalse();
    }

    @Test
    void holdsKeyIsFalseOnceTheKeepWindowHasPassed() {
        TaskRecord removed = leased(task().uniqueKey("removed").keyKeep(SHORT));
        assertThat(store.ack(removed.id(), removed.revision(), false, false, null))
                .isTrue();
        TaskRecord kept = leased(task().uniqueKey("kept").keyKeep(SHORT));
        assertThat(store.ack(kept.id(), kept.revision(), true, true, null)).isTrue();

        advance(PAST_SHORT);

        assertThat(store.holdsKey(removed.id(), "removed")).as("removed").isFalse();
        assertThat(store.holdsKey(kept.id(), "kept")).as("finished").isFalse();
    }

    @Test
    void aRemovedTaskSentAgainWithItsKeptKeyAnswersExistsAndIsNotStoredAgain() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(t.id(), t.revision(), false, false, null)).isTrue();
        TaskRecord again =
                task().id(t.id()).uniqueKey("k").keyKeep(Duration.ofHours(1)).build();

        assertThat(store.enqueue(again)).isEqualTo(EnqueueOutcome.EXISTS);
        assertThat(store.enqueueAll(List.of(again))).containsExactly(EnqueueOutcome.EXISTS);

        assertThat(store.task(t.id())).isEmpty();
        assertThat(store.counts().getOrDefault(queue, StateCounts.ZERO)).isEqualTo(StateCounts.ZERO);
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
    }

    @Test
    void failWithoutKeepDeadRemovesTheTaskAndItsKeyStaysTakenUntilTheWindowEnds() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        Instant keptFrom = now();
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, false, null))
                .isEqualTo(FailOutcome.DEAD);
        assertRemovedButKeyTaken(t, "k", keptFrom.plus(SHORT));
        assertThat(store.purge(TaskState.DEAD, Duration.ZERO, 10)).isZero();
        advance(PAST_SHORT);
        put(task().uniqueKey("k"));
    }

    @Test
    void releaseKeysFreesTheKeyOfARemovedTaskOnceItsWindowPassed() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        Instant keptFrom = now();
        assertThat(store.ack(t.id(), t.revision(), false, false, null)).isTrue();
        long releasedEarly = store.releaseKeys(10);
        assumeBefore(keptFrom.plus(SHORT));
        assertThat(releasedEarly).isZero();
        advance(PAST_SHORT);
        assertThat(store.releaseKeys(10)).isEqualTo(releasesKeysOnItsOwn() ? 0 : 1);
        assertThat(store.releaseKeys(10)).isZero();
        assertThat(store.taskByKey("k")).isEmpty();
        put(task().uniqueKey("k"));
    }

    /** The task is gone from every read, while its key, held until {@code heldUntil}, still answers a same-key enqueue with its id. */
    private void assertRemovedButKeyTaken(TaskRecord t, String key, Instant heldUntil) {
        assertThat(store.task(t.id())).isEmpty();
        assertThat(store.taskByKey(key)).isEmpty();
        assertThat(store.counts().getOrDefault(queue, StateCounts.ZERO)).isEqualTo(StateCounts.ZERO);
        for (TaskState state : TaskState.values()) {
            assertThat(store.tasks(state, null, null, 10).items())
                    .as("%s", state)
                    .isEmpty();
        }
        for (int revision : new int[] {t.revision(), t.revision() + 1}) {
            assertThat(store.retry(t.id(), revision)).isEqualTo(RetryOutcome.NOT_FOUND);
            assertThat(store.cancel(t.id(), revision, "no", null)).isEqualTo(CancelOutcome.NOT_FOUND);
            assertThat(store.delete(t.id(), revision, null))
                    .as("a delete would free the key early")
                    .isEqualTo(DeleteOutcome.NOT_FOUND);
        }
        EnqueueOutcome again = store.enqueue(task().uniqueKey(key).build());
        assumeBefore(heldUntil);
        assertThat(again).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
    }

    @Test
    void retryOfADeadKeyedTaskKeepsTheKeyAndHoldsItAsAnOpenTask() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        TaskRecord dead = get(t.id());
        assertThat(dead.keyUntil()).isNotNull();
        assertThat(store.retry(t.id(), dead.revision() - 1)).isEqualTo(RetryOutcome.STALE);
        assertThat(get(t.id())).isEqualTo(dead);

        assertThat(store.retry(t.id(), dead.revision())).isEqualTo(RetryOutcome.RETRIED);

        TaskRecord retried = get(t.id());
        assertThat(retried.state()).isEqualTo(TaskState.QUEUED);
        assertThat(retried.revision()).isEqualTo(dead.revision() + 1);
        assertThat(retried.attempts()).isZero();
        assertThat(retried.uniqueKey()).isEqualTo("k");
        assertThat(retried.keyKeep()).isEqualTo(Duration.ofHours(1));
        assertThat(retried.keyUntil()).isNull();
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(t.id());
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
        assertThat(store.ack(running.id(), running.revision(), true, true, null))
                .isTrue();
        assertThat(get(t.id()).keyUntil()).isNotNull();
    }

    @Test
    void retryOfACancelledKeyedTaskKeepsTheKeyToo() {
        long id = put(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.cancel(id, get(id).revision(), "no", null)).isEqualTo(CancelOutcome.CANCELLED);

        assertThat(store.retry(id, get(id).revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(get(id).uniqueKey()).isEqualTo("k");
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(id));
    }

    @Test
    void retryOfAnExpiredKeyedTaskKeepsTheKeyAndClearsTheDeadline() {
        long id = put(task().uniqueKey("k").keyKeep(Duration.ofHours(1)).startBy(now().plus(SHORT)));
        advance(PAST_SHORT);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(id);

        assertThat(store.retry(id, get(id).revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(get(id).startBy()).isNull();
        assertThat(get(id).keyUntil()).isNull();
        assertThat(store.taskByKey("k")).contains(get(id));
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(id));
        assertThat(claimOne().id()).isEqualTo(id);
    }

    @Test
    void retryKeepsTheKeyPastItsOldWindowAndStartsAFreshWindowOnCompletion() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(MID));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        Instant oldUntil = get(t.id()).keyUntil();
        assertThat(store.retry(t.id(), get(t.id()).revision())).isEqualTo(RetryOutcome.RETRIED);

        advance(PAST_MID);
        store.releaseKeys(10);
        assertThat(store.purge(TaskState.DEAD, Duration.ZERO, 10)).isZero();
        assertThat(store.taskByKey("k")).contains(get(t.id()));
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));

        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(t.id());
        Instant endedFrom = now();
        assertThat(store.ack(running.id(), running.revision(), true, true, null))
                .isTrue();
        TaskRecord finished = get(t.id());
        assertThat(finished.stateChangedAt()).isAfterOrEqualTo(millis(endedFrom));
        assertThat(finished.keyKeep()).isEqualTo(MID);
        assertThat(finished.keyUntil())
                .isAfter(oldUntil)
                .isEqualTo(finished.stateChangedAt().plus(MID));
        EnqueueOutcome whileHeld = store.enqueue(task().uniqueKey("k").build());
        assumeBefore(finished.keyUntil());
        assertThat(whileHeld).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isEqualTo(1);
        assertRemovedButKeyTaken(finished, "k", finished.keyUntil());

        advance(PAST_MID);
        assertThat(store.taskByKey("k")).isEmpty();
        put(task().uniqueKey("k"));
    }

    @Test
    void retryAfterTheKeyWindowPassedAndAnotherTaskTookTheKeyLeavesTheNewHolder() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        advance(PAST_SHORT);
        long newer = put(task().uniqueKey("k"));
        TaskRecord holder = get(newer);
        TaskRecord before = get(t.id());

        assertThat(store.retry(t.id(), before.revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(get(t.id()).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(t.id()).revision()).isEqualTo(before.revision() + 1);
        assertThat(get(t.id()).uniqueKey()).isNull();
        assertThat(get(newer)).isEqualTo(holder);
        assertThat(store.taskByKey("k")).contains(holder);
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(newer));
    }

    @Test
    void retryAfterTheKeyWindowPassedDoesNotTakeTheKey() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        advance(PAST_SHORT);

        assertThat(store.retry(t.id(), get(t.id()).revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(get(t.id()).uniqueKey()).isNull();
        assertThat(get(t.id()).keyUntil()).isNull();
        assertThat(get(t.id()).keyKeep()).isNull();
        assertThat(store.taskByKey("k")).isEmpty();
        long other = put(task().uniqueKey("k"));
        assertThat(claim(2)).extracting(TaskRecord::id).containsExactlyInAnyOrder(t.id(), other);
    }

    @Test
    void retryWithAReleasedUntilDoneKeyAllowsASecondTaskWithThatKey() {
        TaskRecord t = dead();
        assertThat(t.uniqueKey()).isNull();

        assertThat(store.retry(t.id(), t.revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(get(t.id()).uniqueKey()).isNull();
        assertThat(store.taskByKey("k")).isEmpty();
        long other = put(task().uniqueKey("k"));
        assertThat(claim(2)).extracting(TaskRecord::id).containsExactlyInAnyOrder(t.id(), other);
    }

    @Test
    void retryOfAnUnkeyedTaskOnlyRequeuesIt() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        int revision = get(t.id()).revision();

        assertThat(store.retry(t.id(), revision - 1)).isEqualTo(RetryOutcome.STALE);
        assertThat(store.retry(t.id(), revision)).isEqualTo(RetryOutcome.RETRIED);

        TaskRecord retried = get(t.id());
        assertThat(retried.state()).isEqualTo(TaskState.QUEUED);
        assertThat(retried.uniqueKey()).isNull();
        assertThat(retried.keyKeep()).isNull();
        assertThat(store.retry(t.id(), retried.revision())).isEqualTo(RetryOutcome.NOT_RETRYABLE);
    }

    @Test
    void ackWithAWrongRevisionChangesNothing() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision() + 1, true, true, null)).isFalse();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void ackTwiceSucceedsOnlyOnce() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isFalse();
        assertThat(store.ack(t.id(), t.revision() + 1, true, true, null)).isFalse();
    }

    @Test
    void ackAllAppliesEachAckOnItsOwnAndAnswersInOrder() {
        TaskRecord removed = leased(task());
        TaskRecord stale = leased(task());
        TaskRecord held = leased(task().uniqueKey("k").keyKeep(SHORT));
        Instant keptFrom = now();
        List<Boolean> applied = store.ackAll(
                List.of(
                        new LeaseRef(removed.id(), removed.revision()),
                        new LeaseRef(stale.id(), stale.revision() + 1),
                        new LeaseRef(held.id(), held.revision()),
                        new LeaseRef(removed.id(), removed.revision())),
                false,
                false);
        assertThat(applied).containsExactly(true, false, true, false);
        assertThat(store.task(removed.id())).isEmpty();
        assertThat(get(stale.id()).state()).isEqualTo(TaskState.LEASED);
        assertThat(store.task(held.id())).isEmpty();
        EnqueueOutcome whileHeld = store.enqueue(task().uniqueKey("k").build());
        assumeBefore(keptFrom.plus(SHORT));
        assertThat(whileHeld).isEqualTo(new EnqueueOutcome.DuplicateKey(held.id()));
        assertThat(counts().succeeded()).isZero();
        assertThat(counts().leased()).isEqualTo(1);
        assertThat(store.tasks(TaskState.SUCCEEDED, queue, null, 10).items()).isEmpty();
    }

    @Test
    void ackAllKeepsRecordsAndPayloadsWhenAsked() {
        TaskRecord a = leased(task());
        TaskRecord b = leased(task());
        assertThat(store.ackAll(
                        List.of(new LeaseRef(a.id(), a.revision()), new LeaseRef(b.id(), b.revision())), true, true))
                .containsExactly(true, true);
        for (TaskRecord t : List.of(a, b)) {
            TaskRecord done = get(t.id());
            assertThat(done.state()).isEqualTo(TaskState.SUCCEEDED);
            assertThat(done.revision()).isEqualTo(t.revision() + 1);
            assertThat(done.encodedPayload()).isEqualTo("payload");
        }
        assertThat(store.tasks(TaskState.SUCCEEDED, null, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(a.id(), b.id());
    }

    @Test
    void ackAllOfNoLeasesAnswersNothing() {
        assertThat(store.ackAll(List.of(), true, true)).isEmpty();
    }

    // fail

    @Test
    void failWithRetrySchedulesTheTaskAndRecordsTheFailure() {
        TaskRecord t = leased(task());
        Instant retryAt = millis(now().plus(Duration.ofMinutes(10)));
        FailureInfo info = new FailureInfo(1, millis(now()), "java.io.IOException", "disk", "trace");
        assertThat(store.fail(t.id(), t.revision(), info, retryAt, true, null)).isEqualTo(FailOutcome.RETRY);
        TaskRecord after = get(t.id());
        assertThat(after.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(after.dueAt()).isEqualTo(retryAt);
        assertThat(after.attempts()).isEqualTo(1);
        assertThat(after.revision()).isEqualTo(t.revision() + 1);
        assertThat(after.leaseUntil()).isNull();
        assertThat(after.failures()).containsExactly(info);
        assertThat(after.lastFailure()).isEqualTo(info);
    }

    @Test
    void failureWithoutMessageAndStackRoundTripsAsNulls() {
        TaskRecord retried = leased(task());
        FailureInfo info = new FailureInfo(1, millis(now()), "java.io.IOException", null, null);
        assertThat(store.fail(retried.id(), retried.revision(), info, millis(now().plusSeconds(60)), true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(retried.id()).failures()).containsExactly(info);
        assertThat(get(retried.id()).lastFailure().message()).isNull();
        assertThat(get(retried.id()).lastFailure().stack()).isNull();

        TaskRecord dead = leased(task());
        assertThat(store.fail(dead.id(), dead.revision(), info, null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(get(dead.id()).failures()).containsExactly(info);
    }

    @Test
    void failWithoutRetryKillsTheTask() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        TaskRecord after = get(t.id());
        assertThat(after.state()).isEqualTo(TaskState.DEAD);
        assertThat(after.revision()).isEqualTo(t.revision() + 1);
        assertThat(after.failures()).hasSize(1);
    }

    @Test
    void failKillsTheTaskWhenAttemptsAreExhausted() {
        TaskRecord t = leased(task().maxAttempts(1));
        assertThat(store.fail(t.id(), t.revision(), failure(1), now().plusSeconds(5), true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.DEAD);
    }

    @Test
    void failWithoutKeepDeadRemovesTheTask() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, false, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(store.task(t.id())).isEmpty();
    }

    @Test
    void failWithoutKeepDeadStillRetriesWhenAllowed() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision(), failure(1), now().plusSeconds(5), false, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.SCHEDULED);
    }

    @Test
    void failWithAWrongRevisionIsLostAndChangesNothing() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision() + 1, failure(1), null, true, null))
                .isEqualTo(FailOutcome.LOST);
        TaskRecord after = get(t.id());
        assertThat(after.state()).isEqualTo(TaskState.LEASED);
        assertThat(after.failures()).isNull();
    }

    @Test
    void failuresAreBoundedToTheLastTwenty() {
        long id = put(task().maxAttempts(100));
        for (int attempt = 1; attempt <= 25; attempt++) {
            store.promote(10);
            TaskRecord t = claimOne();
            assertThat(store.fail(t.id(), t.revision(), failure(attempt), now().minusSeconds(1), true, null))
                    .isEqualTo(FailOutcome.RETRY);
        }
        List<FailureInfo> failures = get(id).failures();
        assertThat(failures).hasSize(20);
        assertThat(failures.get(0).attempt()).isEqualTo(6);
        assertThat(failures.get(19).attempt()).isEqualTo(25);
    }

    @Test
    void failExpiredHandlesAnExpiredLease() {
        TaskRecord t = leased(task());
        store.unclaim(t.id(), t.revision());
        TaskRecord short1 = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.failExpired(short1.id(), short1.revision(), failure(1), now().minusSeconds(1), true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(short1.id()).state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(get(short1.id()).failures()).hasSize(1);
    }

    @Test
    void failExpiredKillsAnExhaustedTask() {
        put(task().maxAttempts(1));
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.failExpired(t.id(), t.revision(), failure(1), now().plusSeconds(5), true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.DEAD);
    }

    @Test
    void failExpiredIsLostWhenTheLeaseWasRenewed() {
        put(task());
        Instant claimedFrom = now();
        TaskRecord t = claim(1, MID, null).get(0);
        assertThat(t.leaseUntil()).isAfterOrEqualTo(millis(claimedFrom).plus(MID));
        advance(Duration.ofMillis(100));
        store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        assumeBefore(t.leaseUntil());
        advance(PAST_MID);
        assertThat(store.failExpired(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.LOST);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void failExpiredIsLostWhenTheLeaseIsStillRunning() {
        TaskRecord t = leased(task());
        assertThat(store.failExpired(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.LOST);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void failExpiredWithAWrongRevisionIsLost() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.failExpired(t.id(), t.revision() + 1, failure(1), null, true, null))
                .isEqualTo(FailOutcome.LOST);
    }

    // reschedule and unclaim

    @Test
    void rescheduleGivesTheAttemptBack() {
        TaskRecord t = leased(task());
        Instant due = millis(now().plus(Duration.ofHours(1)));
        assertThat(store.reschedule(t.id(), t.revision(), due)).isTrue();
        TaskRecord after = get(t.id());
        assertThat(after.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(after.dueAt()).isEqualTo(due);
        assertThat(after.attempts()).isZero();
        assertThat(after.revision()).isEqualTo(t.revision() + 1);
        assertThat(after.leaseUntil()).isNull();
    }

    @Test
    void unclaimReturnsTheTaskToItsQueueAtItsOldDue() {
        long id = put(task());
        Instant due = get(id).dueAt();
        TaskRecord t = claimOne();
        assertThat(store.unclaim(t.id(), t.revision())).isTrue();
        TaskRecord after = get(id);
        assertThat(after.state()).isEqualTo(TaskState.QUEUED);
        assertThat(after.dueAt()).isEqualTo(due);
        assertThat(after.attempts()).isZero();
        assertThat(after.revision()).isEqualTo(t.revision() + 1);
        assertThat(claim(1)).extracting(TaskRecord::id).containsExactly(id);
    }

    @Test
    void rescheduleAndUnclaimRejectAWrongRevision() {
        TaskRecord t = leased(task());
        assertThat(store.reschedule(t.id(), t.revision() + 1, now().plusSeconds(5)))
                .isFalse();
        assertThat(store.unclaim(t.id(), t.revision() + 1)).isFalse();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
    }

    /**
     * Tries every operation a runner has on its attempt with the given revision and names those the store
     * accepted. A schedule id adds a schedule effect to each operation that takes one.
     */
    private List<String> acceptedOperations(long id, int revision, String scheduleId) {
        List<String> accepted = new ArrayList<>();
        Instant far = millis(now().plusSeconds(5000));
        Supplier<ScheduleEffect> pause = () -> scheduleId == null ? null : ScheduleEffect.pause(scheduleId, id);
        if (store.renew(List.of(new LeaseRef(id, revision)), LEASE).get(0).owned()) {
            accepted.add("renew");
        }
        ScheduleEffect rearm = scheduleId == null ? null : ScheduleEffect.rearm(scheduleId, id, far);
        if (store.ack(id, revision, true, true, rearm)) {
            accepted.add("ack");
        }
        if (store.fail(id, revision, failure(1), null, true, pause.get()) != FailOutcome.LOST) {
            accepted.add("fail");
        }
        if (store.failExpired(id, revision, failure(1), null, true, pause.get()) != FailOutcome.LOST) {
            accepted.add("failExpired");
        }
        if (store.reschedule(id, revision, far)) {
            accepted.add("reschedule");
        }
        if (store.unclaim(id, revision)) {
            accepted.add("unclaim");
        }
        if (store.saveSteps(id, revision, Map.of("stale", new StepResult(millis(now()), "x")))) {
            accepted.add("saveSteps");
        }
        if (store.progress(id, revision, "stale")) {
            accepted.add("progress");
        }
        if (store.finishCancelled(id, revision, "stale", true, pause.get())) {
            accepted.add("finishCancelled");
        }
        if (store.finishCancelledExpired(id, revision, "stale", true, pause.get())) {
            accepted.add("finishCancelledExpired");
        }
        return accepted;
    }

    @Test
    void anOldRunnerAfterATakeoverChangesNothingAndTheNewRunnerCanFinish() {
        TaskRecord a = runningFixedDelayOccurrence("a", SHORT);
        Map<String, StepResult> aSteps = Map.of("one", new StepResult(millis(now()), "1"));
        assertThat(store.saveSteps(a.id(), a.revision(), aSteps)).isTrue();
        assertThat(store.progress(a.id(), a.revision(), "a is here")).isTrue();
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(a.id());
        assertThat(store.failExpired(
                        a.id(),
                        a.revision(),
                        failure(1),
                        now().minusSeconds(1),
                        true,
                        ScheduleEffect.pause("a", a.id())))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(store.promote(10).moved()).isEqualTo(1);
        TaskRecord b = claimOne();
        assertThat(b.id()).isEqualTo(a.id());
        assertThat(b.revision()).isGreaterThan(a.revision());
        Map<String, StepResult> bSteps = new LinkedHashMap<>(aSteps);
        bSteps.put("two", new StepResult(millis(now()), "2"));
        assertThat(store.saveSteps(b.id(), b.revision(), bSteps)).isTrue();
        assertThat(store.progress(b.id(), b.revision(), "b is here")).isTrue();

        TaskRecord expected = get(b.id());
        StateCounts counts = counts();
        Instant parked = nextFireOf("a");
        long scheduleRevision = revisionOf("a");
        List<ScheduleEntry> definitions = allSchedules();
        assertThat(expected.steps()).isEqualTo(bSteps);
        assertThat(expected.failures()).hasSize(1);

        assertThat(acceptedOperations(a.id(), a.revision(), "a")).isEmpty();

        assertThat(get(b.id())).isEqualTo(expected);
        assertThat(store.taskByKey("sched:a")).contains(expected);
        assertThat(counts()).isEqualTo(counts);
        assertThat(nextFireOf("a")).isEqualTo(parked);
        assertThat(revisionOf("a")).isEqualTo(scheduleRevision);
        assertThat(allSchedules()).isEqualTo(definitions);
        assertThat(pausedOf("a")).isFalse();

        Instant rearmed = millis(now().plusSeconds(90));
        ScheduleEffect effect = ScheduleEffect.rearm("a", b.id(), rearmed);
        assertThat(store.ack(b.id(), b.revision(), true, true, effect)).isTrue();
        assertThat(effect.isApplied()).isTrue();
        assertThat(get(b.id()).state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
    }

    @Test
    void everyRunnerOperationIsRejectedWithTheCurrentRevisionOfATaskThatIsNotLeased() {
        for (TaskState state : List.of(
                TaskState.SUCCEEDED,
                TaskState.DEAD,
                TaskState.CANCELLED,
                TaskState.EXPIRED,
                TaskState.QUEUED,
                TaskState.SCHEDULED)) {
            long id = inState(state, task());
            TaskRecord before = get(id);
            StateCounts counts = counts();

            assertThat(acceptedOperations(id, before.revision(), null))
                    .as("%s", state)
                    .isEmpty();

            assertThat(get(id)).as("%s", state).isEqualTo(before);
            assertThat(counts()).as("%s", state).isEqualTo(counts);
        }
    }

    // steps and progress

    @Test
    void saveStepsReplacesTheMapAndKeepsTheRevision() {
        TaskRecord t = leased(task());
        Map<String, StepResult> first = new LinkedHashMap<>();
        first.put("a", new StepResult(millis(now()), "1"));
        assertThat(store.saveSteps(t.id(), t.revision(), first)).isTrue();
        assertThat(get(t.id()).steps()).isEqualTo(first);
        Map<String, StepResult> second = new LinkedHashMap<>(first);
        second.put("b", new StepResult(millis(now()), null));
        assertThat(store.saveSteps(t.id(), t.revision(), second)).isTrue();
        assertThat(get(t.id()).steps()).isEqualTo(second);

        Map<String, StepResult> third = new LinkedHashMap<>();
        third.put("b", new StepResult(millis(now()), "changed"));
        third.put("c", new StepResult(millis(now()), "3"));
        assertThat(store.saveSteps(t.id(), t.revision(), third)).isTrue();
        TaskRecord replaced = get(t.id());
        assertThat(replaced.steps()).isEqualTo(third).doesNotContainKey("a");
        assertThat(replaced.revision()).isEqualTo(t.revision());

        assertThat(store.saveSteps(t.id(), t.revision(), Map.of())).isTrue();
        TaskRecord cleared = get(t.id());
        assertThat(cleared.steps()).isNullOrEmpty();
        assertThat(cleared.revision()).isEqualTo(t.revision());
        assertThat(cleared.state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void saveStepsRequiresTheLeaseRevision() {
        TaskRecord t = leased(task());
        Map<String, StepResult> steps = Map.of("a", new StepResult(millis(now()), "1"));
        assertThat(store.saveSteps(t.id(), t.revision() + 1, steps)).isFalse();
        assertThat(get(t.id()).steps()).isNull();
        long queued = putNew();
        assertThat(store.saveSteps(queued, 0, steps)).isFalse();
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        assertThat(store.saveSteps(t.id(), t.revision() + 1, steps)).isFalse();
        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isFalse();
    }

    @Test
    void aRetryAttemptSeesTheStepsOfTheEarlierOne() {
        TaskRecord t = leased(task());
        Map<String, StepResult> steps = Map.of("a", new StepResult(millis(now()), "1"));
        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isTrue();
        assertThat(store.fail(t.id(), t.revision(), failure(1), now().minusSeconds(1), true, null))
                .isEqualTo(FailOutcome.RETRY);
        store.promote(10);
        TaskRecord second = claimOne();
        assertThat(second.steps()).isEqualTo(steps);
        assertThat(second.attempts()).isEqualTo(2);
    }

    @Test
    void progressIsStoredWithoutChangingTheRevision() {
        TaskRecord t = leased(task());
        assertThat(store.progress(t.id(), t.revision(), "half way")).isTrue();
        TaskRecord after = get(t.id());
        assertThat(after.progress()).isEqualTo("half way");
        assertThat(after.revision()).isEqualTo(t.revision());
    }

    @Test
    void progressRequiresTheLeaseRevision() {
        TaskRecord t = leased(task());
        assertThat(store.progress(t.id(), t.revision() + 1, "x")).isFalse();
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        assertThat(store.progress(t.id(), t.revision() + 1, "x")).isFalse();
        long queued = putNew();
        assertThat(store.progress(queued, 0, "x")).isFalse();
    }

    // maintenance

    @Test
    void promoteMovesDueTasksToQueuedKeepingDue() {
        Instant dueAt = now().plus(SHORT);
        long id = put(task().dueAt(dueAt));
        long later = put(task().dueAt(now().plus(Duration.ofHours(1))));
        Instant due = get(id).dueAt();
        long movedEarly = store.promote(10).moved();
        assumeBefore(dueAt);
        assertThat(movedEarly).isZero();
        advance(PAST_SHORT);
        PromoteResult result = store.promote(10);
        assertThat(result.moved()).isEqualTo(1);
        assertThat(result.nextDue()).isEqualTo(get(later).dueAt());
        TaskRecord t = get(id);
        assertThat(t.state()).isEqualTo(TaskState.QUEUED);
        assertThat(t.dueAt()).isEqualTo(due);
        assertThat(t.revision()).isEqualTo(1);
        assertThat(get(later).state()).isEqualTo(TaskState.SCHEDULED);
    }

    @Test
    void promoteHonorsTheLimitAndReportsNoNextDueWhenDone() {
        for (int i = 0; i < 3; i++) {
            put(task().dueAt(now().plus(SHORT)));
        }
        advance(PAST_SHORT);
        PromoteResult first = store.promote(2);
        assertThat(first.moved()).isEqualTo(2);
        assertThat(first.nextDue()).isNotNull();
        PromoteResult second = store.promote(2);
        assertThat(second.moved()).isEqualTo(1);
        assertThat(second.nextDue()).isNull();
    }

    @Test
    void promotedTasksCanBeClaimed() {
        long id = put(task().dueAt(now().plus(SHORT)));
        advance(PAST_SHORT);
        store.promote(10);
        assertThat(claim(1)).extracting(TaskRecord::id).containsExactly(id);
    }

    @Test
    void reclaimListsExpiredLeasesOldestFirst() {
        put(task().maxAttempts(5).typeId(type));
        TaskRecord first = claim(1, SHORT, null).get(0);
        advance(Duration.ofMillis(60));
        put(task());
        TaskRecord second = claim(1, SHORT, null).get(0);
        TaskRecord running = leased(task());
        advance(Duration.ofMillis(400));
        List<ExpiredLease> expired = store.reclaim(10);
        assertThat(expired).extracting(ExpiredLease::id).containsExactly(first.id(), second.id());
        ExpiredLease e = expired.get(0);
        assertThat(e.revision()).isEqualTo(first.revision());
        assertThat(e.attempts()).isEqualTo(1);
        assertThat(e.maxAttempts()).isEqualTo(5);
        assertThat(e.typeId()).isEqualTo(type);
        assertThat(e.cancelRequested()).isFalse();
        assertThat(store.reclaim(1)).extracting(ExpiredLease::id).containsExactly(first.id());
        assertThat(get(running.id()).state()).isEqualTo(TaskState.LEASED);
        assertThat(get(first.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void reclaimReportsTheQueueAndTypeOfEachExpiredLease() {
        int otherQueue = store.queueId("lease-queue");
        int otherType = store.typeId("lease-type");
        put(task());
        TaskRecord first = claim(1, SHORT, null).get(0);
        put(task(otherQueue, otherType));
        TaskRecord second = claim(1, SHORT, null, otherQueue).get(0);
        advance(PAST_SHORT);

        assertThat(store.reclaim(10))
                .extracting(ExpiredLease::id, ExpiredLease::queueId, ExpiredLease::typeId)
                .containsExactlyInAnyOrder(tuple(first.id(), queue, type), tuple(second.id(), otherQueue, otherType));
    }

    @Test
    void reclaimIgnoresLeasesThatAreStillLive() {
        TaskRecord running = leased(task());
        assertThat(store.reclaim(10)).isEmpty();
        assertThat(get(running.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void reclaimFlagsACancelRequest() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        assertThat(store.cancel(t.id(), t.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::cancelRequested).containsExactly(true);
    }

    @Test
    void purgeRemovesOldTerminalTasksOfThatStateOnly() {
        TaskRecord done = leased(task());
        assertThat(store.ack(done.id(), done.revision(), true, true, null)).isTrue();
        TaskRecord dead = leased(task());
        assertThat(store.fail(dead.id(), dead.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        long queued = putNew();
        advance(Duration.ofMillis(150));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofHours(1), 10)).isZero();
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 10)).isEqualTo(1);
        assertThat(store.task(done.id())).isEmpty();
        assertThat(get(dead.id()).state()).isEqualTo(TaskState.DEAD);
        assertThat(get(queued).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void purgeKeepsTasksYoungerThanTheRetention() {
        Duration retention = Duration.ofSeconds(1);
        TaskRecord a = leased(task());
        assertThat(store.ack(a.id(), a.revision(), true, true, null)).isTrue();
        advance(retention.plusMillis(100));
        TaskRecord b = leased(task());
        Instant keptFrom = now();
        assertThat(store.ack(b.id(), b.revision(), true, true, null)).isTrue();
        long purged = store.purge(TaskState.SUCCEEDED, retention, 10);
        assumeBefore(keptFrom.plus(retention));
        assertThat(purged).isEqualTo(1);
        assertThat(store.task(a.id())).isEmpty();
        assertThat(store.task(b.id())).isPresent();
    }

    @Test
    void purgeHonorsTheLimitOldestFirst() {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TaskRecord t = leased(task());
            assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
            ids.add(t.id());
            advance(Duration.ofMillis(20));
        }
        advance(Duration.ofMillis(200));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(50), 2)).isEqualTo(2);
        assertThat(store.task(ids.get(0))).isEmpty();
        assertThat(store.task(ids.get(1))).isEmpty();
        assertThat(store.task(ids.get(2))).isPresent();
    }

    @Test
    void purgeRemovesATaskPastRetentionWhoseKeyIsStillHeldAndTheKeyStaysTaken() {
        Duration retention = Duration.ofMillis(500);
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofSeconds(2)));
        Instant keptFrom = now();
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        long purgedEarly = store.purge(TaskState.SUCCEEDED, retention, 10);
        assumeBefore(keptFrom.plus(retention));
        assertThat(purgedEarly).isZero();
        assertThat(store.task(t.id())).isPresent();
        advance(Duration.ofMillis(600));
        assertThat(store.purge(TaskState.SUCCEEDED, retention, 10)).isEqualTo(1);
        assertRemovedButKeyTaken(t, "k", keptFrom.plus(Duration.ofSeconds(2)));
        assertThat(store.purge(TaskState.SUCCEEDED, retention, 10)).isZero();
        advance(Duration.ofSeconds(2));
        put(task().uniqueKey("k"));
    }

    @Test
    void purgeRemovesHeldAndUnheldTasksAlikeInFullBatches() {
        int held = 30;
        Duration window = Duration.ofSeconds(20);
        for (int i = 0; i < held; i++) {
            put(task().uniqueKey("held" + i).keyKeep(window));
        }
        for (int done = 0; done < held; ) {
            List<TaskRecord> batch = claim(100);
            for (TaskRecord t : batch) {
                assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
            }
            done += batch.size();
        }
        for (int i = 0; i < 10; i++) {
            TaskRecord t = leased(task());
            assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        }
        advance(Duration.ofMillis(150));
        List<Long> batches = new ArrayList<>();
        for (long n; (n = store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 10)) > 0; ) {
            batches.add(n);
        }
        assertThat(batches).containsExactly(10L, 10L, 10L, 10L);
        assertThat(store.counts().getOrDefault(queue, StateCounts.ZERO)).isEqualTo(StateCounts.ZERO);
        for (int i = 0; i < held; i++) {
            assertThat(store.enqueue(task().uniqueKey("held" + i).build()))
                    .isInstanceOf(EnqueueOutcome.DuplicateKey.class);
        }
        advance(window);
        put(task().uniqueKey("held0"));
    }

    private static final List<TaskState> PURGED_TERMINALS = List.of(TaskState.CANCELLED, TaskState.EXPIRED);

    /** Takes {@code count} tasks to {@code state} one after another, 20 ms apart, and returns their ids in that order. */
    private List<Long> terminalTasks(TaskState state, int count) {
        List<Long> ids = new ArrayList<>();
        if (state == TaskState.EXPIRED) {
            for (int i = 0; i < count; i++) {
                put(task().startBy(now().plus(SHORT)));
            }
            advance(PAST_SHORT);
            for (int i = 0; i < count; i++) {
                List<Long> expired = ids(store.expire(1));
                assertThat(expired).hasSize(1);
                ids.add(expired.get(0));
                advance(Duration.ofMillis(20));
            }
        } else {
            for (int i = 0; i < count; i++) {
                ids.add(inState(state, task()));
                advance(Duration.ofMillis(20));
            }
        }
        return ids;
    }

    @Test
    void purgeRemovesOldCancelledAndExpiredTasksAndKeepsYoungOnes() {
        for (TaskState state : PURGED_TERMINALS) {
            long old = terminalTasks(state, 1).get(0);
            advance(PAST_MID);
            Instant youngFrom = now();
            long young = terminalTasks(state, 1).get(0);
            Instant youngSince = get(young).stateChangedAt();
            assertThat(youngSince).as("%s young", state).isAfterOrEqualTo(millis(youngFrom));
            Instant youngUntil = youngSince.plus(MID);

            long purged = store.purge(state, MID, 10);
            assumeBefore(youngUntil);
            assertThat(purged).as("%s", state).isEqualTo(1);

            assertThat(store.task(old)).as("%s old", state).isEmpty();
            assertThat(get(young).state()).as("%s young", state).isEqualTo(state);
            long again = store.purge(state, MID, 10);
            assumeBefore(youngUntil);
            assertThat(again).as("%s again", state).isZero();
        }
    }

    @Test
    void purgeRemovesCancelledAndExpiredTasksWhoseKeyIsStillHeldAndTheKeyStaysTaken() {
        for (TaskState state : PURGED_TERMINALS) {
            String key = "held-" + state;
            Instant heldFrom = now();
            long id = inState(state, task().uniqueKey(key).keyKeep(MID));
            Instant heldUntil = get(id).keyUntil();
            assertThat(heldUntil)
                    .as("%s key window", state)
                    .isAfterOrEqualTo(millis(heldFrom).plus(MID));
            advance(Duration.ofMillis(50));

            assertThat(store.purge(state, Duration.ofMillis(10), 10))
                    .as("%s held", state)
                    .isEqualTo(1);
            assertThat(store.task(id)).as("%s removed", state).isEmpty();
            assertThat(store.taskByKey(key)).isEmpty();
            EnqueueOutcome whileHeld = store.enqueue(task().uniqueKey(key).build());
            assumeBefore(heldUntil);
            assertThat(whileHeld).isEqualTo(new EnqueueOutcome.DuplicateKey(id));
            assertThat(store.purge(state, Duration.ofMillis(10), 10))
                    .as("%s again", state)
                    .isZero();

            advance(PAST_MID);
            put(task().uniqueKey(key));
        }
    }

    @Test
    void purgeOfCancelledAndExpiredTasksHonorsTheLimitOldestFirst() {
        for (TaskState state : PURGED_TERMINALS) {
            List<Long> ids = terminalTasks(state, 3);
            advance(Duration.ofMillis(200));

            assertThat(store.purge(state, Duration.ofMillis(50), 2))
                    .as("%s", state)
                    .isEqualTo(2);

            assertThat(store.task(ids.get(0))).isEmpty();
            assertThat(store.task(ids.get(1))).isEmpty();
            assertThat(store.task(ids.get(2))).isPresent();
            assertThat(store.purge(state, Duration.ofMillis(50), 2))
                    .as("%s rest", state)
                    .isEqualTo(1);
        }
    }

    @Test
    void purgeOfCancelledOrExpiredTasksLeavesEveryOtherStateAlone() {
        Map<TaskState, Long> ids = new LinkedHashMap<>();
        for (TaskState state : List.of(
                TaskState.SUCCEEDED,
                TaskState.DEAD,
                TaskState.CANCELLED,
                TaskState.EXPIRED,
                TaskState.QUEUED,
                TaskState.SCHEDULED)) {
            ids.put(state, inState(state, task()));
        }
        advance(Duration.ofMillis(150));
        for (TaskState purged : PURGED_TERMINALS) {
            Map<TaskState, TaskRecord> before = new LinkedHashMap<>();
            ids.forEach((state, id) -> {
                if (state != purged && store.task(id).isPresent()) {
                    before.put(state, get(id));
                }
            });

            assertThat(store.purge(purged, Duration.ofMillis(100), 10))
                    .as("%s", purged)
                    .isEqualTo(1);

            assertThat(store.task(ids.get(purged))).isEmpty();
            before.forEach((state, record) ->
                    assertThat(get(record.id())).as("%s", state).isEqualTo(record));
        }
    }

    @Test
    void releaseKeysFreesOnlyExpiredKeys() {
        TaskRecord expiring = leased(task().uniqueKey("short").keyKeep(SHORT));
        Instant keptFrom = now();
        assertThat(store.ack(expiring.id(), expiring.revision(), true, true, null))
                .isTrue();
        TaskRecord lasting = leased(task().uniqueKey("long").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(lasting.id(), lasting.revision(), true, true, null))
                .isTrue();
        long releasedEarly = store.releaseKeys(10);
        assumeBefore(keptFrom.plus(SHORT));
        assertThat(releasedEarly).isZero();
        advance(PAST_SHORT);
        assertThat(store.releaseKeys(10)).isEqualTo(releasesKeysOnItsOwn() ? 0 : 1);
        assertThat(get(expiring.id()).uniqueKey()).isNull();
        assertThat(get(expiring.id()).keyUntil()).isNull();
        assertThat(get(lasting.id()).uniqueKey()).isEqualTo("long");
        assertThat(store.releaseKeys(10)).isZero();
        put(task().uniqueKey("short"));
        assertThat(store.enqueue(task().uniqueKey("long").build()))
                .isEqualTo(new EnqueueOutcome.DuplicateKey(lasting.id()));
    }

    @Test
    void releaseKeysReleasesAtMostTheLimitAcrossFinishedAndRemovedTasks() {
        for (var key : List.of("kept-1", "kept-2")) {
            TaskRecord t = leased(task().uniqueKey(key).keyKeep(SHORT));
            assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        }
        for (var key : List.of("removed-1", "removed-2")) {
            TaskRecord t = leased(task().uniqueKey(key).keyKeep(SHORT));
            assertThat(store.ack(t.id(), t.revision(), false, false, null)).isTrue();
        }
        advance(PAST_SHORT);
        if (releasesKeysOnItsOwn()) {
            assertThat(store.releaseKeys(3)).isZero();
        } else {
            assertThat(store.releaseKeys(3)).isEqualTo(3);
            assertThat(store.releaseKeys(3)).isEqualTo(1);
            assertThat(store.releaseKeys(3)).isZero();
        }
        for (var key : List.of("kept-1", "kept-2", "removed-1", "removed-2")) {
            put(task().uniqueKey(key));
        }
    }

    // recurring

    private StoredSchedule definition(String id, String cron) {
        return new StoredSchedule(id, type, 1, queue, "acme", cron, "{}", 3, 0);
    }

    /** {@link #definition} with other data, so that a put of it replaces the definition. */
    private StoredSchedule changedData(String id, String cron) {
        return new StoredSchedule(id, type, 1, queue, "acme", cron, "{\"v\":2}", 3, 0);
    }

    private TaskRecord occurrence(Instant fireAt) {
        return task().scheduleId("nightly")
                .dueAt(fireAt)
                .state(TaskState.SCHEDULED)
                .build();
    }

    @Test
    void putScheduleStoresTheDefinitionAndNextFire() {
        Instant fire = millis(now().plus(Duration.ofHours(1)));
        store.putSchedule(definition("nightly", "0 0 * * *"), fire, fire);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("nightly", fire));
        List<DueSchedule> due = store.dueSchedules(fire, 10);
        assertThat(due).hasSize(1);
        StoredSchedule stored = due.get(0).definition();
        assertThat(stored.scheduleId()).isEqualTo("nightly");
        assertThat(stored.cron()).isEqualTo("0 0 * * *");
        assertThat(stored.typeId()).isEqualTo(type);
        assertThat(stored.queueId()).isEqualTo(queue);
        assertThat(stored.account()).isEqualTo("acme");
        assertThat(stored.encodedPayload()).isEqualTo("{}");
        assertThat(stored.maxAttempts()).isEqualTo(3);
        assertThat(due.get(0).fireAt()).isEqualTo(fire);
    }

    @Test
    void putScheduleAssignsANewRevisionToEveryChange() {
        Instant fire = millis(now().plusSeconds(60));
        assertThat(store.putSchedule(definition("a", "* * * * *"), fire, fire))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CREATED);
        long first = store.dueSchedules(fire, 10).get(0).definition().revision();
        assertThat(store.putSchedule(changedData("a", "* * * * *"), fire, fire))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CHANGED);
        long second = store.dueSchedules(fire, 10).get(0).definition().revision();
        assertThat(second).isNotEqualTo(first);
        assertThat(store.dueSchedules(fire, 10).get(0).definition().encodedPayload())
                .isEqualTo("{\"v\":2}");
    }

    @Test
    void putScheduleOfTheStoredDefinitionIsUnchangedAndKeepsTheRevision() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(fullDefinition("a"), fire, fire);
        long revision = store.dueSchedules(fire, 10).get(0).definition().revision();

        assertThat(store.putSchedule(fullDefinition("a"), fire.plusSeconds(5), fire))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.UNCHANGED);
        assertThat(store.putSchedule(fullDefinition("a").withPaused(true), fire, fire))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.UNCHANGED);

        var due = store.dueSchedules(fire, 10).get(0);
        assertThat(due.definition().revision()).isEqualTo(revision);
        assertThat(due.definition().paused()).isFalse();
        assertThat(due.fireAt()).isEqualTo(fire);
    }

    @Test
    void aDueScheduleReadBeforeAnUnchangedPutStillCommits() {
        Instant fire = millis(now().plusSeconds(1));
        Instant next = millis(now().plusSeconds(3600));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        long revision = revisionOf("nightly");

        assertThat(store.putSchedule(definition("nightly", "* * * * *"), fire.plusSeconds(5), fire))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.UNCHANGED);
        assertThat(revisionOf("nightly")).isEqualTo(revision);

        TaskRecord occurrence = occurrence(fire);
        assertThat(store.commitSchedule(due, next, occurrence).isCommitted()).isTrue();
        assertThat(get(occurrence.id()).scheduleId()).isEqualTo("nightly");
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("nightly", next));
    }

    @Test
    void putScheduleThatResetsTheNextFireIsAChange() {
        Instant fire = millis(now().plusSeconds(600));
        Instant sooner = millis(now().plusSeconds(60));
        store.putSchedule(fullDefinition("a"), fire, fire);
        long revision = store.dueSchedules(fire, 10).get(0).definition().revision();

        assertThat(store.putSchedule(fullDefinition("a"), sooner, sooner))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CHANGED);

        var due = store.dueSchedules(fire, 10).get(0);
        assertThat(due.fireAt()).isEqualTo(sooner);
        assertThat(due.definition().revision()).isNotEqualTo(revision);
    }

    @Test
    void putScheduleWithAKeepBoundBeforeTheStoredFireIsAChangeEvenWhenTheNewFireIsTheSame() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(fullDefinition("a"), fire, fire);
        long revision = store.dueSchedules(fire, 10).get(0).definition().revision();

        assertThat(store.putSchedule(fullDefinition("a"), fire, fire.minusSeconds(1)))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CHANGED);
        assertThat(store.dueSchedules(fire, 10).get(0).definition().revision()).isNotEqualTo(revision);
    }

    @Test
    void putScheduleComparesTheMaxRunTime() {
        Instant fire = millis(now().plusSeconds(60));
        var base = fullDefinition("a");
        store.putSchedule(base, fire, fire);
        var longer = new StoredSchedule(
                base.scheduleId(),
                base.typeId(),
                base.payloadVersion(),
                base.queueId(),
                base.account(),
                base.cron(),
                base.zone(),
                base.misfire(),
                base.catchUpMaxFires(),
                base.fixedDelay(),
                base.overlapSkip(),
                base.encodedPayload(),
                base.maxAttempts(),
                base.maxRunTime().multipliedBy(2),
                base.paused(),
                0);

        assertThat(store.putSchedule(longer, fire, fire))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CHANGED);
        assertThat(store.dueSchedules(fire, 10).get(0).definition().maxRunTime())
                .isEqualTo(base.maxRunTime().multipliedBy(2));
    }

    private static final class Fields {
        int typeId;
        int payloadVersion;
        int queueId;
        String account;
        String cron;
        String zone;
        String misfire;
        int catchUpMaxFires;
        Duration fixedDelay;
        boolean overlapSkip;
        String encodedPayload;
        int maxAttempts;
        Duration maxRunTime;

        Fields(StoredSchedule d) {
            typeId = d.typeId();
            payloadVersion = d.payloadVersion();
            queueId = d.queueId();
            account = d.account();
            cron = d.cron();
            zone = d.zone();
            misfire = d.misfire();
            catchUpMaxFires = d.catchUpMaxFires();
            fixedDelay = d.fixedDelay();
            overlapSkip = d.overlapSkip();
            encodedPayload = d.encodedPayload();
            maxAttempts = d.maxAttempts();
            maxRunTime = d.maxRunTime();
        }

        StoredSchedule build(String scheduleId) {
            return new StoredSchedule(
                    scheduleId,
                    typeId,
                    payloadVersion,
                    queueId,
                    account,
                    cron,
                    zone,
                    misfire,
                    catchUpMaxFires,
                    fixedDelay,
                    overlapSkip,
                    encodedPayload,
                    maxAttempts,
                    maxRunTime,
                    false,
                    0);
        }
    }

    private StoredSchedule cronBase() {
        return new StoredSchedule(
                "a",
                type,
                1,
                queue,
                "acme",
                "0 3 * * *",
                "UTC",
                "SKIP",
                3,
                null,
                true,
                "{}",
                3,
                Duration.ofSeconds(5),
                false,
                0);
    }

    private StoredSchedule edited(StoredSchedule base, Consumer<Fields> change) {
        Fields fields = new Fields(base);
        change.accept(fields);
        return fields.build(base.scheduleId());
    }

    @SafeVarargs
    private void assertEveryChangeIsReported(StoredSchedule base, Consumer<Fields>... changes) {
        Instant fire = millis(now().plusSeconds(60));
        store.deleteSchedule(base.scheduleId(), null);
        assertThat(store.putSchedule(base, fire, fire))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CREATED);
        for (Consumer<Fields> change : changes) {
            StoredSchedule variant = edited(base, change);
            assertThat(variant).isNotEqualTo(base);
            long before = revisionOf("a");

            assertThat(store.putSchedule(variant, fire, fire))
                    .as("to %s", variant)
                    .extracting(PutScheduleResult::outcome)
                    .isEqualTo(PutScheduleOutcome.CHANGED);

            assertThat(revisionOf("a")).isNotEqualTo(before);
            assertThat(storedDefinition("a").withRevision(0)).isEqualTo(variant);
            assertThat(store.putSchedule(variant, fire, fire))
                    .extracting(PutScheduleResult::outcome)
                    .isEqualTo(PutScheduleOutcome.UNCHANGED);
            before = revisionOf("a");

            assertThat(store.putSchedule(base, fire, fire))
                    .as("back to %s", base)
                    .extracting(PutScheduleResult::outcome)
                    .isEqualTo(PutScheduleOutcome.CHANGED);

            assertThat(revisionOf("a")).isNotEqualTo(before);
            assertThat(storedDefinition("a").withRevision(0)).isEqualTo(base);
        }
    }

    private List<ScheduleEntry> allSchedules() {
        return store.schedules(null, Integer.MAX_VALUE);
    }

    private StoredSchedule storedDefinition(String scheduleId) {
        return allSchedules().stream()
                .map(ScheduleEntry::definition)
                .filter(d -> d.scheduleId().equals(scheduleId))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void putScheduleReportsAChangeOfTheType() {
        int other = store.typeId("other-type");
        assertEveryChangeIsReported(cronBase(), f -> f.typeId = other);
    }

    @Test
    void putScheduleReportsAChangeOfThePayloadVersion() {
        assertEveryChangeIsReported(cronBase(), f -> f.payloadVersion = 2);
    }

    @Test
    void putScheduleReportsAChangeOfTheQueue() {
        int other = store.queueId("other-queue");
        assertEveryChangeIsReported(cronBase(), f -> f.queueId = other);
    }

    @Test
    void putScheduleReportsAChangeOfTheAccountIncludingNullTransitions() {
        assertEveryChangeIsReported(cronBase(), f -> f.account = "other", f -> f.account = null);
        assertEveryChangeIsReported(edited(cronBase(), f -> f.account = null), f -> f.account = "acme");
    }

    @Test
    void putScheduleReportsAChangeOfTheCronExpressionAndOfSwitchingToAFixedDelay() {
        assertEveryChangeIsReported(cronBase(), f -> f.cron = "0 4 * * *", f -> {
            f.cron = null;
            f.fixedDelay = Duration.ofSeconds(60);
        });
    }

    @Test
    void putScheduleReportsAChangeOfTheZoneIncludingNullTransitions() {
        assertEveryChangeIsReported(cronBase(), f -> f.zone = "Asia/Kolkata", f -> f.zone = null);
        assertEveryChangeIsReported(edited(cronBase(), f -> f.zone = null), f -> f.zone = "UTC");
    }

    @Test
    void putScheduleReportsAChangeOfTheMisfirePolicyIncludingNullTransitions() {
        assertEveryChangeIsReported(cronBase(), f -> f.misfire = "CATCH_UP", f -> f.misfire = null);
        assertEveryChangeIsReported(edited(cronBase(), f -> f.misfire = null), f -> f.misfire = "SKIP");
    }

    @Test
    void putScheduleReportsAChangeOfTheCatchUpLimit() {
        assertEveryChangeIsReported(cronBase(), f -> f.catchUpMaxFires = 4);
    }

    @Test
    void putScheduleReportsAChangeOfTheFixedDelayIncludingNullTransitions() {
        assertEveryChangeIsReported(fullDefinition("a"), f -> f.fixedDelay = Duration.ofSeconds(60), f -> {
            f.fixedDelay = null;
            f.cron = "0 3 * * *";
        });
    }

    @Test
    void putScheduleReportsAChangeOfTheOverlapFlag() {
        assertEveryChangeIsReported(cronBase(), f -> f.overlapSkip = false);
    }

    @Test
    void putScheduleReportsAChangeOfThePayloadIncludingNullTransitions() {
        assertEveryChangeIsReported(cronBase(), f -> f.encodedPayload = "{\"x\":1}", f -> f.encodedPayload = null);
        assertEveryChangeIsReported(edited(cronBase(), f -> f.encodedPayload = null), f -> f.encodedPayload = "{}");
    }

    @Test
    void putScheduleReportsAChangeOfTheMaxAttempts() {
        assertEveryChangeIsReported(cronBase(), f -> f.maxAttempts = 4);
    }

    @Test
    void putScheduleReportsAChangeOfTheMaxRunTimeIncludingNullTransitions() {
        assertEveryChangeIsReported(cronBase(), f -> f.maxRunTime = Duration.ofSeconds(6), f -> f.maxRunTime = null);
        assertEveryChangeIsReported(
                edited(cronBase(), f -> f.maxRunTime = null), f -> f.maxRunTime = Duration.ofSeconds(5));
    }

    @Test
    void putScheduleKeepsTheNextFireWhenTheCronIsUnchanged() {
        Instant first = millis(now().plusSeconds(60));
        Instant other = millis(now().plusSeconds(600));
        store.putSchedule(definition("a", "* * * * *"), first, first);
        store.putSchedule(definition("a", "* * * * *"), other, other);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", first));
        store.putSchedule(definition("a", "0 * * * *"), other, other);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", other));
    }

    @Test
    void putScheduleResetsTheNextFireWhenTheZoneOrFixedDelayChanges() {
        Instant first = millis(now().plusSeconds(60));
        Instant second = millis(now().plusSeconds(600));
        Instant third = millis(now().plusSeconds(1200));
        store.putSchedule(fullDefinition("a"), first, first);
        store.putSchedule(fullDefinition("a"), second, second);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", first));
        StoredSchedule base = fullDefinition("a");
        StoredSchedule otherZone = new StoredSchedule(
                "a",
                type,
                2,
                queue,
                "acme",
                null,
                "UTC",
                "CATCH_UP",
                5,
                Duration.ofSeconds(90),
                true,
                "{}",
                4,
                null,
                false,
                0);
        store.putSchedule(otherZone, second, second);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", second));
        StoredSchedule otherDelay = new StoredSchedule(
                "a",
                type,
                2,
                queue,
                "acme",
                null,
                "UTC",
                "CATCH_UP",
                5,
                Duration.ofSeconds(60),
                true,
                "{}",
                4,
                null,
                false,
                0);
        store.putSchedule(otherDelay, third, third);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", third));
        store.putSchedule(base, first, first);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", first));
    }

    @Test
    void deleteScheduleReportsWhetherItExisted() {
        store.putSchedule(definition("a", "* * * * *"), millis(now().plusSeconds(60)), millis(now().plusSeconds(60)));
        assertThat(store.deleteSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.deleteSchedule("a", null)).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.nextFireTimes()).isEmpty();
    }

    @Test
    void dueSchedulesReturnsOnlyDueDefinitionsUpToTheLimit() {
        Instant base = millis(now());
        store.putSchedule(definition("a", "* * * * *"), base.plusSeconds(10), base.plusSeconds(10));
        store.putSchedule(definition("b", "* * * * *"), base.plusSeconds(20), base.plusSeconds(20));
        store.putSchedule(definition("c", "* * * * *"), base.plusSeconds(3600), base.plusSeconds(3600));
        assertThat(store.dueSchedules(base, 10)).isEmpty();
        assertThat(store.dueSchedules(base.plusSeconds(30), 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactlyInAnyOrder("a", "b");
        assertThat(store.dueSchedules(base.plusSeconds(30), 1)).hasSize(1);
        assertThat(store.dueSchedules(base.plusSeconds(10), 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("a");
    }

    @Test
    void commitScheduleStoresTheOccurrenceAndAdvancesTheDefinition() {
        Instant fire = millis(now().plusSeconds(1));
        Instant next = millis(now().plusSeconds(3600));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        TaskRecord occurrence = occurrence(fire);
        assertThat(store.commitSchedule(due, next, occurrence).isCommitted()).isTrue();
        TaskRecord stored = get(occurrence.id());
        assertThat(stored.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(stored.dueAt()).isEqualTo(fire);
        assertThat(stored.scheduleId()).isEqualTo("nightly");
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("nightly", next));
        assertThat(store.dueSchedules(fire, 10)).isEmpty();
    }

    @Test
    void commitScheduleStoresTheOccurrenceAtTheRevisionItWasGiven() {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        TaskRecord occurrence = task().scheduleId("nightly")
                .dueAt(fire)
                .state(TaskState.SCHEDULED)
                .revision(123_456)
                .build();
        assertThat(store.commitSchedule(due, millis(now().plusSeconds(3600)), occurrence)
                        .isCommitted())
                .isTrue();
        assertThat(get(occurrence.id()).revision()).isEqualTo(123_456);
    }

    @Test
    void commitScheduleWithAStaleDueDoesNothing() {
        Instant fire = millis(now().plusSeconds(1));
        Instant next = millis(now().plusSeconds(3600));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        assertThat(store.commitSchedule(due, next, occurrence(fire))).isEqualTo(CommitOutcome.STORED);
        TaskRecord again = occurrence(fire);
        assertThat(store.commitSchedule(due, next.plusSeconds(60), again)).isEqualTo(CommitOutcome.NOT_COMMITTED);
        assertThat(store.task(again.id())).isEmpty();
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("nightly", next));
    }

    @Test
    void commitScheduleIsRejectedAfterTheDefinitionWasReplacedOrDeleted() {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        store.putSchedule(changedData("nightly", "* * * * *"), fire, fire);
        TaskRecord occurrence = occurrence(fire);
        assertThat(store.commitSchedule(due, fire.plusSeconds(60), occurrence).isCommitted())
                .isFalse();
        assertThat(store.task(occurrence.id())).isEmpty();
        DueSchedule fresh = store.dueSchedules(fire, 10).get(0);
        assertThat(store.deleteSchedule("nightly", null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.commitSchedule(fresh, fire.plusSeconds(60), occurrence).isCommitted())
                .isFalse();
    }

    @Test
    void twoThreadsCommittingTheSameDueScheduleSucceedExactlyOnce() throws Exception {
        Instant fire = millis(now().plusSeconds(1));
        Instant next = millis(now().plusSeconds(3600));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        TaskRecord a = occurrence(fire);
        TaskRecord b = occurrence(fire);
        AtomicInteger turn = new AtomicInteger();
        List<Boolean> results = runConcurrently(2, () -> {
            TaskRecord mine = turn.getAndIncrement() == 0 ? a : b;
            return store.commitSchedule(due, next, mine).isCommitted();
        });
        assertThat(results).containsExactlyInAnyOrder(true, false);
        long stored = List.of(a, b).stream()
                .filter(t -> store.task(t.id()).isPresent())
                .count();
        assertThat(stored).isEqualTo(1);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("nightly", next));
    }

    @Test
    void commitScheduleWithATakenOccurrenceIdWritesNothing() {
        long taken = putNew();
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        TaskRecord occurrence = task().id(taken)
                .scheduleId("nightly")
                .dueAt(fire)
                .state(TaskState.SCHEDULED)
                .build();
        assertThat(store.commitSchedule(due, millis(now().plusSeconds(3600)), occurrence)
                        .isCommitted())
                .isFalse();
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("nightly", fire));
        assertThat(get(taken).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(taken).scheduleId()).isNull();
    }

    @Test
    void commitScheduleOfAFixedDelayWithATakenOccurrenceIdLeavesItUnparked() {
        long taken = putNew();
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(fullDefinition("a"), fire, fire);
        DueSchedule due = dueOf("a", fire);
        TaskRecord occurrence =
                fixedDelayOccurrence("a", fire).toBuilder().id(taken).build();
        assertThat(store.commitSchedule(due, parkedFire(), occurrence).isCommitted())
                .isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire);
        assertThat(get(taken).uniqueKey()).isNull();

        TaskRecord fresh = fixedDelayOccurrence("a", fire);
        Instant parked = parkedFire();
        assertThat(store.commitSchedule(dueOf("a", fire), parked, fresh).isCommitted())
                .isTrue();
        assertThat(get(fresh.id()).state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(nextFireOf("a")).isEqualTo(parked);
    }

    // wave 2: cancel, retry, delete, pause, expire, runners

    private StateCounts counts() {
        return store.counts().getOrDefault(queue, StateCounts.ZERO);
    }

    private TaskRecord dead() {
        TaskRecord t = leased(task().maxAttempts(1).uniqueKey("k"));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        return get(t.id());
    }

    private TaskRecord expired() {
        long id = put(task().uniqueKey("k").startBy(now().plus(SHORT)));
        advance(PAST_SHORT);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(id);
        return get(id);
    }

    private TaskRecord cancelled() {
        long id = put(task().uniqueKey("k"));
        assertThat(store.cancel(id, get(id).revision(), "nope", null)).isEqualTo(CancelOutcome.CANCELLED);
        return get(id);
    }

    @Test
    void finishCancelledEndsALeasedTaskAndBumpsTheRevisionOnce() {
        TaskRecord t = leased(task().uniqueKey("k"));
        Instant before = now();
        assertThat(store.finishCancelled(t.id(), t.revision(), "stop", true, null))
                .isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.revision()).isEqualTo(t.revision() + 1);
        assertThat(done.cancelReason()).isEqualTo("stop");
        assertThat(done.cancelRequestedAt()).isNotNull();
        assertThat(done.leaseUntil()).isNull();
        assertThat(done.runnerId()).isEqualTo(runner);
        assertThat(done.encodedPayload()).isEqualTo("payload");
        assertThat(done.stateChangedAt()).isAfterOrEqualTo(millis(before));
        assertThat(done.uniqueKey()).isNull();
        assertThat(counts().cancelled()).isEqualTo(1);
        assertThat(counts().leased()).isZero();
    }

    @Test
    void finishCancelledKeepsAnEarlierCancelRequestTimeAndDropsThePayloadOnRequest() {
        TaskRecord t = leased(task());
        assertThat(store.cancel(t.id(), t.revision(), "ask", null)).isEqualTo(CancelOutcome.REQUESTED);
        Instant requested = get(t.id()).cancelRequestedAt();
        advance(Duration.ofMillis(50));
        assertThat(store.finishCancelled(t.id(), t.revision(), "done", false, null))
                .isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.cancelRequestedAt()).isEqualTo(requested);
        assertThat(done.cancelReason()).isEqualTo("done");
        assertThat(done.encodedPayload()).isNull();
    }

    @Test
    void finishCancelledIsFencedByRevisionAndState() {
        TaskRecord t = leased(task());
        assertThat(store.finishCancelled(t.id(), t.revision() + 1, "x", true, null))
                .isFalse();
        assertThat(store.finishCancelled(t.id(), t.revision() - 1, "x", true, null))
                .isFalse();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
        assertThat(store.finishCancelled(t.id(), t.revision(), "x", true, null)).isTrue();
        assertThat(store.finishCancelled(t.id(), t.revision() + 1, "x", true, null))
                .isFalse();
        long queued = putNew();
        assertThat(store.finishCancelled(queued, get(queued).revision(), "x", true, null))
                .isFalse();
        assertThat(store.finishCancelled(store.allocateIds(1), 1, "x", true, null))
                .isFalse();
    }

    @Test
    void finishCancelledExpiredEndsATaskWhoseLeaseExpired() {
        put(task().uniqueKey("k"));
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(t.id());
        assertThat(store.finishCancelledExpired(t.id(), t.revision(), "stop", true, null))
                .isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.revision()).isEqualTo(t.revision() + 1);
        assertThat(done.cancelReason()).isEqualTo("stop");
        assertThat(done.uniqueKey()).isNull();
    }

    @Test
    void finishCancelledExpiredIsANoOpAfterTheLeaseWasRenewed() {
        Duration firstLease = Duration.ofSeconds(2);
        TaskRecord t = runningFixedDelayOccurrence("a", firstLease);
        Instant parked = nextFireOf("a");
        List<LeaseStatus> status = store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        assumeBefore(t.leaseUntil());
        assertThat(status).extracting(LeaseStatus::owned).containsExactly(true);
        advance(firstLease.plusMillis(100));
        TaskRecord renewed = get(t.id());
        assertThat(renewed.leaseUntil()).isAfter(now());
        ScheduleEffect effect = ScheduleEffect.pause("a", t.id());

        assertThat(store.finishCancelledExpired(t.id(), t.revision(), "stop", true, effect))
                .isFalse();

        assertThat(get(t.id())).isEqualTo(renewed);
        assertThat(store.taskByKey("sched:a")).contains(renewed);
        assertThat(effect.isApplied()).isFalse();
        assertThat(pausedOf("a")).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(parked);
        assertThat(store.reclaim(10)).isEmpty();
    }

    @Test
    void finishCancelledExpiredIsFencedByRevisionAndState() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.finishCancelledExpired(t.id(), t.revision() + 1, "x", true, null))
                .isFalse();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
        long queued = putNew();
        assertThat(store.finishCancelledExpired(queued, get(queued).revision(), "x", true, null))
                .isFalse();
    }

    @Test
    void finishCancelledByTheRunnerIgnoresTheLeaseExpiry() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.finishCancelled(t.id(), t.revision(), "stop", true, null))
                .isTrue();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.CANCELLED);
    }

    @Test
    void finishCancelledStartsTheKeyKeepWindow() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.finishCancelled(t.id(), t.revision(), "x", true, null)).isTrue();
        assertThat(get(t.id()).uniqueKey()).isEqualTo("k");
        assertThat(get(t.id()).keyUntil()).isNotNull();
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
    }

    @Test
    void aCancelRequestStaysWithATaskThatGoesBackToWaiting() {
        for (String way : List.of("fail with a retry", "reschedule", "unclaim")) {
            TaskRecord t = leased(task());
            assertThat(store.cancel(t.id(), t.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
            Instant requested = get(t.id()).cancelRequestedAt();
            boolean back = switch (way) {
                case "fail with a retry" ->
                    store.fail(t.id(), t.revision(), failure(1), millis(now().minusSeconds(1)), true, null)
                            == FailOutcome.RETRY;
                case "reschedule" -> store.reschedule(t.id(), t.revision(), millis(now().minusSeconds(1)));
                default -> store.unclaim(t.id(), t.revision());
            };
            assertThat(back).as(way).isTrue();
            TaskRecord waiting = get(t.id());
            assertThat(waiting.cancelRequestedAt()).as(way).isEqualTo(requested);
            assertThat(waiting.cancelReason()).as(way).isEqualTo("stop");

            store.promote(10);
            TaskRecord again = claimOne();
            assertThat(again.id()).as(way).isEqualTo(t.id());
            assertThat(again.cancelRequestedAt())
                    .as(way + ", the claimed record")
                    .isEqualTo(requested);
            assertThat(store.renew(List.of(new LeaseRef(t.id(), again.revision())), LEASE))
                    .as(way + ", renew of the next attempt")
                    .containsExactly(new LeaseStatus(t.id(), true, true));
            assertThat(store.finishCancelled(t.id(), again.revision(), "stop", true, null))
                    .as(way)
                    .isTrue();
        }
    }

    @Test
    void aCancelWithoutAReasonRemovesTheReasonOfAnEarlierRequest() {
        TaskRecord running = leased(task());
        assertThat(store.cancel(running.id(), running.revision(), "first", null))
                .isEqualTo(CancelOutcome.REQUESTED);
        assertThat(store.cancel(running.id(), running.revision(), null, null)).isEqualTo(CancelOutcome.REQUESTED);
        assertThat(get(running.id()).cancelReason()).as("a second request").isNull();
        assertThat(store.cancel(running.id(), running.revision(), "again", null))
                .isEqualTo(CancelOutcome.REQUESTED);
        assertThat(store.finishCancelled(running.id(), running.revision(), null, true, null))
                .isTrue();
        assertThat(get(running.id()).cancelReason()).as("finishCancelled").isNull();
        assertThat(get(running.id()).cancelRequestedAt()).isNotNull();

        put(task());
        TaskRecord expired = claim(1, SHORT, null).get(0);
        assertThat(store.cancel(expired.id(), expired.revision(), "first", null))
                .isEqualTo(CancelOutcome.REQUESTED);
        advance(PAST_SHORT);
        assertThat(store.finishCancelledExpired(expired.id(), expired.revision(), null, true, null))
                .isTrue();
        assertThat(get(expired.id()).cancelReason())
                .as("finishCancelledExpired")
                .isNull();

        TaskRecord unclaimed = leased(task());
        assertThat(store.cancel(unclaimed.id(), unclaimed.revision(), "first", null))
                .isEqualTo(CancelOutcome.REQUESTED);
        assertThat(store.unclaim(unclaimed.id(), unclaimed.revision())).isTrue();
        TaskRecord queued = get(unclaimed.id());
        assertThat(store.cancel(queued.id(), queued.revision(), null, null)).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(get(queued.id()).cancelReason())
                .as("cancel of a waiting task")
                .isNull();
    }

    @Test
    void cancelOnAQueuedTaskCancelsItAtOnce() {
        long id = put(task().uniqueKey("k"));
        TaskRecord before = get(id);
        assertThat(store.cancel(id, before.revision(), "why", null)).isEqualTo(CancelOutcome.CANCELLED);
        TaskRecord t = get(id);
        assertThat(t.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(t.revision()).isEqualTo(before.revision() + 1);
        assertThat(t.cancelReason()).isEqualTo("why");
        assertThat(t.cancelRequestedAt()).isNotNull();
        assertThat(t.uniqueKey()).isNull();
        assertThat(counts().queued()).isZero();
        assertThat(counts().cancelled()).isEqualTo(1);
        assertThat(claim(1)).isEmpty();
        put(task().uniqueKey("k"));
    }

    @Test
    void cancelOnAScheduledTaskCancelsItAtOnce() {
        long id = put(task().dueAt(now().plus(Duration.ofHours(1))));
        assertThat(store.cancel(id, get(id).revision(), "later", null)).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(get(id).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(counts().scheduled()).isZero();
        assertThat(counts().cancelled()).isEqualTo(1);
        assertThat(store.promote(10).nextDue()).isNull();
    }

    @Test
    void cancelOnALeasedTaskOnlyRecordsTheRequestAndKeepsTheRevision() {
        TaskRecord t = leased(task());
        assertThat(store.cancel(t.id(), t.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
        TaskRecord after = get(t.id());
        assertThat(after.state()).isEqualTo(TaskState.LEASED);
        assertThat(after.revision()).isEqualTo(t.revision());
        assertThat(after.cancelReason()).isEqualTo("stop");
        assertThat(after.cancelRequestedAt()).isNotNull();
        assertThat(store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE))
                .containsExactly(new LeaseStatus(t.id(), true, true));
        assertThat(counts().leased()).isEqualTo(1);
    }

    @Test
    void cancelWithAnotherRevisionIsStaleAndChangesNothing() {
        long id = putNew();
        int revision = get(id).revision();
        assertThat(store.cancel(id, revision + 1, "x", null)).isEqualTo(CancelOutcome.STALE);
        assertThat(get(id).state()).isEqualTo(TaskState.QUEUED);
        TaskRecord t = claimOne();
        assertThat(store.cancel(id, revision, "x", null)).isEqualTo(CancelOutcome.STALE);
        assertThat(get(id).cancelRequestedAt()).isNull();
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        assertThat(counts().succeeded()).isEqualTo(1);
    }

    @Test
    void cancelOfAMissingTaskIsNotFoundWhateverTheRevision() {
        long missing = store.allocateIds(1);
        for (int revision : new int[] {0, 1, 7}) {
            assertThat(store.cancel(missing, revision, "x", null))
                    .as("revision %d", revision)
                    .isEqualTo(CancelOutcome.NOT_FOUND);
        }
    }

    @Test
    void cancelOfATaskInAFinalStateIsFinishedAndChangesNothing() {
        for (TaskState state : List.of(TaskState.SUCCEEDED, TaskState.DEAD, TaskState.CANCELLED, TaskState.EXPIRED)) {
            long id = inState(state, task());
            TaskRecord before = get(id);
            assertThat(store.cancel(id, before.revision(), "x", null))
                    .as("%s", state)
                    .isEqualTo(CancelOutcome.FINISHED);
            assertThat(get(id)).as("%s", state).isEqualTo(before);
        }
    }

    @Test
    void cancelOfATaskInAFinalStateWithAnotherRevisionIsStale() {
        for (TaskState state : List.of(TaskState.SUCCEEDED, TaskState.DEAD, TaskState.CANCELLED, TaskState.EXPIRED)) {
            long id = inState(state, task());
            assertThat(store.cancel(id, get(id).revision() + 1, "x", null))
                    .as("%s", state)
                    .isEqualTo(CancelOutcome.STALE);
        }
    }

    @Test
    void retryReturnsADeadTaskToTheQueueWithItsHistory() {
        TaskRecord t = dead();
        assertThat(t.uniqueKey()).isNull();
        assertThat(store.retry(t.id(), t.revision())).isEqualTo(RetryOutcome.RETRIED);
        TaskRecord r = get(t.id());
        assertThat(r.state()).isEqualTo(TaskState.QUEUED);
        assertThat(r.attempts()).isZero();
        assertThat(r.revision()).isEqualTo(t.revision() + 1);
        assertThat(r.failures()).hasSize(1);
        assertThat(r.runnerId()).isNull();
        assertThat(r.leaseUntil()).isNull();
        assertThat(r.cancelRequestedAt()).isNull();
        assertThat(r.cancelReason()).isNull();
        assertThat(r.uniqueKey()).isNull();
        assertThat(r.keyUntil()).isNull();
        assertThat(r.dueAt()).isAfterOrEqualTo(millis(t.stateChangedAt()));
        assertThat(counts().dead()).isZero();
        assertThat(counts().queued()).isEqualTo(1);
        assertThat(claimOne().id()).isEqualTo(t.id());
    }

    @Test
    void retryWorksFromCancelledAndExpiredWithReleasedKeys() {
        TaskRecord c = cancelled();
        assertThat(store.retry(c.id(), c.revision())).isEqualTo(RetryOutcome.RETRIED);
        assertThat(get(c.id()).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(c.id()).cancelReason()).isNull();
        assertThat(get(c.id()).uniqueKey()).isNull();
        TaskRecord e = expired();
        assertThat(store.retry(e.id(), e.revision())).isEqualTo(RetryOutcome.RETRIED);
        assertThat(get(e.id()).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(e.id()).attempts()).isZero();
        long other = put(task().uniqueKey("k"));
        assertThat(get(other).uniqueKey()).isEqualTo("k");
        assertThat(counts().queued()).isEqualTo(3);
        assertThat(counts().cancelled()).isZero();
        assertThat(counts().expired()).isZero();
    }

    @Test
    void retryRefusesOtherStatesAndStaleRevisions() {
        TaskRecord q = get(putNew());
        assertThat(store.retry(q.id(), q.revision())).isEqualTo(RetryOutcome.NOT_RETRYABLE);
        assertThat(get(q.id())).isEqualTo(q);
        TaskRecord scheduled = get(put(task().dueAt(now().plus(Duration.ofHours(1)))));
        assertThat(store.retry(scheduled.id(), scheduled.revision())).isEqualTo(RetryOutcome.NOT_RETRYABLE);
        assertThat(get(scheduled.id())).isEqualTo(scheduled);
        TaskRecord l = claimOne();
        assertThat(l.id()).isEqualTo(q.id());
        assertThat(store.retry(l.id(), l.revision())).isEqualTo(RetryOutcome.NOT_RETRYABLE);
        assertThat(get(l.id())).isEqualTo(l);
        assertThat(store.ack(l.id(), l.revision(), true, true, null)).isTrue();
        TaskRecord s = get(l.id());
        assertThat(store.retry(s.id(), s.revision())).isEqualTo(RetryOutcome.NOT_RETRYABLE);
        assertThat(get(s.id())).isEqualTo(s);
        TaskRecord d = dead();
        assertThat(store.retry(d.id(), d.revision() - 1)).isEqualTo(RetryOutcome.STALE);
        assertThat(get(d.id())).isEqualTo(d);
    }

    @Test
    void retryOfAMissingTaskIsNotFoundWhateverTheRevision() {
        long missing = store.allocateIds(1);
        for (int revision : new int[] {0, 1, 7}) {
            assertThat(store.retry(missing, revision))
                    .as("revision %d", revision)
                    .isEqualTo(RetryOutcome.NOT_FOUND);
        }
    }

    @Test
    void retryOfATaskInAnotherStateWithAnotherRevisionIsStale() {
        for (TaskState state : List.of(TaskState.QUEUED, TaskState.SCHEDULED, TaskState.LEASED, TaskState.SUCCEEDED)) {
            long id = inState(state, task());
            TaskRecord before = get(id);
            assertThat(store.retry(id, before.revision() + 1)).as("%s", state).isEqualTo(RetryOutcome.STALE);
            assertThat(get(id)).as("%s", state).isEqualTo(before);
        }
    }

    @Test
    void deleteRemovesTasksInEveryStateButLeased() {
        long expiredId = inState(TaskState.EXPIRED, task().uniqueKey("kx").keyKeep(Duration.ofHours(1)));
        assertThat(store.taskByKey("kx")).map(TaskRecord::id).contains(expiredId);
        TaskRecord d = dead();
        TaskRecord c = cancelled();
        TaskRecord ok = leased(task());
        assertThat(store.ack(ok.id(), ok.revision(), true, true, null)).isTrue();
        long queued = putNew();
        long scheduled = put(task().dueAt(now().plus(Duration.ofHours(1))));
        for (long id : List.of(queued, scheduled, ok.id(), d.id(), c.id(), expiredId)) {
            assertThat(store.delete(id, get(id).revision(), null)).isEqualTo(DeleteOutcome.DELETED);
            assertThat(store.task(id)).isEmpty();
        }
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
        assertThat(store.promote(10).nextDue()).isNull();
        assertThat(store.expire(10)).isEmpty();
        assertThat(store.taskByKey("kx")).isEmpty();
        put(task().uniqueKey("kx"));
    }

    @Test
    void deleteReleasesTheUniqueKey() {
        long id = put(task().uniqueKey("k"));
        assertThat(store.delete(id, get(id).revision(), null)).isEqualTo(DeleteOutcome.DELETED);
        put(task().uniqueKey("k"));
    }

    @Test
    void deleteOfALeasedTaskIsLeasedAndChangesNothing() {
        TaskRecord l = leased(task());
        assertThat(store.delete(l.id(), l.revision(), null)).isEqualTo(DeleteOutcome.LEASED);
        assertThat(get(l.id())).isEqualTo(l);
        assertThat(counts().leased()).isEqualTo(1);
    }

    @Test
    void deleteWithAnotherRevisionIsStaleAndChangesNothing() {
        long q = putNew();
        TaskRecord before = get(q);
        assertThat(store.delete(q, before.revision() + 1, null)).isEqualTo(DeleteOutcome.STALE);
        assertThat(get(q)).isEqualTo(before);
        assertThat(counts().queued()).isEqualTo(1);
    }

    @Test
    void deleteOfALeasedTaskWithAnotherRevisionIsStale() {
        TaskRecord l = leased(task());
        assertThat(store.delete(l.id(), l.revision() + 1, null)).isEqualTo(DeleteOutcome.STALE);
        assertThat(get(l.id())).isEqualTo(l);
    }

    @Test
    void deleteOfAMissingTaskIsNotFoundWhateverTheRevision() {
        long missing = store.allocateIds(1);
        for (int revision : new int[] {0, 1, 7}) {
            assertThat(store.delete(missing, revision, null))
                    .as("revision %d", revision)
                    .isEqualTo(DeleteOutcome.NOT_FOUND);
        }
    }

    @Test
    void pausedQueuesAreSkippedByClaimUntilResumed() {
        int second = store.queueId("second");
        putNew();
        long other = put(task(second, type));
        store.pauseQueue(queue, true);
        assertThat(claim(10, queue, second)).extracting(TaskRecord::id).containsExactly(other);
        assertThat(claim(10)).isEmpty();
        assertThat(store.queues()).contains(new QueueInfo(queue, "main", true), new QueueInfo(second, "second", false));
        store.pauseQueue(queue, false);
        assertThat(claim(10)).hasSize(1);
        assertThat(store.queues()).contains(new QueueInfo(queue, "main", false));
    }

    @Test
    void queuesListsEveryRegisteredQueueInIdOrder() {
        int second = store.queueId("second");
        assertThat(store.queues())
                .containsExactly(
                        new QueueInfo(idle, "idle", false),
                        new QueueInfo(queue, "main", false),
                        new QueueInfo(second, "second", false));
    }

    @Test
    void pausingAQueueKeepsTasksQueuedAndEnqueueWorks() {
        store.pauseQueue(queue, true);
        long id = putNew();
        assertThat(get(id).state()).isEqualTo(TaskState.QUEUED);
        assertThat(store.promote(10).moved()).isZero();
        assertThat(counts().queued()).isEqualTo(1);
    }

    @Test
    void aPausedQueueStillPromotesScheduledWorkAndExpiresOverdueWorkWhileClaimsStayBlocked() {
        Duration window = Duration.ofSeconds(2);
        store.pauseQueue(queue, true);
        Instant dueAt = now().plus(window);
        long scheduled = put(task().dueAt(dueAt));
        TaskRecord waiting = get(scheduled);
        assumeBefore(dueAt);
        assertThat(waiting.state()).isEqualTo(TaskState.SCHEDULED);
        long overdue = put(task().startBy(now().plus(window)));
        long lateScheduled = put(task().dueAt(now().plus(Duration.ofHours(1))).startBy(now().plus(window)));
        advance(window.plusMillis(100));

        assertThat(store.promote(10).moved()).isEqualTo(1);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactlyInAnyOrder(overdue, lateScheduled);

        assertThat(get(scheduled).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(overdue).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(get(lateScheduled).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(counts()).isEqualTo(new StateCounts(1, 0, 0, 0, 0, 0, 2));
        assertThat(claim(10)).isEmpty();
        assertThat(get(scheduled).attempts()).isZero();

        store.pauseQueue(queue, false);
        assertThat(claim(10)).extracting(TaskRecord::id).containsExactly(scheduled);
    }

    @Test
    void claimNeverReturnsATaskWhoseStartByHasPassed() {
        Instant startBy = now().plus(SHORT);
        long late = put(task().startBy(startBy));
        long fine = put(task().startBy(now().plus(Duration.ofHours(1))));
        List<TaskRecord> claimedEarly = claim(10);
        assumeBefore(startBy);
        assertThat(claimedEarly).extracting(TaskRecord::id).containsExactlyInAnyOrder(late, fine);
        TaskRecord a = get(late);
        assertThat(store.unclaim(a.id(), a.revision())).isTrue();
        TaskRecord b = get(fine);
        assertThat(store.unclaim(b.id(), b.revision())).isTrue();
        advance(PAST_SHORT);
        assertThat(claim(10)).extracting(TaskRecord::id).containsExactly(fine);
        assertThat(get(late).attempts()).isZero();
    }

    @Test
    void expireMovesOverdueQueuedAndScheduledTasksAndReleasesKeys() {
        Instant startBy = now().plus(SHORT);
        long queued = put(task().uniqueKey("q").startBy(startBy));
        long scheduled =
                put(task().uniqueKey("s").dueAt(now().plus(Duration.ofHours(1))).startBy(startBy));
        long keep = put(task().startBy(now().plus(Duration.ofHours(1))));
        long none = putNew();
        int revision = get(queued).revision();
        List<Long> expiredEarly = ids(store.expire(10));
        assumeBefore(startBy);
        assertThat(expiredEarly).isEmpty();
        advance(PAST_SHORT);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactlyInAnyOrder(queued, scheduled);
        assertThat(get(queued).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(get(queued).revision()).isEqualTo(revision + 1);
        assertThat(get(queued).uniqueKey()).isNull();
        assertThat(get(scheduled).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(get(keep).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(none).state()).isEqualTo(TaskState.QUEUED);
        assertThat(counts().expired()).isEqualTo(2);
        assertThat(counts().queued()).isEqualTo(2);
        assertThat(counts().scheduled()).isZero();
        assertThat(store.promote(10).nextDue()).isNull();
        assertThat(store.expire(10)).isEmpty();
        put(task().uniqueKey("q"));
        put(task().uniqueKey("s"));
    }

    @Test
    void expireRespectsTheLimitAndNeverTouchesLeasedTasks() {
        Instant startBy = now().plus(SHORT);
        long leasedId = put(task().startBy(startBy));
        TaskRecord l = claimOne();
        assumeBefore(startBy);
        put(task().startBy(now().plus(SHORT)));
        put(task().startBy(now().plus(SHORT)));
        advance(PAST_SHORT);
        assertThat(store.expire(1)).hasSize(1);
        assertThat(store.expire(10)).hasSize(1);
        assertThat(get(leasedId).state()).isEqualTo(TaskState.LEASED);
        assertThat(get(leasedId).revision()).isEqualTo(l.revision());
        assertThat(counts().expired()).isEqualTo(2);
    }

    @Test
    void expireKeepsTheKeyWindowOfATaskWithKeyKeep() {
        long id = put(task().uniqueKey("k").keyKeep(Duration.ofHours(1)).startBy(now().plus(SHORT)));
        advance(PAST_SHORT);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(id);
        assertThat(get(id).uniqueKey()).isEqualTo("k");
        assertThat(get(id).keyUntil()).isNotNull();
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(id));
    }

    @Test
    void allocatedRunnersAreListedAndHeartbeatUpdatesThem() {
        List<RunnerRecord> before = store.runners();
        assertThat(before).extracting(RunnerRecord::id).contains(runner);
        RunnerRecord w =
                before.stream().filter(x -> x.id() == runner).findFirst().orElseThrow();
        assertThat(w.label()).isEqualTo("contract-test");
        assertThat(w.running()).isZero();
        assertThat(w.accepting()).isTrue();
        assertThat(w.queueIds()).containsExactly(queue);
        assertThat(w.typeIds()).containsExactly(type);
        advance(PAST_SHORT);
        store.heartbeat(runner, "renamed", Set.of(), Set.of(), 4, false);
        RunnerRecord after = store.runners().stream()
                .filter(x -> x.id() == runner)
                .findFirst()
                .orElseThrow();
        assertThat(after.running()).isEqualTo(4);
        assertThat(after.accepting()).isFalse();
        assertThat(after.lastSeenAt()).isAfter(w.lastSeenAt());
        assertThat(after.startedAt()).isEqualTo(w.startedAt());
        assertThat(after.label()).isEqualTo("contract-test");
        assertThat(after.queueIds()).containsExactly(queue);
        assertThat(after.typeIds()).containsExactly(type);
    }

    @Test
    void heartbeatCanResumeAcceptingWork() {
        beat(runner, 1, false);
        beat(runner, 0, true);
        assertThat(store.runners())
                .filteredOn(w -> w.id() == runner)
                .singleElement()
                .satisfies(w -> {
                    assertThat(w.accepting()).isTrue();
                    assertThat(w.running()).isZero();
                });
    }

    @Test
    void queuedByTypeCountsOnlyQueuedTasksAcrossQueuesAndTypes() {
        int second = store.queueId("second");
        int other = store.typeId("other");
        assertThat(store.queuedByType()).isEmpty();
        for (var state : List.of(
                TaskState.LEASED,
                TaskState.SUCCEEDED,
                TaskState.DEAD,
                TaskState.CANCELLED,
                TaskState.EXPIRED,
                TaskState.SCHEDULED)) {
            inState(state, task());
        }
        for (int i = 0; i < 105; i++) put(task());
        long otherId = put(task(queue, other));
        put(task(second, type));
        put(task(second, other));
        put(task(second, other));
        store.pauseQueue(second, true);

        assertThat(store.queuedByType())
                .isEqualTo(Map.of(queue, Map.of(type, 105L, other, 1L), second, Map.of(type, 1L, other, 2L)));
        assertThat(claim(1, LEASE, Set.of(other))).extracting(TaskRecord::id).containsExactly(otherId);
        assertThat(store.queuedByType())
                .isEqualTo(Map.of(queue, Map.of(type, 105L), second, Map.of(type, 1L, other, 2L)));
        claim(105);
        assertThat(store.queuedByType()).isEqualTo(Map.of(second, Map.of(type, 1L, other, 2L)));
    }

    @Test
    void runnerCoverageRoundTripsMultipleAndEmptySets() {
        var queues = new HashSet<>(Set.of(queue, store.queueId("other")));
        var types = new HashSet<>(Set.of(type, store.typeId("other")));
        int known = store.allocateRunnerId("known", queues, types);
        int empty = store.allocateRunnerId("empty", Set.of(), Set.of());
        var expectedQueues = Set.copyOf(queues);
        var expectedTypes = Set.copyOf(types);
        queues.clear();
        types.clear();
        store.heartbeat(known, "known", expectedQueues, expectedTypes, 1, true);
        store.heartbeat(empty, "empty", Set.of(), Set.of(), 0, true);

        assertThat(store.runners())
                .filteredOn(w -> w.id() == known)
                .singleElement()
                .satisfies(w -> {
                    assertThat(w.queueIds()).isEqualTo(expectedQueues);
                    assertThat(w.typeIds()).isEqualTo(expectedTypes);
                });
        assertThat(store.runners())
                .filteredOn(w -> w.id() == empty)
                .singleElement()
                .satisfies(w -> {
                    assertThat(w.queueIds()).isEmpty();
                    assertThat(w.typeIds()).isEmpty();
                });
    }

    @Test
    void purgeRunnersDeletesOnlyRunnersNotSeenRecently() {
        int stale = store.allocateRunnerId("stale", Set.of(), Set.of());
        advance(PAST_SHORT);
        Instant seenFrom = now();
        beat(runner, 0, true);
        Instant seen = store.runners().stream()
                .filter(w -> w.id() == runner)
                .findFirst()
                .orElseThrow()
                .lastSeenAt();
        assertThat(seen).as("the heartbeat is the time last seen").isAfterOrEqualTo(millis(seenFrom));
        Instant seenUntil = seen.plus(SHORT);
        long purged = store.purgeRunners(SHORT);
        assumeBefore(seenUntil);
        assertThat(purged).isEqualTo(1);
        assertThat(store.runners())
                .extracting(RunnerRecord::id)
                .contains(runner)
                .doesNotContain(stale);
        long purgedAgain = store.purgeRunners(SHORT);
        assumeBefore(seenUntil);
        assertThat(purgedAgain).isZero();
    }

    @Test
    void aHeartbeatRestoresAPurgedRunnerUnderTheSameId() {
        advance(PAST_SHORT);
        assertThat(store.purgeRunners(SHORT)).isEqualTo(1);
        assertThat(store.runners()).extracting(RunnerRecord::id).doesNotContain(runner);
        Instant before = now().truncatedTo(ChronoUnit.MILLIS);

        store.heartbeat(runner, "contract-test", Set.of(queue), Set.of(type), 2, false);

        assertThat(store.runners())
                .filteredOn(w -> w.id() == runner)
                .singleElement()
                .satisfies(w -> {
                    assertThat(w.label()).isEqualTo("contract-test");
                    assertThat(w.queueIds()).containsExactly(queue);
                    assertThat(w.typeIds()).containsExactly(type);
                    assertThat(w.running()).isEqualTo(2);
                    assertThat(w.accepting()).isFalse();
                    assertThat(w.startedAt()).isAfterOrEqualTo(before).isEqualTo(w.lastSeenAt());
                });
        long purgedAgain = store.purgeRunners(SHORT);
        assumeBefore(before.plus(SHORT));
        assertThat(purgedAgain).isZero();
        assertThat(store.allocateRunnerId("next", Set.of(), Set.of())).isGreaterThan(runner);
    }

    private void beat(int runnerId, int running, boolean accepting) {
        store.heartbeat(runnerId, "contract-test", Set.of(queue), Set.of(type), running, accepting);
    }

    // races on maintenance, recurring commits as the runner makes them, the largest allowed values

    private static final int RACING_THREADS = 6;
    private static final Duration HELD_KEY_WINDOW = Duration.ofSeconds(10);
    private static final List<TaskState> RACED_PURGE_STATES =
            List.of(TaskState.SUCCEEDED, TaskState.DEAD, TaskState.CANCELLED, TaskState.EXPIRED);

    private record Turn<T>(int thread, T result) {}

    private static <T> List<Turn<T>> raceNumbered(int threads, Function<Integer, T> body) throws Exception {
        AtomicInteger next = new AtomicInteger();
        return runConcurrently(threads, () -> {
            int thread = next.getAndIncrement();
            return new Turn<>(thread, body.apply(thread));
        });
    }

    private List<TaskRecord> expiredLeases(int count, Duration lease, Function<Integer, TaskRecord.Builder> builders) {
        for (int i = 0; i < count; i++) {
            put(builders.apply(i));
        }
        List<TaskRecord> leases = claim(count, lease, null);
        assertThat(leases).hasSize(count);
        return leases;
    }

    private FailureInfo racingFailure(int thread) {
        return new FailureInfo(1, millis(now()), "race", "thread " + thread, null);
    }

    private void raceFailExpired(boolean retries, boolean keepDead) throws Exception {
        int count = 4;
        Instant retryAt = millis(now().plus(Duration.ofHours(1)));
        List<TaskRecord> leases = expiredLeases(count, SHORT, i -> task());
        advance(PAST_SHORT);

        List<Turn<Map<Long, FailOutcome>>> raced = raceNumbered(RACING_THREADS, thread -> {
            Map<Long, FailOutcome> outcomes = new LinkedHashMap<>();
            for (TaskRecord lease : leases) {
                outcomes.put(
                        lease.id(),
                        store.failExpired(
                                lease.id(),
                                lease.revision(),
                                racingFailure(thread),
                                retries ? retryAt : null,
                                keepDead,
                                null));
            }
            return outcomes;
        });

        for (TaskRecord lease : leases) {
            List<Turn<FailOutcome>> applied = raced.stream()
                    .map(r -> new Turn<>(r.thread(), r.result().get(lease.id())))
                    .filter(r -> r.result() != FailOutcome.LOST)
                    .toList();
            assertThat(applied)
                    .as("threads whose failExpired of task %s applied", lease.id())
                    .hasSize(1);
            assertThat(applied.get(0).result()).isEqualTo(retries ? FailOutcome.RETRY : FailOutcome.DEAD);
            if (!retries && !keepDead) {
                assertThat(store.task(lease.id())).isEmpty();
                continue;
            }
            TaskRecord stored = get(lease.id());
            assertThat(stored.state()).isEqualTo(retries ? TaskState.SCHEDULED : TaskState.DEAD);
            assertThat(stored.revision()).as("revision of %s", lease.id()).isEqualTo(lease.revision() + 1);
            assertThat(stored.attempts()).isEqualTo(1);
            assertThat(stored.leaseUntil()).isNull();
            assertThat(stored.failures()).as("failures of %s", lease.id()).hasSize(1);
            assertThat(stored.failures().get(0).message())
                    .isEqualTo("thread " + applied.get(0).thread());
        }
        long ended = retries || !keepDead ? 0 : count;
        assertThat(counts()).isEqualTo(new StateCounts(0, retries ? count : 0, 0, 0, ended, 0, 0));
    }

    @Test
    void concurrentFailExpiredWithARetryAppliesOnceAndRecordsOneFailure() throws Exception {
        raceFailExpired(true, true);
    }

    @Test
    void concurrentFailExpiredThatKillsTheTaskAppliesOnceAndRecordsOneFailure() throws Exception {
        raceFailExpired(false, true);
    }

    @Test
    void concurrentFailExpiredThatKillsAndRemovesTheTaskAppliesOnce() throws Exception {
        raceFailExpired(false, false);
    }

    private void raceFinishCancelledExpired(boolean requested, boolean keepPayload) throws Exception {
        int count = 4;
        List<TaskRecord> leases = expiredLeases(
                count, MID, i -> task().uniqueKey("cancel-race-" + i).keyKeep(Duration.ofHours(1)));
        Map<Long, Instant> requestedAt = new LinkedHashMap<>();
        if (requested) {
            for (TaskRecord lease : leases) {
                assertThat(store.cancel(lease.id(), lease.revision(), "asked", null))
                        .isEqualTo(CancelOutcome.REQUESTED);
                requestedAt.put(lease.id(), get(lease.id()).cancelRequestedAt());
            }
        }
        advance(PAST_MID);

        List<Turn<Map<Long, Boolean>>> raced = raceNumbered(RACING_THREADS, thread -> {
            Map<Long, Boolean> results = new LinkedHashMap<>();
            for (TaskRecord lease : leases) {
                results.put(
                        lease.id(),
                        store.finishCancelledExpired(
                                lease.id(), lease.revision(), "reason " + thread, keepPayload, null));
            }
            return results;
        });

        for (TaskRecord lease : leases) {
            List<Integer> applied = raced.stream()
                    .filter(r -> r.result().get(lease.id()))
                    .map(Turn::thread)
                    .toList();
            assertThat(applied)
                    .as("threads whose finishCancelledExpired of task %s applied", lease.id())
                    .hasSize(1);
            TaskRecord stored = get(lease.id());
            assertThat(stored.state()).isEqualTo(TaskState.CANCELLED);
            assertThat(stored.revision()).as("revision of %s", lease.id()).isEqualTo(lease.revision() + 1);
            assertThat(stored.cancelReason()).isEqualTo("reason " + applied.get(0));
            assertThat(stored.leaseUntil()).isNull();
            assertThat(stored.cancelRequestedAt()).isNotNull();
            if (requested) {
                assertThat(stored.cancelRequestedAt()).isEqualTo(requestedAt.get(lease.id()));
            }
            assertThat(stored.encodedPayload()).isEqualTo(keepPayload ? "payload" : null);
            assertThat(stored.uniqueKey()).isEqualTo("cancel-race-" + leases.indexOf(lease));
            assertThat(stored.keyUntil())
                    .isBetween(
                            stored.stateChangedAt().plus(Duration.ofHours(1)).minusMillis(5),
                            stored.stateChangedAt().plus(Duration.ofHours(1)).plusMillis(5));
        }
        assertThat(counts()).isEqualTo(new StateCounts(0, 0, 0, 0, 0, count, 0));
    }

    @Test
    void concurrentFinishCancelledExpiredEndsEachTaskOnce() throws Exception {
        raceFinishCancelledExpired(false, true);
    }

    @Test
    void concurrentFinishCancelledExpiredEndsEachTaskOnceAndKeepsTheCancelRequestAndDropsThePayloadOnRequest()
            throws Exception {
        raceFinishCancelledExpired(true, false);
    }

    @Test
    void failExpiredAndFinishCancelledExpiredRacingOnOneExpiredLeaseApplyExactlyOne() throws Exception {
        int count = 4;
        List<TaskRecord> leases = expiredLeases(count, SHORT, i -> task());
        advance(PAST_SHORT);

        List<Turn<Map<Long, Boolean>>> raced = raceNumbered(RACING_THREADS, thread -> {
            Map<Long, Boolean> results = new LinkedHashMap<>();
            for (TaskRecord lease : leases) {
                results.put(
                        lease.id(),
                        thread % 2 == 0
                                ? store.failExpired(
                                                lease.id(), lease.revision(), racingFailure(thread), null, true, null)
                                        == FailOutcome.DEAD
                                : store.finishCancelledExpired(
                                        lease.id(), lease.revision(), "reason " + thread, true, null));
            }
            return results;
        });

        for (TaskRecord lease : leases) {
            List<Integer> applied = raced.stream()
                    .filter(r -> r.result().get(lease.id()))
                    .map(Turn::thread)
                    .toList();
            assertThat(applied).as("threads that ended task %s", lease.id()).hasSize(1);
            TaskRecord stored = get(lease.id());
            assertThat(stored.revision()).as("revision of %s", lease.id()).isEqualTo(lease.revision() + 1);
            assertThat(stored.leaseUntil()).isNull();
            if (applied.get(0) % 2 == 0) {
                assertThat(stored.state()).isEqualTo(TaskState.DEAD);
                assertThat(stored.failures()).hasSize(1);
                assertThat(stored.failures().get(0).message()).isEqualTo("thread " + applied.get(0));
            } else {
                assertThat(stored.state()).isEqualTo(TaskState.CANCELLED);
                assertThat(stored.cancelReason()).isEqualTo("reason " + applied.get(0));
                assertThat(stored.failures()).isNull();
            }
        }
        StateCounts counts = counts();
        assertThat(counts.dead() + counts.cancelled()).isEqualTo(count);
        assertThat(counts.leased()).isZero();
    }

    @Test
    void concurrentEndsOfAnExpiredFixedDelayOccurrenceApplyTheScheduleEffectOnce() throws Exception {
        Instant fire = millis(now().plusSeconds(1));
        for (String id : List.of("failed", "cancelled")) {
            store.putSchedule(fullDefinition(id), fire, fire);
            assertThat(store.commitSchedule(dueOf(id, fire), parkedFire(), fixedDelayOccurrence(id, fire))
                            .isCommitted())
                    .isTrue();
        }
        advance(Duration.ofMillis(1100));
        assertThat(store.promote(10).moved()).isEqualTo(2);
        List<TaskRecord> running = claim(2, SHORT, null);
        advance(PAST_SHORT);
        TaskRecord failed = running.stream()
                .filter(t -> "failed".equals(t.scheduleId()))
                .findFirst()
                .orElseThrow();
        TaskRecord cancelled = running.stream()
                .filter(t -> "cancelled".equals(t.scheduleId()))
                .findFirst()
                .orElseThrow();

        List<Turn<List<ScheduleEffect>>> raced = raceNumbered(RACING_THREADS, thread -> {
            ScheduleEffect failing = ScheduleEffect.pause("failed", failed.id());
            ScheduleEffect cancelling = ScheduleEffect.pause("cancelled", cancelled.id());
            store.failExpired(failed.id(), failed.revision(), racingFailure(thread), null, true, failing);
            store.finishCancelledExpired(cancelled.id(), cancelled.revision(), "stop", true, cancelling);
            return List.of(failing, cancelling);
        });

        assertThat(raced.stream().filter(r -> r.result().get(0).isApplied()).count())
                .as("effects applied by failExpired")
                .isEqualTo(1);
        assertThat(raced.stream().filter(r -> r.result().get(1).isApplied()).count())
                .as("effects applied by finishCancelledExpired")
                .isEqualTo(1);
        assertThat(get(failed.id()).state()).isEqualTo(TaskState.DEAD);
        assertThat(get(cancelled.id()).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(pausedOf("failed")).isTrue();
        assertThat(pausedOf("cancelled")).isTrue();
    }

    @Test
    void concurrentPromotesMoveEachDueTaskExactlyOnce() throws Exception {
        int perQueue = 15;
        List<Long> due = new ArrayList<>();
        for (int i = 0; i < perQueue; i++) {
            due.add(put(task().dueAt(now().plus(SHORT))));
            due.add(put(task(idle, type).dueAt(now().plus(SHORT))));
        }
        long later = put(task().dueAt(millis(now().plus(Duration.ofHours(1)))));
        Map<Long, TaskRecord> before = new LinkedHashMap<>();
        due.forEach(id -> before.put(id, get(id)));
        advance(PAST_SHORT);

        List<Turn<Integer>> raced = raceNumbered(RACING_THREADS, thread -> {
            int moved = 0;
            for (int n; (n = store.promote(4).moved()) > 0; ) {
                moved += n;
            }
            return moved;
        });

        assertThat(raced.stream().mapToInt(Turn::result).sum()).isEqualTo(due.size());
        for (long id : due) {
            TaskRecord stored = get(id);
            assertThat(stored.state()).as("state of %s", id).isEqualTo(TaskState.QUEUED);
            assertThat(stored.revision())
                    .as("revision of %s", id)
                    .isEqualTo(before.get(id).revision() + 1);
            assertThat(stored.dueAt()).isEqualTo(before.get(id).dueAt());
        }
        assertThat(get(later).state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(counts()).isEqualTo(new StateCounts(perQueue, 1, 0, 0, 0, 0, 0));
        assertThat(store.counts().getOrDefault(idle, StateCounts.ZERO))
                .isEqualTo(new StateCounts(perQueue, 0, 0, 0, 0, 0, 0));
        assertThat(store.promote(10).nextDue()).isEqualTo(get(later).dueAt());
        assertThat(claim(100, LEASE, null, queue, idle))
                .extracting(TaskRecord::id)
                .containsExactlyInAnyOrderElementsOf(due);
    }

    @Test
    void concurrentExpiresMoveEachOverdueTaskExactlyOnce() throws Exception {
        int overdue = 24;
        List<Long> ids = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < overdue; i++) {
            Instant startBy = now().plus(SHORT);
            if (i % 3 == 0) {
                ids.add(put(task().startBy(startBy)));
            } else if (i % 3 == 1) {
                ids.add(put(task().dueAt(now().plus(Duration.ofHours(1))).startBy(startBy)));
            } else {
                keys.add("expire-race-" + i);
                ids.add(put(task().uniqueKey("expire-race-" + i).startBy(startBy)));
            }
        }
        List<Long> fine = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            fine.add(put(task().startBy(now().plus(Duration.ofHours(1)))));
        }
        Map<Long, Integer> revisions = new LinkedHashMap<>();
        ids.forEach(id -> revisions.put(id, get(id).revision()));
        advance(PAST_SHORT);

        List<Turn<List<Long>>> raced = raceNumbered(RACING_THREADS, thread -> {
            List<Long> mine = new ArrayList<>();
            for (List<Long> got; !(got = ids(store.expire(5))).isEmpty(); ) {
                mine.addAll(got);
            }
            return mine;
        });

        List<Long> all = raced.stream().flatMap(r -> r.result().stream()).toList();
        assertThat(all).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(ids);
        for (long id : ids) {
            assertThat(get(id).state()).as("state of %s", id).isEqualTo(TaskState.EXPIRED);
            assertThat(get(id).revision()).as("revision of %s", id).isEqualTo(revisions.get(id) + 1);
        }
        fine.forEach(id -> assertThat(get(id).state()).isEqualTo(TaskState.QUEUED));
        assertThat(counts()).isEqualTo(new StateCounts(fine.size(), 0, 0, 0, 0, 0, overdue));
        keys.forEach(key -> put(task().uniqueKey(key)));
    }

    private List<Long> endedTasks(TaskState state, int count, Function<Integer, TaskRecord.Builder> builders) {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            TaskRecord.Builder builder = builders.apply(i);
            ids.add(put(state == TaskState.EXPIRED ? builder.startBy(now().plus(SHORT)) : builder));
        }
        switch (state) {
            case SUCCEEDED -> {
                List<LeaseRef> leases = claim(count).stream()
                        .map(t -> new LeaseRef(t.id(), t.revision()))
                        .toList();
                assertThat(store.ackAll(leases, true, true)).hasSize(count).containsOnly(true);
            }
            case DEAD -> {
                List<TaskRecord> leases = claim(count);
                assertThat(leases).hasSize(count);
                for (TaskRecord t : leases) {
                    assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                            .isEqualTo(FailOutcome.DEAD);
                }
            }
            case CANCELLED -> {
                for (long id : ids) {
                    assertThat(store.cancel(id, get(id).revision(), "stop", null))
                            .isEqualTo(CancelOutcome.CANCELLED);
                }
            }
            case EXPIRED -> {
                advance(PAST_SHORT);
                assertThat(store.expire(count)).extracting(ExpiredTask::id).containsExactlyInAnyOrderElementsOf(ids);
            }
            default -> throw new IllegalArgumentException(state.toString());
        }
        return ids;
    }

    @Test
    void concurrentPurgesRemoveEachEndedTaskOnceAndLeaveTheHeldKeysTaken() throws Exception {
        int count = 12;
        Map<TaskState, List<Long>> ended = new LinkedHashMap<>();
        for (TaskState state : RACED_PURGE_STATES) {
            ended.put(
                    state,
                    endedTasks(
                            state,
                            count,
                            i -> task().uniqueKey(state + "-" + i).keyKeep(i % 2 == 0 ? HELD_KEY_WINDOW : null)));
        }
        advance(PAST_SHORT);

        for (TaskState state : RACED_PURGE_STATES) {
            List<Turn<Long>> raced = raceNumbered(RACING_THREADS, thread -> {
                long removed = 0;
                for (long n; (n = store.purge(state, SHORT, 3)) > 0; ) {
                    removed += n;
                }
                return removed;
            });

            assertThat(raced.stream().mapToLong(Turn::result).sum())
                    .as("tasks removed in %s", state)
                    .isEqualTo(count);
            ended.get(state)
                    .forEach(id ->
                            assertThat(store.task(id)).as("%s %s", state, id).isEmpty());
            assertThat(counts().of(state)).as("%s left", state).isZero();
        }
        for (TaskState state : RACED_PURGE_STATES) {
            for (int i = 0; i < count; i++) {
                String key = state + "-" + i;
                if (i % 2 == 0) {
                    assertThat(store.enqueue(task().uniqueKey(key).build()))
                            .as("same-key enqueue after the purge of %s", key)
                            .isEqualTo(new EnqueueOutcome.DuplicateKey(
                                    ended.get(state).get(i)));
                } else {
                    put(task().uniqueKey(key));
                }
            }
        }
    }

    private List<Long> ackedWithKeys(String prefix, int count, boolean keep) {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(put(task().uniqueKey(prefix + i).keyKeep(SHORT)));
        }
        List<LeaseRef> leases = claim(count).stream()
                .map(t -> new LeaseRef(t.id(), t.revision()))
                .toList();
        assertThat(store.ackAll(leases, keep, keep)).hasSize(count).containsOnly(true);
        return ids;
    }

    @Test
    void concurrentReleaseKeysReleaseEachFinishedKeyOnce() throws Exception {
        int each = 12;
        List<Long> kept = ackedWithKeys("kept-", each, true);
        ackedWithKeys("removed-", each, false);
        advance(PAST_SHORT);

        List<Turn<Integer>> raced = raceNumbered(RACING_THREADS, thread -> {
            int released = 0;
            for (int n; (n = store.releaseKeys(3)) > 0; ) {
                released += n;
            }
            return released;
        });

        assertThat(raced.stream().mapToInt(Turn::result).sum()).isEqualTo(releasesKeysOnItsOwn() ? 0 : 2 * each);
        for (long id : kept) {
            assertThat(get(id).uniqueKey()).as("key of %s", id).isNull();
            assertThat(get(id).keyUntil()).as("key window of %s", id).isNull();
        }
        assertThat(store.releaseKeys(100)).isZero();
        for (int i = 0; i < each; i++) {
            put(task().uniqueKey("kept-" + i));
            put(task().uniqueKey("removed-" + i));
        }
    }

    @Test
    void purgesAndKeyReleasesRacingOnTheSameFinishedTasksEndEachTaskAndKeyOnce() throws Exception {
        int each = 12;
        ackedWithKeys("kept-", each, true);
        ackedWithKeys("more-", each, true);
        advance(PAST_SHORT);

        List<Turn<Long>> raced = raceNumbered(RACING_THREADS, thread -> {
            long done = 0;
            if (thread % 2 == 0) {
                for (long n; (n = store.purge(TaskState.SUCCEEDED, Duration.ZERO, 3)) > 0; ) {
                    done += n;
                }
            } else {
                for (long n; (n = store.releaseKeys(3)) > 0; ) {
                    done += n;
                }
            }
            return done;
        });

        long purged = raced.stream()
                .filter(r -> r.thread() % 2 == 0)
                .mapToLong(Turn::result)
                .sum();
        long released = raced.stream()
                .filter(r -> r.thread() % 2 == 1)
                .mapToLong(Turn::result)
                .sum();
        assertThat(purged).as("tasks removed").isEqualTo(2 * each);
        assertThat(released).as("keys released").isBetween(0L, releasesKeysOnItsOwn() ? 0L : 2L * each);
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
        for (int i = 0; i < each; i++) {
            put(task().uniqueKey("kept-" + i));
            put(task().uniqueKey("more-" + i));
        }
    }

    private TaskRecord runnerOccurrence(DueSchedule due) {
        StoredSchedule definition = due.definition();
        TaskRecord.Builder builder = TaskRecord.builder()
                .id(store.allocateIds(1))
                .revision(java.util.concurrent.ThreadLocalRandom.current().nextInt(1 << 30))
                .queueId(definition.queueId())
                .typeId(definition.typeId())
                .payloadVersion(definition.payloadVersion())
                .state(due.fireAt().isAfter(now()) ? TaskState.SCHEDULED : TaskState.QUEUED)
                .maxAttempts(definition.maxAttempts())
                .maxRunTime(definition.maxRunTime())
                .createdAt(millis(now()))
                .dueAt(due.fireAt())
                .encodedPayload(definition.encodedPayload())
                .account(definition.account())
                .scheduleId(due.scheduleId());
        if (definition.isFixedDelay()) {
            builder.headers(Map.of("bth4j.fixed-delay", definition.fixedDelay().toString()));
        }
        if (definition.overlapSkip()) {
            builder.uniqueKey("sched:" + due.scheduleId());
        }
        return builder.build();
    }

    private Instant neverAfter(Instant fire) {
        return fire.plus(Duration.ofDays(365L * 100));
    }

    /** One turn of the runner for the due definition: reads it from {@code dueSchedules}, builds the occurrence and commits it. */
    private TaskRecord commitRunnerFire(String scheduleId, Instant expectedFire, Instant nextFire) {
        DueSchedule due = dueOf(scheduleId, now());
        assertThat(due.fireAt()).isEqualTo(expectedFire);
        TaskRecord occurrence = runnerOccurrence(due);
        assertThat(store.commitSchedule(due, nextFire, occurrence).isCommitted())
                .isTrue();
        return occurrence;
    }

    private TaskRecord commitRunnerFire(
            String scheduleId, Instant expectedFire, Instant nextFire, CommitOutcome expected) {
        DueSchedule due = dueOf(scheduleId, now());
        assertThat(due.fireAt()).isEqualTo(expectedFire);
        TaskRecord occurrence = runnerOccurrence(due);
        assertThat(store.commitSchedule(due, nextFire, occurrence)).isEqualTo(expected);
        return occurrence;
    }

    @Test
    void severalRunnersCommittingTheSameDueDefinitionsCreateOneOccurrenceEach() throws Exception {
        int definitions = 6;
        Instant fire = millis(now().minusSeconds(30));
        for (int i = 0; i < definitions; i++) {
            store.putSchedule(definition("race" + i, "* * * * *"), fire, fire);
        }
        Map<String, Long> revisions = new LinkedHashMap<>();
        for (int i = 0; i < definitions; i++) {
            revisions.put("race" + i, revisionOf("race" + i));
        }

        List<Turn<List<TaskRecord>>> raced = raceNumbered(RACING_THREADS, thread -> {
            List<TaskRecord> committed = new ArrayList<>();
            for (DueSchedule due : store.dueSchedules(now(), 100)) {
                TaskRecord occurrence = runnerOccurrence(due);
                if (store.commitSchedule(due, due.fireAt().plusSeconds(3600), occurrence)
                        .isCommitted()) {
                    committed.add(occurrence);
                }
            }
            return committed;
        });

        List<TaskRecord> committed =
                raced.stream().flatMap(r -> r.result().stream()).toList();
        assertThat(committed).extracting(TaskRecord::scheduleId).doesNotHaveDuplicates();
        assertThat(committed).hasSize(definitions);
        committed.forEach(o -> assertStoredAsGiven(o, TaskState.SCHEDULED, "commit of " + o.scheduleId()));
        assertThat(counts()).isEqualTo(new StateCounts(0, definitions, 0, 0, 0, 0, 0));
        revisions.forEach((id, revision) -> {
            assertThat(nextFireOf(id)).isEqualTo(fire.plusSeconds(3600));
            assertThat(revisionOf(id)).isEqualTo(revision);
        });
    }

    @Test
    void commitScheduleOfALateFireStoresTheRunnersOccurrenceAsScheduledForTheFireAndPromoteQueuesIt() {
        Instant fire = millis(now().minusSeconds(90));
        Instant next = millis(now().plusSeconds(3600));
        store.putSchedule(
                new StoredSchedule(
                        "late",
                        type,
                        2,
                        queue,
                        "acme",
                        "*/5 * * * *",
                        "Europe/Berlin",
                        "FIRE_ONCE_NOW",
                        0,
                        null,
                        false,
                        "{\"a\":1}",
                        4,
                        Duration.ofMinutes(10),
                        false,
                        0),
                fire,
                fire);
        long revision = revisionOf("late");
        DueSchedule due = dueOf("late", now());
        assertThat(due.fireAt()).isEqualTo(fire);
        TaskRecord occurrence = runnerOccurrence(due);
        assertThat(occurrence.state()).isEqualTo(TaskState.QUEUED);

        Instant before = now();
        assertThat(store.commitSchedule(due, next, occurrence).isCommitted()).isTrue();
        Instant after = now();

        assertStoredAsGiven(occurrence, TaskState.SCHEDULED, "a commit for a fire in the past");
        assertThat(get(occurrence.id()).stateChangedAt()).isBetween(millis(before), after.plusMillis(1));
        assertThat(counts()).isEqualTo(new StateCounts(0, 1, 0, 0, 0, 0, 0));
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("late", next));
        assertThat(revisionOf("late")).isEqualTo(revision);
        assertThat(store.dueSchedules(now(), 10)).isEmpty();

        assertThat(store.promote(10).moved()).isEqualTo(1);
        TaskRecord queued = get(occurrence.id());
        assertThat(queued.state()).isEqualTo(TaskState.QUEUED);
        assertThat(queued.dueAt()).isEqualTo(fire);
        assertThat(queued.revision()).isEqualTo(occurrence.revision() + 1);
        assertThat(claimOne().id()).isEqualTo(occurrence.id());
    }

    @Test
    void successiveLateFiresEachStoreAnOccurrenceDueAtTheirOwnFireAndKeepTheDefinitionRevision() {
        Instant first = millis(now().minusSeconds(180));
        store.putSchedule(definition("catch-up", "* * * * *"), first, first);
        long revision = revisionOf("catch-up");

        List<TaskRecord> occurrences = new ArrayList<>();
        for (int k = 0; k < 3; k++) {
            Instant fire = first.plusSeconds(60L * k);
            occurrences.add(commitRunnerFire("catch-up", fire, fire.plusSeconds(60)));
        }

        occurrences.forEach(o -> assertStoredAsGiven(o, TaskState.SCHEDULED, "late fire " + o.dueAt()));
        assertThat(nextFireOf("catch-up")).isEqualTo(first.plusSeconds(180));
        assertThat(revisionOf("catch-up")).isEqualTo(revision);
        assertThat(store.promote(10).moved()).isEqualTo(3);
        assertThat(claim(10))
                .extracting(TaskRecord::id)
                .containsExactlyElementsOf(
                        occurrences.stream().map(TaskRecord::id).toList());
    }

    @Test
    void commitScheduleOfALateFixedDelayFireStoresTheRunnersOccurrenceParksTheDefinitionAndTheEndRearmsIt() {
        Instant fire = millis(now().minusSeconds(90));
        store.putSchedule(fullDefinition("fixed"), fire, fire);
        long revision = revisionOf("fixed");
        DueSchedule due = dueOf("fixed", now());
        TaskRecord occurrence = runnerOccurrence(due);
        assertThat(occurrence.state()).isEqualTo(TaskState.QUEUED);
        assertThat(occurrence.headers()).containsOnlyKeys("bth4j.fixed-delay");
        assertThat(occurrence.uniqueKey()).isEqualTo("sched:fixed");
        Instant parked = neverAfter(fire);

        assertThat(store.commitSchedule(due, parked, occurrence).isCommitted()).isTrue();

        assertStoredAsGiven(occurrence, TaskState.SCHEDULED, "a fixed-delay commit for a fire in the past");
        assertThat(store.taskByKey("sched:fixed")).contains(get(occurrence.id()));
        assertThat(nextFireOf("fixed")).isEqualTo(parked);
        assertThat(revisionOf("fixed")).isEqualTo(revision);
        assertThat(store.dueSchedules(now(), 10)).isEmpty();

        assertThat(store.promote(10).moved()).isEqualTo(1);
        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(occurrence.id());
        Instant rearmed = millis(now().plusSeconds(90));
        ScheduleEffect effect = ScheduleEffect.rearm("fixed", running.id(), rearmed);
        assertThat(store.ack(running.id(), running.revision(), true, true, effect))
                .isTrue();
        assertThat(effect.isApplied()).isTrue();
        assertThat(nextFireOf("fixed")).isEqualTo(rearmed);
        assertThat(revisionOf("fixed")).isEqualTo(revision);
        assertThat(store.taskByKey("sched:fixed")).isEmpty();
        assertThat(store.dueSchedules(rearmed, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("fixed");
    }

    @Test
    void aCronOccurrenceWhoseOverlapKeyIsHeldIsSkippedWhileTheDefinitionStillAdvances() {
        Instant base = millis(now().minusSeconds(60));
        store.putSchedule(
                new StoredSchedule(
                        "overlap",
                        type,
                        1,
                        queue,
                        "acme",
                        "* * * * *",
                        null,
                        null,
                        0,
                        null,
                        true,
                        "{}",
                        3,
                        null,
                        false,
                        0),
                base,
                base);
        long revision = revisionOf("overlap");

        TaskRecord holder = commitRunnerFire("overlap", base, base.plusSeconds(1), CommitOutcome.STORED);
        assertStoredAsGiven(holder, TaskState.SCHEDULED, "the first fire");
        assertThat(store.taskByKey("sched:overlap")).contains(get(holder.id()));

        TaskRecord skippedWhileScheduled =
                commitRunnerFire("overlap", base.plusSeconds(1), base.plusSeconds(2), CommitOutcome.SKIPPED);
        assertThat(store.task(skippedWhileScheduled.id())).isEmpty();
        assertThat(nextFireOf("overlap")).isEqualTo(base.plusSeconds(2));
        assertStoredAsGiven(holder, TaskState.SCHEDULED, "a skipped fire while the holder is scheduled");

        assertThat(store.promote(10).moved()).isEqualTo(1);
        TaskRecord skippedWhileQueued =
                commitRunnerFire("overlap", base.plusSeconds(2), base.plusSeconds(3), CommitOutcome.SKIPPED);
        assertThat(store.task(skippedWhileQueued.id())).isEmpty();
        assertThat(nextFireOf("overlap")).isEqualTo(base.plusSeconds(3));
        assertThat(get(holder.id()).state()).isEqualTo(TaskState.QUEUED);

        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(holder.id());
        TaskRecord skippedWhileLeased =
                commitRunnerFire("overlap", base.plusSeconds(3), base.plusSeconds(4), CommitOutcome.SKIPPED);
        assertThat(store.task(skippedWhileLeased.id())).isEmpty();
        assertThat(nextFireOf("overlap")).isEqualTo(base.plusSeconds(4));
        assertThat(get(holder.id())).isEqualTo(running);
        assertThat(store.taskByKey("sched:overlap")).contains(running);
        assertThat(counts()).isEqualTo(new StateCounts(0, 0, 1, 0, 0, 0, 0));

        assertThat(store.ack(running.id(), running.revision(), true, true, null))
                .isTrue();
        TaskRecord next = commitRunnerFire("overlap", base.plusSeconds(4), base.plusSeconds(5), CommitOutcome.STORED);
        assertStoredAsGiven(next, TaskState.SCHEDULED, "the fire after the holder ended");
        assertThat(store.taskByKey("sched:overlap")).contains(get(next.id()));
        assertThat(revisionOf("overlap")).isEqualTo(revision);
        assertThat(counts()).isEqualTo(new StateCounts(0, 1, 0, 1, 0, 0, 0));
    }

    @Test
    void aSkippedFixedDelayFireLeavesTheRecordedOccurrenceTheRunningOneAndItsEndRearmsTheDefinition() {
        Instant fire = millis(now().minusSeconds(90));
        store.putSchedule(fullDefinition("fixed"), fire, fire);
        TaskRecord first = commitRunnerFire("fixed", fire, neverAfter(fire));
        assertThat(store.promote(10).moved()).isEqualTo(1);
        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(first.id());

        Instant again = millis(now().minusSeconds(10));
        assertThat(store.setNextFire("fixed", revisionOf("fixed"), neverAfter(fire), again))
                .isTrue();
        TaskRecord skipped = commitRunnerFire("fixed", again, neverAfter(again));

        assertThat(store.task(skipped.id())).isEmpty();
        assertThat(nextFireOf("fixed")).isEqualTo(neverAfter(again));
        assertThat(get(running.id())).isEqualTo(running);
        assertThat(store.taskByKey("sched:fixed")).contains(running);

        Instant rearmed = millis(now().plusSeconds(90));
        ScheduleEffect forTheRunning = ScheduleEffect.rearm("fixed", running.id(), rearmed);
        assertThat(store.ack(running.id(), running.revision(), true, true, forTheRunning))
                .isTrue();
        assertThat(forTheRunning.isApplied()).isTrue();
        assertThat(nextFireOf("fixed")).isEqualTo(rearmed);
    }

    private enum Largest {
        ASCII,
        MULTIBYTE
    }

    private static final int LARGEST_ATTEMPTS = TaskRequest.MAX_ATTEMPTS;
    private static final Duration LARGEST_RUN_TIME = TaskRequest.MAX_RUN_TIME;
    private static final int LARGEST_KEY_LENGTH = TaskRequest.MAX_UNIQUE_KEY_LENGTH;
    private static final int LARGEST_ACCOUNT_LENGTH = TaskRequest.MAX_ACCOUNT_LENGTH;
    private static final int LARGEST_HEADERS_BYTES = TaskRequest.MAX_HEADERS_BYTES;
    private static final int LARGEST_PAYLOAD_BYTES = TaskRequest.MAX_PAYLOAD_BYTES;

    private static final String TWO_BYTES = "\u00e9";
    private static final String THREE_BYTES = "\u8a9e";
    private static final String FOUR_BYTES = "\uD83D\uDE00";

    private static int utf8Bytes(String text) {
        return text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    private static int utf8Bytes(Map<String, String> headers) {
        return headers.entrySet().stream()
                .mapToInt(e -> utf8Bytes(e.getKey()) + utf8Bytes(e.getValue()))
                .sum();
    }

    /** Exactly {@code chars} characters ending in {@code marker}; the multi-byte kind alternates a 2-byte and a 3-byte character. */
    private static String largestText(Largest kind, char marker, int chars) {
        StringBuilder text = new StringBuilder(chars);
        for (int i = 0; i < chars - 1; i++) {
            text.append(kind == Largest.ASCII ? String.valueOf(marker) : i % 2 == 0 ? TWO_BYTES : THREE_BYTES);
        }
        return text.append(marker).toString();
    }

    private static String largestPayload(Largest kind) {
        String payload = kind == Largest.ASCII
                ? "x".repeat(LARGEST_PAYLOAD_BYTES)
                : FOUR_BYTES.repeat(LARGEST_PAYLOAD_BYTES / 4);
        assertThat(utf8Bytes(payload)).isEqualTo(LARGEST_PAYLOAD_BYTES);
        return payload;
    }

    private static Map<String, String> largestHeaders(Largest kind) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (kind == Largest.ASCII) {
            headers.put("h", "v".repeat(LARGEST_HEADERS_BYTES - 1));
        } else {
            for (int i = 0; i < 64; i++) {
                headers.put("h%02d".formatted(i), THREE_BYTES.repeat(40) + TWO_BYTES + "xxx");
            }
        }
        assertThat(utf8Bytes(headers)).isEqualTo(LARGEST_HEADERS_BYTES);
        return headers;
    }

    private static boolean sameText(String stored, String given) {
        return stored != null && stored.equals(given);
    }

    private TaskRecord.Builder largestTask(Largest kind, char tag, Duration keyKeep) {
        return task().revision(424_242)
                .payloadVersion(Integer.MAX_VALUE)
                .maxAttempts(LARGEST_ATTEMPTS)
                .maxRunTime(LARGEST_RUN_TIME)
                .createdAt(millis(now().minusSeconds(17)))
                .dueAt(millis(now().minusSeconds(1)))
                .encodedPayload(largestPayload(kind))
                .account(largestText(kind, 'a', LARGEST_ACCOUNT_LENGTH))
                .headers(largestHeaders(kind))
                .uniqueKey(largestText(kind, tag, LARGEST_KEY_LENGTH))
                .keyKeep(keyKeep)
                .scheduleId(largestText(kind, 's', 128));
    }

    private void assertLargestStored(TaskRecord given, TaskState state, String path) {
        TaskRecord stored = get(given.id());
        assertThat(sameText(stored.encodedPayload(), given.encodedPayload()))
                .as(
                        "payload after %s (stored %s chars, given %s)",
                        path, lengthOf(stored.encodedPayload()), lengthOf(given.encodedPayload()))
                .isTrue();
        assertStoredAsGiven(
                given.toBuilder().encodedPayload(stored.encodedPayload()).build(), state, path);
    }

    private static String lengthOf(String text) {
        return text == null ? "no" : String.valueOf(text.length());
    }

    @Test
    void theLargestValuesOfANewTaskAreStoredAsGivenOnEveryWayIn() {
        for (Largest kind : Largest.values()) {
            TaskRecord single = largestTask(kind, '1', Duration.ofHours(1)).build();
            TaskRecord batchA = largestTask(kind, '2', Duration.ofHours(1)).build();
            TaskRecord batchB = largestTask(kind, '3', Duration.ofHours(1)).build();
            Instant fire = millis(now().minusSeconds(1));
            TaskRecord occurrence =
                    largestTask(kind, '4', Duration.ofHours(1)).dueAt(fire).build();
            assertThat(single.maxAttempts()).isEqualTo(32767);
            assertThat(single.maxRunTime()).isEqualTo(Duration.ofMillis(Integer.MAX_VALUE));
            assertThat(single.uniqueKey()).hasSize(256);
            assertThat(single.account()).hasSize(128);

            assertThat(store.enqueue(single)).isEqualTo(EnqueueOutcome.STORED);
            assertThat(store.enqueueAll(List.of(batchA, batchB)))
                    .containsExactly(EnqueueOutcome.STORED, EnqueueOutcome.STORED);
            store.putSchedule(definition(occurrence.scheduleId(), "* * * * *"), fire, fire);
            assertThat(store.commitSchedule(
                                    dueOf(occurrence.scheduleId(), now()), millis(now().plusSeconds(3600)), occurrence)
                            .isCommitted())
                    .isTrue();

            assertLargestStored(single, TaskState.QUEUED, kind + " enqueue");
            assertLargestStored(batchA, TaskState.QUEUED, kind + " enqueueAll");
            assertLargestStored(batchB, TaskState.QUEUED, kind + " enqueueAll");
            assertLargestStored(occurrence, TaskState.SCHEDULED, kind + " commitSchedule");
            for (TaskRecord given : List.of(single, batchA, batchB, occurrence)) {
                assertThat(store.taskByKey(given.uniqueKey()))
                        .as("%s taskByKey", kind)
                        .map(TaskRecord::id)
                        .contains(given.id());
                assertThat(store.enqueue(task().uniqueKey(given.uniqueKey()).build()))
                        .as("%s same-key enqueue", kind)
                        .isEqualTo(new EnqueueOutcome.DuplicateKey(given.id()));
            }
        }
    }

    @Test
    void uniqueKeysThatDifferOnlyInTheirLastOrFirstCharacterAtTheLengthLimitDoNotCollide() {
        for (Largest kind : Largest.values()) {
            String key = largestText(kind, 'k', LARGEST_KEY_LENGTH);
            String otherLast = key.substring(0, LARGEST_KEY_LENGTH - 1) + "K";
            String otherFirst = "K" + key.substring(1);
            long holder = put(task().uniqueKey(key));
            long last = put(task().uniqueKey(otherLast));
            long first = put(task().uniqueKey(otherFirst));

            assertThat(store.taskByKey(key)).map(TaskRecord::id).contains(holder);
            assertThat(store.taskByKey(otherLast)).map(TaskRecord::id).contains(last);
            assertThat(store.taskByKey(otherFirst)).map(TaskRecord::id).contains(first);
            assertThat(get(holder).uniqueKey()).isEqualTo(key);
            assertThat(store.enqueue(task().uniqueKey(otherLast).build()))
                    .isEqualTo(new EnqueueOutcome.DuplicateKey(last));
        }
    }

    @Test
    void theLargestValuesSurviveEveryTransitionOfATask() {
        for (Largest kind : Largest.values()) {
            TaskRecord given = largestTask(kind, 'c', MID).build();
            String path = kind.toString();
            assertThat(store.enqueue(given)).isEqualTo(EnqueueOutcome.STORED);

            TaskRecord claimed = claimOne();
            assertCarried(given, claimed, path + " claim");
            assertThat(sameText(claimed.encodedPayload(), given.encodedPayload()))
                    .as("claimed payload")
                    .isTrue();
            assertThat(claimed.maxAttempts()).isEqualTo(32767);
            assertThat(store.fail(
                            claimed.id(), claimed.revision(), failure(1), millis(now().minusSeconds(1)), true, null))
                    .as("%s fail of attempt 1 of 32767", path)
                    .isEqualTo(FailOutcome.RETRY);
            assertCarried(given, get(given.id()), path + " fail with a retry");
            assertThat(store.promote(10).moved()).isEqualTo(1);

            assertThat(claim(1, SHORT, null)).extracting(TaskRecord::id).containsExactly(given.id());
            advance(PAST_SHORT);
            assertThat(store.reclaim(10)).singleElement().satisfies(e -> {
                assertThat(e.id()).isEqualTo(given.id());
                assertThat(e.attempts()).isEqualTo(2);
                assertThat(e.maxAttempts()).isEqualTo(32767);
            });
            TaskRecord expired = get(given.id());
            assertThat(store.failExpired(
                            expired.id(), expired.revision(), failure(2), millis(now().minusSeconds(1)), true, null))
                    .as("%s failExpired of attempt 2 of 32767", path)
                    .isEqualTo(FailOutcome.RETRY);
            assertCarried(given, get(given.id()), path + " failExpired with a retry");
            assertThat(store.promote(10).moved()).isEqualTo(1);

            TaskRecord third = claimOne();
            assertThat(third.attempts()).isEqualTo(3);
            assertCarried(given, third, path + " third claim");
            Instant endedFrom = now();
            assertThat(store.ack(third.id(), third.revision(), true, true, null))
                    .isTrue();
            TaskRecord done = get(given.id());
            assertThat(done.stateChangedAt()).isAfterOrEqualTo(millis(endedFrom));
            assertCarried(given, done, path + " ack");
            assertThat(sameText(done.encodedPayload(), given.encodedPayload()))
                    .as("payload after ack")
                    .isTrue();
            assertThat(done.keyUntil())
                    .isBetween(
                            done.stateChangedAt().plus(MID).minusMillis(5),
                            done.stateChangedAt().plus(MID).plusMillis(5));
            EnqueueOutcome whileHeld =
                    store.enqueue(task().uniqueKey(given.uniqueKey()).build());
            assumeBefore(done.keyUntil());
            assertThat(whileHeld).isEqualTo(new EnqueueOutcome.DuplicateKey(given.id()));

            advance(Duration.ofMillis(50));
            assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(10), 10))
                    .isEqualTo(1);
            assertThat(store.task(given.id())).isEmpty();
            EnqueueOutcome afterPurge =
                    store.enqueue(task().uniqueKey(given.uniqueKey()).build());
            assumeBefore(done.keyUntil());
            assertThat(afterPurge)
                    .as("%s same-key enqueue after the purge", path)
                    .isEqualTo(new EnqueueOutcome.DuplicateKey(given.id()));
            advance(PAST_MID);
            assertThat(store.releaseKeys(10)).isEqualTo(releasesKeysOnItsOwn() ? 0 : 1);
            long again = put(task().uniqueKey(given.uniqueKey()));
            assertThat(store.cancel(again, get(again).revision(), "done", null)).isEqualTo(CancelOutcome.CANCELLED);
        }
    }

    @Test
    void aTaskWithTheLargestMaxAttemptsRetriesAfterEachFailureAndKeepsItsLimit() {
        long id = put(task().maxAttempts(LARGEST_ATTEMPTS));
        TaskRecord first = claimOne();
        assertThat(first.maxAttempts()).isEqualTo(32767);
        assertThat(store.fail(first.id(), first.revision(), failure(1), millis(now().minusSeconds(1)), true, null))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(id).maxAttempts()).isEqualTo(32767);
        assertThat(store.promote(10).moved()).isEqualTo(1);

        TaskRecord second = claimOne();
        assertThat(second.attempts()).isEqualTo(2);
        assertThat(store.fail(second.id(), second.revision(), failure(2), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        TaskRecord dead = get(id);
        assertThat(dead.state()).isEqualTo(TaskState.DEAD);
        assertThat(dead.maxAttempts()).isEqualTo(32767);

        assertThat(store.retry(id, dead.revision())).isEqualTo(RetryOutcome.RETRIED);
        TaskRecord retried = get(id);
        assertThat(retried.state()).isEqualTo(TaskState.QUEUED);
        assertThat(retried.maxAttempts()).isEqualTo(32767);
        TaskRecord third = claimOne();
        assertThat(third.attempts()).isEqualTo(1);
        assertThat(store.fail(third.id(), third.revision(), failure(1), millis(now().minusSeconds(1)), true, null))
                .isEqualTo(FailOutcome.RETRY);
    }

    @Test
    void headersAtTheByteLimitRoundTripWhicheverCharactersFillThem() {
        List<Map<String, String>> variants = new ArrayList<>();
        for (String unit : List.of("v", TWO_BYTES, THREE_BYTES, FOUR_BYTES)) {
            int unitBytes = utf8Bytes(unit);
            int room = LARGEST_HEADERS_BYTES - 1;
            String filler = unit.repeat(room / unitBytes) + "x".repeat(room % unitBytes);
            variants.add(Map.of("k", filler));
            variants.add(Map.of(filler, "v"));
        }
        variants.add(largestHeaders(Largest.MULTIBYTE));
        for (Map<String, String> headers : variants) {
            assertThat(utf8Bytes(headers)).isEqualTo(LARGEST_HEADERS_BYTES);
            long id = put(task().headers(headers));
            assertThat(get(id).headers())
                    .as("headers of %s bytes", utf8Bytes(headers))
                    .isEqualTo(headers);
            TaskRecord claimed = claimOne();
            assertThat(claimed.id()).isEqualTo(id);
            assertThat(claimed.headers()).isEqualTo(headers);
        }
    }

    @Test
    void aPayloadAtTheByteLimitRoundTripsWhicheverCharactersFillIt() {
        for (String unit : List.of("x", TWO_BYTES, THREE_BYTES, FOUR_BYTES)) {
            int unitBytes = utf8Bytes(unit);
            String payload =
                    unit.repeat(LARGEST_PAYLOAD_BYTES / unitBytes) + "x".repeat(LARGEST_PAYLOAD_BYTES % unitBytes);
            assertThat(utf8Bytes(payload)).isEqualTo(LARGEST_PAYLOAD_BYTES);
            long id = put(task().encodedPayload(payload));
            assertThat(sameText(get(id).encodedPayload(), payload))
                    .as("payload of %s characters of %s", payload.length(), unit)
                    .isTrue();
            TaskRecord claimed = claimOne();
            assertThat(claimed.id()).isEqualTo(id);
            assertThat(sameText(claimed.encodedPayload(), payload))
                    .as("claimed payload of %s", unit)
                    .isTrue();
            assertThat(store.ack(claimed.id(), claimed.revision(), true, true, null))
                    .isTrue();
            assertThat(sameText(get(id).encodedPayload(), payload))
                    .as("kept payload of %s", unit)
                    .isTrue();
        }
    }

    private StoredSchedule largestDefinition(Largest kind, boolean fixedDelay) {
        return new StoredSchedule(
                largestText(kind, fixedDelay ? 'f' : 's', 128),
                type,
                Integer.MAX_VALUE,
                queue,
                largestText(kind, 'a', LARGEST_ACCOUNT_LENGTH),
                fixedDelay ? null : "*/5 * * * *",
                "America/Argentina/Buenos_Aires",
                StoredSchedule.MISFIRE_CATCH_UP,
                Integer.MAX_VALUE,
                fixedDelay ? Duration.ofMillis(Long.MAX_VALUE) : null,
                true,
                largestPayload(kind),
                LARGEST_ATTEMPTS,
                LARGEST_RUN_TIME,
                false,
                0);
    }

    private static final Map<String, Function<StoredSchedule, Object>> DEFINITION_FIELDS = definitionFields();

    private static Map<String, Function<StoredSchedule, Object>> definitionFields() {
        Map<String, Function<StoredSchedule, Object>> fields = new LinkedHashMap<>();
        fields.put("scheduleId", StoredSchedule::scheduleId);
        fields.put("typeId", StoredSchedule::typeId);
        fields.put("payloadVersion", StoredSchedule::payloadVersion);
        fields.put("queueId", StoredSchedule::queueId);
        fields.put("account", StoredSchedule::account);
        fields.put("cron", StoredSchedule::cron);
        fields.put("zone", StoredSchedule::zone);
        fields.put("misfire", StoredSchedule::misfire);
        fields.put("catchUpMaxFires", StoredSchedule::catchUpMaxFires);
        fields.put("fixedDelay", StoredSchedule::fixedDelay);
        fields.put("overlapSkip", StoredSchedule::overlapSkip);
        fields.put("maxAttempts", StoredSchedule::maxAttempts);
        fields.put("maxRunTime", StoredSchedule::maxRunTime);
        fields.put("paused", StoredSchedule::paused);
        return Collections.unmodifiableMap(fields);
    }

    private static void assertSameDefinition(StoredSchedule given, StoredSchedule stored, String path) {
        DEFINITION_FIELDS.forEach((name, field) ->
                assertThat(field.apply(stored)).as("%s of %s", name, path).isEqualTo(field.apply(given)));
        assertThat(sameText(stored.encodedPayload(), given.encodedPayload()))
                .as("payload of %s", path)
                .isTrue();
    }

    @Test
    void aRecurringDefinitionWithTheLargestValuesRoundTripsAndItsOccurrenceCarriesThem() {
        Instant fire = millis(now().minusSeconds(1));
        for (Largest kind : Largest.values()) {
            for (boolean fixedDelay : new boolean[] {false, true}) {
                StoredSchedule given = largestDefinition(kind, fixedDelay);
                String id = given.scheduleId();
                String path = kind + (fixedDelay ? " fixed delay" : " cron");

                assertThat(store.putSchedule(given, fire, fire))
                        .as("first put of %s", path)
                        .isEqualTo(new PutScheduleResult(PutScheduleOutcome.CREATED, fire));
                StoredSchedule stored = storedDefinition(id);
                assertSameDefinition(given, stored, path);
                assertSameDefinition(given, dueOf(id, now()).definition(), path + " from dueSchedules");
                assertThat(store.putSchedule(given, fire, fire))
                        .as("second put of %s", path)
                        .isEqualTo(new PutScheduleResult(PutScheduleOutcome.UNCHANGED, fire));
                assertThat(revisionOf(id)).isEqualTo(stored.revision());

                DueSchedule due = dueOf(id, now());
                TaskRecord occurrence = runnerOccurrence(due);
                assertThat(occurrence.maxAttempts()).isEqualTo(32767);
                assertThat(occurrence.maxRunTime()).isEqualTo(Duration.ofMillis(Integer.MAX_VALUE));
                assertThat(occurrence.payloadVersion()).isEqualTo(Integer.MAX_VALUE);
                Instant next = fixedDelay ? neverAfter(fire) : fire.plusSeconds(300);
                assertThat(store.commitSchedule(due, next, occurrence).isCommitted())
                        .as("commit of %s", path)
                        .isTrue();
                assertLargestStored(occurrence, TaskState.SCHEDULED, path + " occurrence");
                assertThat(nextFireOf(id)).isEqualTo(next);
                assertThat(revisionOf(id)).isEqualTo(stored.revision());
            }
        }
    }

    @Test
    void everyNewTransitionKeepsTheCountsExact() {
        TaskRecord l = leased(task());
        long a = putNew();
        long b = putNew();
        long c = put(task().dueAt(now().plus(Duration.ofHours(1))));
        assertThat(counts()).isEqualTo(new StateCounts(2, 1, 1, 0, 0, 0, 0));
        assertThat(store.cancel(a, get(a).revision(), "x", null)).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(counts()).isEqualTo(new StateCounts(1, 1, 1, 0, 0, 1, 0));
        assertThat(store.cancel(l.id(), l.revision(), "x", null)).isEqualTo(CancelOutcome.REQUESTED);
        assertThat(counts()).isEqualTo(new StateCounts(1, 1, 1, 0, 0, 1, 0));
        assertThat(store.finishCancelled(l.id(), l.revision(), "x", true, null)).isTrue();
        assertThat(counts()).isEqualTo(new StateCounts(1, 1, 0, 0, 0, 2, 0));
        assertThat(store.retry(a, get(a).revision())).isEqualTo(RetryOutcome.RETRIED);
        assertThat(counts()).isEqualTo(new StateCounts(2, 1, 0, 0, 0, 1, 0));
        assertThat(store.delete(c, get(c).revision(), null)).isEqualTo(DeleteOutcome.DELETED);
        assertThat(store.delete(b, get(b).revision(), null)).isEqualTo(DeleteOutcome.DELETED);
        assertThat(counts()).isEqualTo(new StateCounts(1, 0, 0, 0, 0, 1, 0));
    }

    // wave 2: recurring

    private StoredSchedule fullDefinition(String id) {
        return new StoredSchedule(
                id,
                type,
                2,
                queue,
                "acme",
                null,
                "Europe/Berlin",
                "CATCH_UP",
                5,
                Duration.ofSeconds(90),
                true,
                "{\"a\":1}",
                4,
                Duration.ofMinutes(10),
                false,
                0);
    }

    @Test
    void recurringDefinitionsAreListedByTheCodePointsOfTheirIdsNotByALocaleOrByUtf16Units() {
        Instant fire = millis(now().plusSeconds(60));
        String privateUse = "\uE000";
        String beyondTheBasicPlane = "\uD83D\uDE00";
        for (String id :
                new String[] {beyondTheBasicPlane, "b", "A", privateUse, "a", "B", "a" + beyondTheBasicPlane}) {
            store.putSchedule(definition(id, "0 3 * * *"), fire, fire);
        }
        assertThat(allSchedules())
                .extracting(s -> s.definition().scheduleId())
                .containsExactly("A", "B", "a", "a" + beyondTheBasicPlane, "b", privateUse, beyondTheBasicPlane);
    }

    private static List<String> scheduleIds(List<ScheduleEntry> entries) {
        return entries.stream().map(e -> e.definition().scheduleId()).toList();
    }

    private static final String PRIVATE_USE = "\uE000";
    private static final String BEYOND_THE_BASIC_PLANE = "\uD83D\uDE00";

    private void putFiveSchedules() {
        Instant fire = millis(now().plusSeconds(60));
        for (String id : new String[] {BEYOND_THE_BASIC_PLANE, "b", "A", PRIVATE_USE, "a"}) {
            store.putSchedule(definition(id, "0 3 * * *"), fire, fire);
        }
    }

    @Test
    void schedulesPageThroughEveryDefinitionOnceInIdOrder() {
        putFiveSchedules();

        List<String> seen = new ArrayList<>();
        List<Integer> sizes = new ArrayList<>();
        String after = null;
        for (int round = 0; round < 10; round++) {
            List<ScheduleEntry> page = store.schedules(after, 2);
            if (page.isEmpty()) break;
            sizes.add(page.size());
            seen.addAll(scheduleIds(page));
            after = page.getLast().definition().scheduleId();
        }

        assertThat(sizes).containsExactly(2, 2, 1);
        assertThat(seen).containsExactly("A", "a", "b", PRIVATE_USE, BEYOND_THE_BASIC_PLANE);
    }

    @Test
    void aSchedulePageCarriesTheDefinitionAndItsNextFireTime() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("only", "0 3 * * *"), fire, fire);

        List<ScheduleEntry> page = store.schedules(null, 10);

        assertThat(page).hasSize(1);
        assertThat(page.getFirst().nextFireAt()).isEqualTo(fire);
        assertThat(page.getFirst().definition().scheduleId()).isEqualTo("only");
        assertThat(page).isEqualTo(allSchedules());
    }

    @Test
    void schedulesAfterAnIdThatDoesNotExistStartBehindWhereItWouldBe() {
        putFiveSchedules();

        assertThat(scheduleIds(store.schedules("B", 10)))
                .containsExactly("a", "b", PRIVATE_USE, BEYOND_THE_BASIC_PLANE);
        assertThat(scheduleIds(store.schedules("a" + BEYOND_THE_BASIC_PLANE, 10)))
                .containsExactly("b", PRIVATE_USE, BEYOND_THE_BASIC_PLANE);
        assertThat(scheduleIds(store.schedules("", 10)))
                .containsExactly("A", "a", "b", PRIVATE_USE, BEYOND_THE_BASIC_PLANE);
        assertThat(store.schedules(BEYOND_THE_BASIC_PLANE, 10)).isEmpty();
        assertThat(store.schedules(BEYOND_THE_BASIC_PLANE + "z", 10)).isEmpty();
    }

    @Test
    void schedulesAreEmptyWithoutDefinitionsAndTheLimitIsHonoured() {
        assertThat(store.schedules(null, 3)).isEmpty();
        assertThat(store.upcomingSchedules(3)).isEmpty();
        putFiveSchedules();

        assertThat(store.schedules(null, 1)).hasSize(1);
        assertThat(store.schedules(null, 4)).hasSize(4);
        assertThat(store.schedules(null, 5)).hasSize(5);
        assertThat(store.schedules(null, 6)).hasSize(5);
    }

    @Test
    void aScheduleListingWithALimitBelowOneIsRefused() {
        putFiveSchedules();
        for (int bad : new int[] {0, -1}) {
            assertThatThrownBy(() -> store.schedules(null, bad)).isInstanceOf(TaskValidationException.class);
            assertThatThrownBy(() -> store.upcomingSchedules(bad)).isInstanceOf(TaskValidationException.class);
        }
    }

    @Test
    void upcomingSchedulesAreOrderedByNextFireThenByIdAndLeavePausedOnesOut() {
        Instant base = millis(now().plusSeconds(600));
        Instant tie = base.plusSeconds(10);
        store.putSchedule(definition("late", "0 3 * * *"), base.plusSeconds(60), base.plusSeconds(60));
        store.putSchedule(definition(BEYOND_THE_BASIC_PLANE, "0 3 * * *"), tie, tie);
        store.putSchedule(definition("b", "0 3 * * *"), tie, tie);
        store.putSchedule(definition(PRIVATE_USE, "0 3 * * *"), tie, tie);
        store.putSchedule(definition("first", "0 3 * * *"), base, base);
        store.putSchedule(definition("paused", "0 3 * * *"), base.minusSeconds(60), base.minusSeconds(60));
        assertThat(store.pauseSchedule("paused", null)).isEqualTo(ScheduleOutcome.APPLIED);

        assertThat(scheduleIds(store.upcomingSchedules(10)))
                .containsExactly("first", "b", PRIVATE_USE, BEYOND_THE_BASIC_PLANE, "late");
        assertThat(scheduleIds(store.upcomingSchedules(3))).containsExactly("first", "b", PRIVATE_USE);
        assertThat(scheduleIds(store.upcomingSchedules(1))).containsExactly("first");
        assertThat(store.upcomingSchedules(10))
                .extracting(ScheduleEntry::nextFireAt)
                .startsWith(base, tie);
    }

    @Test
    void aResumedScheduleIsUpcomingAgain() {
        Instant fire = millis(now().plusSeconds(600));
        store.putSchedule(definition("a", "0 3 * * *"), fire, fire);
        assertThat(store.pauseSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.upcomingSchedules(10)).isEmpty();

        assertThat(store.resumeSchedule("a", null, null)).isEqualTo(ScheduleOutcome.APPLIED);

        assertThat(scheduleIds(store.upcomingSchedules(10))).containsExactly("a");
    }

    @Test
    void scheduleFindsOneDefinitionWithItsNextFireTime() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "0 3 * * *"), fire, fire);
        store.putSchedule(definition("b", "0 4 * * *"), fire.plusSeconds(5), fire.plusSeconds(5));
        assertThat(store.pauseSchedule("b", null)).isEqualTo(ScheduleOutcome.APPLIED);

        ScheduleEntry a = store.schedule("a").orElseThrow();
        assertThat(a.nextFireAt()).isEqualTo(fire);
        assertThat(a.definition().cron()).isEqualTo("0 3 * * *");
        assertThat(a.definition().paused()).isFalse();
        ScheduleEntry b = store.schedule("b").orElseThrow();
        assertThat(b.nextFireAt()).isEqualTo(fire.plusSeconds(5));
        assertThat(b.definition().paused()).isTrue();
        assertThat(List.of(a, b)).isEqualTo(allSchedules());
        assertThat(store.schedule("c")).isEmpty();
        assertThat(store.schedule("")).isEmpty();
        assertThat(store.schedule("A")).isEmpty();
    }

    @Test
    void runnersAreListedInTheOrderOfTheirIds() {
        List<Integer> ids = new ArrayList<>();
        ids.add(runner);
        for (int i = 0; i < 12; i++) {
            ids.add(store.allocateRunnerId("order-" + i, Set.of(queue), Set.of(type)));
        }
        beat(ids.get(7), 1, true);
        beat(ids.get(2), 0, false);

        assertThat(store.runners()).extracting(RunnerRecord::id).containsExactlyElementsOf(ids);
        assertThat(ids).isSorted();
    }

    @Test
    void recurringDefinitionsRoundTripEveryField() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(fullDefinition("fixed"), fire, fire);
        store.putSchedule(
                new StoredSchedule(
                        "cron", type, 1, queue, null, "0 3 * * *", null, null, 0, null, false, null, 3, null, false, 0),
                fire.plusSeconds(5),
                fire.plusSeconds(5));
        List<ScheduleEntry> all = allSchedules();
        assertThat(all).extracting(s -> s.definition().scheduleId()).containsExactly("cron", "fixed");
        ScheduleEntry fixed = all.get(1);
        assertThat(fixed.nextFireAt()).isEqualTo(fire);
        StoredSchedule d = fixed.definition();
        assertThat(d.typeId()).isEqualTo(type);
        assertThat(d.payloadVersion()).isEqualTo(2);
        assertThat(d.queueId()).isEqualTo(queue);
        assertThat(d.account()).isEqualTo("acme");
        assertThat(d.cron()).isNull();
        assertThat(d.zone()).isEqualTo("Europe/Berlin");
        assertThat(d.misfire()).isEqualTo("CATCH_UP");
        assertThat(d.catchUpMaxFires()).isEqualTo(5);
        assertThat(d.fixedDelay()).isEqualTo(Duration.ofSeconds(90));
        assertThat(d.isFixedDelay()).isTrue();
        assertThat(d.overlapSkip()).isTrue();
        assertThat(d.encodedPayload()).isEqualTo("{\"a\":1}");
        assertThat(d.maxAttempts()).isEqualTo(4);
        assertThat(d.paused()).isFalse();
        StoredSchedule c = all.get(0).definition();
        assertThat(c.cron()).isEqualTo("0 3 * * *");
        assertThat(c.zone()).isNull();
        assertThat(c.misfire()).isNull();
        assertThat(c.fixedDelay()).isNull();
        assertThat(c.account()).isNull();
        assertThat(c.encodedPayload()).isNull();
        assertThat(c.overlapSkip()).isFalse();
        assertThat(store.dueSchedules(fire, 10).get(0).definition()).isEqualTo(d);
    }

    @Test
    void putScheduleStoresTheGivenPausedFlagOnlyForANewDefinitionAndReplacesEveryOtherField() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(fullDefinition("a").withPaused(true), fire, fire);
        assertThat(allSchedules().get(0).definition().paused()).isTrue();
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        StoredSchedule d = allSchedules().get(0).definition();
        assertThat(d.cron()).isEqualTo("* * * * *");
        assertThat(d.fixedDelay()).isNull();
        assertThat(d.zone()).isNull();
        assertThat(d.paused()).isTrue();
        assertThat(allSchedules()).hasSize(1);
    }

    @Test
    void putScheduleKeepsAnOperatorsPauseAndAnUnpauseSurvivesToo() {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        assertThat(store.pauseSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        assertThat(allSchedules().get(0).definition().paused()).isTrue();
        assertThat(store.dueSchedules(fire, 10)).isEmpty();
        assertThat(store.resumeSchedule("a", null, null)).isEqualTo(ScheduleOutcome.APPLIED);
        store.putSchedule(definition("a", "* * * * *").withPaused(true), fire, fire);
        assertThat(allSchedules().get(0).definition().paused()).isFalse();
        assertThat(store.dueSchedules(fire, 10)).hasSize(1);
    }

    @Test
    void commitScheduleIsRefusedWhileTheDefinitionIsPaused() {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        DueSchedule due = store.dueSchedules(fire, 10).get(0);
        assertThat(store.pauseSchedule("nightly", null)).isEqualTo(ScheduleOutcome.APPLIED);
        TaskRecord occurrence = occurrence(fire);
        assertThat(store.commitSchedule(due, fire.plusSeconds(60), occurrence).isCommitted())
                .isFalse();
        assertThat(store.task(occurrence.id())).isEmpty();
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("nightly", fire));
    }

    @Test
    void retryOfAnExpiredTaskClearsStartByAndTheTaskIsClaimable() {
        TaskRecord e = expired();
        assertThat(e.startBy()).isNotNull();
        assertThat(store.retry(e.id(), e.revision())).isEqualTo(RetryOutcome.RETRIED);
        assertThat(get(e.id()).startBy()).isNull();
        advance(PAST_SHORT);
        assertThat(store.expire(10)).isEmpty();
        assertThat(claimOne().id()).isEqualTo(e.id());
    }

    @Test
    void pausedDefinitionsAreNotDueAndResumeRestoresTheStoredNextFire() {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        store.putSchedule(definition("b", "* * * * *"), fire, fire);
        assertThat(store.pauseSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.dueSchedules(fire, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("b");
        ScheduleEntry paused = allSchedules().get(0);
        assertThat(paused.definition().paused()).isTrue();
        assertThat(paused.nextFireAt()).isEqualTo(fire);
        assertThat(store.nextFireTimes()).containsEntry("a", fire);
        assertThat(store.resumeSchedule("a", null, null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.dueSchedules(fire, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactlyInAnyOrder("a", "b");
        assertThat(allSchedules().get(0).definition().paused()).isFalse();
    }

    @Test
    void pauseScheduleAndSetNextFireReportUnknownDefinitions() {
        assertThat(store.pauseSchedule("missing", null)).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.resumeSchedule("missing", null, null)).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.resumeSchedule("missing", null, millis(now()))).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.triggerSchedule("missing", null, millis(now()))).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.nextFireTimes()).isEmpty();
    }

    @Test
    void triggerScheduleReplacesTheNextFireAndInvalidatesAStaleCommit() {
        Instant fire = millis(now().plusSeconds(3600));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        Instant sooner = millis(now().plusSeconds(1));
        assertThat(store.triggerSchedule("nightly", null, sooner)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("nightly", sooner));
        DueSchedule due = store.dueSchedules(sooner, 10).get(0);
        assertThat(due.fireAt()).isEqualTo(sooner);
        assertThat(store.triggerSchedule("nightly", null, fire)).isEqualTo(ScheduleOutcome.APPLIED);
        TaskRecord occurrence = occurrence(sooner);
        assertThat(store.commitSchedule(due, fire.plusSeconds(60), occurrence).isCommitted())
                .isFalse();
        assertThat(store.task(occurrence.id())).isEmpty();
        assertThat(store.dueSchedules(sooner, 10)).isEmpty();
    }

    @Test
    void triggerScheduleAssignsANewRevisionAndPausingKeepsIt() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        store.triggerSchedule("a", null, fire.plusSeconds(5));
        long triggered = revisionOf("a");
        assertThat(triggered).isNotEqualTo(revision);
        store.pauseSchedule("a", null);
        assertThat(revisionOf("a")).isEqualTo(triggered);
    }

    @Test
    void deleteScheduleAppliesOnlyTheRevisionTheCallerSaw() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        List<ScheduleEntry> before = allSchedules();

        assertThat(store.deleteSchedule("a", revision + 1)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(allSchedules()).isEqualTo(before);
        assertThat(store.deleteSchedule("missing", revision)).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.deleteSchedule("a", revision)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(allSchedules()).isEmpty();
    }

    @Test
    void deleteScheduleWithoutARevisionAppliesWhateverTheRevisionIs() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        store.triggerSchedule("a", null, fire.plusSeconds(5));

        assertThat(store.deleteSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.deleteSchedule("a", null)).isEqualTo(ScheduleOutcome.NOT_FOUND);
    }

    @Test
    void pauseScheduleAppliesOnlyTheRevisionTheCallerSawAndKeepsIt() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        List<ScheduleEntry> before = allSchedules();

        assertThat(store.pauseSchedule("a", revision + 1)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(allSchedules()).isEqualTo(before);
        assertThat(store.pauseSchedule("missing", revision)).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.pauseSchedule("a", revision)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(allSchedules().get(0).definition().paused()).isTrue();
        assertThat(revisionOf("a")).isEqualTo(revision);
        assertThat(store.pauseSchedule("a", revision)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.pauseSchedule("a", revision + 1)).isEqualTo(ScheduleOutcome.STALE);
    }

    @Test
    void pauseScheduleWithoutARevisionAppliesWhateverTheRevisionIs() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        store.triggerSchedule("a", null, fire.plusSeconds(5));

        assertThat(store.pauseSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(allSchedules().get(0).definition().paused()).isTrue();
    }

    @Test
    void resumeScheduleAppliesOnlyTheRevisionTheCallerSaw() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        store.pauseSchedule("a", null);
        List<ScheduleEntry> before = allSchedules();
        Instant later = millis(now().plusSeconds(500));

        assertThat(store.resumeSchedule("a", revision + 1, later)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(allSchedules()).isEqualTo(before);
        assertThat(store.resumeSchedule("missing", revision, later)).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.resumeSchedule("a", revision, later)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(allSchedules().get(0).definition().paused()).isFalse();
        assertThat(store.nextFireTimes()).containsEntry("a", later);
        assertThat(revisionOf("a")).isNotEqualTo(revision);
    }

    @Test
    void resumeScheduleOfADefinitionThatIsNotPausedChecksTheRevisionAndChangesNothing() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        List<ScheduleEntry> before = allSchedules();

        assertThat(store.resumeSchedule("a", revision + 1, null)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(store.resumeSchedule("a", revision, null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(allSchedules()).isEqualTo(before);
    }

    @Test
    void resumeScheduleWithoutARevisionAppliesWhateverTheRevisionIs() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        store.pauseSchedule("a", null);
        store.triggerSchedule("a", null, fire.plusSeconds(5));

        assertThat(store.resumeSchedule("a", null, null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(allSchedules().get(0).definition().paused()).isFalse();
    }

    @Test
    void triggerScheduleAppliesOnlyTheRevisionTheCallerSaw() {
        Instant fire = millis(now().plusSeconds(3600));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        List<ScheduleEntry> before = allSchedules();
        Instant sooner = millis(now().plusSeconds(1));

        assertThat(store.triggerSchedule("a", revision + 1, sooner)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(allSchedules()).isEqualTo(before);
        assertThat(store.dueSchedules(sooner, 10)).isEmpty();
        assertThat(store.triggerSchedule("missing", revision, sooner)).isEqualTo(ScheduleOutcome.NOT_FOUND);
        assertThat(store.triggerSchedule("a", revision, sooner)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", sooner));
        assertThat(dueOf("a", sooner).triggered()).isTrue();
    }

    @Test
    void triggerScheduleWithoutARevisionAppliesWhateverTheRevisionIs() {
        Instant fire = millis(now().plusSeconds(3600));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        store.triggerSchedule("a", null, fire.plusSeconds(5));
        Instant sooner = millis(now().plusSeconds(1));

        assertThat(store.triggerSchedule("a", null, sooner)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", sooner));
    }

    @Test
    void thePauseOfAStaleRevisionIsRefusedAfterAPutThatChangesTheDefinition() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long seen = revisionOf("a");
        store.putSchedule(changedData("a", "* * * * *"), fire, fire);

        assertThat(revisionOf("a")).isNotEqualTo(seen);
        assertThat(store.pauseSchedule("a", seen)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(store.resumeSchedule("a", seen, null)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(store.triggerSchedule("a", seen, fire)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(store.deleteSchedule("a", seen)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(allSchedules()).hasSize(1);
        assertThat(allSchedules().get(0).definition().encodedPayload()).isEqualTo("{\"v\":2}");
        assertThat(allSchedules().get(0).definition().paused()).isFalse();
    }

    @Test
    void aPutThatChangesNothingKeepsTheRevisionAnOperatorSaw() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long seen = revisionOf("a");
        store.putSchedule(definition("a", "* * * * *"), fire, fire);

        assertThat(store.pauseSchedule("a", seen)).isEqualTo(ScheduleOutcome.APPLIED);
    }

    @Test
    void aRunnerFireKeepsTheRevisionAnOperatorSaw() {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(definition("nightly", "* * * * *"), fire, fire);
        long seen = revisionOf("nightly");
        DueSchedule due = store.dueSchedules(fire, 10).get(0);

        assertThat(store.commitSchedule(due, fire.plusSeconds(60), occurrence(fire))
                        .isCommitted())
                .isTrue();

        assertThat(revisionOf("nightly")).isEqualTo(seen);
        assertThat(store.pauseSchedule("nightly", seen)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.triggerSchedule("nightly", seen, fire)).isEqualTo(ScheduleOutcome.APPLIED);
    }

    @Test
    void aTriggerMakesTheRevisionBeforeItStale() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long seen = revisionOf("a");
        assertThat(store.triggerSchedule("a", seen, fire.plusSeconds(5))).isEqualTo(ScheduleOutcome.APPLIED);

        assertThat(store.pauseSchedule("a", seen)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(store.triggerSchedule("a", seen, fire)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(store.deleteSchedule("a", seen)).isEqualTo(ScheduleOutcome.STALE);
        assertThat(store.nextFireTimes()).containsOnly(Map.entry("a", fire.plusSeconds(5)));
    }

    @Test
    void aTriggeredFireIsReportedAsTriggeredAndAWriteThatKeepsTheFireTimeKeepsTheMark() {
        Instant fire = millis(now().plusSeconds(3600));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        assertThat(dueOf("a", fire).triggered()).isFalse();

        Instant asked = millis(now().plusSeconds(1));
        assertThat(store.triggerSchedule("a", null, asked)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(dueOf("a", asked).triggered()).isTrue();

        store.pauseSchedule("a", null);
        store.resumeSchedule("a", null, null);
        assertThat(dueOf("a", asked).triggered())
                .as("after a pause and a resume")
                .isTrue();
        assertThat(store.putSchedule(definition("a", "* * * * *"), fire, asked))
                .isEqualTo(new PutScheduleResult(PutScheduleOutcome.UNCHANGED, asked));
        assertThat(dueOf("a", asked).triggered()).as("after an unchanged put").isTrue();
        assertThat(store.putSchedule(changedData("a", "* * * * *"), fire, asked))
                .isEqualTo(new PutScheduleResult(PutScheduleOutcome.CHANGED, asked));
        assertThat(dueOf("a", asked).triggered())
                .as("after a put that keeps the fire time")
                .isTrue();
    }

    @Test
    void everyWriteThatReplacesATriggeredFireTimeRemovesTheMark() {
        Instant fire = millis(now().plusSeconds(3600));
        Instant asked = millis(now().plusSeconds(1));
        List<String> ids = List.of("commit", "runner-write", "other-timing", "stale-put", "resume");
        for (String id : ids) {
            store.putSchedule(definition(id, "* * * * *"), fire, fire);
            assertThat(store.triggerSchedule(id, null, asked)).isEqualTo(ScheduleOutcome.APPLIED);
            assertThat(dueOf(id, asked).triggered()).as(id).isTrue();
        }
        Instant next = asked.plusSeconds(60);

        DueSchedule due = dueOf("commit", asked);
        assertThat(store.commitSchedule(due, next, runnerOccurrence(due)).isCommitted())
                .isTrue();
        assertThat(store.setNextFire("runner-write", revisionOf("runner-write"), asked, next))
                .isTrue();
        store.putSchedule(definition("other-timing", "0 * * * *"), next, next);
        // the same definition, but its stored fire time is later than the put keeps
        store.putSchedule(definition("stale-put", "* * * * *"), next, asked.minusMillis(1));
        store.pauseSchedule("resume", null);
        store.resumeSchedule("resume", null, next);

        for (String id : ids) {
            assertThat(nextFireOf(id)).as("next fire of %s", id).isEqualTo(next);
            assertThat(dueOf(id, next).triggered()).as(id).isFalse();
        }
        assertThat(store.triggerSchedule("commit", null, asked)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(dueOf("commit", asked).triggered()).as("triggered again").isTrue();
    }

    @Test
    void initRefusesAStoreThatKeepsAnotherSchemaVersion() {
        assumeTrue(writeOtherSchemaVersion(namespace), "the store keeps no schema version");
        assertThatThrownBy(store::init).isInstanceOf(TaskStoreException.class);
        assertThatThrownBy(store::init)
                .as("a refused init leaves the stored version as it is")
                .isInstanceOf(TaskStoreException.class);
    }

    @Test
    void initRefusesAStoreThatHoldsTasksWithoutASchemaVersion() {
        putNew();
        assumeTrue(removeSchemaVersion(namespace), "the store keeps no schema version");
        assertThatThrownBy(store::init).isInstanceOf(TaskStoreException.class);
        assertThatThrownBy(store::init)
                .as("a refused init writes no schema version")
                .isInstanceOf(TaskStoreException.class);
    }

    @Test
    void resumingADefinitionThatIsNotPausedChangesNothing() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");

        assertThat(store.resumeSchedule("a", null, fire.plusSeconds(500))).isEqualTo(ScheduleOutcome.APPLIED);

        assertThat(nextFireOf("a")).isEqualTo(fire);
        assertThat(revisionOf("a")).isEqualTo(revision);
        assertThat(pausedOf("a")).isFalse();
    }

    @Test
    void resumingWithANextFireReplacesItUnderANewRevisionAndWithoutKeepsTheStoredOne() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        store.putSchedule(definition("b", "* * * * *"), fire, fire);
        store.pauseSchedule("a", null);
        store.pauseSchedule("b", null);
        long revision = revisionOf("a");

        assertThat(store.resumeSchedule("a", null, fire.plusSeconds(500))).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.resumeSchedule("b", null, null)).isEqualTo(ScheduleOutcome.APPLIED);

        assertThat(pausedOf("a")).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire.plusSeconds(500));
        assertThat(revisionOf("a")).isNotEqualTo(revision);
        assertThat(store.dueSchedules(fire.plusSeconds(500), 10))
                .extracting(DueSchedule::scheduleId)
                .contains("a");
        assertThat(pausedOf("b")).isFalse();
        assertThat(nextFireOf("b")).isEqualTo(fire);
    }

    @Test
    void resumingWhileTheRecordedOccurrenceRunsKeepsTheStoredFireUntilThatOccurrenceEnds() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant parked = nextFireOf("a");
        long revision = revisionOf("a");
        store.pauseSchedule("a", null);

        assertThat(store.resumeSchedule("a", null, millis(now().plusSeconds(60))))
                .isEqualTo(ScheduleOutcome.APPLIED);

        assertThat(pausedOf("a")).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(parked);
        assertThat(revisionOf("a")).isEqualTo(revision);
        Instant rearmed = millis(now().plusSeconds(90));
        ScheduleEffect end = ScheduleEffect.rearm("a", running.id(), rearmed);
        assertThat(store.ack(running.id(), running.revision(), true, true, end)).isTrue();
        assertThat(end.isApplied()).isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
    }

    @Test
    void resumingAfterTheRecordedOccurrenceFinishedTakesTheGivenFire() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        long revision = revisionOf("a");
        store.pauseSchedule("a", null);
        assertThat(store.ack(running.id(), running.revision(), true, true, null))
                .isTrue();
        Instant next = millis(now().plusSeconds(60));

        assertThat(store.resumeSchedule("a", null, next)).isEqualTo(ScheduleOutcome.APPLIED);

        assertThat(pausedOf("a")).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(next);
        assertThat(revisionOf("a")).isNotEqualTo(revision);
    }

    @Test
    void anOperatorTriggerForgetsTheRunningOccurrenceSoItsEffectDoesNotUndoTheTrigger() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant trigger = millis(now().plusSeconds(30));
        assertThat(store.triggerSchedule("a", null, trigger)).isEqualTo(ScheduleOutcome.APPLIED);

        ScheduleEffect rearm = ScheduleEffect.rearm("a", running.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(running.id(), running.revision(), true, true, rearm))
                .isTrue();

        assertThat(rearm.isApplied()).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(trigger);
        assertThat(pausedOf("a")).isFalse();
    }

    @Test
    void anOperatorTriggerForgetsTheRunningOccurrenceSoItsDeathDoesNotPause() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant trigger = millis(now().plusSeconds(30));
        assertThat(store.triggerSchedule("a", null, trigger)).isEqualTo(ScheduleOutcome.APPLIED);

        ScheduleEffect pause = ScheduleEffect.pause("a", running.id());
        assertThat(store.fail(running.id(), running.revision(), failure(1), null, true, pause))
                .isEqualTo(FailOutcome.DEAD);

        assertThat(pause.isApplied()).isFalse();
        assertThat(pausedOf("a")).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(trigger);
    }

    @Test
    void putScheduleReportsTheNextFireItStores() {
        Instant fire = millis(now().plusSeconds(60));
        assertThat(store.putSchedule(definition("a", "* * * * *"), fire, fire))
                .isEqualTo(new PutScheduleResult(PutScheduleOutcome.CREATED, fire));
        assertThat(store.putSchedule(definition("a", "* * * * *"), fire.plusSeconds(5), fire))
                .isEqualTo(new PutScheduleResult(PutScheduleOutcome.UNCHANGED, fire));
        assertThat(store.putSchedule(definition("a", "0 * * * *"), fire.plusSeconds(5), fire.plusSeconds(5)))
                .isEqualTo(new PutScheduleResult(PutScheduleOutcome.CHANGED, fire.plusSeconds(5)));
        assertThat(store.putSchedule(changedData("a", "0 * * * *"), fire.plusSeconds(9), fire.plusSeconds(9)))
                .isEqualTo(new PutScheduleResult(PutScheduleOutcome.CHANGED, fire.plusSeconds(5)));
    }

    private long revisionOf(String scheduleId) {
        return allSchedules().stream()
                .filter(r -> r.definition().scheduleId().equals(scheduleId))
                .findFirst()
                .orElseThrow()
                .definition()
                .revision();
    }

    private Instant nextFireOf(String scheduleId) {
        return store.nextFireTimes().get(scheduleId);
    }

    @Test
    void setNextFireWithTheCurrentRevisionAndFireTimeReplacesTheNextFireAndKeepsTheRevision() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        assertThat(store.setNextFire("a", revision, fire, fire.plusSeconds(500)))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(fire.plusSeconds(500));
        assertThat(revisionOf("a")).isEqualTo(revision);
        assertThat(store.dueSchedules(fire.plusSeconds(500), 10)).hasSize(1);
    }

    @Test
    void setNextFireWithAStaleRevisionDoesNothing() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long stale = revisionOf("a");
        store.putSchedule(changedData("a", "* * * * *"), fire, fire);
        assertThat(store.setNextFire("a", stale, fire, fire.plusSeconds(500))).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire);

        long current = revisionOf("a");
        assertThat(store.triggerSchedule("a", null, fire.plusSeconds(10))).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.setNextFire("a", current, fire.plusSeconds(10), fire.plusSeconds(500)))
                .isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire.plusSeconds(10));
    }

    @Test
    void setNextFireWithAStaleFireTimeDoesNothing() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        assertThat(store.setNextFire("a", revision, fire.plusSeconds(1), fire.plusSeconds(500)))
                .isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire);
    }

    @Test
    void setNextFireWithARevisionDoesNothingForAPausedOrUnknownDefinition() {
        Instant fire = millis(now().plusSeconds(60));
        store.putSchedule(definition("a", "* * * * *"), fire, fire);
        long revision = revisionOf("a");
        assertThat(store.pauseSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.setNextFire("a", revision, fire, fire.plusSeconds(500)))
                .isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire);
        assertThat(store.setNextFire("missing", 1, fire, fire)).isFalse();
    }

    private Instant parkedFire() {
        return millis(now().plus(Duration.ofDays(365 * 100L)));
    }

    private TaskRecord runningFixedDelayOccurrence(String scheduleId) {
        return runningFixedDelayOccurrence(scheduleId, LEASE);
    }

    /** Commits an occurrence of a fixed-delay definition with the unique key of an overlap-skipping one, and leases it. */
    private TaskRecord runningFixedDelayOccurrence(String scheduleId, Duration lease) {
        TaskRecord occurrence = committedFixedDelayOccurrence(scheduleId);
        advance(Duration.ofSeconds(2));
        store.promote(10);
        TaskRecord running = claim(1, lease, null).get(0);
        assertThat(running.id()).isEqualTo(occurrence.id());
        return running;
    }

    /** Commits an occurrence of a fresh fixed-delay definition; it waits, due in a second. */
    private TaskRecord committedFixedDelayOccurrence(String scheduleId) {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(fullDefinition(scheduleId), fire, fire);
        DueSchedule due = dueOf(scheduleId, fire);
        TaskRecord occurrence = fixedDelayOccurrence(scheduleId, fire);
        assertThat(store.commitSchedule(due, parkedFire(), occurrence).isCommitted())
                .isTrue();
        return occurrence;
    }

    private StoredSchedule withFixedDelay(StoredSchedule d, long delayMs) {
        return new StoredSchedule(
                d.scheduleId(),
                d.typeId(),
                d.payloadVersion(),
                d.queueId(),
                d.account(),
                d.cron(),
                d.zone(),
                d.misfire(),
                d.catchUpMaxFires(),
                Duration.ofMillis(delayMs),
                d.overlapSkip(),
                d.encodedPayload(),
                d.maxAttempts(),
                d.maxRunTime(),
                d.paused(),
                d.revision());
    }

    private DueSchedule dueOf(String scheduleId, Instant upTo) {
        return store.dueSchedules(upTo, 10).stream()
                .filter(d -> d.scheduleId().equals(scheduleId))
                .findFirst()
                .orElseThrow();
    }

    private TaskRecord fixedDelayOccurrence(String scheduleId, Instant fire) {
        return task().scheduleId(scheduleId)
                .dueAt(fire)
                .state(TaskState.SCHEDULED)
                .uniqueKey("sched:" + scheduleId)
                .build();
    }

    private boolean pausedOf(String scheduleId) {
        return allSchedules().stream()
                .filter(r -> r.definition().scheduleId().equals(scheduleId))
                .findFirst()
                .orElseThrow()
                .definition()
                .paused();
    }

    @Test
    void ackRearmsTheDefinitionInTheSameStep() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        long revision = revisionOf("a");
        assertThat(nextFireOf("a")).isAfter(now().plus(Duration.ofDays(1000)));
        Instant rearmed = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isTrue();
        assertThat(get(running.id()).state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
        assertThat(revisionOf("a")).isEqualTo(revision);
        assertThat(store.dueSchedules(rearmed, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("a");
    }

    @Test
    void ackWithoutKeepingTheRecordStillRearmsTheDefinition() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant rearmed = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(),
                        running.revision(),
                        false,
                        false,
                        ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isTrue();
        assertThat(store.task(running.id())).isEmpty();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
    }

    @Test
    void anEffectForATaskThatIsNotTheCurrentOccurrenceDoesNothingAndTheOccurrenceStillRearms() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant parked = nextFireOf("a");
        TaskRecord other = leased(task().scheduleId("a"));
        Instant wrong = millis(now().plusSeconds(5000));
        assertThat(store.ack(other.id(), other.revision(), true, true, ScheduleEffect.rearm("a", other.id(), wrong)))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(parked);

        Instant rearmed = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
    }

    @Test
    void anEffectIsAppliedOnceAndNothingChangesWhenTheLeaseIsLost() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant parked = nextFireOf("a");
        Instant rearmed = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(),
                        running.revision() + 1,
                        true,
                        true,
                        ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isFalse();
        assertThat(nextFireOf("a")).isEqualTo(parked);

        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isTrue();
        assertThat(store.setNextFire("a", revisionOf("a"), rearmed, rearmed.plusSeconds(1)))
                .isTrue();
        TaskRecord again = leased(task().scheduleId("a"));
        assertThat(store.ack(
                        again.id(),
                        again.revision(),
                        true,
                        true,
                        ScheduleEffect.rearm("a", running.id(), rearmed.plusSeconds(1000))))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed.plusSeconds(1));
    }

    @Test
    void anEffectOnAPausedDefinitionStoresTheNextFireAndResumingMakesItDue() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        assertThat(store.pauseSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        Instant rearmed = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isTrue();
        assertThat(pausedOf("a")).isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
        assertThat(store.dueSchedules(rearmed, 10)).isEmpty();

        assertThat(store.resumeSchedule("a", null, null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.dueSchedules(rearmed, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("a");
    }

    @Test
    void aSkippedOccurrenceParksTheDefinitionAndTheRunningOccurrenceStillRearmsIt() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant trigger = millis(now());
        assertThat(store.triggerSchedule("a", null, trigger)).isEqualTo(ScheduleOutcome.APPLIED);
        DueSchedule due = dueOf("a", trigger);
        TaskRecord second = fixedDelayOccurrence("a", trigger);
        Instant parked = parkedFire();
        assertThat(store.commitSchedule(due, parked, second).isCommitted()).isTrue();
        assertThat(store.task(second.id())).isEmpty();
        assertThat(nextFireOf("a")).isEqualTo(parked);

        Instant rearmed = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
    }

    @Test
    void aNewerOccurrenceTakesOverTheEffect() {
        TaskRecord first = runningFixedDelayOccurrence("a");
        Instant trigger = millis(now());
        assertThat(store.triggerSchedule("a", null, trigger)).isEqualTo(ScheduleOutcome.APPLIED);
        TaskRecord second =
                task().scheduleId("a").dueAt(trigger).state(TaskState.SCHEDULED).build();
        assertThat(store.commitSchedule(dueOf("a", trigger), parkedFire(), second)
                        .isCommitted())
                .isTrue();
        Instant late = millis(now().plusSeconds(5000));
        assertThat(store.ack(first.id(), first.revision(), true, true, ScheduleEffect.rearm("a", first.id(), late)))
                .isTrue();
        assertThat(nextFireOf("a")).isAfter(now().plus(Duration.ofDays(1000)));
        store.promote(10);
        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(second.id());
        Instant rearmed = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
    }

    @Test
    void anEffectOnADeletedDefinitionDoesNotRecreateIt() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        assertThat(store.deleteSchedule("a", null)).isEqualTo(ScheduleOutcome.APPLIED);
        assertThat(store.ack(
                        running.id(),
                        running.revision(),
                        true,
                        true,
                        ScheduleEffect.rearm("a", running.id(), millis(now().plusSeconds(90)))))
                .isTrue();
        assertThat(store.nextFireTimes()).isEmpty();
        assertThat(allSchedules()).isEmpty();
    }

    @Test
    void failThatKillsTheTaskPausesTheDefinitionAndAFailThatRetriesDoesNot() {
        TaskRecord first = runningFixedDelayOccurrence("a");
        assertThat(store.fail(
                        first.id(),
                        first.revision(),
                        failure(1),
                        now().plusSeconds(5),
                        true,
                        ScheduleEffect.pause("a", first.id())))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(pausedOf("a")).isFalse();

        advance(Duration.ofSeconds(10));
        store.promote(10);
        TaskRecord second = claimOne();
        assertThat(second.id()).isEqualTo(first.id());
        assertThat(store.fail(
                        second.id(), second.revision(), failure(2), null, true, ScheduleEffect.pause("a", second.id())))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(pausedOf("a")).isTrue();
        assertThat(store.dueSchedules(now().plus(Duration.ofDays(1000)), 10)).isEmpty();
    }

    @Test
    void failThatKillsAnotherTaskLeavesTheDefinitionAlone() {
        runningFixedDelayOccurrence("a");
        TaskRecord other = leased(task().scheduleId("a"));
        assertThat(store.fail(
                        other.id(), other.revision(), failure(1), null, true, ScheduleEffect.pause("a", other.id())))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(pausedOf("a")).isFalse();
    }

    @Test
    void failExpiredThatKillsTheTaskPausesTheDefinition() {
        TaskRecord running = runningFixedDelayOccurrence("a", SHORT);
        advance(PAST_SHORT);
        assertThat(store.failExpired(
                        running.id(),
                        running.revision(),
                        failure(1),
                        null,
                        true,
                        ScheduleEffect.pause("a", running.id())))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(pausedOf("a")).isTrue();
    }

    @Test
    void finishCancelledPausesTheDefinitionOfTheEndedOccurrence() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        assertThat(store.finishCancelled(
                        running.id(), running.revision(), "stop", true, ScheduleEffect.pause("a", running.id())))
                .isTrue();
        assertThat(get(running.id()).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(pausedOf("a")).isTrue();
    }

    @Test
    void finishCancelledExpiredPausesTheDefinitionOfTheEndedOccurrence() {
        TaskRecord running = runningFixedDelayOccurrence("a", SHORT);
        assertThat(store.cancel(running.id(), running.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(running.id());
        ScheduleEffect effect = ScheduleEffect.pause("a", running.id());
        assertThat(store.finishCancelledExpired(running.id(), get(running.id()).revision(), "stop", true, effect))
                .isTrue();
        assertThat(get(running.id()).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(pausedOf("a")).isTrue();
        assertThat(effect.isApplied()).isTrue();
    }

    @Test
    void finishCancelledExpiredLeavesTheDefinitionAloneForAnotherOccurrence() {
        TaskRecord running = runningFixedDelayOccurrence("a", SHORT);
        assertThat(store.cancel(running.id(), running.revision(), "stop", null)).isEqualTo(CancelOutcome.REQUESTED);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(running.id());
        ScheduleEffect effect = ScheduleEffect.pause("a", store.allocateIds(1));
        assertThat(store.finishCancelledExpired(running.id(), get(running.id()).revision(), "stop", true, effect))
                .isTrue();
        assertThat(get(running.id()).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(pausedOf("a")).isFalse();
        assertThat(effect.isApplied()).isFalse();
    }

    @Test
    void aSkippedCommitAdoptsTheKeyHolderWhenNoOccurrenceIsRecorded() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant next = millis(now().plusSeconds(1));
        store.putSchedule(withFixedDelay(fullDefinition("a"), 120_000L), next, next);
        advance(Duration.ofSeconds(2));
        TaskRecord second = fixedDelayOccurrence("a", next);
        Instant parked = parkedFire();
        assertThat(store.commitSchedule(dueOf("a", next), parked, second).isCommitted())
                .isTrue();
        assertThat(store.task(second.id())).isEmpty();
        assertThat(nextFireOf("a")).isEqualTo(parked);

        Instant rearmed = millis(now().plusSeconds(90));
        ScheduleEffect effect = ScheduleEffect.rearm("a", running.id(), rearmed);
        assertThat(store.ack(running.id(), running.revision(), true, true, effect))
                .isTrue();
        assertThat(effect.isApplied()).isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
    }

    @Test
    void aSkippedCommitDoesNotAdoptAFinishedOccurrenceThatStillHoldsTheKey() {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(fullDefinition("a"), fire, fire);
        TaskRecord first = fixedDelayOccurrence("a", fire).toBuilder()
                .keyKeep(Duration.ofHours(1))
                .build();
        assertThat(store.commitSchedule(dueOf("a", fire), parkedFire(), first).isCommitted())
                .isTrue();
        advance(Duration.ofSeconds(2));
        store.promote(10);
        TaskRecord running = claimOne();
        Instant next = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), next)))
                .isTrue();

        TaskRecord second = fixedDelayOccurrence("a", next);
        Instant parked = parkedFire();
        assertThat(store.commitSchedule(dueOf("a", next), parked, second).isCommitted())
                .isTrue();
        assertThat(store.task(second.id())).isEmpty();

        assertThat(store.putSchedule(fullDefinition("a"), parked, parked).outcome())
                .as("a put of the same definition, which is changed only when an occurrence is recorded")
                .isEqualTo(PutScheduleOutcome.UNCHANGED);
    }

    @Test
    void aSkippedCommitDoesNotAdoptAKeyHolderThatIsNotAnOccurrenceOfTheSchedule() {
        Instant fire = millis(now().plusSeconds(1));
        store.putSchedule(fullDefinition("a"), fire, fire);
        TaskRecord foreign = leased(task().uniqueKey("sched:a"));
        advance(Duration.ofSeconds(2));
        TaskRecord second = fixedDelayOccurrence("a", fire);
        Instant parked = parkedFire();
        assertThat(store.commitSchedule(dueOf("a", fire), parked, second).isCommitted())
                .isTrue();
        assertThat(store.task(second.id())).isEmpty();
        assertThat(nextFireOf("a")).isEqualTo(parked);

        ScheduleEffect effect = ScheduleEffect.rearm("a", foreign.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(foreign.id(), foreign.revision(), true, true, effect))
                .isTrue();
        assertThat(effect.isApplied()).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(parked);
    }

    @Test
    void effectsReportWhetherTheyChangedTheDefinition() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        ScheduleEffect lost = ScheduleEffect.rearm("a", running.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(running.id(), running.revision() + 1, true, true, lost))
                .isFalse();
        assertThat(lost.isApplied()).isFalse();

        TaskRecord other = leased(task().scheduleId("a"));
        ScheduleEffect stale = ScheduleEffect.rearm("a", other.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(other.id(), other.revision(), true, true, stale)).isTrue();
        assertThat(stale.isApplied()).isFalse();

        ScheduleEffect current = ScheduleEffect.rearm("a", running.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(running.id(), running.revision(), true, true, current))
                .isTrue();
        assertThat(current.isApplied()).isTrue();
    }

    @Test
    void aFailThatRetriesDoesNotApplyTheEffectAndOneThatKillsDoes() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        ScheduleEffect first = ScheduleEffect.pause("a", running.id());
        assertThat(store.fail(running.id(), running.revision(), failure(1), now().plusSeconds(5), true, first))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(first.isApplied()).isFalse();

        advance(Duration.ofSeconds(10));
        store.promote(10);
        TaskRecord again = claimOne();
        ScheduleEffect second = ScheduleEffect.pause("a", again.id());
        assertThat(store.fail(again.id(), again.revision(), failure(2), null, true, second))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(second.isApplied()).isTrue();
    }

    @Test
    void finishCancelledAndFailExpiredReportTheEffect() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        ScheduleEffect cancel = ScheduleEffect.pause("a", running.id());
        assertThat(store.finishCancelled(running.id(), running.revision(), "stop", true, cancel))
                .isTrue();
        assertThat(cancel.isApplied()).isTrue();

        TaskRecord expiring = runningFixedDelayOccurrence("b", SHORT);
        advance(PAST_SHORT);
        ScheduleEffect expired = ScheduleEffect.pause("b", expiring.id());
        assertThat(store.failExpired(expiring.id(), expiring.revision(), failure(1), null, true, expired))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(expired.isApplied()).isTrue();
    }

    @Test
    void cancelOfAWaitingFixedDelayOccurrencePausesTheDefinitionInTheSameStep() {
        TaskRecord waiting = committedFixedDelayOccurrence("a");
        ScheduleEffect effect = ScheduleEffect.pause("a", waiting.id());
        assertThat(store.cancel(waiting.id(), get(waiting.id()).revision(), "stop", effect))
                .isEqualTo(CancelOutcome.CANCELLED);
        assertThat(effect.isApplied()).isTrue();
        assertThat(pausedOf("a")).isTrue();
    }

    @Test
    void cancelOfALeasedOccurrenceOnlyRequestsTheCancelAndPausesNothing() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        ScheduleEffect effect = ScheduleEffect.pause("a", running.id());
        assertThat(store.cancel(running.id(), running.revision(), "stop", effect))
                .isEqualTo(CancelOutcome.REQUESTED);
        assertThat(effect.isApplied()).isFalse();
        assertThat(pausedOf("a")).isFalse();
    }

    @Test
    void cancelOfAnOccurrenceThatIsNoLongerTheRecordedOneLeavesTheDefinitionAlone() {
        TaskRecord waiting = committedFixedDelayOccurrence("a");
        Instant next = millis(now().plusSeconds(90));
        store.putSchedule(withFixedDelay(fullDefinition("a"), 120_000L), next, next);
        ScheduleEffect effect = ScheduleEffect.pause("a", waiting.id());
        assertThat(store.cancel(waiting.id(), get(waiting.id()).revision(), "stop", effect))
                .isEqualTo(CancelOutcome.CANCELLED);
        assertThat(effect.isApplied()).isFalse();
        assertThat(pausedOf("a")).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(next);
    }

    @Test
    void deleteOfAWaitingFixedDelayOccurrencePausesTheDefinitionAndAStaleOneDoesNot() {
        TaskRecord waiting = committedFixedDelayOccurrence("a");
        ScheduleEffect effect = ScheduleEffect.pause("a", waiting.id());
        assertThat(store.delete(waiting.id(), get(waiting.id()).revision(), effect))
                .isEqualTo(DeleteOutcome.DELETED);
        assertThat(effect.isApplied()).isTrue();
        assertThat(pausedOf("a")).isTrue();

        TaskRecord second = committedFixedDelayOccurrence("b");
        Instant next = millis(now().plusSeconds(90));
        store.putSchedule(withFixedDelay(fullDefinition("b"), 120_000L), next, next);
        ScheduleEffect stale = ScheduleEffect.pause("b", second.id());
        assertThat(store.delete(second.id(), get(second.id()).revision(), stale))
                .isEqualTo(DeleteOutcome.DELETED);
        assertThat(stale.isApplied()).isFalse();
        assertThat(pausedOf("b")).isFalse();
    }

    @Test
    void putScheduleWithUnchangedTimingKeepsTheParkWhileTheOccurrenceRunsAndRecoversAfterIt() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant parked = nextFireOf("a");
        long revision = revisionOf("a");
        Instant next = millis(now().plusSeconds(90));
        assertThat(store.putSchedule(fullDefinition("a"), next, next))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.UNCHANGED);
        assertThat(nextFireOf("a")).isEqualTo(parked);
        assertThat(revisionOf("a")).isEqualTo(revision);

        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), next)))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(next);
        assertThat(revisionOf("a")).isEqualTo(revision);
    }

    @Test
    void putScheduleRecoversAParkedDefinitionOnceItsOccurrenceIsGone() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        assertThat(store.ack(running.id(), running.revision(), true, true, null))
                .isTrue();
        assertThat(nextFireOf("a")).isAfter(now().plus(Duration.ofDays(1000)));
        assertRecoveredFromTheEndedOccurrence(running);
    }

    @Test
    void putScheduleRecoversAParkedDefinitionWhoseOccurrenceWasRemoved() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        assertThat(store.ack(running.id(), running.revision(), false, false, null))
                .isTrue();
        assertThat(store.task(running.id())).isEmpty();
        assertRecoveredFromTheEndedOccurrence(running);
    }

    private void assertRecoveredFromTheEndedOccurrence(TaskRecord ended) {
        long revision = revisionOf("a");
        Instant next = millis(now().plusSeconds(90));

        assertThat(store.putSchedule(fullDefinition("a"), next, next))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.CHANGED);

        assertThat(nextFireOf("a")).isEqualTo(next);
        assertThat(revisionOf("a")).isNotEqualTo(revision);
        TaskRecord other = leased(task().scheduleId("a"));
        ScheduleEffect stale = ScheduleEffect.rearm("a", ended.id(), millis(now().plusSeconds(5000)));
        assertThat(store.ack(other.id(), other.revision(), true, true, stale)).isTrue();
        assertThat(stale.isApplied()).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(next);
        long recovered = revisionOf("a");
        assertThat(store.putSchedule(fullDefinition("a"), next.plusSeconds(60), next.plusSeconds(60)))
                .extracting(PutScheduleResult::outcome)
                .isEqualTo(PutScheduleOutcome.UNCHANGED);
        assertThat(revisionOf("a")).isEqualTo(recovered);
    }

    @Test
    void putScheduleKeepsAHealthyFixedDelayDefinition() {
        Instant soon = millis(now().plusSeconds(30));
        store.putSchedule(fullDefinition("a"), soon, soon);
        Instant next = millis(now().plusSeconds(90));
        store.putSchedule(fullDefinition("a"), next, next);
        assertThat(nextFireOf("a")).isEqualTo(soon);
    }

    @Test
    void putScheduleWithChangedTimingForgetsTheOccurrenceInFlight() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant next = millis(now().plusSeconds(90));
        store.putSchedule(definition("a", "* * * * *"), next, next);
        assertThat(nextFireOf("a")).isEqualTo(next);
        assertThat(store.ack(
                        running.id(),
                        running.revision(),
                        true,
                        true,
                        ScheduleEffect.rearm("a", running.id(), next.plusSeconds(5000))))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(next);
    }

    @Test
    void putScheduleResetsACronDefinitionPushedBeyondTheKeepBound() {
        Instant first = millis(now().plusSeconds(60));
        Instant second = first.plusSeconds(60);
        store.putSchedule(definition("a", "* * * * *"), first, second);
        store.putSchedule(definition("a", "* * * * *"), first.plusSeconds(10), second.plusSeconds(10));
        assertThat(nextFireOf("a")).isEqualTo(first);

        Instant pushed = millis(now().plus(Duration.ofDays(30)));
        assertThat(store.setNextFire("a", revisionOf("a"), first, pushed)).isTrue();
        store.putSchedule(definition("a", "* * * * *"), first, second);
        assertThat(nextFireOf("a")).isEqualTo(first);
        assertThat(store.dueSchedules(first, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("a");
    }

    private TaskRecord keyedOccurrence(Instant fireAt) {
        return task().scheduleId("nightly")
                .dueAt(fireAt)
                .state(TaskState.SCHEDULED)
                .uniqueKey("sched:nightly")
                .build();
    }

    // inspection

    @Test
    void taskByKeyFindsTheOpenHolder() {
        TaskRecord leased = leased(task().uniqueKey("l"));
        long queued = put(task().uniqueKey("q"));
        long scheduled = put(task().uniqueKey("s").dueAt(millis(now().plus(Duration.ofDays(1)))));

        assertThat(store.taskByKey("q")).map(TaskRecord::id).contains(queued);
        assertThat(store.taskByKey("s")).map(TaskRecord::id).contains(scheduled);
        assertThat(store.taskByKey("l")).map(TaskRecord::id).contains(leased.id());
        assertThat(store.taskByKey("q").orElseThrow().uniqueKey()).isEqualTo("q");
    }

    @Test
    void taskByKeyFindsTheFinishedHolderInsideKeyKeepAndNotAfter() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        Instant keptFrom = now();
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();

        Optional<TaskRecord> holder = store.taskByKey("k");
        assumeBefore(keptFrom.plus(SHORT));
        assertThat(holder).map(TaskRecord::id).contains(t.id());
        assertThat(holder.orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        advance(PAST_SHORT);
        assertThat(store.taskByKey("k")).isEmpty();
    }

    @Test
    void taskByKeyIsEmptyForAnUnknownKey() {
        put(task().uniqueKey("known"));

        assertThat(store.taskByKey("unknown")).isEmpty();
    }

    @Test
    void taskByKeyIsEmptyOnceAckReleasedTheKeyWithoutKeyKeep() {
        TaskRecord t = leased(task().uniqueKey("k"));
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();

        assertThat(store.taskByKey("k")).isEmpty();
    }

    @Test
    void taskByKeyReturnsTheNewHolderOnceTheOldWindowPassedAndTheKeyWasReused() {
        TaskRecord old = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.ack(old.id(), old.revision(), true, true, null)).isTrue();
        advance(PAST_SHORT);
        long fresh = put(task().uniqueKey("k"));

        assertThat(store.taskByKey("k")).map(TaskRecord::id).contains(fresh);
    }

    @Test
    void taskByKeyReturnsTheRetriedTaskInsideItsKeepWindow() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(store.taskByKey("k")).isPresent();
        assertThat(store.retry(t.id(), get(t.id()).revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(store.taskByKey("k")).contains(get(t.id()));
        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(t.id());
        assertThat(store.taskByKey("k")).contains(running);
    }

    @Test
    void taskByKeyFindsADeadOrCancelledHolderInsideKeyKeep() {
        TaskRecord dead = leased(task().uniqueKey("dead").keyKeep(Duration.ofHours(1)));
        assertThat(store.fail(dead.id(), dead.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        long queued = put(task().uniqueKey("cancelled").keyKeep(Duration.ofHours(1)));
        assertThat(store.cancel(queued, get(queued).revision(), "stop", null)).isEqualTo(CancelOutcome.CANCELLED);

        assertThat(store.taskByKey("dead")).map(TaskRecord::state).contains(TaskState.DEAD);
        assertThat(store.taskByKey("cancelled")).map(TaskRecord::state).contains(TaskState.CANCELLED);
    }

    @Test
    void taskByKeyIsEmptyAfterReleaseKeysRanPastTheWindow() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        advance(PAST_SHORT);
        store.releaseKeys(10);

        assertThat(store.taskByKey("k")).isEmpty();
        assertThat(get(t.id()).uniqueKey()).isNull();
    }

    @Test
    void taskReportsATaskWithAnUnreadableStoredValue() {
        long id = put(task());
        assumeTrue(corruptStoredValue(namespace, id), "the store cannot hold an unreadable value");

        assertThatThrownBy(() -> store.task(id))
                .isInstanceOfSatisfying(
                        UnreadableTaskException.class,
                        e -> assertThat(e.taskId()).isEqualTo(id));
    }

    @Test
    void aListingLeavesOutAnUnreadableTaskAndWalksOnPastIt() {
        long bad = put(task());
        long good = put(task());
        assumeTrue(corruptStoredValue(namespace, bad), "the store cannot hold an unreadable value");

        assertThat(store.tasks(TaskState.QUEUED, queue, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(good);
        assertThat(store.tasks(TaskState.QUEUED, null, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(good);
        var first = store.tasks(TaskState.QUEUED, queue, null, 1);
        assertThat(first.items()).isEmpty();
        assertThat(first.next()).isNotNull();
        assertThat(store.tasks(TaskState.QUEUED, queue, first.next(), 1).items())
                .extracting(TaskRecord::id)
                .containsExactly(good);
        assertThat(store.counts().get(queue).queued()).isEqualTo(2);
    }

    @Test
    void aClaimLeavesOutAnUnreadableTaskAndEndsItsLeaseSoItCanBeEndedAndDeleted() {
        long bad = put(task());
        long good = put(task());
        assumeTrue(corruptStoredValue(namespace, bad), "the store cannot hold an unreadable value");

        assertThat(claim(10)).extracting(TaskRecord::id).containsExactly(good);

        List<ExpiredLease> expired = store.reclaim(10);
        assertThat(expired).extracting(ExpiredLease::id).containsExactly(bad);
        ExpiredLease lease = expired.getFirst();
        assertThat(store.failExpired(bad, lease.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(store.reclaim(10)).isEmpty();
        assertThat(store.counts().get(queue).dead()).isEqualTo(1);
        assertThat(store.delete(bad, lease.revision() + 1, null)).isEqualTo(DeleteOutcome.DELETED);
        assertThat(store.counts().get(queue).dead()).isZero();
    }

    @Test
    void taskIsEmptyForAnUnknownId() {
        assertThat(store.task(store.allocateIds(1))).isEmpty();
    }

    @Test
    void countsGroupTasksByQueueAndState() {
        int second = store.queueId("second");
        putNew();
        putNew();
        put(task().dueAt(now().plus(Duration.ofHours(1))));
        put(task(second, type));
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
        TaskRecord d = leased(task());
        assertThat(store.fail(d.id(), d.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        TaskRecord l = leased(task());
        Map<Integer, StateCounts> counts = store.counts();
        StateCounts main = counts.get(queue);
        assertThat(main.queued()).isEqualTo(2);
        assertThat(main.scheduled()).isEqualTo(1);
        assertThat(main.leased()).isEqualTo(1);
        assertThat(main.succeeded()).isEqualTo(1);
        assertThat(main.dead()).isEqualTo(1);
        assertThat(main.cancelled()).isZero();
        assertThat(main.of(TaskState.QUEUED)).isEqualTo(2);
        assertThat(counts.get(second).queued()).isEqualTo(1);
        assertThat(l.state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void countsOmitOrZeroQueuesWithoutTasks() {
        int empty = store.queueId("empty");
        putNew();
        assertThat(store.counts().getOrDefault(empty, StateCounts.ZERO)).isEqualTo(StateCounts.ZERO);
    }

    @Test
    void tasksListsOneStateOrderedByStateChangedThenIdWithKeysetPaging() {
        List<TaskRecord> claimed = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            putNew();
        }
        claimed.addAll(claim(4));
        List<TaskRecord> ackOrder = List.of(claimed.get(2), claimed.get(0), claimed.get(3), claimed.get(1));
        for (TaskRecord t : ackOrder) {
            assertThat(store.ack(t.id(), t.revision(), true, true, null)).isTrue();
            advance(Duration.ofMillis(20));
        }
        List<Long> expected = ackOrder.stream().map(TaskRecord::id).toList();
        List<TaskRecord> all = store.tasks(TaskState.SUCCEEDED, null, null, 10).items();
        assertThat(all).extracting(TaskRecord::id).containsExactlyElementsOf(expected);
        TaskPage page1 = store.tasks(TaskState.SUCCEEDED, null, null, 3);
        assertThat(page1.items()).extracting(TaskRecord::id).containsExactlyElementsOf(expected.subList(0, 3));
        TaskPage page2 = store.tasks(TaskState.SUCCEEDED, null, page1.next(), 3);
        assertThat(page2.items()).extracting(TaskRecord::id).containsExactly(expected.get(3));
        assertThat(page2.next()).isNull();
    }

    @Test
    void queuedAndScheduledTasksAreListedByDueThenIdWithKeysetPaging() {
        long queuedLate = put(task().dueAt(now().minus(Duration.ofSeconds(10))));
        advance(Duration.ofMillis(20));
        long queuedEarly = put(task().dueAt(now().minus(Duration.ofSeconds(20))));
        long scheduledLate = put(task().dueAt(now().plus(Duration.ofHours(2))));
        advance(Duration.ofMillis(20));
        long scheduledEarly = put(task().dueAt(now().plus(Duration.ofHours(1))));

        var expected = Map.of(
                TaskState.QUEUED, List.of(queuedEarly, queuedLate),
                TaskState.SCHEDULED, List.of(scheduledEarly, scheduledLate));
        expected.forEach((state, ids) -> {
            TaskPage first = store.tasks(state, queue, null, 1);
            TaskPage second = store.tasks(state, queue, first.next(), 1);
            assertThat(Stream.concat(first.items().stream(), second.items().stream())
                            .map(TaskRecord::id))
                    .as(state.name())
                    .containsExactlyElementsOf(ids);
            assertThat(second.next()).isNull();
        });
    }

    @Test
    void cursorWalkListsEachTaskOnceWhileTheCursorTaskIsCancelledBetweenPages() {
        for (int limit : new int[] {1, 2, 4}) {
            int walkQueue = store.queueId("walk" + limit);
            List<Long> all = new ArrayList<>();
            for (int i = 0; i < 9; i++) {
                all.add(put(task(walkQueue, type)));
            }
            List<Long> seen = new ArrayList<>();
            String cursor = null;
            boolean cancelled = false;
            for (int page = 0; page < 30; page++) {
                TaskPage got = store.tasks(TaskState.QUEUED, walkQueue, cursor, limit);
                got.items().forEach(t -> seen.add(t.id()));
                if (got.next() == null) {
                    break;
                }
                if (!cancelled) {
                    TaskRecord last = got.items().get(got.items().size() - 1);
                    assertThat(store.cancel(last.id(), last.revision(), "between pages", null))
                            .isEqualTo(CancelOutcome.CANCELLED);
                    cancelled = true;
                }
                cursor = got.next();
            }
            assertThat(cancelled).isTrue();
            assertThat(seen).as("limit %d", limit).containsExactlyInAnyOrderElementsOf(all);
        }
    }

    @Test
    void cursorWalkTerminatesAndListsEachUnmovedTaskOnceWhileTasksAreClaimedAndDeferredBetweenPages() {
        int other = store.typeId("other");
        for (int limit : new int[] {1, 3, 4}) {
            int walkQueue = store.queueId("defer" + limit);
            List<Long> all = new ArrayList<>();
            long deferred = put(task(walkQueue, other).dueAt(millis(now().minusSeconds(60))));
            all.add(deferred);
            for (int i = 0; i < 9; i++) {
                all.add(put(task(walkQueue, type)));
            }
            List<Long> seen = new ArrayList<>();
            List<Long> moved = new ArrayList<>();
            moved.add(deferred);
            String cursor = null;
            boolean ended = false;
            for (int page = 0; page < 50 && !ended; page++) {
                TaskPage got = store.tasks(TaskState.QUEUED, walkQueue, cursor, limit);
                got.items().forEach(t -> seen.add(t.id()));
                cursor = got.next();
                ended = cursor == null;
                if (page == 0) {
                    List<TaskRecord> claimed = claim(3, LEASE, Set.of(type), walkQueue);
                    assertThat(claimed).hasSize(3);
                    claimed.forEach(t -> moved.add(t.id()));
                }
            }
            assertThat(ended).as("limit %d terminates", limit).isTrue();
            List<Long> unmoved = new ArrayList<>(all);
            unmoved.removeAll(moved);
            for (long id : unmoved) {
                assertThat(seen).as("limit %d, task %d", limit, id).containsOnlyOnce(id);
            }
        }
    }

    @Test
    void cursorWalkKeepsIdOrderInsideAGroupOfTasksWithTheSameSortValue() {
        for (int i = 0; i < 7; i++) {
            put(task());
        }
        List<TaskRecord> group = claim(7);
        assertThat(group).hasSize(7);
        assertThat(group.stream().map(t -> t.stateChangedAt().toEpochMilli()).distinct())
                .as("one claim gives the group a single sort value")
                .hasSize(1);
        List<Long> byId = group.stream().map(TaskRecord::id).sorted().toList();
        for (int limit : new int[] {1, 2, 3}) {
            List<Long> seen = new ArrayList<>();
            String cursor = null;
            for (int page = 0; page < 30; page++) {
                TaskPage got = store.tasks(TaskState.LEASED, null, cursor, limit);
                got.items().forEach(t -> seen.add(t.id()));
                cursor = got.next();
                if (cursor == null) {
                    break;
                }
            }
            assertThat(seen).as("limit %d", limit).containsExactlyElementsOf(byId);
        }
    }

    @Test
    void cursorPageHasNoNextOnTheLastPageAndAMalformedCursorThrows() {
        long a = putNew();
        long b = putNew();
        long c = putNew();

        TaskPage first = store.tasks(TaskState.QUEUED, null, (String) null, 2);
        assertThat(first.items()).hasSize(2);
        assertThat(first.next()).isNotNull();
        TaskPage last = store.tasks(TaskState.QUEUED, null, first.next(), 2);
        assertThat(last.items()).hasSize(1);
        assertThat(last.next()).isNull();
        assertThat(Stream.concat(first.items().stream(), last.items().stream()).map(TaskRecord::id))
                .containsExactlyInAnyOrder(a, b, c);
        assertThat(store.tasks(TaskState.QUEUED, null, (String) null, 10).next())
                .isNull();
        assertThat(store.tasks(TaskState.DEAD, null, (String) null, 10).items()).isEmpty();

        for (String bad : new String[] {"garbage", "1", ":5", "5:", "a:b", "a.b:c", "1:2:3", ""}) {
            assertThatThrownBy(() -> store.tasks(TaskState.QUEUED, null, bad, 10))
                    .as("cursor '%s'", bad)
                    .isInstanceOf(TaskValidationException.class);
        }
    }

    @Test
    void aCursorNamingATimeFarFromAnyClockIsRefusedOrReadAsAPositionAndNeverFailsTheStore() {
        putNew();
        for (String far : new String[] {
            Long.MIN_VALUE + ":1",
            "-300000000000000000:1",
            "-1000000000000000:1",
            "9223372036854775:1",
            "9224318016000000:1",
            Long.MAX_VALUE + ":1"
        }) {
            Throwable refused = catchThrowable(() -> store.tasks(TaskState.QUEUED, null, far, 10));
            if (refused != null) {
                assertThat(refused).as("cursor '%s'", far).isInstanceOf(TaskValidationException.class);
            }
        }
    }

    @Test
    void cursorPageHasNoNextWhenTheFirstPageHoldsExactlyTheLimitAndHasOneWhenOneMoreExists() {
        long a = putNew();
        long b = putNew();
        long c = putNew();

        TaskPage exact = store.tasks(TaskState.QUEUED, null, (String) null, 3);
        assertThat(exact.items()).extracting(TaskRecord::id).containsExactlyInAnyOrder(a, b, c);
        assertThat(exact.next()).isNull();

        long d = putNew();
        TaskPage first = store.tasks(TaskState.QUEUED, null, (String) null, 3);
        assertThat(first.items()).hasSize(3);
        assertThat(first.next()).isNotNull();
        TaskPage second = store.tasks(TaskState.QUEUED, null, first.next(), 3);
        assertThat(second.items()).hasSize(1);
        assertThat(second.next()).isNull();
        assertThat(Stream.concat(first.items().stream(), second.items().stream())
                        .map(TaskRecord::id))
                .containsExactlyInAnyOrder(a, b, c, d);
    }

    @Test
    void cursorPageHasNoNextWhenALeasedGroupWithTheSameSortValueFillsThePageExactly() {
        for (int i = 0; i < 4; i++) {
            put(task());
        }
        List<Long> byId = claim(4).stream().map(TaskRecord::id).sorted().toList();

        TaskPage exact = store.tasks(TaskState.LEASED, null, (String) null, 4);
        assertThat(exact.items()).extracting(TaskRecord::id).containsExactlyElementsOf(byId);
        assertThat(exact.next()).isNull();
        TaskPage first = store.tasks(TaskState.LEASED, null, (String) null, 3);
        assertThat(first.items()).extracting(TaskRecord::id).containsExactlyElementsOf(byId.subList(0, 3));
        TaskPage second = store.tasks(TaskState.LEASED, null, first.next(), 3);
        assertThat(second.items()).extracting(TaskRecord::id).containsExactly(byId.get(3));
        assertThat(second.next()).isNull();
    }

    @Test
    void tasksFiltersByStateAndQueue() {
        int second = store.queueId("second");
        long inMain = putNew();
        long inSecond = put(task(second, type));
        assertThat(store.tasks(TaskState.QUEUED, null, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactlyInAnyOrder(inMain, inSecond);
        assertThat(store.tasks(TaskState.QUEUED, second, null, 10).items())
                .extracting(TaskRecord::id)
                .containsExactly(inSecond);
        assertThat(store.tasks(TaskState.DEAD, null, null, 10).items()).isEmpty();
        assertThat(store.tasks(TaskState.QUEUED, null, null, 1).items()).hasSize(1);
    }

    @Test
    void countsCoverEveryRegisteredQueueWithZerosForAnEmptyOne() {
        putNew();
        Map<Integer, StateCounts> counts = store.counts();
        assertThat(counts.keySet())
                .containsExactlyInAnyOrderElementsOf(store.queueNames().keySet());
        assertThat(counts.get(idle)).isEqualTo(new StateCounts(0, 0, 0, 0, 0, 0, 0));
        assertThat(counts.get(queue).queued()).isEqualTo(1);
    }

    // awkward text, ordering promises, time boundaries and batch sizes

    private static final List<String> AWKWARD_TEXTS = List.of(
            "plain",
            "PLAIN",
            "smile " + codePoints(0x1F600) + " family " + codePoints(0x1F468, 0x200D, 0x1F469, 0x200D, 0x1F467)
                    + " math " + codePoints(0x1D538) + " end",
            codePoints(0x1F600),
            "café 日本語 العربية Да",
            "double \" single ' back` tick",
            "back\\slash \\n \\\\ \\\" \\u0041 end\\",
            "colon:colon::k:1:",
            ":",
            "line1\nline2\r\nline3\ttab\t",
            "\nleading and trailing\n",
            codePoints(0x2028, 0x2029, 0x85, 0xA0),
            codePoints(0x01, 0x1F, 0x7F),
            "%",
            "_",
            "%_%",
            "a%b",
            "a_b",
            "axb",
            "ab",
            "{\"a\":1,\"b\":[true,null,\"x\"],\"c\":{\"d\":\"e\"}}",
            "[1,2,3]",
            "null",
            "=",
            "==",
            "=x",
            "x=",
            "a=b",
            " leading",
            "trailing ",
            " both ",
            "inner  double  spaces",
            "'; DROP TABLE x; --",
            "a.b*c+d?[e](f){g}|h^i$",
            "%s %d %1$s ${x} {{x}} @NAME@ KEYS[1] <b>&amp;</b>");

    private static String codePoints(int... points) {
        return new String(points, 0, points.length);
    }

    private TaskRecord.Builder awkwardTask(String text, boolean later) {
        return task().account(text)
                .headers(Map.of(text, text))
                .uniqueKey(text)
                .retryPolicy(text)
                .scheduleId(text)
                .encodedPayload(text)
                .dueAt(later ? millis(now().plus(Duration.ofHours(1))) : null);
    }

    private static void assertAwkwardProducerFields(TaskRecord actual, String text, String path) {
        assertThat(actual.account()).as("account %s after %s", text, path).isEqualTo(text);
        assertThat(actual.headers()).as("headers %s after %s", text, path).isEqualTo(Map.of(text, text));
        assertThat(actual.uniqueKey()).as("unique key %s after %s", text, path).isEqualTo(text);
        assertThat(actual.retryPolicy())
                .as("retry policy %s after %s", text, path)
                .isEqualTo(text);
        assertThat(actual.scheduleId())
                .as("schedule id %s after %s", text, path)
                .isEqualTo(text);
        assertThat(actual.encodedPayload())
                .as("payload %s after %s", text, path)
                .isEqualTo(text);
    }

    private List<TaskRecord> walkAllPagesOf(TaskState state, Integer queueId, int limit) {
        List<TaskRecord> all = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            TaskPage page = store.tasks(state, queueId, cursor, limit);
            all.addAll(page.items());
            cursor = page.next();
        } while (cursor != null && ++pages < 10_000);
        assertThat(cursor).as("walk of %s ended", state).isNull();
        return all;
    }

    private void assertAwkwardTextOfNewTasksRoundTrips(
            Function<List<TaskRecord>, List<EnqueueOutcome>> write, String path) {
        List<TaskRecord> given = new ArrayList<>();
        Map<Long, String> textById = new LinkedHashMap<>();
        for (int i = 0; i < AWKWARD_TEXTS.size(); i++) {
            TaskRecord record = awkwardTask(AWKWARD_TEXTS.get(i), i % 2 == 1).build();
            given.add(record);
            textById.put(record.id(), AWKWARD_TEXTS.get(i));
        }
        assertThat(write.apply(given))
                .as("outcomes of %s", path)
                .hasSize(given.size())
                .containsOnly(EnqueueOutcome.STORED);

        for (TaskRecord record : given) {
            String text = textById.get(record.id());
            TaskRecord stored = get(record.id());
            assertAwkwardProducerFields(stored, text, path + " read by task");
            assertThat(store.taskByKey(text))
                    .as("taskByKey %s after %s", text, path)
                    .contains(stored);
            assertThat(store.enqueue(task().uniqueKey(text).build()))
                    .as("second task with key %s after %s", text, path)
                    .isEqualTo(new EnqueueOutcome.DuplicateKey(record.id()));
        }
        for (TaskState state : List.of(TaskState.QUEUED, TaskState.SCHEDULED)) {
            List<TaskRecord> listed = walkAllPagesOf(state, queue, 7);
            assertThat(listed)
                    .extracting(TaskRecord::id)
                    .as("%s listing after %s", state, path)
                    .containsExactlyInAnyOrderElementsOf(given.stream()
                            .filter(r -> (r.dueAt() == null) == (state == TaskState.QUEUED))
                            .map(TaskRecord::id)
                            .toList());
            for (TaskRecord item : listed) {
                assertAwkwardProducerFields(item, textById.get(item.id()), path + " read by the " + state + " listing");
            }
        }
        List<TaskRecord> claimed = claim(given.size());
        assertThat(claimed).as("claim after %s", path).hasSize(given.size() / 2 + given.size() % 2);
        for (TaskRecord item : claimed) {
            assertAwkwardProducerFields(item, textById.get(item.id()), path + " read by claim");
            assertThat(item).as("claim of %s", item.id()).isEqualTo(get(item.id()));
        }
    }

    @Test
    void awkwardTextOfANewTaskRoundTripsThroughEnqueue() {
        assertAwkwardTextOfNewTasksRoundTrips(
                tasks -> tasks.stream().map(store::enqueue).toList(), "enqueue");
    }

    @Test
    void awkwardTextOfANewTaskRoundTripsThroughEnqueueAll() {
        assertAwkwardTextOfNewTasksRoundTrips(store::enqueueAll, "enqueueAll");
    }

    @Test
    void awkwardProgressStepAndCancelReasonTextRoundTripsThroughLeasesAndCancels() {
        int n = AWKWARD_TEXTS.size();
        for (int i = 0; i < n; i++) {
            putNew();
        }
        List<TaskRecord> claimed = claim(n);
        assertThat(claimed).hasSize(n);
        Instant at = millis(now());
        Map<Long, String> textById = new LinkedHashMap<>();
        Map<Long, Map<String, StepResult>> stepsById = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            String text = AWKWARD_TEXTS.get(i);
            TaskRecord t = claimed.get(i);
            Map<String, StepResult> steps = new LinkedHashMap<>();
            steps.put(text, new StepResult(at, text));
            steps.put("no-result", new StepResult(at, null));
            assertThat(store.progress(t.id(), t.revision(), text))
                    .as("progress %s", text)
                    .isTrue();
            assertThat(store.saveSteps(t.id(), t.revision(), steps))
                    .as("steps %s", text)
                    .isTrue();
            textById.put(t.id(), text);
            stepsById.put(t.id(), steps);
        }
        for (TaskRecord t : claimed) {
            TaskRecord read = get(t.id());
            assertThat(read.progress()).as("progress read by task").isEqualTo(textById.get(t.id()));
            assertThat(read.steps())
                    .as("steps of %s read by task", textById.get(t.id()))
                    .isEqualTo(stepsById.get(t.id()));
        }
        for (TaskRecord item : walkAllPagesOf(TaskState.LEASED, queue, 7)) {
            assertThat(item.progress()).as("progress read by the listing").isEqualTo(textById.get(item.id()));
            assertThat(item.steps())
                    .as("steps of %s read by the listing", textById.get(item.id()))
                    .isEqualTo(stepsById.get(item.id()));
        }

        Map<Long, String> reasonById = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            String text = AWKWARD_TEXTS.get(i);
            String next = AWKWARD_TEXTS.get((i + 1) % n);
            TaskRecord t = claimed.get(i);
            if (i % 3 == 0) {
                assertThat(store.cancel(t.id(), t.revision(), text, null))
                        .as("cancel request %s", text)
                        .isEqualTo(CancelOutcome.REQUESTED);
                assertThat(get(t.id()).cancelReason()).as("reason of a request").isEqualTo(text);
                assertThat(store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE))
                        .containsExactly(new LeaseStatus(t.id(), true, true));
                assertThat(store.finishCancelled(t.id(), t.revision(), next, i % 2 == 0, null))
                        .as("finishCancelled %s", next)
                        .isTrue();
                assertThat(get(t.id()).cancelReason())
                        .as("reason of finishCancelled")
                        .isEqualTo(next);
                reasonById.put(t.id(), next);
            } else if (i % 3 == 1) {
                assertThat(store.ack(t.id(), t.revision(), true, false, null))
                        .as("ack %s", text)
                        .isTrue();
                assertThat(get(t.id()).steps())
                        .as("steps of %s after ack", text)
                        .isEqualTo(stepsById.get(t.id()));
            }
            boolean later = i % 2 == 1;
            long waiting = put(later ? task().dueAt(millis(now().plus(Duration.ofHours(1)))) : task());
            assertThat(store.cancel(waiting, get(waiting).revision(), text, null))
                    .as("cancel of a waiting task %s", text)
                    .isEqualTo(CancelOutcome.CANCELLED);
            assertThat(get(waiting).cancelReason()).as("reason of a cancel").isEqualTo(text);
            reasonById.put(waiting, text);
        }
        List<TaskRecord> cancelled = walkAllPagesOf(TaskState.CANCELLED, null, 7);
        assertThat(cancelled).extracting(TaskRecord::id).containsExactlyInAnyOrderElementsOf(reasonById.keySet());
        for (TaskRecord item : cancelled) {
            assertThat(item.cancelReason()).as("reason read by the listing").isEqualTo(reasonById.get(item.id()));
        }
    }

    @Test
    void awkwardFailureTextRoundTripsThroughFailAndTheClaimThatFollows() {
        int n = AWKWARD_TEXTS.size();
        for (int i = 0; i < n; i++) {
            putNew();
        }
        List<TaskRecord> claimed = claim(n);
        assertThat(claimed).hasSize(n);
        Instant at = millis(now());
        Map<Long, FailureInfo> firstById = new LinkedHashMap<>();
        Map<Long, Integer> positionById = new LinkedHashMap<>();
        int retries = 0;
        for (int i = 0; i < n; i++) {
            String text = AWKWARD_TEXTS.get(i);
            TaskRecord t = claimed.get(i);
            FailureInfo info = new FailureInfo(1, at, text, text, text);
            boolean retry = i % 2 == 0;
            assertThat(store.fail(t.id(), t.revision(), info, retry ? millis(now().minusSeconds(1)) : null, true, null))
                    .as("fail %s", text)
                    .isEqualTo(retry ? FailOutcome.RETRY : FailOutcome.DEAD);
            retries += retry ? 1 : 0;
            firstById.put(t.id(), info);
            positionById.put(t.id(), i);
            assertThat(get(t.id()).failures()).as("failures of %s", text).containsExactly(info);
        }
        for (TaskState state : List.of(TaskState.SCHEDULED, TaskState.DEAD)) {
            List<TaskRecord> listed = walkAllPagesOf(state, queue, 7);
            assertThat(listed).as("%s listing", state).hasSize(state == TaskState.SCHEDULED ? retries : n - retries);
            for (TaskRecord item : listed) {
                assertThat(item.failures())
                        .as("failure read by the %s listing", state)
                        .containsExactly(firstById.get(item.id()));
            }
        }
        assertThat(store.promote(n).moved()).isEqualTo(retries);
        for (TaskRecord dead : walkAllPagesOf(TaskState.DEAD, queue, 100)) {
            assertThat(store.retry(dead.id(), dead.revision())).isEqualTo(RetryOutcome.RETRIED);
        }

        List<TaskRecord> again = claim(n);
        assertThat(again).hasSize(n);
        for (TaskRecord t : again) {
            FailureInfo first = firstById.get(t.id());
            assertThat(t).as("claim of %s", first.type()).isEqualTo(get(t.id()));
            assertThat(t.failures()).as("failures read by claim").containsExactly(first);
            String next = AWKWARD_TEXTS.get((positionById.get(t.id()) + 1) % n);
            FailureInfo second = new FailureInfo(2, at.plusSeconds(1), next, next, next);
            assertThat(store.fail(t.id(), t.revision(), second, null, true, null))
                    .as("second failure %s", next)
                    .isEqualTo(FailOutcome.DEAD);
            assertThat(get(t.id()).failures()).as("both failures").containsExactly(first, second);
        }
        List<TaskRecord> dead = walkAllPagesOf(TaskState.DEAD, queue, 7);
        assertThat(dead).hasSize(n);
        for (TaskRecord item : dead) {
            assertThat(item.failures()).hasSize(2).first().isEqualTo(firstById.get(item.id()));
        }
    }

    @Test
    void awkwardTextSurvivesTheExpiredLeasePaths() {
        int n = AWKWARD_TEXTS.size();
        for (int i = 0; i < n; i++) {
            putNew();
        }
        List<TaskRecord> claimed = claim(n, SHORT, null);
        assertThat(claimed).hasSize(n);
        advance(PAST_SHORT);
        assertThat(store.reclaim(n)).hasSize(n);
        Instant at = millis(now());
        Map<Long, FailureInfo> failures = new LinkedHashMap<>();
        Map<Long, String> reasons = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            String text = AWKWARD_TEXTS.get(i);
            TaskRecord t = claimed.get(i);
            if (i % 3 == 2) {
                assertThat(store.finishCancelledExpired(t.id(), t.revision(), text, i % 2 == 0, null))
                        .as("finishCancelledExpired %s", text)
                        .isTrue();
                reasons.put(t.id(), text);
                continue;
            }
            boolean retry = i % 3 == 0;
            FailureInfo info = new FailureInfo(1, at, text, text, text);
            assertThat(store.failExpired(
                            t.id(), t.revision(), info, retry ? millis(now().minusSeconds(1)) : null, true, null))
                    .as("failExpired %s", text)
                    .isEqualTo(retry ? FailOutcome.RETRY : FailOutcome.DEAD);
            failures.put(t.id(), info);
        }
        assertThat(reasons.size() + failures.size()).isEqualTo(n);
        for (TaskRecord t : walkAllPagesOf(TaskState.CANCELLED, queue, 7)) {
            assertThat(t.cancelReason()).as("reason read by the listing").isEqualTo(reasons.get(t.id()));
        }
        for (TaskState state : List.of(TaskState.SCHEDULED, TaskState.DEAD)) {
            for (TaskRecord t : walkAllPagesOf(state, queue, 7)) {
                assertThat(t.failures())
                        .as("failure read by the %s listing", state)
                        .containsExactly(failures.get(t.id()));
            }
        }
        failures.forEach((id, info) -> assertThat(get(id).failures()).containsExactly(info));
        reasons.forEach((id, reason) -> assertThat(get(id).cancelReason()).isEqualTo(reason));
    }

    @Test
    void awkwardQueueAndTypeNamesAreRegisteredListedAndKeepTheirTasks() {
        Map<String, Integer> queueIds = new LinkedHashMap<>();
        Map<String, Integer> typeIds = new LinkedHashMap<>();
        for (String text : AWKWARD_TEXTS) {
            queueIds.put(text, store.queueId(text));
            typeIds.put(text, store.typeId(text));
        }
        assertThat(queueIds.values()).as("queue ids").doesNotHaveDuplicates().doesNotContain(queue, idle);
        assertThat(typeIds.values()).as("type ids").doesNotHaveDuplicates().doesNotContain(type);
        Map<Integer, String> queueNames = store.queueNames();
        Map<Integer, String> typeNames = store.typeNames();
        List<QueueInfo> queues = store.queues();
        for (String text : AWKWARD_TEXTS) {
            assertThat(store.queueId(text)).as("queue %s again", text).isEqualTo(queueIds.get(text));
            assertThat(store.typeId(text)).as("type %s again", text).isEqualTo(typeIds.get(text));
            assertThat(queueNames).as("queueNames for %s", text).containsEntry(queueIds.get(text), text);
            assertThat(typeNames).as("typeNames for %s", text).containsEntry(typeIds.get(text), text);
            assertThat(queues).as("queues() for %s", text).contains(new QueueInfo(queueIds.get(text), text, false));
        }
        assertThat(queues).extracting(QueueInfo::id).as("queues in id order").isSorted();

        for (String text : AWKWARD_TEXTS) {
            put(task(queueIds.get(text), typeIds.get(text)));
        }
        Map<Integer, StateCounts> counts = store.counts();
        Map<Integer, Map<Integer, Long>> queuedByType = store.queuedByType();
        for (String text : AWKWARD_TEXTS) {
            assertThat(counts.get(queueIds.get(text)))
                    .as("counts of queue %s", text)
                    .isEqualTo(new StateCounts(1, 0, 0, 0, 0, 0, 0));
            assertThat(queuedByType.get(queueIds.get(text)))
                    .as("queued by type in queue %s", text)
                    .isEqualTo(Map.of(typeIds.get(text), 1L));
        }
        String paused = AWKWARD_TEXTS.get(2);
        store.pauseQueue(queueIds.get(paused), true);
        assertThat(store.queues())
                .filteredOn(QueueInfo::paused)
                .extracting(QueueInfo::name)
                .containsExactly(paused);
        assertThat(claim(1, LEASE, Set.of(typeIds.get(paused)), queueIds.get(paused)))
                .as("claim from the paused queue")
                .isEmpty();
        for (String text : AWKWARD_TEXTS) {
            if (text.equals(paused)) {
                continue;
            }
            List<TaskRecord> claimed = claim(1, LEASE, Set.of(typeIds.get(text)), queueIds.get(text));
            assertThat(claimed).as("claim from queue %s", text).hasSize(1);
            assertThat(claimed.get(0).queueId()).isEqualTo(queueIds.get(text));
            assertThat(claimed.get(0).typeId()).isEqualTo(typeIds.get(text));
        }
    }

    @Test
    void awkwardRunnerLabelsRoundTripAndComeBackWithAHeartbeat() {
        Map<Integer, String> labels = new LinkedHashMap<>();
        for (String text : AWKWARD_TEXTS) {
            labels.put(store.allocateRunnerId(text, Set.of(queue), Set.of(type)), text);
        }
        List<RunnerRecord> before = store.runners();
        labels.forEach((id, text) -> assertThat(before)
                .filteredOn(w -> w.id() == id)
                .singleElement()
                .satisfies(w -> assertThat(w.label())
                        .as("label %s after allocateRunnerId", text)
                        .isEqualTo(text)));

        advance(PAST_SHORT);
        Instant seenFrom = now();
        beat(runner, 0, true);
        long purged = store.purgeRunners(SHORT);
        assumeBefore(seenFrom.plus(SHORT));
        assertThat(purged).isEqualTo(labels.size());
        assertThat(store.runners()).extracting(RunnerRecord::id).doesNotContainAnyElementsOf(labels.keySet());
        labels.forEach((id, text) -> store.heartbeat(id, text, Set.of(queue), Set.of(type), 1, true));
        List<RunnerRecord> after = store.runners();
        labels.forEach((id, text) -> assertThat(after)
                .filteredOn(w -> w.id() == id)
                .singleElement()
                .satisfies(w -> assertThat(w.label())
                        .as("label %s after a heartbeat", text)
                        .isEqualTo(text)));
    }

    private StoredSchedule awkwardDefinition(String text) {
        return new StoredSchedule(text, type, 1, queue, text, "* * * * *", text, 3, 0);
    }

    private DueSchedule awkwardDueSchedule(String scheduleId, Instant upTo) {
        return store.dueSchedules(upTo, 1000).stream()
                .filter(d -> d.scheduleId().equals(scheduleId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("schedule " + scheduleId + " is not due"));
    }

    @Test
    void awkwardRecurringDefinitionTextRoundTripsAndEveryOperationFindsTheDefinitionById() {
        int n = AWKWARD_TEXTS.size();
        Instant fire = millis(now().plusSeconds(60));
        for (String text : AWKWARD_TEXTS) {
            assertThat(store.putSchedule(awkwardDefinition(text), fire, fire))
                    .as("put %s", text)
                    .isEqualTo(new PutScheduleResult(PutScheduleOutcome.CREATED, fire));
        }
        Map<String, Instant> schedules = store.nextFireTimes();
        List<ScheduleEntry> stored = allSchedules();
        assertThat(schedules).hasSize(n);
        assertThat(stored).hasSize(n);
        Map<String, Long> revisions = new LinkedHashMap<>();
        for (String text : AWKWARD_TEXTS) {
            assertThat(schedules).as("next fire of %s", text).containsEntry(text, fire);
            ScheduleEntry item = stored.stream()
                    .filter(s -> s.definition().scheduleId().equals(text))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("definition " + text + " is not listed"));
            assertThat(item.definition().withRevision(0))
                    .as("definition %s", text)
                    .isEqualTo(awkwardDefinition(text));
            assertThat(item.nextFireAt())
                    .as("next fire of %s in the definitions", text)
                    .isEqualTo(fire);
            revisions.put(text, item.definition().revision());
        }
        List<DueSchedule> due = store.dueSchedules(fire, 1000);
        assertThat(due).extracting(DueSchedule::scheduleId).containsExactlyInAnyOrderElementsOf(AWKWARD_TEXTS);
        for (DueSchedule d : due) {
            assertThat(d.fireAt()).as("fire time of %s", d.scheduleId()).isEqualTo(fire);
            assertThat(d.definition().withRevision(0))
                    .as("due definition %s", d.scheduleId())
                    .isEqualTo(awkwardDefinition(d.scheduleId()));
            assertThat(d.definition().revision()).isEqualTo(revisions.get(d.scheduleId()));
        }
        for (String text : AWKWARD_TEXTS) {
            assertThat(store.putSchedule(awkwardDefinition(text), fire, fire).outcome())
                    .as("second put %s", text)
                    .isEqualTo(PutScheduleOutcome.UNCHANGED);
            assertThat(revisionOf(text)).as("revision of %s", text).isEqualTo(revisions.get(text));
        }

        for (String text : AWKWARD_TEXTS) {
            assertThat(store.pauseSchedule(text, null)).as("pause %s", text).isEqualTo(ScheduleOutcome.APPLIED);
        }
        assertThat(store.dueSchedules(fire, 1000)).as("due while paused").isEmpty();
        for (String text : AWKWARD_TEXTS) {
            assertThat(store.resumeSchedule(text, null, null))
                    .as("resume %s", text)
                    .isEqualTo(ScheduleOutcome.APPLIED);
        }
        assertThat(store.dueSchedules(fire, 1000)).as("due after resume").hasSize(n);
        Instant later = fire.plusSeconds(5);
        for (String text : AWKWARD_TEXTS) {
            assertThat(store.triggerSchedule(text, null, later))
                    .as("triggerSchedule %s", text)
                    .isEqualTo(ScheduleOutcome.APPLIED);
        }
        Map<String, Instant> moved = store.nextFireTimes();
        for (String text : AWKWARD_TEXTS) {
            assertThat(moved).as("next fire of %s after triggerSchedule", text).containsEntry(text, later);
        }
        for (String text : AWKWARD_TEXTS) {
            assertThat(store.deleteSchedule(text, null)).as("delete %s", text).isEqualTo(ScheduleOutcome.APPLIED);
        }
        assertThat(store.nextFireTimes()).isEmpty();
        assertThat(allSchedules()).isEmpty();
    }

    @Test
    void awkwardTextOfAnOccurrenceAndItsOverlapKeyRoundTripsThroughCommitRecurring() {
        Instant fire = millis(now().plusSeconds(1));
        Instant next = millis(now().plusSeconds(3600));
        for (String text : AWKWARD_TEXTS) {
            store.putSchedule(awkwardDefinition(text), fire, fire);
        }
        for (String text : AWKWARD_TEXTS) {
            String key = "sched:" + text;
            TaskRecord occurrence = awkwardTask(text, true)
                    .dueAt(fire)
                    .uniqueKey(key)
                    .state(TaskState.SCHEDULED)
                    .build();
            assertThat(store.commitSchedule(awkwardDueSchedule(text, fire), next, occurrence)
                            .isCommitted())
                    .as("commit %s", text)
                    .isTrue();
            TaskRecord stored = get(occurrence.id());
            assertThat(stored.scheduleId()).as("schedule id %s", text).isEqualTo(text);
            assertThat(stored.account()).as("account %s", text).isEqualTo(text);
            assertThat(stored.headers()).as("headers %s", text).isEqualTo(Map.of(text, text));
            assertThat(stored.retryPolicy()).as("retry policy %s", text).isEqualTo(text);
            assertThat(stored.encodedPayload()).as("payload %s", text).isEqualTo(text);
            assertThat(stored.uniqueKey()).as("overlap key %s", text).isEqualTo(key);
            assertThat(store.taskByKey(key)).as("taskByKey %s", key).contains(stored);
            assertThat(store.nextFireTimes()).as("next fire %s", text).containsEntry(text, next);

            assertThat(store.triggerSchedule(text, null, fire))
                    .as("trigger %s", text)
                    .isEqualTo(ScheduleOutcome.APPLIED);
            TaskRecord overlapping = awkwardTask(text, true)
                    .dueAt(fire)
                    .uniqueKey(key)
                    .state(TaskState.SCHEDULED)
                    .build();
            assertThat(store.commitSchedule(awkwardDueSchedule(text, fire), next, overlapping)
                            .isCommitted())
                    .as("overlapping commit %s", text)
                    .isTrue();
            assertThat(store.task(overlapping.id()))
                    .as("overlapping occurrence %s", text)
                    .isEmpty();
            assertThat(store.taskByKey(key)).as("holder of %s", key).contains(stored);
            assertThat(store.nextFireTimes())
                    .as("next fire %s after the skip", text)
                    .containsEntry(text, next);
        }
    }

    @Test
    void reclaimWithALimitTakesTheLeasesThatRanOutFirst() {
        int count = 6;
        Instant base = millis(now().minusSeconds(100));
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(put(task().dueAt(base.minusSeconds(i))));
        }
        List<Long> claimOrder = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            claimOrder.add(claim(1, SHORT, null).get(0).id());
            advance(Duration.ofMillis(20));
        }
        assertThat(claimOrder)
                .as("a later id is due earlier, so it is claimed first")
                .containsExactlyElementsOf(ids.reversed());
        advance(PAST_SHORT);

        for (int limit : new int[] {1, 2, 4, 6, 10}) {
            assertThat(store.reclaim(limit))
                    .as("reclaim(%d)", limit)
                    .extracting(ExpiredLease::id)
                    .containsExactlyElementsOf(claimOrder.subList(0, Math.min(limit, count)));
        }
        assertThat(store.counts().get(queue).leased()).isEqualTo(count);
    }

    @Test
    void expireWithALimitTakesTheOverdueTasksWithTheEarliestStartByFirst() {
        int count = 6;
        Instant deadline = millis(now().plus(SHORT));
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            TaskRecord.Builder builder = task().startBy(deadline.plusMillis(10L * (count - i)));
            if (i % 2 == 1) {
                builder.dueAt(millis(now().plus(Duration.ofHours(1))));
            }
            ids.add(put(builder));
        }
        List<Long> byDeadline = ids.reversed();
        advance(PAST_SHORT);

        assertThat(store.expire(2))
                .extracting(ExpiredTask::id)
                .as("expire(2)")
                .containsExactlyInAnyOrderElementsOf(byDeadline.subList(0, 2));
        assertThat(store.expire(3))
                .extracting(ExpiredTask::id)
                .as("expire(3)")
                .containsExactlyInAnyOrderElementsOf(byDeadline.subList(2, 5));
        assertThat(get(byDeadline.get(5)).state()).isEqualTo(TaskState.QUEUED);
        assertThat(store.expire(5)).extracting(ExpiredTask::id).as("expire(5)").containsExactly(byDeadline.get(5));
        assertThat(store.expire(5)).isEmpty();
    }

    /**
     * Takes {@code count} new tasks to a terminal state, ending them one after another 20 ms apart against the
     * order of their ids, and returns the ids in the order they ended.
     */
    private List<Long> finishedAgainstIdOrder(TaskState state, int count) {
        List<Long> finished = new ArrayList<>();
        if (state == TaskState.EXPIRED) {
            Instant deadline = millis(now().plus(SHORT));
            for (int i = 0; i < count; i++) {
                put(task().startBy(deadline.plusMillis(10L * (count - i))));
            }
            advance(PAST_SHORT);
            for (int i = 0; i < count; i++) {
                finished.addAll(ids(store.expire(1)));
                advance(Duration.ofMillis(20));
            }
            assertThat(finished).hasSize(count);
            return finished;
        }
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(put(task()));
        }
        Map<Long, TaskRecord> claimed = new LinkedHashMap<>();
        if (state != TaskState.CANCELLED) {
            claim(count).forEach(t -> claimed.put(t.id(), t));
            assertThat(claimed).hasSize(count);
        }
        for (int i = count - 1; i >= 0; i--) {
            long id = ids.get(i);
            boolean ended = switch (state) {
                case SUCCEEDED -> store.ack(id, claimed.get(id).revision(), true, true, null);
                case DEAD ->
                    store.fail(id, claimed.get(id).revision(), failure(1), null, true, null) == FailOutcome.DEAD;
                default -> store.cancel(id, get(id).revision(), "stop", null) == CancelOutcome.CANCELLED;
            };
            assertThat(ended).as("%s of task %d", state, id).isTrue();
            finished.add(id);
            advance(Duration.ofMillis(20));
        }
        return finished;
    }

    @Test
    void purgeWithALimitRemovesTheTasksThatEndedFirstInEveryTerminalState() {
        for (TaskState state : List.of(TaskState.SUCCEEDED, TaskState.DEAD, TaskState.CANCELLED, TaskState.EXPIRED)) {
            List<Long> finished = finishedAgainstIdOrder(state, 6);
            advance(Duration.ofMillis(100));
            for (int round = 1; round <= 3; round++) {
                assertThat(store.purge(state, Duration.ofMillis(50), 2))
                        .as("%s round %d", state, round)
                        .isEqualTo(2);
                for (int i = 0; i < finished.size(); i++) {
                    assertThat(store.task(finished.get(i)).isPresent())
                            .as("%s task %d of the ending order after round %d", state, i, round)
                            .isEqualTo(i >= 2 * round);
                }
            }
            assertThat(store.purge(state, Duration.ofMillis(50), 2))
                    .as("%s after the last round", state)
                    .isZero();
        }
    }

    @Test
    void purgeWithALimitLargerThanOneQueueGoesOnInTheNextQueueAndTakesTheOldestOfEach() {
        int second = store.queueId("second");
        List<Long> inMain = new ArrayList<>();
        List<Long> inSecond = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            inMain.add(put(task()));
            inSecond.add(put(task(second, type)));
        }
        Map<Long, TaskRecord> claimed = new LinkedHashMap<>();
        claim(12, LEASE, null, queue, second).forEach(t -> claimed.put(t.id(), t));
        assertThat(claimed).hasSize(12);
        for (int i = 5; i >= 0; i--) {
            for (long id : new long[] {inMain.get(i), inSecond.get(i)}) {
                assertThat(store.ack(id, claimed.get(id).revision(), true, true, null))
                        .isTrue();
            }
            advance(Duration.ofMillis(20));
        }
        advance(Duration.ofMillis(100));

        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(50), 8)).isEqualTo(8);
        int removed = 0;
        for (List<Long> ids : List.of(inMain, inSecond)) {
            List<Long> endingOrder = ids.reversed();
            List<Long> survivors = endingOrder.stream()
                    .filter(id -> store.task(id).isPresent())
                    .toList();
            assertThat(survivors)
                    .as("survivors of the queue, ids in ending order %s", endingOrder)
                    .containsExactlyElementsOf(
                            endingOrder.subList(endingOrder.size() - survivors.size(), endingOrder.size()));
            removed += endingOrder.size() - survivors.size();
        }
        assertThat(removed).isEqualTo(8);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(50), 8)).isEqualTo(4);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(50), 8)).isZero();
    }

    @Test
    void releaseKeysWithALimitReleasesThatManyPerCallAndEveryEndedKeyInTheEnd() {
        List<String> ended = new ArrayList<>();
        List<String> live = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            String key = "ended-" + i;
            TaskRecord t = leased(task().uniqueKey(key).keyKeep(SHORT));
            assertThat(store.ack(t.id(), t.revision(), i % 2 == 0, true, null)).isTrue();
            ended.add(key);
        }
        for (int i = 0; i < 5; i++) {
            String key = "live-" + i;
            TaskRecord t = leased(task().uniqueKey(key).keyKeep(Duration.ofHours(1)));
            assertThat(store.ack(t.id(), t.revision(), i % 2 == 0, true, null)).isTrue();
            live.add(key);
        }
        advance(PAST_SHORT);

        if (releasesKeysOnItsOwn()) {
            assertThat(store.releaseKeys(5)).isZero();
        } else {
            assertThat(store.releaseKeys(5)).as("first call").isEqualTo(5);
            assertThat(store.releaseKeys(5)).as("second call").isEqualTo(5);
            assertThat(store.releaseKeys(5)).as("third call").isEqualTo(2);
            assertThat(store.releaseKeys(5)).as("fourth call").isZero();
        }
        for (String key : ended) {
            assertThat(store.enqueue(task().uniqueKey(key).build()))
                    .as("enqueue with the ended key %s", key)
                    .isEqualTo(EnqueueOutcome.STORED);
        }
        for (String key : live) {
            assertThat(store.enqueue(task().uniqueKey(key).build()))
                    .as("enqueue with the live key %s", key)
                    .isInstanceOf(EnqueueOutcome.DuplicateKey.class);
        }
    }

    private long queuedInEveryQueue() {
        return store.counts().values().stream().mapToLong(StateCounts::queued).sum();
    }

    @Test
    void promoteWithALimitMovesThatManyAcrossQueuesAndNeverMovesATaskThatIsNotDue() {
        int second = store.queueId("second");
        int third = store.queueId("third");
        for (int round = 0; round < 4; round++) {
            for (int q : List.of(queue, second, third)) {
                put(task(q, type).dueAt(now().plus(SHORT)));
            }
        }
        long notDue = put(task().dueAt(millis(now().plus(Duration.ofHours(1)))));
        advance(PAST_SHORT);

        PromoteResult first = store.promote(5);
        assertThat(first.moved()).as("first call").isEqualTo(5);
        assertThat(first.nextDue())
                .as("a task that is due is still waiting")
                .isBefore(now().plus(Duration.ofMinutes(30)));
        assertThat(queuedInEveryQueue()).isEqualTo(5);
        assertThat(store.promote(5).moved()).as("second call").isEqualTo(5);
        assertThat(queuedInEveryQueue()).isEqualTo(10);
        PromoteResult last = store.promote(5);
        assertThat(last.moved()).as("third call").isEqualTo(2);
        assertThat(last.nextDue()).isEqualTo(get(notDue).dueAt());
        assertThat(store.promote(5).moved()).as("fourth call").isZero();
        assertThat(queuedInEveryQueue()).isEqualTo(12);
        assertThat(get(notDue).state()).isEqualTo(TaskState.SCHEDULED);
    }

    @Test
    void dueSchedulesWithALimitReturnsOnlyDueDefinitionsAndCommittingEachReachesThemAll() {
        Instant base = millis(now().plusSeconds(1));
        Instant upTo = base.plusSeconds(10);
        Set<String> dueIds = new HashSet<>();
        for (int i = 0; i < 7; i++) {
            String id = "due-" + i;
            store.putSchedule(definition(id, "* * * * *"), base.plusSeconds(i), base.plusSeconds(i));
            dueIds.add(id);
        }
        Instant far = millis(now().plus(Duration.ofHours(2)));
        store.putSchedule(definition("far", "* * * * *"), far, far);
        store.putSchedule(definition("paused", "* * * * *"), base, base);
        assertThat(store.pauseSchedule("paused", null)).isEqualTo(ScheduleOutcome.APPLIED);

        List<DueSchedule> three = store.dueSchedules(upTo, 3);
        assertThat(three)
                .extracting(DueSchedule::scheduleId)
                .hasSize(3)
                .doesNotHaveDuplicates()
                .isSubsetOf(dueIds);
        for (DueSchedule d : three) {
            assertThat(d.fireAt()).isEqualTo(nextFireOf(d.scheduleId()));
            assertThat(d.definition()).isEqualTo(storedDefinition(d.scheduleId()));
        }
        assertThat(store.dueSchedules(upTo, 100))
                .extracting(DueSchedule::scheduleId)
                .containsExactlyInAnyOrderElementsOf(dueIds);

        List<String> seen = new ArrayList<>();
        List<Integer> sizes = new ArrayList<>();
        for (List<DueSchedule> batch = store.dueSchedules(upTo, 3);
                !batch.isEmpty();
                batch = store.dueSchedules(upTo, 3)) {
            sizes.add(batch.size());
            for (DueSchedule d : batch) {
                TaskRecord occurrence = task().scheduleId(d.scheduleId())
                        .dueAt(d.fireAt())
                        .state(TaskState.SCHEDULED)
                        .build();
                assertThat(store.commitSchedule(d, far, occurrence).isCommitted())
                        .as("commit %s", d.scheduleId())
                        .isTrue();
                seen.add(d.scheduleId());
            }
        }
        assertThat(sizes).containsExactly(3, 3, 1);
        assertThat(seen).containsExactlyInAnyOrderElementsOf(dueIds);
        assertThat(store.nextFireTimes()).containsEntry("far", far).containsEntry("paused", base);
    }

    private void orderingSkipIdsTo(long target) {
        long next = store.idHighWaterMark();
        if (next < target) {
            store.allocateIds((int) (target - next));
        }
    }

    /** Eleven tasks in two queues taken to {@code state}, in groups that share a time, with ids from {@code firstId}. */
    private List<Long> orderingGroup(TaskState state, long firstId, int q1, int q2) {
        orderingSkipIdsTo(firstId);
        Instant past = millis(now().minusSeconds(100));
        Instant future = millis(now().plus(Duration.ofHours(1)));
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            TaskRecord.Builder builder = task(i % 2 == 0 ? q1 : q2, type);
            switch (state) {
                case QUEUED -> builder.dueAt(past.plusSeconds((10 - i) / 3));
                case SCHEDULED -> builder.dueAt(future.plusSeconds((10 - i) / 3));
                case EXPIRED -> builder.startBy(now().plus(SHORT));
                default -> {}
            }
            ids.add(put(builder));
        }
        switch (state) {
            case LEASED -> {
                for (int size : new int[] {4, 4, 3}) {
                    assertThat(claim(size, LEASE, null, q1, q2)).hasSize(size);
                    advance(Duration.ofMillis(20));
                }
            }
            case SUCCEEDED -> {
                List<LeaseRef> refs = claim(11, LEASE, null, q1, q2).stream()
                        .map(t -> new LeaseRef(t.id(), t.revision()))
                        .toList();
                assertThat(refs).hasSize(11);
                for (int from = 0; from < refs.size(); from += 4) {
                    assertThat(store.ackAll(refs.subList(from, Math.min(from + 4, refs.size())), true, true))
                            .containsOnly(true);
                    advance(Duration.ofMillis(20));
                }
            }
            case DEAD -> {
                List<TaskRecord> claimed = claim(11, LEASE, null, q1, q2);
                assertThat(claimed).hasSize(11);
                for (int i = 0; i < claimed.size(); i++) {
                    assertThat(store.fail(claimed.get(i).id(), claimed.get(i).revision(), failure(1), null, true, null))
                            .isEqualTo(FailOutcome.DEAD);
                    if (i % 3 == 2) {
                        advance(Duration.ofMillis(20));
                    }
                }
            }
            case CANCELLED -> {
                for (int i = 0; i < ids.size(); i++) {
                    assertThat(store.cancel(ids.get(i), get(ids.get(i)).revision(), "stop", null))
                            .isEqualTo(CancelOutcome.CANCELLED);
                    if (i % 3 == 2) {
                        advance(Duration.ofMillis(20));
                    }
                }
            }
            case EXPIRED -> {
                advance(PAST_SHORT);
                for (int size : new int[] {4, 4, 3}) {
                    assertThat(store.expire(size)).hasSize(size);
                    advance(Duration.ofMillis(20));
                }
            }
            default -> {}
        }
        return ids;
    }

    private void assertListedInTheStatedOrder(TaskState state, List<Long> ids, int q1, int q2) {
        boolean waiting = state == TaskState.QUEUED || state == TaskState.SCHEDULED;
        Function<TaskRecord, Instant> sortKey = waiting ? TaskRecord::dueAt : TaskRecord::stateChangedAt;
        List<TaskRecord> expected = ids.stream()
                .map(this::get)
                .sorted(Comparator.comparing(sortKey).thenComparing(TaskRecord::id, orderOfIdsThatShareATime()))
                .toList();
        assertThat(expected).extracting(TaskRecord::state).containsOnly(state);
        List<Integer> filters = new ArrayList<>();
        filters.add(null);
        filters.add(q1);
        filters.add(q2);
        for (Integer queueId : filters) {
            List<Long> expectedIds = expected.stream()
                    .filter(t -> queueId == null || t.queueId() == queueId)
                    .map(TaskRecord::id)
                    .toList();
            for (int limit : new int[] {100, 4}) {
                assertThat(walkAllPagesOf(state, queueId, limit))
                        .extracting(TaskRecord::id)
                        .as(
                                "%s listing of %s with limit %d",
                                state, queueId == null ? "every queue" : "queue " + queueId, limit)
                        .containsExactlyElementsOf(expectedIds);
            }
        }
    }

    @Test
    void listingsFollowTheStatedOrderInEveryStateAcrossQueuesAndIdsOfDifferentLength() {
        List<TaskState> states = List.of(
                TaskState.QUEUED,
                TaskState.SCHEDULED,
                TaskState.LEASED,
                TaskState.SUCCEEDED,
                TaskState.DEAD,
                TaskState.CANCELLED,
                TaskState.EXPIRED);
        long[] firstIds = {5, 95, 995, 9995, 10_100, 10_200, 10_300};
        for (int s = 0; s < states.size(); s++) {
            TaskState state = states.get(s);
            int q1 = store.queueId("order-" + state + "-a");
            int q2 = store.queueId("order-" + state + "-b");
            List<Long> ids = orderingGroup(state, firstIds[s], q1, q2);
            assertListedInTheStatedOrder(state, ids, q1, q2);
        }
    }

    @Test
    void aTaskDueBeforeNowIsClaimableAndOneDueLaterIsNotUntilItsTimeHasPassed() {
        long immediate = put(task().dueAt(null));
        long ago = put(task().dueAt(millis(now().minusSeconds(1))));
        Instant soonAt = millis(now().plus(MID));
        long soon = put(task().dueAt(soonAt));
        assertThat(get(immediate).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(ago).state()).isEqualTo(TaskState.QUEUED);
        TaskState soonState = get(soon).state();
        assumeBefore(soonAt);
        assertThat(soonState).isEqualTo(TaskState.SCHEDULED);

        List<TaskRecord> claimed = claim(10);
        long movedEarly = store.promote(10).moved();
        assumeBefore(soonAt);
        assertThat(claimed).extracting(TaskRecord::id).containsExactlyInAnyOrder(immediate, ago);
        assertThat(movedEarly).isZero();
        advance(PAST_MID);
        assertThat(store.promote(10).moved()).isEqualTo(1);
        assertThat(claim(10)).extracting(TaskRecord::id).containsExactly(soon);
    }

    @Test
    void aLeaseWithTimeLeftIsNotReclaimedAndOneThatRanOutIs() {
        TaskRecord longLease = leased(task());
        put(task());
        Instant midFrom = now();
        TaskRecord midLease = claim(1, MID, null).get(0);
        assertThat(midLease.leaseUntil()).isAfterOrEqualTo(millis(midFrom).plus(MID));
        put(task());
        Instant shortFrom = now();
        TaskRecord shortLease = claim(1, SHORT, null).get(0);
        assertThat(shortLease.leaseUntil()).isAfterOrEqualTo(millis(shortFrom).plus(SHORT));
        List<ExpiredLease> early = store.reclaim(10);
        assumeBefore(shortLease.leaseUntil());
        assertThat(early).as("right after the claims").isEmpty();

        advance(PAST_SHORT);
        List<ExpiredLease> afterShort = store.reclaim(10);
        assumeBefore(midLease.leaseUntil());
        assertThat(afterShort)
                .as("after the short lease")
                .extracting(ExpiredLease::id)
                .containsExactly(shortLease.id());
        advance(PAST_MID.minus(PAST_SHORT));
        assertThat(store.reclaim(10))
                .as("after the middle lease")
                .extracting(ExpiredLease::id)
                .containsExactly(shortLease.id(), midLease.id());
        assertThat(get(longLease.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void aStartByInThePastExpiresTheTaskAndOneInTheFutureDoesNot() {
        long past = put(task().startBy(now().minusSeconds(1)));
        long scheduledPast = put(task().dueAt(now().plus(Duration.ofHours(1))).startBy(now().minusSeconds(1)));
        long future = put(task().startBy(now().plus(Duration.ofHours(1))));
        long none = putNew();

        assertThat(claim(10)).extracting(TaskRecord::id).containsExactlyInAnyOrder(future, none);
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactlyInAnyOrder(past, scheduledPast);
        assertThat(get(past).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(get(scheduledPast).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(get(future).state()).isEqualTo(TaskState.LEASED);
        assertThat(store.expire(10)).isEmpty();
    }

    @Test
    void aKeyWindowThatEndedNoLongerBlocksAnEnqueueOnEveryWayATaskEnds() {
        Duration window = MID;
        Map<String, Long> holders = new LinkedHashMap<>();
        TaskRecord acked = leased(task().uniqueKey("acked").keyKeep(window));
        Instant keptFrom = now();
        assertThat(store.ack(acked.id(), acked.revision(), true, true, null)).isTrue();
        holders.put("acked", acked.id());
        TaskRecord removed = leased(task().uniqueKey("removed").keyKeep(window));
        assertThat(store.ack(removed.id(), removed.revision(), false, false, null))
                .isTrue();
        holders.put("removed", removed.id());
        TaskRecord dead = leased(task().uniqueKey("dead").keyKeep(window));
        assertThat(store.fail(dead.id(), dead.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        holders.put("dead", dead.id());
        TaskRecord gone = leased(task().uniqueKey("gone").keyKeep(window));
        assertThat(store.fail(gone.id(), gone.revision(), failure(1), null, false, null))
                .isEqualTo(FailOutcome.DEAD);
        holders.put("gone", gone.id());
        TaskRecord cancelled = leased(task().uniqueKey("cancelled").keyKeep(window));
        assertThat(store.finishCancelled(cancelled.id(), cancelled.revision(), "stop", true, null))
                .isTrue();
        holders.put("cancelled", cancelled.id());
        TaskRecord batchedKept = leased(task().uniqueKey("batched-kept").keyKeep(window));
        TaskRecord batchedKept2 = leased(task().uniqueKey("batched-kept-2").keyKeep(window));
        assertThat(store.ackAll(
                        List.of(
                                new LeaseRef(batchedKept.id(), batchedKept.revision()),
                                new LeaseRef(batchedKept2.id(), batchedKept2.revision())),
                        true,
                        true))
                .containsExactly(true, true);
        holders.put("batched-kept", batchedKept.id());
        holders.put("batched-kept-2", batchedKept2.id());
        TaskRecord batchedGone = leased(task().uniqueKey("batched-gone").keyKeep(window));
        TaskRecord batchedGone2 = leased(task().uniqueKey("batched-gone-2").keyKeep(window));
        assertThat(store.ackAll(
                        List.of(
                                new LeaseRef(batchedGone.id(), batchedGone.revision()),
                                new LeaseRef(batchedGone2.id(), batchedGone2.revision())),
                        false,
                        false))
                .containsExactly(true, true);
        holders.put("batched-gone", batchedGone.id());
        holders.put("batched-gone-2", batchedGone2.id());
        long queued = put(task().uniqueKey("cancel-queued").keyKeep(window));
        assertThat(store.cancel(queued, get(queued).revision(), "stop", null)).isEqualTo(CancelOutcome.CANCELLED);
        holders.put("cancel-queued", queued);
        long scheduled =
                put(task().uniqueKey("cancel-scheduled").keyKeep(window).dueAt(now().plus(Duration.ofHours(1))));
        assertThat(store.cancel(scheduled, get(scheduled).revision(), "stop", null))
                .isEqualTo(CancelOutcome.CANCELLED);
        holders.put("cancel-scheduled", scheduled);
        long expiring = put(task().uniqueKey("expired").keyKeep(window).startBy(now().plus(SHORT)));

        holders.forEach((key, id) -> {
            EnqueueOutcome whileOpen = store.enqueue(task().uniqueKey(key).build());
            assumeBefore(keptFrom.plus(window));
            assertThat(whileOpen).as("%s while its window is open", key).isEqualTo(new EnqueueOutcome.DuplicateKey(id));
        });
        advance(PAST_MID);
        Instant expiredFrom = now();
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(expiring);
        Instant expiredUntil = get(expiring).keyUntil();
        assertThat(expiredUntil).isAfterOrEqualTo(millis(expiredFrom).plus(window));
        EnqueueOutcome whileExpiredOpen =
                store.enqueue(task().uniqueKey("expired").build());
        assumeBefore(expiredUntil);
        assertThat(whileExpiredOpen)
                .as("expired while its window is open")
                .isEqualTo(new EnqueueOutcome.DuplicateKey(expiring));

        Map<String, Long> reused = new LinkedHashMap<>();
        for (String key : holders.keySet()) {
            TaskRecord again = task().uniqueKey(key).build();
            assertThat(store.enqueue(again))
                    .as("%s after its window ended, with no releaseKeys call", key)
                    .isEqualTo(EnqueueOutcome.STORED);
            assertThat(store.taskByKey(key))
                    .map(TaskRecord::id)
                    .as("holder of %s", key)
                    .contains(again.id());
            reused.put(key, again.id());
        }
        advance(PAST_MID);
        TaskRecord afterExpiry = task().uniqueKey("expired").build();
        assertThat(store.enqueue(afterExpiry))
                .as("expired after its window ended")
                .isEqualTo(EnqueueOutcome.STORED);
        reused.put("expired", afterExpiry.id());

        store.releaseKeys(100);
        reused.forEach(
                (key, id) -> assertThat(store.enqueue(task().uniqueKey(key).build()))
                        .as("%s after releaseKeys", key)
                        .isEqualTo(new EnqueueOutcome.DuplicateKey(id)));
    }

    @Test
    void thePurgeRetentionCutoffKeepsYoungerTasksAndRemovesOlderOnesInEveryTerminalState() {
        List<Long> toAck = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            toAck.add(put(task()));
        }
        Map<Long, TaskRecord> claimed = new LinkedHashMap<>();
        claim(4).forEach(t -> claimed.put(t.id(), t));
        assertThat(claimed).hasSize(4);
        long cancelOld = putNew();
        long cancelYoung = putNew();
        Instant deadline = millis(now().plus(SHORT));
        put(task().startBy(deadline));
        put(task().startBy(deadline.plusMillis(10)));
        advance(Duration.ofMillis(250));

        Map<TaskState, Long> older = new LinkedHashMap<>();
        Map<TaskState, Long> younger = new LinkedHashMap<>();
        endOneOfEach(older, claimed.get(toAck.get(0)), claimed.get(toAck.get(1)), cancelOld);
        advance(Duration.ofMillis(500));
        Instant youngerFrom = now();
        endOneOfEach(younger, claimed.get(toAck.get(2)), claimed.get(toAck.get(3)), cancelYoung);
        advance(Duration.ofMillis(200));

        for (TaskState state : older.keySet()) {
            long purged = store.purge(state, MID, 10);
            assumeBefore(youngerFrom.plus(MID));
            assertThat(purged).as("%s purge", state).isEqualTo(1);
            assertThat(store.task(older.get(state))).as("older %s", state).isEmpty();
            assertThat(store.task(younger.get(state))).as("younger %s", state).isPresent();
        }
    }

    private void endOneOfEach(Map<TaskState, Long> ended, TaskRecord toAck, TaskRecord toKill, long toCancel) {
        assertThat(store.ack(toAck.id(), toAck.revision(), true, true, null)).isTrue();
        ended.put(TaskState.SUCCEEDED, toAck.id());
        assertThat(store.fail(toKill.id(), toKill.revision(), failure(1), null, true, null))
                .isEqualTo(FailOutcome.DEAD);
        ended.put(TaskState.DEAD, toKill.id());
        assertThat(store.cancel(toCancel, get(toCancel).revision(), "stop", null))
                .isEqualTo(CancelOutcome.CANCELLED);
        ended.put(TaskState.CANCELLED, toCancel);
        List<Long> expired = ids(store.expire(1));
        assertThat(expired).hasSize(1);
        ended.put(TaskState.EXPIRED, expired.get(0));
    }

    private TaskRecord bulkTask(long id, int queueId, int typeId, Instant due, int n) {
        TaskRecord.Builder builder = TaskRecord.builder()
                .id(id)
                .queueId(queueId)
                .typeId(typeId)
                .maxAttempts(3)
                .createdAt(millis(now()))
                .dueAt(due)
                .encodedPayload("payload-" + n)
                .account("account-" + n % 7)
                .headers(Map.of("n", Integer.toString(n)));
        if (n % 10 == 0) {
            builder.uniqueKey("bulk-key-" + n);
        }
        return builder.build();
    }

    private static List<LeaseRef> bulkLeaseRefs(List<TaskRecord> claimed) {
        return claimed.stream().map(t -> new LeaseRef(t.id(), t.revision())).toList();
    }

    @Test
    void aBatchOfFiveThousandTasksIsStoredListedCountedAndDrainedInBatchesOfAThousand() {
        int total = 5000;
        int second = store.queueId("second");
        int other = store.typeId("other");
        long first = store.allocateIds(total);
        Instant base = millis(now().minus(Duration.ofHours(1)));
        List<TaskRecord> batch = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            batch.add(
                    bulkTask(first + i, i % 5 == 0 ? second : queue, i % 2 == 0 ? type : other, base.plusMillis(i), i));
        }

        assertThat(store.enqueueAll(batch)).as("outcomes").hasSize(total).containsOnly(EnqueueOutcome.STORED);
        Map<Integer, StateCounts> counts = store.counts();
        assertThat(counts.get(queue)).isEqualTo(new StateCounts(4000, 0, 0, 0, 0, 0, 0));
        assertThat(counts.get(second)).isEqualTo(new StateCounts(1000, 0, 0, 0, 0, 0, 0));
        Map<Integer, Map<Integer, Long>> expectedByType = new java.util.HashMap<>();
        for (TaskRecord record : batch) {
            expectedByType
                    .computeIfAbsent(record.queueId(), k -> new java.util.HashMap<>())
                    .merge(record.typeId(), 1L, Long::sum);
        }
        assertThat(store.queuedByType()).isEqualTo(expectedByType);

        List<TaskRecord> listed = walkAllPagesOf(TaskState.QUEUED, null, 1000);
        assertThat(listed)
                .extracting(TaskRecord::id)
                .containsExactlyElementsOf(batch.stream().map(TaskRecord::id).toList());
        List<Long> different = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            TaskRecord a = listed.get(i);
            TaskRecord g = batch.get(i);
            if (a.queueId() != g.queueId()
                    || a.typeId() != g.typeId()
                    || !a.dueAt().equals(g.dueAt())
                    || !Objects.equals(a.encodedPayload(), g.encodedPayload())
                    || !Objects.equals(a.account(), g.account())
                    || !Objects.equals(a.headers(), g.headers())
                    || !Objects.equals(a.uniqueKey(), g.uniqueKey())) {
                different.add(g.id());
            }
        }
        assertThat(different).as("tasks that read back differently").isEmpty();
        assertThat(walkAllPagesOf(TaskState.QUEUED, second, 700))
                .extracting(TaskRecord::id)
                .containsExactlyElementsOf(batch.stream()
                        .filter(r -> r.queueId() == second)
                        .map(TaskRecord::id)
                        .toList());
        assertThat(store.taskByKey("bulk-key-4990")).map(TaskRecord::id).contains(first + 4990);
        assertThat(store.taskByKey("bulk-key-4995")).isEmpty();

        int more = 3000;
        long firstMore = store.allocateIds(more);
        List<TaskRecord> mixed = new ArrayList<>();
        List<EnqueueOutcome> expectedOutcomes = new ArrayList<>();
        for (int j = 0; j < more; j++) {
            switch (j % 3) {
                case 0 -> {
                    mixed.add(batch.get(j).toBuilder()
                            .uniqueKey(null)
                            .encodedPayload("conflicting-" + j)
                            .build());
                    expectedOutcomes.add(EnqueueOutcome.EXISTS);
                }
                case 1 -> {
                    int holder = 10 * ((j / 3) % 500);
                    mixed.add(bulkTask(firstMore + j, queue, type, base.plusMillis(total + j), total + j).toBuilder()
                            .uniqueKey("bulk-key-" + holder)
                            .build());
                    expectedOutcomes.add(new EnqueueOutcome.DuplicateKey(first + holder));
                }
                default -> {
                    mixed.add(
                            bulkTask(firstMore + j, queue, type, base.plusMillis(total + j), total + j + 1).toBuilder()
                                    .uniqueKey(null)
                                    .build());
                    expectedOutcomes.add(EnqueueOutcome.STORED);
                }
            }
        }
        assertThat(store.enqueueAll(mixed)).as("mixed outcomes").containsExactlyElementsOf(expectedOutcomes);
        assertThat(get(first).encodedPayload()).isEqualTo("payload-0");
        assertThat(store.task(firstMore + 1)).isEmpty();
        assertThat(get(firstMore + 2).state()).isEqualTo(TaskState.QUEUED);
        assertThat(queuedInEveryQueue()).isEqualTo(6000);

        List<Integer> sizes = new ArrayList<>();
        for (List<TaskRecord> got = claim(1000, queue, second); !got.isEmpty(); got = claim(1000, queue, second)) {
            sizes.add(got.size());
            assertThat(store.ackAll(bulkLeaseRefs(got), false, false))
                    .hasSize(got.size())
                    .containsOnly(true);
        }
        assertThat(sizes).containsExactly(1000, 1000, 1000, 1000, 1000, 1000);
        assertThat(queuedInEveryQueue()).isZero();
        assertThat(store.queuedByType()).isEmpty();
    }

    @Test
    void aThousandLeasesAreClaimedRenewedAndAcknowledgedInOneCallEach() {
        int total = 1200;
        long first = store.allocateIds(total);
        Instant base = millis(now().minus(Duration.ofHours(1)));
        List<TaskRecord> batch = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            batch.add(bulkTask(first + i, queue, type, base.plusMillis(i), i));
        }
        assertThat(store.enqueueAll(batch)).hasSize(total).containsOnly(EnqueueOutcome.STORED);

        List<TaskRecord> claimed = claim(1000);
        assertThat(claimed)
                .extracting(TaskRecord::id)
                .containsExactlyElementsOf(
                        batch.subList(0, 1000).stream().map(TaskRecord::id).toList());
        assertThat(counts()).isEqualTo(new StateCounts(200, 0, 1000, 0, 0, 0, 0));
        assertThat(walkAllPagesOf(TaskState.LEASED, queue, 1000)).hasSize(1000);

        List<LeaseRef> refs = bulkLeaseRefs(claimed);
        List<LeaseRef> sent = new ArrayList<>(refs);
        for (int stale : new int[] {100, 700}) {
            sent.set(stale, new LeaseRef(refs.get(stale).id(), refs.get(stale).revision() + 1));
        }
        assertThat(store.cancel(refs.get(500).id(), refs.get(500).revision(), "stop", null))
                .isEqualTo(CancelOutcome.REQUESTED);

        List<LeaseStatus> statuses = store.renew(sent, Duration.ofMinutes(10));
        assertThat(statuses).hasSize(1000);
        List<String> wrongStatus = new ArrayList<>();
        for (int i = 0; i < statuses.size(); i++) {
            LeaseStatus s = statuses.get(i);
            boolean stale = i == 100 || i == 700;
            if (s.id() != refs.get(i).id() || s.owned() == stale || s.cancelRequested() != (i == 500)) {
                wrongStatus.add(i + ": " + s);
            }
        }
        assertThat(wrongStatus).as("statuses that differ from what was sent").isEmpty();
        TaskRecord renewed = get(refs.get(0).id());
        assertThat(renewed.leaseUntil()).isAfter(renewed.claimedAt().plus(Duration.ofMinutes(9)));
        TaskRecord untouched = get(refs.get(100).id());
        assertThat(untouched.leaseUntil()).isBefore(untouched.claimedAt().plus(Duration.ofMinutes(1)));

        List<Boolean> acked = store.ackAll(sent, true, false);
        assertThat(acked).hasSize(1000);
        List<Integer> wrongAck = new ArrayList<>();
        for (int i = 0; i < acked.size(); i++) {
            if (acked.get(i) == (i == 100 || i == 700)) {
                wrongAck.add(i);
            }
        }
        assertThat(wrongAck)
                .as("acknowledgements that differ from what was sent")
                .isEmpty();
        assertThat(counts()).isEqualTo(new StateCounts(200, 0, 2, 998, 0, 0, 0));
        List<TaskRecord> done = walkAllPagesOf(TaskState.SUCCEEDED, queue, 1000);
        assertThat(done).hasSize(998);
        assertThat(done.stream().filter(t -> t.encodedPayload() != null).map(TaskRecord::id))
                .as("succeeded tasks that kept their payload")
                .isEmpty();

        List<TaskRecord> rest = claim(1000);
        assertThat(rest).hasSize(200);
        assertThat(store.ackAll(bulkLeaseRefs(rest), false, false)).hasSize(200).containsOnly(true);
        advance(Duration.ofMillis(50));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(10), 1000))
                .isEqualTo(998);
        assertThat(counts()).isEqualTo(new StateCounts(0, 0, 2, 0, 0, 0, 0));
    }
}

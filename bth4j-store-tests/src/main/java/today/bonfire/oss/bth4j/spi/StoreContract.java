package today.bonfire.oss.bth4j.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    protected FullStore store;
    private int worker;
    private int queue;
    private int event;

    /** @return a store with no state, isolated from every other namespace */
    protected abstract FullStore newStore(String namespace);

    /** @return the clock the store reads, when the test can control it; otherwise the system clock */
    protected Clock clock() {
        return Clock.systemUTC();
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

    /** @return the largest queue or event id the store under test accepts; the registry test is skipped above 1000 */
    protected int registryBound() {
        return 32767;
    }

    @BeforeEach
    void setUpStore() {
        store = newStore("contract" + COUNTER.incrementAndGet() + "x" + System.nanoTime() % 100_000);
        store.init();
        worker = store.allocateWorkerId("contract-test");
        queue = store.queueId("main");
        event = store.eventId("work");
    }

    private Instant now() {
        return clock().instant();
    }

    private TaskRecord.Builder task() {
        return task(queue, event);
    }

    private TaskRecord.Builder task(int queueId, int eventId) {
        return TaskRecord.builder()
                .id(store.allocateIds(1))
                .queueId(queueId)
                .eventId(eventId)
                .maxAttempts(3)
                .created(now())
                .data("payload");
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
        return store.getTask(id).orElseThrow(() -> new AssertionError("task " + id + " is missing"));
    }

    private List<TaskRecord> claim(int count, int... queueIds) {
        return claim(count, LEASE, null, queueIds);
    }

    private List<TaskRecord> claim(int count, Duration lease, Set<Integer> events, int... queueIds) {
        List<Integer> queues = new ArrayList<>();
        for (int q : queueIds) {
            queues.add(q);
        }
        if (queues.isEmpty()) {
            queues.add(queue);
        }
        return store.claim(new ClaimRequest(queues, count, lease, worker, events));
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
                results.add(f.get());
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    // ids and registries

    @Test
    void infoReportsTheSchemaVersion() {
        assertThat(store.info().schemaVersion()).isEqualTo(StoreInfo.SCHEMA_VERSION);
        assertThat(store.info().engine()).isNotBlank();
    }

    @Test
    void initIsRepeatable() {
        store.init();
        store.init();
        assertThat(store.info().schemaVersion()).isEqualTo(StoreInfo.SCHEMA_VERSION);
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
    void allocateWorkerIdIsUniqueAcrossCalls() {
        int a = store.allocateWorkerId("a");
        int b = store.allocateWorkerId("b");
        assertThat(a).isNotEqualTo(b).isNotEqualTo(worker);
        assertThat(b).isNotEqualTo(worker);
    }

    @Test
    void queueIdIsStablePermanentAndStartsAtOne() {
        TaskStore fresh = newStore("fresh" + COUNTER.incrementAndGet());
        assertThat(fresh.queueId("a")).isEqualTo(1);
        assertThat(fresh.queueId("b")).isEqualTo(2);
        assertThat(fresh.queueId("a")).isEqualTo(1);
    }

    @Test
    void eventIdIsStablePermanentAndStartsAtOne() {
        TaskStore fresh = newStore("fresh" + COUNTER.incrementAndGet());
        assertThat(fresh.eventId("a")).isEqualTo(1);
        assertThat(fresh.eventId("b")).isEqualTo(2);
        assertThat(fresh.eventId("a")).isEqualTo(1);
    }

    @Test
    void queueAndEventRegistriesAreIndependent() {
        TaskStore fresh = newStore("fresh" + COUNTER.incrementAndGet());
        assertThat(fresh.queueId("x")).isEqualTo(1);
        assertThat(fresh.eventId("y")).isEqualTo(1);
        assertThat(fresh.queueNames()).containsOnly(Map.entry(1, "x"));
        assertThat(fresh.eventNames()).containsOnly(Map.entry(1, "y"));
    }

    @Test
    void registryNamesMapIdToName() {
        int other = store.queueId("other");
        int ev = store.eventId("other-event");
        assertThat(store.queueNames()).containsEntry(queue, "main").containsEntry(other, "other");
        assertThat(store.eventNames()).containsEntry(event, "work").containsEntry(ev, "other-event");
    }

    @Test
    void registryRefusesMoreNamesThanItsBound() {
        int bound = registryBound();
        Assumptions.assumeTrue(bound <= 1000, "bound too large to fill against this store");
        TaskStore fresh = newStore("fresh" + COUNTER.incrementAndGet());
        for (int i = 1; i <= bound; i++) {
            assertThat(fresh.queueId("q" + i)).isEqualTo(i);
        }
        assertThatThrownBy(() -> fresh.queueId("one-too-many")).isInstanceOf(TaskStoreException.class);
        assertThat(fresh.queueId("q1")).isEqualTo(1);
    }

    // enqueue

    @Test
    void enqueueStoresAnImmediateTaskAsQueuedWithStoreTimes() {
        Instant before = now();
        long id = put(task().due(null));
        Instant after = now();
        TaskRecord t = get(id);
        assertThat(t.state()).isEqualTo(TaskState.QUEUED);
        assertThat(t.due()).isBetween(millis(before), after.plusMillis(1));
        assertThat(t.stateChanged()).isBetween(millis(before), after.plusMillis(1));
        assertThat(t.attempts()).isZero();
        assertThat(t.queueId()).isEqualTo(queue);
        assertThat(t.eventId()).isEqualTo(event);
        assertThat(t.data()).isEqualTo("payload");
    }

    @Test
    void enqueueWithFutureDueIsScheduledAndKeepsDue() {
        Instant due = millis(now().plus(Duration.ofHours(1)));
        long id = put(task().due(due));
        TaskRecord t = get(id);
        assertThat(t.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(t.due()).isEqualTo(due);
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
                .maxRun(Duration.ofSeconds(9)));
        TaskRecord t = get(id);
        assertThat(t.account()).isEqualTo("acme");
        assertThat(t.headers()).isEqualTo(headers);
        assertThat(t.scheduleId()).isEqualTo("nightly");
        assertThat(t.retryPolicy()).isEqualTo("slow");
        assertThat(t.retryOf()).isEqualTo(7L);
        assertThat(t.payloadVersion()).isEqualTo(4);
        assertThat(t.maxRun()).isEqualTo(Duration.ofSeconds(9));
        assertThat(t.maxAttempts()).isEqualTo(3);
    }

    @Test
    void enqueueOfAnExistingIdReturnsExistsAndChangesNothing() {
        TaskRecord original = task().data("first").build();
        assertThat(store.enqueue(original)).isEqualTo(EnqueueOutcome.STORED);
        assertThat(store.enqueue(original.toBuilder().data("second").build())).isEqualTo(EnqueueOutcome.EXISTS);
        assertThat(get(original.id()).data()).isEqualTo("first");
    }

    @Test
    void enqueueOfAnExistingIdReturnsExistsInEveryState() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        assertThat(store.enqueue(t.toBuilder().build())).isEqualTo(EnqueueOutcome.EXISTS);
    }

    @Test
    void duplicateKeyNamesTheHolder() {
        long holder = put(task().uniqueKey("order-1"));
        TaskRecord second = task().uniqueKey("order-1").build();
        assertThat(store.enqueue(second)).isEqualTo(new EnqueueOutcome.DuplicateKey(holder));
        assertThat(store.getTask(second.id())).isEmpty();
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
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        put(task().uniqueKey("k"));
    }

    @Test
    void keyIsReleasedWhenTheTaskDiesWithoutKeyKeep() {
        TaskRecord t = leased(task().uniqueKey("k"));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
        put(task().uniqueKey("k"));
    }

    @Test
    void keyStaysHeldAfterTerminalUntilKeyUntil() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.keyUntil())
                .isBetween(
                        done.stateChanged().plus(SHORT).minusMillis(5),
                        done.stateChanged().plus(SHORT).plusMillis(5));
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
        advance(PAST_SHORT);
        put(task().uniqueKey("k"));
    }

    @Test
    void enqueueAllStoresInOrderAndReportsEachOutcome() {
        TaskRecord a = task().uniqueKey("k").build();
        TaskRecord b = task().uniqueKey("k").build();
        TaskRecord c = task().build();
        List<EnqueueOutcome> outcomes = store.enqueueAll(List.of(a, b, c, a));
        assertThat(outcomes)
                .containsExactly(
                        EnqueueOutcome.STORED,
                        new EnqueueOutcome.DuplicateKey(a.id()),
                        EnqueueOutcome.STORED,
                        EnqueueOutcome.EXISTS);
        assertThat(store.getTask(b.id())).isEmpty();
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
        assertThat(t.workerId()).isEqualTo(worker);
        assertThat(t.claimedAt()).isAfterOrEqualTo(millis(before));
        assertThat(t.leaseUntil()).isAfterOrEqualTo(millis(before).plus(LEASE));
        assertThat(claimed.revision()).isEqualTo(t.revision());
        assertThat(claimed.state()).isEqualTo(TaskState.LEASED);
        assertThat(claimed.attempts()).isEqualTo(1);
    }

    @Test
    void claimKeepsDue() {
        long id = put(task());
        Instant due = get(id).due();
        claimOne();
        assertThat(get(id).due()).isEqualTo(due);
    }

    @Test
    void claimReturnsNothingFromAnEmptyQueue() {
        assertThat(claim(5)).isEmpty();
    }

    @Test
    void claimOrdersByDueThenId() {
        Instant base = millis(now().minusSeconds(60));
        long late = put(task().due(base.plusSeconds(10)));
        long earlyB = put(task().due(base));
        long earlyA = put(task().due(base));
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
        assertThat(store.tasks(TaskState.QUEUED, queue, 0, 10)).hasSize(2);
    }

    @Test
    void claimReadsQueuesInRequestOrder() {
        int second = store.queueId("second");
        long inMain = put(task());
        long inSecond = put(task(second, event));
        List<Long> order = claim(2, second, queue).stream().map(TaskRecord::id).toList();
        assertThat(order).containsExactly(inSecond, inMain);
    }

    @Test
    void claimReadsALaterQueueOnlyToFillTheCount() {
        int second = store.queueId("second");
        long inMain = put(task());
        long inSecond = put(task(second, event));
        assertThat(claim(1, queue, second)).extracting(TaskRecord::id).containsExactly(inMain);
        assertThat(get(inSecond).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void claimIgnoresScheduledTasks() {
        put(task().due(now().plus(Duration.ofHours(1))));
        assertThat(claim(5)).isEmpty();
    }

    @Test
    void claimSkipsUnknownEventsWithoutCountingAnAttempt() {
        int other = store.eventId("other");
        long known = put(task());
        long unknown = put(task(queue, other));
        List<TaskRecord> claimed = claim(5, LEASE, Set.of(event));
        assertThat(claimed).extracting(TaskRecord::id).containsExactly(known);
        TaskRecord skipped = get(unknown);
        assertThat(skipped.attempts()).isZero();
        assertThat(skipped.state()).isIn(TaskState.QUEUED, TaskState.SCHEDULED);
    }

    @Test
    void claimWithNullKnownEventsTakesEveryEvent() {
        int other = store.eventId("other");
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
            int w = store.allocateWorkerId("t");
            List<Long> mine = new ArrayList<>();
            while (true) {
                List<TaskRecord> got = store.claim(new ClaimRequest(List.of(queue), 5, LEASE, w, null));
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
    void renewReportsACancelRequest() {
        TaskRecord t = leased(task().cancelRequestedAt(now()).cancelReason("stop"));
        List<LeaseStatus> status = store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        assertThat(status).containsExactly(new LeaseStatus(t.id(), true, true));
    }

    // ack

    @Test
    void ackKeepMovesToSucceededAndDropsTheData() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), true, false)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(done.revision()).isEqualTo(t.revision() + 1);
        assertThat(done.data()).isNull();
        assertThat(done.leaseUntil()).isNull();
        assertThat(done.workerId()).isEqualTo(worker);
        assertThat(done.stateChanged()).isAfterOrEqualTo(get(t.id()).claimedAt());
    }

    @Test
    void ackKeepPayloadKeepsDataAndSteps() {
        TaskRecord t = leased(task());
        Map<String, StepResult> steps = Map.of("one", new StepResult(millis(now()), "{\"a\":1}"));
        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isTrue();
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.data()).isEqualTo("payload");
        assertThat(done.steps()).isEqualTo(steps);
    }

    @Test
    void ackDropsThePayloadButKeepsTheStepsWhenKeepPayloadIsFalse() {
        TaskRecord t = leased(task());
        Map<String, StepResult> steps = Map.of("one", new StepResult(millis(now()), null));
        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isTrue();
        assertThat(store.ack(t.id(), t.revision(), true, false)).isTrue();
        assertThat(get(t.id()).steps()).isEqualTo(steps);
        assertThat(get(t.id()).data()).isNull();
    }

    @Test
    void ackWithoutKeepRemovesTheTask() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), false, false)).isTrue();
        assertThat(store.getTask(t.id())).isEmpty();
    }

    @Test
    void ackWithoutKeepButWithKeyKeepKeepsTheKeyTakenUntilKeyUntil() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.ack(t.id(), t.revision(), false, false)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(done.data()).isNull();
        assertThat(done.keyUntil()).isNotNull();
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isZero();
        advance(PAST_SHORT);
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isEqualTo(1);
        assertThat(store.getTask(t.id())).isEmpty();
        put(task().uniqueKey("k"));
    }

    @Test
    void failWithoutKeepDeadButWithKeyKeepKeepsTheKeyTakenUntilKeyUntil() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, false)).isEqualTo(FailOutcome.DEAD);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.DEAD);
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
        advance(PAST_SHORT);
        assertThat(store.purge(TaskState.DEAD, Duration.ZERO, 10)).isEqualTo(1);
        assertThat(store.getTask(t.id())).isEmpty();
    }

    @Test
    void ackWithAWrongRevisionChangesNothing() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision() + 1, true, true)).isFalse();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void ackTwiceSucceedsOnlyOnce() {
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        assertThat(store.ack(t.id(), t.revision(), true, true)).isFalse();
        assertThat(store.ack(t.id(), t.revision() + 1, true, true)).isFalse();
    }

    // fail

    @Test
    void failWithRetrySchedulesTheTaskAndRecordsTheFailure() {
        TaskRecord t = leased(task());
        Instant retryAt = millis(now().plus(Duration.ofMinutes(10)));
        FailureInfo info = new FailureInfo(1, millis(now()), "java.io.IOException", "disk", "trace");
        assertThat(store.fail(t.id(), t.revision(), info, retryAt, true)).isEqualTo(FailOutcome.RETRY);
        TaskRecord after = get(t.id());
        assertThat(after.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(after.due()).isEqualTo(retryAt);
        assertThat(after.attempts()).isEqualTo(1);
        assertThat(after.revision()).isEqualTo(t.revision() + 1);
        assertThat(after.leaseUntil()).isNull();
        assertThat(after.failures()).containsExactly(info);
        assertThat(after.lastFailure()).isEqualTo(info);
    }

    @Test
    void failWithoutRetryKillsTheTask() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
        TaskRecord after = get(t.id());
        assertThat(after.state()).isEqualTo(TaskState.DEAD);
        assertThat(after.revision()).isEqualTo(t.revision() + 1);
        assertThat(after.failures()).hasSize(1);
    }

    @Test
    void failKillsTheTaskWhenAttemptsAreExhausted() {
        TaskRecord t = leased(task().maxAttempts(1));
        assertThat(store.fail(t.id(), t.revision(), failure(1), now().plusSeconds(5), true))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.DEAD);
    }

    @Test
    void failWithoutKeepDeadRemovesTheTask() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, false)).isEqualTo(FailOutcome.DEAD);
        assertThat(store.getTask(t.id())).isEmpty();
    }

    @Test
    void failWithoutKeepDeadStillRetriesWhenAllowed() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision(), failure(1), now().plusSeconds(5), false))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.SCHEDULED);
    }

    @Test
    void failWithAWrongRevisionIsLostAndChangesNothing() {
        TaskRecord t = leased(task());
        assertThat(store.fail(t.id(), t.revision() + 1, failure(1), null, true)).isEqualTo(FailOutcome.LOST);
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
            assertThat(store.fail(t.id(), t.revision(), failure(attempt), now().minusSeconds(1), true))
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
        assertThat(store.failExpired(short1.id(), short1.revision(), failure(1), now().minusSeconds(1), true))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(short1.id()).state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(get(short1.id()).failures()).hasSize(1);
    }

    @Test
    void failExpiredKillsAnExhaustedTask() {
        put(task().maxAttempts(1));
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.failExpired(t.id(), t.revision(), failure(1), now().plusSeconds(5), true))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.DEAD);
    }

    @Test
    void failExpiredIsLostWhenTheLeaseWasRenewed() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(t.id());
        store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        assertThat(store.failExpired(t.id(), t.revision(), failure(1), null, true))
                .isEqualTo(FailOutcome.LOST);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void failExpiredIsLostWhenTheLeaseIsStillRunning() {
        TaskRecord t = leased(task());
        assertThat(store.failExpired(t.id(), t.revision(), failure(1), null, true))
                .isEqualTo(FailOutcome.LOST);
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void failExpiredWithAWrongRevisionIsLost() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.failExpired(t.id(), t.revision() + 1, failure(1), null, true))
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
        assertThat(after.due()).isEqualTo(due);
        assertThat(after.attempts()).isZero();
        assertThat(after.revision()).isEqualTo(t.revision() + 1);
        assertThat(after.leaseUntil()).isNull();
    }

    @Test
    void unclaimReturnsTheTaskToItsQueueAtItsOldDue() {
        long id = put(task());
        Instant due = get(id).due();
        TaskRecord t = claimOne();
        assertThat(store.unclaim(t.id(), t.revision())).isTrue();
        TaskRecord after = get(id);
        assertThat(after.state()).isEqualTo(TaskState.QUEUED);
        assertThat(after.due()).isEqualTo(due);
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
        TaskRecord after = get(t.id());
        assertThat(after.steps()).isEqualTo(second);
        assertThat(after.revision()).isEqualTo(t.revision());
    }

    @Test
    void saveStepsRequiresTheLeaseRevision() {
        TaskRecord t = leased(task());
        Map<String, StepResult> steps = Map.of("a", new StepResult(millis(now()), "1"));
        assertThat(store.saveSteps(t.id(), t.revision() + 1, steps)).isFalse();
        assertThat(get(t.id()).steps()).isNull();
        long queued = putNew();
        assertThat(store.saveSteps(queued, 0, steps)).isFalse();
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        assertThat(store.saveSteps(t.id(), t.revision() + 1, steps)).isFalse();
        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isFalse();
    }

    @Test
    void aRetryAttemptSeesTheStepsOfTheEarlierOne() {
        TaskRecord t = leased(task());
        Map<String, StepResult> steps = Map.of("a", new StepResult(millis(now()), "1"));
        assertThat(store.saveSteps(t.id(), t.revision(), steps)).isTrue();
        assertThat(store.fail(t.id(), t.revision(), failure(1), now().minusSeconds(1), true))
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
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        assertThat(store.progress(t.id(), t.revision() + 1, "x")).isFalse();
        long queued = putNew();
        assertThat(store.progress(queued, 0, "x")).isFalse();
    }

    // maintenance

    @Test
    void promoteMovesDueTasksToQueuedKeepingDue() {
        long id = put(task().due(now().plus(SHORT)));
        long later = put(task().due(now().plus(Duration.ofHours(1))));
        Instant due = get(id).due();
        assertThat(store.promote(10).moved()).isZero();
        advance(PAST_SHORT);
        PromoteResult result = store.promote(10);
        assertThat(result.moved()).isEqualTo(1);
        assertThat(result.nextDue()).isEqualTo(get(later).due());
        TaskRecord t = get(id);
        assertThat(t.state()).isEqualTo(TaskState.QUEUED);
        assertThat(t.due()).isEqualTo(due);
        assertThat(t.revision()).isEqualTo(1);
        assertThat(get(later).state()).isEqualTo(TaskState.SCHEDULED);
    }

    @Test
    void promoteHonorsTheLimitAndReportsNoNextDueWhenDone() {
        for (int i = 0; i < 3; i++) {
            put(task().due(now().plus(SHORT)));
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
        long id = put(task().due(now().plus(SHORT)));
        advance(PAST_SHORT);
        store.promote(10);
        assertThat(claim(1)).extracting(TaskRecord::id).containsExactly(id);
    }

    @Test
    void reclaimListsExpiredLeasesOldestFirst() {
        put(task().maxAttempts(5).eventId(event));
        TaskRecord first = claim(1, SHORT, null).get(0);
        advance(Duration.ofMillis(60));
        put(task());
        TaskRecord second = claim(1, SHORT, null).get(0);
        TaskRecord running = leased(task());
        assertThat(store.reclaim(10)).isEmpty();
        advance(Duration.ofMillis(400));
        List<ExpiredLease> expired = store.reclaim(10);
        assertThat(expired).extracting(ExpiredLease::id).containsExactly(first.id(), second.id());
        ExpiredLease e = expired.get(0);
        assertThat(e.revision()).isEqualTo(first.revision());
        assertThat(e.attempts()).isEqualTo(1);
        assertThat(e.maxAttempts()).isEqualTo(5);
        assertThat(e.eventId()).isEqualTo(event);
        assertThat(e.cancelRequested()).isFalse();
        assertThat(store.reclaim(1)).hasSize(1);
        assertThat(get(running.id()).state()).isEqualTo(TaskState.LEASED);
        assertThat(get(first.id()).state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void reclaimFlagsACancelRequest() {
        put(task().cancelRequestedAt(now()));
        claim(1, SHORT, null);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::cancelRequested).containsExactly(true);
    }

    @Test
    void renewedLeasesAreNotReclaimed() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(Duration.ofMillis(100));
        store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        advance(Duration.ofMillis(200));
        assertThat(store.reclaim(10)).isEmpty();
    }

    @Test
    void purgeRemovesOldTerminalTasksOfThatStateOnly() {
        TaskRecord done = leased(task());
        assertThat(store.ack(done.id(), done.revision(), true, true)).isTrue();
        TaskRecord dead = leased(task());
        assertThat(store.fail(dead.id(), dead.revision(), failure(1), null, true))
                .isEqualTo(FailOutcome.DEAD);
        long queued = putNew();
        advance(Duration.ofMillis(150));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofHours(1), 10)).isZero();
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 10)).isEqualTo(1);
        assertThat(store.getTask(done.id())).isEmpty();
        assertThat(get(dead.id()).state()).isEqualTo(TaskState.DEAD);
        assertThat(get(queued).state()).isEqualTo(TaskState.QUEUED);
    }

    @Test
    void purgeKeepsTasksYoungerThanTheRetention() {
        TaskRecord a = leased(task());
        assertThat(store.ack(a.id(), a.revision(), true, true)).isTrue();
        advance(Duration.ofMillis(300));
        TaskRecord b = leased(task());
        assertThat(store.ack(b.id(), b.revision(), true, true)).isTrue();
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(200), 10)).isEqualTo(1);
        assertThat(store.getTask(a.id())).isEmpty();
        assertThat(store.getTask(b.id())).isPresent();
    }

    @Test
    void purgeHonorsTheLimitOldestFirst() {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TaskRecord t = leased(task());
            assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
            ids.add(t.id());
            advance(Duration.ofMillis(20));
        }
        advance(Duration.ofMillis(200));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(50), 2)).isEqualTo(2);
        assertThat(store.getTask(ids.get(0))).isEmpty();
        assertThat(store.getTask(ids.get(1))).isEmpty();
        assertThat(store.getTask(ids.get(2))).isPresent();
    }

    @Test
    void purgeKeepsTasksWhoseKeyIsStillHeld() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofMillis(500)));
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        advance(Duration.ofMillis(150));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 10)).isZero();
        assertThat(store.getTask(t.id())).isPresent();
        advance(Duration.ofMillis(500));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 10)).isEqualTo(1);
    }

    @Test
    void releaseKeysFreesOnlyExpiredKeys() {
        TaskRecord expiring = leased(task().uniqueKey("short").keyKeep(SHORT));
        assertThat(store.ack(expiring.id(), expiring.revision(), true, true)).isTrue();
        TaskRecord lasting = leased(task().uniqueKey("long").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(lasting.id(), lasting.revision(), true, true)).isTrue();
        assertThat(store.releaseKeys(10)).isZero();
        advance(PAST_SHORT);
        assertThat(store.releaseKeys(10)).isEqualTo(1);
        assertThat(get(expiring.id()).uniqueKey()).isNull();
        assertThat(get(expiring.id()).keyUntil()).isNull();
        assertThat(get(lasting.id()).uniqueKey()).isEqualTo("long");
        assertThat(store.releaseKeys(10)).isZero();
        put(task().uniqueKey("short"));
        assertThat(store.enqueue(task().uniqueKey("long").build()))
                .isEqualTo(new EnqueueOutcome.DuplicateKey(lasting.id()));
    }

    @Test
    void nextDueIsTheEarliestScheduledDue() {
        assertThat(store.nextDue()).isNull();
        putNew();
        assertThat(store.nextDue()).isNull();
        Instant soon = millis(now().plus(Duration.ofMinutes(5)));
        put(task().due(millis(now().plus(Duration.ofHours(2)))));
        put(task().due(soon));
        assertThat(store.nextDue()).isEqualTo(soon);
    }

    // recurring

    private RecurringDefinition definition(String id, String cron) {
        return new RecurringDefinition(id, event, 1, queue, "acme", cron, "{}", 3, 0);
    }

    private TaskRecord occurrence(Instant fireAt) {
        return task().scheduleId("nightly")
                .due(fireAt)
                .state(TaskState.SCHEDULED)
                .build();
    }

    @Test
    void putRecurringStoresTheDefinitionAndNextFire() {
        Instant fire = millis(now().plus(Duration.ofHours(1)));
        store.putRecurring(definition("nightly", "0 0 * * *"), fire);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("nightly", fire));
        List<DueSchedule> due = store.dueRecurring(fire, 10);
        assertThat(due).hasSize(1);
        RecurringDefinition stored = due.get(0).definition();
        assertThat(stored.scheduleId()).isEqualTo("nightly");
        assertThat(stored.cron()).isEqualTo("0 0 * * *");
        assertThat(stored.eventId()).isEqualTo(event);
        assertThat(stored.queueId()).isEqualTo(queue);
        assertThat(stored.account()).isEqualTo("acme");
        assertThat(stored.data()).isEqualTo("{}");
        assertThat(stored.maxAttempts()).isEqualTo(3);
        assertThat(due.get(0).fireAt()).isEqualTo(fire);
    }

    @Test
    void putRecurringAssignsANewRevisionEachTime() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(definition("a", "* * * * *"), fire);
        long first = store.dueRecurring(fire, 10).get(0).definition().revision();
        store.putRecurring(definition("a", "* * * * *"), fire);
        long second = store.dueRecurring(fire, 10).get(0).definition().revision();
        assertThat(second).isNotEqualTo(first);
    }

    @Test
    void putRecurringKeepsTheNextFireWhenTheCronIsUnchanged() {
        Instant first = millis(now().plusSeconds(60));
        Instant other = millis(now().plusSeconds(600));
        store.putRecurring(definition("a", "* * * * *"), first);
        store.putRecurring(definition("a", "* * * * *"), other);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("a", first));
        store.putRecurring(definition("a", "0 * * * *"), other);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("a", other));
    }

    @Test
    void putRecurringResetsTheNextFireWhenTheZoneOrFixedDelayChanges() {
        Instant first = millis(now().plusSeconds(60));
        Instant second = millis(now().plusSeconds(600));
        Instant third = millis(now().plusSeconds(1200));
        store.putRecurring(fullDefinition("a"), first);
        store.putRecurring(fullDefinition("a"), second);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("a", first));
        RecurringDefinition base = fullDefinition("a");
        RecurringDefinition otherZone = new RecurringDefinition(
                "a", event, 2, queue, "acme", null, "UTC", "CATCH_UP", 5, 90_000L, true, "{}", 4, false, 0);
        store.putRecurring(otherZone, second);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("a", second));
        RecurringDefinition otherDelay = new RecurringDefinition(
                "a", event, 2, queue, "acme", null, "UTC", "CATCH_UP", 5, 60_000L, true, "{}", 4, false, 0);
        store.putRecurring(otherDelay, third);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("a", third));
        store.putRecurring(base, first);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("a", first));
    }

    @Test
    void deleteRecurringReportsWhetherItExisted() {
        store.putRecurring(definition("a", "* * * * *"), millis(now().plusSeconds(60)));
        assertThat(store.deleteRecurring("a")).isTrue();
        assertThat(store.deleteRecurring("a")).isFalse();
        assertThat(store.recurringSchedules()).isEmpty();
    }

    @Test
    void dueRecurringReturnsOnlyDueDefinitionsUpToTheLimit() {
        Instant base = millis(now());
        store.putRecurring(definition("a", "* * * * *"), base.plusSeconds(10));
        store.putRecurring(definition("b", "* * * * *"), base.plusSeconds(20));
        store.putRecurring(definition("c", "* * * * *"), base.plusSeconds(3600));
        assertThat(store.dueRecurring(base, 10)).isEmpty();
        assertThat(store.dueRecurring(base.plusSeconds(30), 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactlyInAnyOrder("a", "b");
        assertThat(store.dueRecurring(base.plusSeconds(30), 1)).hasSize(1);
        assertThat(store.dueRecurring(base.plusSeconds(10), 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("a");
    }

    @Test
    void commitRecurringStoresTheOccurrenceAndAdvancesTheDefinition() {
        Instant fire = millis(now().plusSeconds(1));
        Instant next = millis(now().plusSeconds(3600));
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        DueSchedule due = store.dueRecurring(fire, 10).get(0);
        TaskRecord occurrence = occurrence(fire);
        assertThat(store.commitRecurring(due, next, occurrence)).isTrue();
        TaskRecord stored = get(occurrence.id());
        assertThat(stored.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(stored.due()).isEqualTo(fire);
        assertThat(stored.scheduleId()).isEqualTo("nightly");
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("nightly", next));
        assertThat(store.dueRecurring(fire, 10)).isEmpty();
    }

    @Test
    void commitRecurringWithAStaleDueDoesNothing() {
        Instant fire = millis(now().plusSeconds(1));
        Instant next = millis(now().plusSeconds(3600));
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        DueSchedule due = store.dueRecurring(fire, 10).get(0);
        assertThat(store.commitRecurring(due, next, occurrence(fire))).isTrue();
        TaskRecord again = occurrence(fire);
        assertThat(store.commitRecurring(due, next.plusSeconds(60), again)).isFalse();
        assertThat(store.getTask(again.id())).isEmpty();
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("nightly", next));
    }

    @Test
    void commitRecurringIsRejectedAfterTheDefinitionWasReplacedOrDeleted() {
        Instant fire = millis(now().plusSeconds(1));
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        DueSchedule due = store.dueRecurring(fire, 10).get(0);
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        TaskRecord occurrence = occurrence(fire);
        assertThat(store.commitRecurring(due, fire.plusSeconds(60), occurrence)).isFalse();
        assertThat(store.getTask(occurrence.id())).isEmpty();
        DueSchedule fresh = store.dueRecurring(fire, 10).get(0);
        assertThat(store.deleteRecurring("nightly")).isTrue();
        assertThat(store.commitRecurring(fresh, fire.plusSeconds(60), occurrence))
                .isFalse();
    }

    @Test
    void twoThreadsCommittingTheSameDueScheduleSucceedExactlyOnce() throws Exception {
        Instant fire = millis(now().plusSeconds(1));
        Instant next = millis(now().plusSeconds(3600));
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        DueSchedule due = store.dueRecurring(fire, 10).get(0);
        TaskRecord a = occurrence(fire);
        TaskRecord b = occurrence(fire);
        AtomicInteger turn = new AtomicInteger();
        List<Boolean> results = runConcurrently(2, () -> {
            TaskRecord mine = turn.getAndIncrement() == 0 ? a : b;
            return store.commitRecurring(due, next, mine);
        });
        assertThat(results).containsExactlyInAnyOrder(true, false);
        long stored = List.of(a, b).stream()
                .filter(t -> store.getTask(t.id()).isPresent())
                .count();
        assertThat(stored).isEqualTo(1);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("nightly", next));
    }

    // wave 2: cancel, retry, delete, pause, expire, workers

    private StateCounts counts() {
        return store.counts().getOrDefault(queue, StateCounts.ZERO);
    }

    private TaskRecord dead() {
        TaskRecord t = leased(task().maxAttempts(1).uniqueKey("k"));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
        return get(t.id());
    }

    private TaskRecord expired() {
        long id = put(task().uniqueKey("k").startBy(now().plus(SHORT)));
        advance(PAST_SHORT);
        assertThat(store.expire(10)).containsExactly(id);
        return get(id);
    }

    private TaskRecord cancelled() {
        long id = put(task().uniqueKey("k"));
        assertThat(store.cancel(id, get(id).revision(), "nope")).isEqualTo(CancelOutcome.CANCELLED);
        return get(id);
    }

    @Test
    void finishCancelledEndsALeasedTaskAndBumpsTheRevisionOnce() {
        TaskRecord t = leased(task().uniqueKey("k"));
        Instant before = now();
        assertThat(store.finishCancelled(t.id(), t.revision(), "stop", true)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.revision()).isEqualTo(t.revision() + 1);
        assertThat(done.cancelReason()).isEqualTo("stop");
        assertThat(done.cancelRequestedAt()).isNotNull();
        assertThat(done.leaseUntil()).isNull();
        assertThat(done.workerId()).isEqualTo(worker);
        assertThat(done.data()).isEqualTo("payload");
        assertThat(done.stateChanged()).isAfterOrEqualTo(millis(before));
        assertThat(done.uniqueKey()).isNull();
        assertThat(counts().cancelled()).isEqualTo(1);
        assertThat(counts().leased()).isZero();
    }

    @Test
    void finishCancelledKeepsAnEarlierCancelRequestTimeAndDropsThePayloadOnRequest() {
        TaskRecord t = leased(task());
        assertThat(store.cancel(t.id(), t.revision(), "ask")).isEqualTo(CancelOutcome.REQUESTED);
        Instant requested = get(t.id()).cancelRequestedAt();
        advance(Duration.ofMillis(50));
        assertThat(store.finishCancelled(t.id(), t.revision(), "done", false)).isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.cancelRequestedAt()).isEqualTo(requested);
        assertThat(done.cancelReason()).isEqualTo("done");
        assertThat(done.data()).isNull();
    }

    @Test
    void finishCancelledIsFencedByRevisionAndState() {
        TaskRecord t = leased(task());
        assertThat(store.finishCancelled(t.id(), t.revision() + 1, "x", true)).isFalse();
        assertThat(store.finishCancelled(t.id(), t.revision() - 1, "x", true)).isFalse();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
        assertThat(store.finishCancelled(t.id(), t.revision(), "x", true)).isTrue();
        assertThat(store.finishCancelled(t.id(), t.revision() + 1, "x", true)).isFalse();
        long queued = putNew();
        assertThat(store.finishCancelled(queued, get(queued).revision(), "x", true))
                .isFalse();
        assertThat(store.finishCancelled(store.allocateIds(1), 1, "x", true)).isFalse();
    }

    @Test
    void finishCancelledStartsTheKeyKeepWindow() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.finishCancelled(t.id(), t.revision(), "x", true)).isTrue();
        assertThat(get(t.id()).uniqueKey()).isEqualTo("k");
        assertThat(get(t.id()).keyUntil()).isNotNull();
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
    }

    @Test
    void cancelOnAQueuedTaskCancelsItAtOnce() {
        long id = put(task().uniqueKey("k"));
        TaskRecord before = get(id);
        assertThat(store.cancel(id, before.revision(), "why")).isEqualTo(CancelOutcome.CANCELLED);
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
        long id = put(task().due(now().plus(Duration.ofHours(1))));
        assertThat(store.cancel(id, get(id).revision(), "later")).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(get(id).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(counts().scheduled()).isZero();
        assertThat(counts().cancelled()).isEqualTo(1);
        assertThat(store.nextDue()).isNull();
    }

    @Test
    void cancelOnALeasedTaskOnlyRecordsTheRequestAndKeepsTheRevision() {
        TaskRecord t = leased(task());
        assertThat(store.cancel(t.id(), t.revision(), "stop")).isEqualTo(CancelOutcome.REQUESTED);
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
    void cancelWithAStaleRevisionOrFinishedTaskIsStale() {
        long id = putNew();
        int revision = get(id).revision();
        assertThat(store.cancel(id, revision + 1, "x")).isEqualTo(CancelOutcome.STALE);
        assertThat(get(id).state()).isEqualTo(TaskState.QUEUED);
        TaskRecord t = claimOne();
        assertThat(store.cancel(id, revision, "x")).isEqualTo(CancelOutcome.STALE);
        assertThat(get(id).cancelRequestedAt()).isNull();
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        assertThat(store.cancel(id, get(id).revision(), "x")).isEqualTo(CancelOutcome.STALE);
        assertThat(store.cancel(store.allocateIds(1), 1, "x")).isEqualTo(CancelOutcome.STALE);
        assertThat(counts().succeeded()).isEqualTo(1);
    }

    @Test
    void retryReturnsADeadTaskToTheQueueWithItsHistory() {
        TaskRecord t = dead();
        assertThat(t.uniqueKey()).isNull();
        assertThat(store.retry(t.id(), t.revision())).isTrue();
        TaskRecord r = get(t.id());
        assertThat(r.state()).isEqualTo(TaskState.QUEUED);
        assertThat(r.attempts()).isZero();
        assertThat(r.revision()).isEqualTo(t.revision() + 1);
        assertThat(r.failures()).hasSize(1);
        assertThat(r.workerId()).isNull();
        assertThat(r.leaseUntil()).isNull();
        assertThat(r.cancelRequestedAt()).isNull();
        assertThat(r.cancelReason()).isNull();
        assertThat(r.uniqueKey()).isNull();
        assertThat(r.keyUntil()).isNull();
        assertThat(r.due()).isAfterOrEqualTo(millis(t.stateChanged()));
        assertThat(counts().dead()).isZero();
        assertThat(counts().queued()).isEqualTo(1);
        assertThat(claimOne().id()).isEqualTo(t.id());
    }

    @Test
    void retryWorksFromCancelledAndExpiredAndClearsTheKey() {
        TaskRecord c = cancelled();
        assertThat(store.retry(c.id(), c.revision())).isTrue();
        assertThat(get(c.id()).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(c.id()).cancelReason()).isNull();
        assertThat(get(c.id()).uniqueKey()).isNull();
        TaskRecord e = expired();
        assertThat(store.retry(e.id(), e.revision())).isTrue();
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
        assertThat(store.retry(q.id(), q.revision())).isFalse();
        TaskRecord l = claimOne();
        assertThat(l.id()).isEqualTo(q.id());
        assertThat(store.retry(l.id(), l.revision())).isFalse();
        assertThat(store.ack(l.id(), l.revision(), true, true)).isTrue();
        TaskRecord s = get(l.id());
        assertThat(store.retry(s.id(), s.revision())).isFalse();
        TaskRecord d = dead();
        assertThat(store.retry(d.id(), d.revision() - 1)).isFalse();
        assertThat(get(d.id()).state()).isEqualTo(TaskState.DEAD);
        assertThat(store.retry(store.allocateIds(1), 1)).isFalse();
    }

    @Test
    void deleteRemovesTasksInEveryStateButLeased() {
        TaskRecord d = dead();
        TaskRecord c = cancelled();
        TaskRecord ok = leased(task());
        assertThat(store.ack(ok.id(), ok.revision(), true, true)).isTrue();
        long queued = putNew();
        long scheduled = put(task().due(now().plus(Duration.ofHours(1))));
        for (long id : List.of(queued, scheduled, ok.id(), d.id(), c.id())) {
            assertThat(store.delete(id, get(id).revision())).isTrue();
            assertThat(store.getTask(id)).isEmpty();
        }
        assertThat(counts()).isEqualTo(StateCounts.ZERO);
        assertThat(store.nextDue()).isNull();
    }

    @Test
    void deleteReleasesTheUniqueKey() {
        long id = put(task().uniqueKey("k"));
        assertThat(store.delete(id, get(id).revision())).isTrue();
        put(task().uniqueKey("k"));
    }

    @Test
    void deleteRefusesLeasedTasksAndStaleRevisions() {
        TaskRecord l = leased(task());
        assertThat(store.delete(l.id(), l.revision())).isFalse();
        assertThat(get(l.id()).state()).isEqualTo(TaskState.LEASED);
        long q = putNew();
        assertThat(store.delete(q, get(q).revision() + 1)).isFalse();
        assertThat(store.getTask(q)).isPresent();
        assertThat(store.delete(store.allocateIds(1), 1)).isFalse();
        assertThat(counts().leased()).isEqualTo(1);
        assertThat(counts().queued()).isEqualTo(1);
    }

    @Test
    void pausedQueuesAreSkippedByClaimUntilResumed() {
        int second = store.queueId("second");
        putNew();
        long other = put(task(second, event));
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
                .containsExactly(new QueueInfo(queue, "main", false), new QueueInfo(second, "second", false));
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
    void claimNeverReturnsATaskWhoseStartByHasPassed() {
        long late = put(task().startBy(now().plus(SHORT)));
        long fine = put(task().startBy(now().plus(Duration.ofHours(1))));
        assertThat(claim(10)).extracting(TaskRecord::id).containsExactlyInAnyOrder(late, fine);
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
        long queued = put(task().uniqueKey("q").startBy(now().plus(SHORT)));
        long scheduled =
                put(task().uniqueKey("s").due(now().plus(Duration.ofHours(1))).startBy(now().plus(SHORT)));
        long keep = put(task().startBy(now().plus(Duration.ofHours(1))));
        long none = putNew();
        int revision = get(queued).revision();
        assertThat(store.expire(10)).isEmpty();
        advance(PAST_SHORT);
        assertThat(store.expire(10)).containsExactlyInAnyOrder(queued, scheduled);
        assertThat(get(queued).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(get(queued).revision()).isEqualTo(revision + 1);
        assertThat(get(queued).uniqueKey()).isNull();
        assertThat(get(scheduled).state()).isEqualTo(TaskState.EXPIRED);
        assertThat(get(keep).state()).isEqualTo(TaskState.QUEUED);
        assertThat(get(none).state()).isEqualTo(TaskState.QUEUED);
        assertThat(counts().expired()).isEqualTo(2);
        assertThat(counts().queued()).isEqualTo(2);
        assertThat(counts().scheduled()).isZero();
        assertThat(store.nextDue()).isNull();
        assertThat(store.expire(10)).isEmpty();
        put(task().uniqueKey("q"));
        put(task().uniqueKey("s"));
    }

    @Test
    void expireRespectsTheLimitAndNeverTouchesLeasedTasks() {
        long leasedId = put(task().startBy(now().plus(SHORT)));
        TaskRecord l = claimOne();
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
        assertThat(store.expire(10)).containsExactly(id);
        assertThat(get(id).uniqueKey()).isEqualTo("k");
        assertThat(get(id).keyUntil()).isNotNull();
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(id));
    }

    @Test
    void allocatedWorkersAreListedAndHeartbeatUpdatesThem() {
        List<WorkerInfo> before = store.workers();
        assertThat(before).extracting(WorkerInfo::id).contains(worker);
        WorkerInfo w = before.stream().filter(x -> x.id() == worker).findFirst().orElseThrow();
        assertThat(w.label()).isEqualTo("contract-test");
        assertThat(w.running()).isZero();
        advance(PAST_SHORT);
        store.heartbeat(worker, 4);
        WorkerInfo after = store.workers().stream()
                .filter(x -> x.id() == worker)
                .findFirst()
                .orElseThrow();
        assertThat(after.running()).isEqualTo(4);
        assertThat(after.lastSeen()).isAfter(w.lastSeen());
        assertThat(after.startedAt()).isEqualTo(w.startedAt());
        assertThat(after.label()).isEqualTo("contract-test");
    }

    @Test
    void purgeWorkersDeletesOnlyWorkersNotSeenRecently() {
        int stale = store.allocateWorkerId("stale");
        advance(PAST_SHORT);
        store.heartbeat(worker, 0);
        assertThat(store.purgeWorkers(SHORT)).isEqualTo(1);
        assertThat(store.workers()).extracting(WorkerInfo::id).contains(worker).doesNotContain(stale);
        assertThat(store.purgeWorkers(SHORT)).isZero();
    }

    @Test
    void everyNewTransitionKeepsTheCountsExact() {
        TaskRecord l = leased(task());
        long a = putNew();
        long b = putNew();
        long c = put(task().due(now().plus(Duration.ofHours(1))));
        assertThat(counts()).isEqualTo(new StateCounts(2, 1, 1, 0, 0, 0, 0));
        assertThat(store.cancel(a, get(a).revision(), "x")).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(counts()).isEqualTo(new StateCounts(1, 1, 1, 0, 0, 1, 0));
        assertThat(store.cancel(l.id(), l.revision(), "x")).isEqualTo(CancelOutcome.REQUESTED);
        assertThat(counts()).isEqualTo(new StateCounts(1, 1, 1, 0, 0, 1, 0));
        assertThat(store.finishCancelled(l.id(), l.revision(), "x", true)).isTrue();
        assertThat(counts()).isEqualTo(new StateCounts(1, 1, 0, 0, 0, 2, 0));
        assertThat(store.retry(a, get(a).revision())).isTrue();
        assertThat(counts()).isEqualTo(new StateCounts(2, 1, 0, 0, 0, 1, 0));
        assertThat(store.delete(c, get(c).revision())).isTrue();
        assertThat(store.delete(b, get(b).revision())).isTrue();
        assertThat(counts()).isEqualTo(new StateCounts(1, 0, 0, 0, 0, 1, 0));
    }

    // wave 2: recurring

    private RecurringDefinition fullDefinition(String id) {
        return new RecurringDefinition(
                id,
                event,
                2,
                queue,
                "acme",
                null,
                "Europe/Berlin",
                "CATCH_UP",
                5,
                90_000L,
                true,
                "{\"a\":1}",
                4,
                false,
                0);
    }

    @Test
    void recurringDefinitionsRoundTripEveryField() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(fullDefinition("fixed"), fire);
        store.putRecurring(
                new RecurringDefinition(
                        "cron", event, 1, queue, null, "0 3 * * *", null, null, 0, null, false, null, 3, false, 0),
                fire.plusSeconds(5));
        List<RecurringSchedule> all = store.recurringDefinitions();
        assertThat(all).extracting(s -> s.definition().scheduleId()).containsExactly("cron", "fixed");
        RecurringSchedule fixed = all.get(1);
        assertThat(fixed.nextFire()).isEqualTo(fire);
        RecurringDefinition d = fixed.definition();
        assertThat(d.eventId()).isEqualTo(event);
        assertThat(d.payloadVersion()).isEqualTo(2);
        assertThat(d.queueId()).isEqualTo(queue);
        assertThat(d.account()).isEqualTo("acme");
        assertThat(d.cron()).isNull();
        assertThat(d.zone()).isEqualTo("Europe/Berlin");
        assertThat(d.misfire()).isEqualTo("CATCH_UP");
        assertThat(d.catchUpMax()).isEqualTo(5);
        assertThat(d.fixedDelayMs()).isEqualTo(90_000L);
        assertThat(d.isFixedDelay()).isTrue();
        assertThat(d.overlapSkip()).isTrue();
        assertThat(d.data()).isEqualTo("{\"a\":1}");
        assertThat(d.maxAttempts()).isEqualTo(4);
        assertThat(d.paused()).isFalse();
        RecurringDefinition c = all.get(0).definition();
        assertThat(c.cron()).isEqualTo("0 3 * * *");
        assertThat(c.zone()).isNull();
        assertThat(c.misfire()).isNull();
        assertThat(c.fixedDelayMs()).isNull();
        assertThat(c.account()).isNull();
        assertThat(c.data()).isNull();
        assertThat(c.overlapSkip()).isFalse();
        assertThat(store.dueRecurring(fire, 10).get(0).definition()).isEqualTo(d);
    }

    @Test
    void putRecurringStoresTheGivenPausedFlagOnlyForANewDefinitionAndReplacesEveryOtherField() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(fullDefinition("a").withPaused(true), fire);
        assertThat(store.recurringDefinitions().get(0).definition().paused()).isTrue();
        store.putRecurring(definition("a", "* * * * *"), fire);
        RecurringDefinition d = store.recurringDefinitions().get(0).definition();
        assertThat(d.cron()).isEqualTo("* * * * *");
        assertThat(d.fixedDelayMs()).isNull();
        assertThat(d.zone()).isNull();
        assertThat(d.paused()).isTrue();
        assertThat(store.recurringDefinitions()).hasSize(1);
    }

    @Test
    void putRecurringKeepsAnOperatorsPauseAndAnUnpauseSurvivesToo() {
        Instant fire = millis(now().plusSeconds(1));
        store.putRecurring(definition("a", "* * * * *"), fire);
        assertThat(store.pauseRecurring("a", true)).isTrue();
        store.putRecurring(definition("a", "* * * * *"), fire);
        assertThat(store.recurringDefinitions().get(0).definition().paused()).isTrue();
        assertThat(store.dueRecurring(fire, 10)).isEmpty();
        assertThat(store.pauseRecurring("a", false)).isTrue();
        store.putRecurring(definition("a", "* * * * *").withPaused(true), fire);
        assertThat(store.recurringDefinitions().get(0).definition().paused()).isFalse();
        assertThat(store.dueRecurring(fire, 10)).hasSize(1);
    }

    @Test
    void commitRecurringIsRefusedWhileTheDefinitionIsPaused() {
        Instant fire = millis(now().plusSeconds(1));
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        DueSchedule due = store.dueRecurring(fire, 10).get(0);
        assertThat(store.pauseRecurring("nightly", true)).isTrue();
        TaskRecord occurrence = occurrence(fire);
        assertThat(store.commitRecurring(due, fire.plusSeconds(60), occurrence)).isFalse();
        assertThat(store.getTask(occurrence.id())).isEmpty();
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("nightly", fire));
    }

    @Test
    void retryOfAnExpiredTaskClearsStartByAndTheTaskIsClaimable() {
        TaskRecord e = expired();
        assertThat(e.startBy()).isNotNull();
        assertThat(store.retry(e.id(), e.revision())).isTrue();
        assertThat(get(e.id()).startBy()).isNull();
        advance(PAST_SHORT);
        assertThat(store.expire(10)).isEmpty();
        assertThat(claimOne().id()).isEqualTo(e.id());
    }

    @Test
    void pausedDefinitionsAreNotDueAndResumeRestoresTheStoredNextFire() {
        Instant fire = millis(now().plusSeconds(1));
        store.putRecurring(definition("a", "* * * * *"), fire);
        store.putRecurring(definition("b", "* * * * *"), fire);
        assertThat(store.pauseRecurring("a", true)).isTrue();
        assertThat(store.dueRecurring(fire, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("b");
        RecurringSchedule paused = store.recurringDefinitions().get(0);
        assertThat(paused.definition().paused()).isTrue();
        assertThat(paused.nextFire()).isEqualTo(fire);
        assertThat(store.recurringSchedules()).containsEntry("a", fire);
        assertThat(store.pauseRecurring("a", false)).isTrue();
        assertThat(store.dueRecurring(fire, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactlyInAnyOrder("a", "b");
        assertThat(store.recurringDefinitions().get(0).definition().paused()).isFalse();
    }

    @Test
    void pauseRecurringAndSetNextFireReportUnknownDefinitions() {
        assertThat(store.pauseRecurring("missing", true)).isFalse();
        assertThat(store.setNextFire("missing", millis(now()))).isFalse();
        assertThat(store.recurringSchedules()).isEmpty();
    }

    @Test
    void setNextFireReplacesTheNextFireAndInvalidatesAStaleCommit() {
        Instant fire = millis(now().plusSeconds(3600));
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        Instant sooner = millis(now().plusSeconds(1));
        assertThat(store.setNextFire("nightly", sooner)).isTrue();
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("nightly", sooner));
        DueSchedule due = store.dueRecurring(sooner, 10).get(0);
        assertThat(due.fireAt()).isEqualTo(sooner);
        assertThat(store.setNextFire("nightly", fire)).isTrue();
        TaskRecord occurrence = occurrence(sooner);
        assertThat(store.commitRecurring(due, fire.plusSeconds(60), occurrence)).isFalse();
        assertThat(store.getTask(occurrence.id())).isEmpty();
        assertThat(store.dueRecurring(sooner, 10)).isEmpty();
    }

    @Test
    void setNextFireKeepsTheRevisionOfTheDefinition() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(definition("a", "* * * * *"), fire);
        long revision = store.recurringDefinitions().get(0).definition().revision();
        store.setNextFire("a", fire.plusSeconds(5));
        store.pauseRecurring("a", true);
        assertThat(store.recurringDefinitions().get(0).definition().revision()).isEqualTo(revision);
    }

    @Test
    void putRecurringKeepsTheNextFireOfAFixedDelayDefinitionWhenReplaced() {
        Instant first = millis(now().plusSeconds(60));
        store.putRecurring(fullDefinition("a"), first);
        store.putRecurring(fullDefinition("a"), first.plusSeconds(500));
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("a", first));
    }

    @Test
    void commitRecurringWithAHeldUniqueKeyAdvancesTheDefinitionButStoresNothing() {
        Instant fire = millis(now().plusSeconds(1));
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        DueSchedule first = store.dueRecurring(fire, 10).get(0);
        TaskRecord one = keyedOccurrence(fire);
        assertThat(store.commitRecurring(first, fire.plusSeconds(60), one)).isTrue();
        assertThat(get(one.id()).state()).isEqualTo(TaskState.SCHEDULED);

        DueSchedule second = store.dueRecurring(fire.plusSeconds(60), 10).get(0);
        TaskRecord two = keyedOccurrence(fire.plusSeconds(60));
        assertThat(store.commitRecurring(second, fire.plusSeconds(120), two)).isTrue();
        assertThat(store.getTask(two.id())).isEmpty();
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("nightly", fire.plusSeconds(120)));
        assertThat(store.tasks(TaskState.SCHEDULED, null, 0, 10))
                .extracting(TaskRecord::id)
                .containsExactly(one.id());
    }

    @Test
    void commitRecurringStoresAgainOnceTheKeyHolderFinished() {
        Instant fire = millis(now().plusSeconds(1));
        store.putRecurring(definition("nightly", "* * * * *"), fire);
        TaskRecord one = keyedOccurrence(fire);
        assertThat(store.commitRecurring(store.dueRecurring(fire, 10).get(0), fire.plusSeconds(60), one))
                .isTrue();
        assertThat(store.promote(10).moved()).isZero();
        advance(Duration.ofSeconds(2));
        assertThat(store.promote(10).moved()).isEqualTo(1);
        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(one.id());
        assertThat(store.ack(running.id(), running.revision(), true, true)).isTrue();

        DueSchedule next = store.dueRecurring(fire.plusSeconds(60), 10).get(0);
        TaskRecord three = keyedOccurrence(fire.plusSeconds(60));
        assertThat(store.commitRecurring(next, fire.plusSeconds(120), three)).isTrue();
        assertThat(get(three.id()).state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(store.recurringSchedules()).containsOnly(Map.entry("nightly", fire.plusSeconds(120)));
    }

    private TaskRecord keyedOccurrence(Instant fireAt) {
        return task().scheduleId("nightly")
                .due(fireAt)
                .state(TaskState.SCHEDULED)
                .uniqueKey("sched:nightly")
                .build();
    }

    // inspection

    @Test
    void getTaskIsEmptyForAnUnknownId() {
        assertThat(store.getTask(store.allocateIds(1))).isEmpty();
    }

    @Test
    void countsGroupTasksByQueueAndState() {
        int second = store.queueId("second");
        putNew();
        putNew();
        put(task().due(now().plus(Duration.ofHours(1))));
        put(task(second, event));
        TaskRecord t = leased(task());
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        TaskRecord d = leased(task());
        assertThat(store.fail(d.id(), d.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
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
            assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
            advance(Duration.ofMillis(20));
        }
        List<Long> expected = ackOrder.stream().map(TaskRecord::id).toList();
        List<TaskRecord> all = store.tasks(TaskState.SUCCEEDED, null, 0, 10);
        assertThat(all).extracting(TaskRecord::id).containsExactlyElementsOf(expected);
        List<TaskRecord> page1 = store.tasks(TaskState.SUCCEEDED, null, 0, 3);
        assertThat(page1).extracting(TaskRecord::id).containsExactlyElementsOf(expected.subList(0, 3));
        List<TaskRecord> page2 =
                store.tasks(TaskState.SUCCEEDED, null, page1.get(2).id(), 3);
        assertThat(page2).extracting(TaskRecord::id).containsExactly(expected.get(3));
        assertThat(store.tasks(TaskState.SUCCEEDED, null, expected.get(3), 3)).isEmpty();
    }

    @Test
    void tasksFiltersByStateAndQueue() {
        int second = store.queueId("second");
        long inMain = putNew();
        long inSecond = put(task(second, event));
        assertThat(store.tasks(TaskState.QUEUED, null, 0, 10))
                .extracting(TaskRecord::id)
                .containsExactlyInAnyOrder(inMain, inSecond);
        assertThat(store.tasks(TaskState.QUEUED, second, 0, 10))
                .extracting(TaskRecord::id)
                .containsExactly(inSecond);
        assertThat(store.tasks(TaskState.DEAD, null, 0, 10)).isEmpty();
        assertThat(store.tasks(TaskState.QUEUED, null, 0, 1)).hasSize(1);
    }
}

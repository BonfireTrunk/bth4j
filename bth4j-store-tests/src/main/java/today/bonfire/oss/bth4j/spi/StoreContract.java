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
import java.util.stream.Stream;
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
    private static final Duration MID = Duration.ofMillis(600);
    private static final Duration PAST_MID = Duration.ofMillis(700);

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

    /** @return whether finished keys are released by the store itself, so that {@link MaintenanceStore#releaseKeys} has nothing to count */
    protected boolean releasesKeysOnItsOwn() {
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
        assertThat(store.info().schemaVersion()).isPositive();
        assertThat(store.info().engine()).isNotBlank();
    }

    @Test
    void initIsRepeatable() {
        var version = store.info().schemaVersion();
        store.init();
        store.init();
        assertThat(store.info().schemaVersion()).isEqualTo(version);
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
        TaskRecord t = claim(1, MID, null).get(0);
        advance(Duration.ofMillis(100));
        List<LeaseStatus> status = store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        assertThat(status).containsExactly(new LeaseStatus(t.id(), true, false));
        advance(PAST_MID);
        assertThat(store.reclaim(10)).isEmpty();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
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
    void retryOfADeadKeyedTaskKeepsTheKeyAndHoldsItAsAnOpenTask() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
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
        assertThat(store.ack(running.id(), running.revision(), true, true)).isTrue();
        assertThat(get(t.id()).keyUntil()).isNotNull();
    }

    @Test
    void retryOfACancelledKeyedTaskKeepsTheKeyToo() {
        long id = put(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.cancel(id, get(id).revision(), "no")).isEqualTo(CancelOutcome.CANCELLED);

        assertThat(store.retry(id, get(id).revision())).isEqualTo(RetryOutcome.RETRIED);

        assertThat(get(id).uniqueKey()).isEqualTo("k");
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(id));
    }

    @Test
    void retryOfAnExpiredKeyedTaskKeepsTheKeyAndClearsTheDeadline() {
        long id = put(task().uniqueKey("k").keyKeep(Duration.ofHours(1)).startBy(now().plus(SHORT)));
        advance(PAST_SHORT);
        assertThat(store.expire(10)).containsExactly(id);

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
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
        Instant oldUntil = get(t.id()).keyUntil();
        assertThat(store.retry(t.id(), get(t.id()).revision())).isEqualTo(RetryOutcome.RETRIED);

        advance(PAST_MID);
        store.releaseKeys(10);
        assertThat(store.purge(TaskState.DEAD, Duration.ZERO, 10)).isZero();
        assertThat(store.taskByKey("k")).contains(get(t.id()));
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));

        TaskRecord running = claimOne();
        assertThat(running.id()).isEqualTo(t.id());
        assertThat(store.ack(running.id(), running.revision(), true, true)).isTrue();
        TaskRecord finished = get(t.id());
        assertThat(finished.keyKeep()).isEqualTo(MID);
        assertThat(finished.keyUntil())
                .isAfter(oldUntil)
                .isEqualTo(finished.stateChanged().plus(MID));
        assertThat(store.enqueue(task().uniqueKey("k").build())).isEqualTo(new EnqueueOutcome.DuplicateKey(t.id()));
        assertThat(store.purge(TaskState.SUCCEEDED, Duration.ZERO, 10)).isZero();

        advance(PAST_MID);
        assertThat(store.taskByKey("k")).isEmpty();
        put(task().uniqueKey("k"));
    }

    @Test
    void retryAfterTheKeyWindowPassedAndAnotherTaskTookTheKeyLeavesTheNewHolder() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
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
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
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
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
        int revision = get(t.id()).revision();

        assertThat(store.retry(t.id(), revision - 1)).isEqualTo(RetryOutcome.STALE);
        assertThat(store.retry(t.id(), revision)).isEqualTo(RetryOutcome.RETRIED);

        TaskRecord retried = get(t.id());
        assertThat(retried.state()).isEqualTo(TaskState.QUEUED);
        assertThat(retried.uniqueKey()).isNull();
        assertThat(retried.keyKeep()).isNull();
        assertThat(store.retry(t.id(), retried.revision())).isEqualTo(RetryOutcome.STALE);
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
    void failureWithoutMessageAndStackRoundTripsAsNulls() {
        TaskRecord retried = leased(task());
        FailureInfo info = new FailureInfo(1, millis(now()), "java.io.IOException", null, null);
        assertThat(store.fail(retried.id(), retried.revision(), info, millis(now().plusSeconds(60)), true))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(get(retried.id()).failures()).containsExactly(info);
        assertThat(get(retried.id()).lastFailure().message()).isNull();
        assertThat(get(retried.id()).lastFailure().stack()).isNull();

        TaskRecord dead = leased(task());
        assertThat(store.fail(dead.id(), dead.revision(), info, null, true)).isEqualTo(FailOutcome.DEAD);
        assertThat(get(dead.id()).failures()).containsExactly(info);
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
        TaskRecord t = claim(1, MID, null).get(0);
        advance(Duration.ofMillis(100));
        store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE);
        advance(PAST_MID);
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
    void heldTasksAheadOfPurgeableOnesDoNotStarveThePurge() {
        int held = 1100;
        Duration window = Duration.ofSeconds(20);
        for (int i = 0; i < held; i++) {
            put(task().uniqueKey("held" + i).keyKeep(window));
        }
        for (int done = 0; done < held; ) {
            List<TaskRecord> batch = claim(100);
            for (TaskRecord t : batch) {
                assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
            }
            done += batch.size();
        }
        List<Long> purgeable = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TaskRecord t = leased(task());
            assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
            purgeable.add(t.id());
        }
        advance(Duration.ofMillis(150));
        long removed = 0;
        for (int call = 0; call < 3 && removed < purgeable.size(); call++) {
            removed += store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 10);
        }
        assertThat(removed).isEqualTo(purgeable.size());
        purgeable.forEach(id -> assertThat(store.getTask(id)).isEmpty());
        assertThat(store.counts().get(queue).succeeded()).isEqualTo(held);
        advance(window);
        long rest = 0;
        for (long n; (n = store.purge(TaskState.SUCCEEDED, Duration.ofMillis(100), 500)) > 0; ) {
            rest += n;
        }
        assertThat(rest).isEqualTo(held);
        assertThat(store.counts().getOrDefault(queue, StateCounts.ZERO).succeeded())
                .isZero();
    }

    @Test
    void releaseKeysFreesOnlyExpiredKeys() {
        TaskRecord expiring = leased(task().uniqueKey("short").keyKeep(SHORT));
        assertThat(store.ack(expiring.id(), expiring.revision(), true, true)).isTrue();
        TaskRecord lasting = leased(task().uniqueKey("long").keyKeep(Duration.ofHours(1)));
        assertThat(store.ack(lasting.id(), lasting.revision(), true, true)).isTrue();
        assertThat(store.releaseKeys(10)).isZero();
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
    void finishCancelledExpiredEndsATaskWhoseLeaseExpired() {
        put(task().uniqueKey("k"));
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(t.id());
        assertThat(store.finishCancelledExpired(t.id(), t.revision(), "stop", true))
                .isTrue();
        TaskRecord done = get(t.id());
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.revision()).isEqualTo(t.revision() + 1);
        assertThat(done.cancelReason()).isEqualTo("stop");
        assertThat(done.uniqueKey()).isNull();
    }

    @Test
    void finishCancelledExpiredIsANoOpAfterTheLeaseWasRenewed() {
        put(task().uniqueKey("k"));
        TaskRecord t = claim(1, MID, null).get(0);
        advance(Duration.ofMillis(100));
        assertThat(store.renew(List.of(new LeaseRef(t.id(), t.revision())), LEASE))
                .extracting(LeaseStatus::owned)
                .containsExactly(true);
        assertThat(store.finishCancelledExpired(t.id(), t.revision(), "stop", true))
                .isFalse();
        TaskRecord after = get(t.id());
        assertThat(after.state()).isEqualTo(TaskState.LEASED);
        assertThat(after.revision()).isEqualTo(t.revision());
        assertThat(after.uniqueKey()).isEqualTo("k");
    }

    @Test
    void finishCancelledExpiredIsFencedByRevisionAndState() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.finishCancelledExpired(t.id(), t.revision() + 1, "x", true))
                .isFalse();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.LEASED);
        long queued = putNew();
        assertThat(store.finishCancelledExpired(queued, get(queued).revision(), "x", true))
                .isFalse();
    }

    @Test
    void finishCancelledByTheWorkerIgnoresTheLeaseExpiry() {
        put(task());
        TaskRecord t = claim(1, SHORT, null).get(0);
        advance(PAST_SHORT);
        assertThat(store.finishCancelled(t.id(), t.revision(), "stop", true)).isTrue();
        assertThat(get(t.id()).state()).isEqualTo(TaskState.CANCELLED);
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
        assertThat(store.retry(t.id(), t.revision())).isEqualTo(RetryOutcome.RETRIED);
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
        assertThat(store.retry(q.id(), q.revision())).isEqualTo(RetryOutcome.STALE);
        assertThat(get(q.id())).isEqualTo(q);
        TaskRecord scheduled = get(put(task().due(now().plus(Duration.ofHours(1)))));
        assertThat(store.retry(scheduled.id(), scheduled.revision())).isEqualTo(RetryOutcome.STALE);
        assertThat(get(scheduled.id())).isEqualTo(scheduled);
        TaskRecord l = claimOne();
        assertThat(l.id()).isEqualTo(q.id());
        assertThat(store.retry(l.id(), l.revision())).isEqualTo(RetryOutcome.STALE);
        assertThat(get(l.id())).isEqualTo(l);
        assertThat(store.ack(l.id(), l.revision(), true, true)).isTrue();
        TaskRecord s = get(l.id());
        assertThat(store.retry(s.id(), s.revision())).isEqualTo(RetryOutcome.STALE);
        assertThat(get(s.id())).isEqualTo(s);
        TaskRecord d = dead();
        assertThat(store.retry(d.id(), d.revision() - 1)).isEqualTo(RetryOutcome.STALE);
        assertThat(get(d.id())).isEqualTo(d);
        assertThat(store.retry(store.allocateIds(1), 1)).isEqualTo(RetryOutcome.STALE);
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
        assertThat(store.retry(a, get(a).revision())).isEqualTo(RetryOutcome.RETRIED);
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
        assertThat(store.retry(e.id(), e.revision())).isEqualTo(RetryOutcome.RETRIED);
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
    void setNextFireAssignsANewRevisionAndPausingKeepsIt() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(definition("a", "* * * * *"), fire);
        long revision = revisionOf("a");
        store.setNextFire("a", fire.plusSeconds(5));
        long triggered = revisionOf("a");
        assertThat(triggered).isNotEqualTo(revision);
        store.pauseRecurring("a", true);
        assertThat(revisionOf("a")).isEqualTo(triggered);
    }

    private long revisionOf(String scheduleId) {
        return store.recurringDefinitions().stream()
                .filter(r -> r.definition().scheduleId().equals(scheduleId))
                .findFirst()
                .orElseThrow()
                .definition()
                .revision();
    }

    private Instant nextFireOf(String scheduleId) {
        return store.recurringSchedules().get(scheduleId);
    }

    @Test
    void setNextFireWithTheCurrentRevisionAndFireTimeReplacesTheNextFireAndKeepsTheRevision() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(definition("a", "* * * * *"), fire);
        long revision = revisionOf("a");
        assertThat(store.setNextFire("a", revision, fire, fire.plusSeconds(500)))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(fire.plusSeconds(500));
        assertThat(revisionOf("a")).isEqualTo(revision);
        assertThat(store.dueRecurring(fire.plusSeconds(500), 10)).hasSize(1);
    }

    @Test
    void setNextFireWithAStaleRevisionDoesNothing() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(definition("a", "* * * * *"), fire);
        long stale = revisionOf("a");
        store.putRecurring(definition("a", "* * * * *"), fire);
        assertThat(store.setNextFire("a", stale, fire, fire.plusSeconds(500))).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire);

        long current = revisionOf("a");
        assertThat(store.setNextFire("a", fire.plusSeconds(10))).isTrue();
        assertThat(store.setNextFire("a", current, fire.plusSeconds(10), fire.plusSeconds(500)))
                .isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire.plusSeconds(10));
    }

    @Test
    void setNextFireWithAStaleFireTimeDoesNothing() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(definition("a", "* * * * *"), fire);
        long revision = revisionOf("a");
        assertThat(store.setNextFire("a", revision, fire.plusSeconds(1), fire.plusSeconds(500)))
                .isFalse();
        assertThat(nextFireOf("a")).isEqualTo(fire);
    }

    @Test
    void setNextFireWithARevisionDoesNothingForAPausedOrUnknownDefinition() {
        Instant fire = millis(now().plusSeconds(60));
        store.putRecurring(definition("a", "* * * * *"), fire);
        long revision = revisionOf("a");
        assertThat(store.pauseRecurring("a", true)).isTrue();
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
        store.putRecurring(fullDefinition(scheduleId), fire);
        DueSchedule due = dueOf(scheduleId, fire);
        TaskRecord occurrence = fixedDelayOccurrence(scheduleId, fire);
        assertThat(store.commitRecurring(due, parkedFire(), occurrence)).isTrue();
        return occurrence;
    }

    private RecurringDefinition withFixedDelay(RecurringDefinition d, long delayMs) {
        return new RecurringDefinition(
                d.scheduleId(),
                d.eventId(),
                d.payloadVersion(),
                d.queueId(),
                d.account(),
                d.cron(),
                d.zone(),
                d.misfire(),
                d.catchUpMax(),
                delayMs,
                d.overlapSkip(),
                d.data(),
                d.maxAttempts(),
                d.paused(),
                d.revision());
    }

    private DueSchedule dueOf(String scheduleId, Instant upTo) {
        return store.dueRecurring(upTo, 10).stream()
                .filter(d -> d.scheduleId().equals(scheduleId))
                .findFirst()
                .orElseThrow();
    }

    private TaskRecord fixedDelayOccurrence(String scheduleId, Instant fire) {
        return task().scheduleId(scheduleId)
                .due(fire)
                .state(TaskState.SCHEDULED)
                .uniqueKey("sched:" + scheduleId)
                .build();
    }

    private boolean pausedOf(String scheduleId) {
        return store.recurringDefinitions().stream()
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
        assertThat(store.dueRecurring(rearmed, 10))
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
        assertThat(store.getTask(running.id())).isEmpty();
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
        assertThat(store.pauseRecurring("a", true)).isTrue();
        Instant rearmed = millis(now().plusSeconds(90));
        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), rearmed)))
                .isTrue();
        assertThat(pausedOf("a")).isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
        assertThat(store.dueRecurring(rearmed, 10)).isEmpty();

        assertThat(store.pauseRecurring("a", false)).isTrue();
        assertThat(store.dueRecurring(rearmed, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("a");
    }

    @Test
    void aSkippedOccurrenceParksTheDefinitionAndTheRunningOccurrenceStillRearmsIt() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant trigger = millis(now());
        assertThat(store.setNextFire("a", trigger)).isTrue();
        DueSchedule due = dueOf("a", trigger);
        TaskRecord second = fixedDelayOccurrence("a", trigger);
        Instant parked = parkedFire();
        assertThat(store.commitRecurring(due, parked, second)).isTrue();
        assertThat(store.getTask(second.id())).isEmpty();
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
        assertThat(store.setNextFire("a", trigger)).isTrue();
        TaskRecord second =
                task().scheduleId("a").due(trigger).state(TaskState.SCHEDULED).build();
        assertThat(store.commitRecurring(dueOf("a", trigger), parkedFire(), second))
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
        assertThat(store.deleteRecurring("a")).isTrue();
        assertThat(store.ack(
                        running.id(),
                        running.revision(),
                        true,
                        true,
                        ScheduleEffect.rearm("a", running.id(), millis(now().plusSeconds(90)))))
                .isTrue();
        assertThat(store.recurringSchedules()).isEmpty();
        assertThat(store.recurringDefinitions()).isEmpty();
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
        assertThat(store.dueRecurring(now().plus(Duration.ofDays(1000)), 10)).isEmpty();
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
        assertThat(store.cancel(running.id(), running.revision(), "stop")).isEqualTo(CancelOutcome.REQUESTED);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(running.id());
        ScheduleEffect effect = ScheduleEffect.pause("a", running.id());
        assertThat(store.finishCancelledExpired(running.id(), get(running.id()).revision(), "stop", true, effect))
                .isTrue();
        assertThat(get(running.id()).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(pausedOf("a")).isTrue();
        assertThat(effect.applied()).isTrue();
    }

    @Test
    void finishCancelledExpiredLeavesTheDefinitionAloneForAnotherOccurrence() {
        TaskRecord running = runningFixedDelayOccurrence("a", SHORT);
        assertThat(store.cancel(running.id(), running.revision(), "stop")).isEqualTo(CancelOutcome.REQUESTED);
        advance(PAST_SHORT);
        assertThat(store.reclaim(10)).extracting(ExpiredLease::id).containsExactly(running.id());
        ScheduleEffect effect = ScheduleEffect.pause("a", store.allocateIds(1));
        assertThat(store.finishCancelledExpired(running.id(), get(running.id()).revision(), "stop", true, effect))
                .isTrue();
        assertThat(get(running.id()).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(pausedOf("a")).isFalse();
        assertThat(effect.applied()).isFalse();
    }

    @Test
    void aSkippedCommitAdoptsTheKeyHolderWhenNoOccurrenceIsRecorded() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant next = millis(now().plusSeconds(1));
        store.putRecurring(withFixedDelay(fullDefinition("a"), 120_000L), next, next);
        advance(Duration.ofSeconds(2));
        TaskRecord second = fixedDelayOccurrence("a", next);
        Instant parked = parkedFire();
        assertThat(store.commitRecurring(dueOf("a", next), parked, second)).isTrue();
        assertThat(store.getTask(second.id())).isEmpty();
        assertThat(nextFireOf("a")).isEqualTo(parked);

        Instant rearmed = millis(now().plusSeconds(90));
        ScheduleEffect effect = ScheduleEffect.rearm("a", running.id(), rearmed);
        assertThat(store.ack(running.id(), running.revision(), true, true, effect))
                .isTrue();
        assertThat(effect.applied()).isTrue();
        assertThat(nextFireOf("a")).isEqualTo(rearmed);
    }

    @Test
    void aSkippedCommitDoesNotAdoptAKeyHolderThatIsNotAnOccurrenceOfTheSchedule() {
        Instant fire = millis(now().plusSeconds(1));
        store.putRecurring(fullDefinition("a"), fire);
        TaskRecord foreign = leased(task().uniqueKey("sched:a"));
        advance(Duration.ofSeconds(2));
        TaskRecord second = fixedDelayOccurrence("a", fire);
        Instant parked = parkedFire();
        assertThat(store.commitRecurring(dueOf("a", fire), parked, second)).isTrue();
        assertThat(store.getTask(second.id())).isEmpty();
        assertThat(nextFireOf("a")).isEqualTo(parked);

        ScheduleEffect effect = ScheduleEffect.rearm("a", foreign.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(foreign.id(), foreign.revision(), true, true, effect))
                .isTrue();
        assertThat(effect.applied()).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(parked);
    }

    @Test
    void effectsReportWhetherTheyChangedTheDefinition() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        ScheduleEffect lost = ScheduleEffect.rearm("a", running.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(running.id(), running.revision() + 1, true, true, lost))
                .isFalse();
        assertThat(lost.applied()).isFalse();

        TaskRecord other = leased(task().scheduleId("a"));
        ScheduleEffect stale = ScheduleEffect.rearm("a", other.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(other.id(), other.revision(), true, true, stale)).isTrue();
        assertThat(stale.applied()).isFalse();

        ScheduleEffect current = ScheduleEffect.rearm("a", running.id(), millis(now().plusSeconds(90)));
        assertThat(store.ack(running.id(), running.revision(), true, true, current))
                .isTrue();
        assertThat(current.applied()).isTrue();
    }

    @Test
    void aFailThatRetriesDoesNotApplyTheEffectAndOneThatKillsDoes() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        ScheduleEffect first = ScheduleEffect.pause("a", running.id());
        assertThat(store.fail(running.id(), running.revision(), failure(1), now().plusSeconds(5), true, first))
                .isEqualTo(FailOutcome.RETRY);
        assertThat(first.applied()).isFalse();

        advance(Duration.ofSeconds(10));
        store.promote(10);
        TaskRecord again = claimOne();
        ScheduleEffect second = ScheduleEffect.pause("a", again.id());
        assertThat(store.fail(again.id(), again.revision(), failure(2), null, true, second))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(second.applied()).isTrue();
    }

    @Test
    void finishCancelledAndFailExpiredReportTheEffect() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        ScheduleEffect cancel = ScheduleEffect.pause("a", running.id());
        assertThat(store.finishCancelled(running.id(), running.revision(), "stop", true, cancel))
                .isTrue();
        assertThat(cancel.applied()).isTrue();

        TaskRecord expiring = runningFixedDelayOccurrence("b", SHORT);
        advance(PAST_SHORT);
        ScheduleEffect expired = ScheduleEffect.pause("b", expiring.id());
        assertThat(store.failExpired(expiring.id(), expiring.revision(), failure(1), null, true, expired))
                .isEqualTo(FailOutcome.DEAD);
        assertThat(expired.applied()).isTrue();
    }

    @Test
    void cancelOfAWaitingFixedDelayOccurrencePausesTheDefinitionInTheSameStep() {
        TaskRecord waiting = committedFixedDelayOccurrence("a");
        ScheduleEffect effect = ScheduleEffect.pause("a", waiting.id());
        assertThat(store.cancel(waiting.id(), get(waiting.id()).revision(), "stop", effect))
                .isEqualTo(CancelOutcome.CANCELLED);
        assertThat(effect.applied()).isTrue();
        assertThat(pausedOf("a")).isTrue();
    }

    @Test
    void cancelOfALeasedOccurrenceOnlyRequestsTheCancelAndPausesNothing() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        ScheduleEffect effect = ScheduleEffect.pause("a", running.id());
        assertThat(store.cancel(running.id(), running.revision(), "stop", effect))
                .isEqualTo(CancelOutcome.REQUESTED);
        assertThat(effect.applied()).isFalse();
        assertThat(pausedOf("a")).isFalse();
    }

    @Test
    void cancelOfAnOccurrenceThatIsNoLongerTheRecordedOneLeavesTheDefinitionAlone() {
        TaskRecord waiting = committedFixedDelayOccurrence("a");
        Instant next = millis(now().plusSeconds(90));
        store.putRecurring(withFixedDelay(fullDefinition("a"), 120_000L), next, next);
        ScheduleEffect effect = ScheduleEffect.pause("a", waiting.id());
        assertThat(store.cancel(waiting.id(), get(waiting.id()).revision(), "stop", effect))
                .isEqualTo(CancelOutcome.CANCELLED);
        assertThat(effect.applied()).isFalse();
        assertThat(pausedOf("a")).isFalse();
        assertThat(nextFireOf("a")).isEqualTo(next);
    }

    @Test
    void deleteOfAWaitingFixedDelayOccurrencePausesTheDefinitionAndAStaleOneDoesNot() {
        TaskRecord waiting = committedFixedDelayOccurrence("a");
        ScheduleEffect effect = ScheduleEffect.pause("a", waiting.id());
        assertThat(store.delete(waiting.id(), get(waiting.id()).revision(), effect))
                .isTrue();
        assertThat(effect.applied()).isTrue();
        assertThat(pausedOf("a")).isTrue();

        TaskRecord second = committedFixedDelayOccurrence("b");
        Instant next = millis(now().plusSeconds(90));
        store.putRecurring(withFixedDelay(fullDefinition("b"), 120_000L), next, next);
        ScheduleEffect stale = ScheduleEffect.pause("b", second.id());
        assertThat(store.delete(second.id(), get(second.id()).revision(), stale))
                .isTrue();
        assertThat(stale.applied()).isFalse();
        assertThat(pausedOf("b")).isFalse();
    }

    @Test
    void putRecurringWithUnchangedTimingKeepsTheParkWhileTheOccurrenceRunsAndRecoversAfterIt() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant parked = nextFireOf("a");
        Instant next = millis(now().plusSeconds(90));
        store.putRecurring(fullDefinition("a"), next, next);
        assertThat(nextFireOf("a")).isEqualTo(parked);

        assertThat(store.ack(
                        running.id(), running.revision(), true, true, ScheduleEffect.rearm("a", running.id(), next)))
                .isTrue();
        assertThat(nextFireOf("a")).isEqualTo(next);
    }

    @Test
    void putRecurringRecoversAParkedDefinitionOnceItsOccurrenceIsGone() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        assertThat(store.ack(running.id(), running.revision(), true, true)).isTrue();
        assertThat(nextFireOf("a")).isAfter(now().plus(Duration.ofDays(1000)));
        Instant next = millis(now().plusSeconds(90));
        store.putRecurring(fullDefinition("a"), next, next);
        assertThat(nextFireOf("a")).isEqualTo(next);
    }

    @Test
    void putRecurringRecoversAParkedDefinitionWhoseOccurrenceWasRemoved() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        assertThat(store.ack(running.id(), running.revision(), false, false)).isTrue();
        assertThat(store.getTask(running.id())).isEmpty();
        Instant next = millis(now().plusSeconds(90));
        store.putRecurring(fullDefinition("a"), next, next);
        assertThat(nextFireOf("a")).isEqualTo(next);
    }

    @Test
    void putRecurringKeepsAHealthyFixedDelayDefinition() {
        Instant soon = millis(now().plusSeconds(30));
        store.putRecurring(fullDefinition("a"), soon);
        Instant next = millis(now().plusSeconds(90));
        store.putRecurring(fullDefinition("a"), next, next);
        assertThat(nextFireOf("a")).isEqualTo(soon);
    }

    @Test
    void putRecurringWithChangedTimingForgetsTheOccurrenceInFlight() {
        TaskRecord running = runningFixedDelayOccurrence("a");
        Instant next = millis(now().plusSeconds(90));
        store.putRecurring(definition("a", "* * * * *"), next, next);
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
    void putRecurringResetsACronDefinitionPushedBeyondTheKeepBound() {
        Instant first = millis(now().plusSeconds(60));
        Instant second = first.plusSeconds(60);
        store.putRecurring(definition("a", "* * * * *"), first, second);
        store.putRecurring(definition("a", "* * * * *"), first.plusSeconds(10), second.plusSeconds(10));
        assertThat(nextFireOf("a")).isEqualTo(first);

        Instant pushed = millis(now().plus(Duration.ofDays(30)));
        assertThat(store.setNextFire("a", revisionOf("a"), first, pushed)).isTrue();
        store.putRecurring(definition("a", "* * * * *"), first, second);
        assertThat(nextFireOf("a")).isEqualTo(first);
        assertThat(store.dueRecurring(first, 10))
                .extracting(DueSchedule::scheduleId)
                .containsExactly("a");
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
    void taskByKeyFindsTheOpenHolder() {
        TaskRecord leased = leased(task().uniqueKey("l"));
        long queued = put(task().uniqueKey("q"));
        long scheduled = put(task().uniqueKey("s").due(millis(now().plus(Duration.ofDays(1)))));

        assertThat(store.taskByKey("q")).map(TaskRecord::id).contains(queued);
        assertThat(store.taskByKey("s")).map(TaskRecord::id).contains(scheduled);
        assertThat(store.taskByKey("l")).map(TaskRecord::id).contains(leased.id());
        assertThat(store.taskByKey("q").orElseThrow().uniqueKey()).isEqualTo("q");
    }

    @Test
    void taskByKeyFindsTheFinishedHolderInsideKeyKeepAndNotAfter() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();

        assertThat(store.taskByKey("k")).map(TaskRecord::id).contains(t.id());
        assertThat(store.taskByKey("k").orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
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
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();

        assertThat(store.taskByKey("k")).isEmpty();
    }

    @Test
    void taskByKeyReturnsTheNewHolderOnceTheOldWindowPassedAndTheKeyWasReused() {
        TaskRecord old = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.ack(old.id(), old.revision(), true, true)).isTrue();
        advance(PAST_SHORT);
        long fresh = put(task().uniqueKey("k"));

        assertThat(store.taskByKey("k")).map(TaskRecord::id).contains(fresh);
    }

    @Test
    void taskByKeyReturnsTheRetriedTaskInsideItsKeepWindow() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(Duration.ofHours(1)));
        assertThat(store.fail(t.id(), t.revision(), failure(1), null, true)).isEqualTo(FailOutcome.DEAD);
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
        assertThat(store.fail(dead.id(), dead.revision(), failure(1), null, true))
                .isEqualTo(FailOutcome.DEAD);
        long queued = put(task().uniqueKey("cancelled").keyKeep(Duration.ofHours(1)));
        assertThat(store.cancel(queued, get(queued).revision(), "stop")).isEqualTo(CancelOutcome.CANCELLED);

        assertThat(store.taskByKey("dead")).map(TaskRecord::state).contains(TaskState.DEAD);
        assertThat(store.taskByKey("cancelled")).map(TaskRecord::state).contains(TaskState.CANCELLED);
    }

    @Test
    void taskByKeyIsEmptyAfterReleaseKeysRanPastTheWindow() {
        TaskRecord t = leased(task().uniqueKey("k").keyKeep(SHORT));
        assertThat(store.ack(t.id(), t.revision(), true, true)).isTrue();
        advance(PAST_SHORT);
        store.releaseKeys(10);

        assertThat(store.taskByKey("k")).isEmpty();
        assertThat(get(t.id()).uniqueKey()).isNull();
    }

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
    void tasksWalkTerminatesAndListsEachTaskOnceWhileATaskIsDeferred() {
        int other = store.eventId("other");
        List<Long> all = new ArrayList<>();
        all.add(put(task(queue, other).due(millis(now().minusSeconds(60)))));
        for (int i = 0; i < 9; i++) {
            all.add(putNew());
        }
        List<TaskRecord> claimed = claim(3, LEASE, Set.of(event));
        assertThat(claimed).hasSize(3);
        List<Long> queued = new ArrayList<>(all);
        claimed.forEach(t -> queued.remove(t.id()));
        for (int limit : new int[] {1, 3, 4, 20}) {
            List<Long> seen = new ArrayList<>();
            long after = 0;
            for (int page = 0; page < 30; page++) {
                List<TaskRecord> got = store.tasks(TaskState.QUEUED, null, after, limit);
                if (got.isEmpty()) {
                    break;
                }
                got.forEach(t -> seen.add(t.id()));
                after = got.get(got.size() - 1).id();
            }
            assertThat(seen).as("limit %d", limit).containsExactlyInAnyOrderElementsOf(queued);
        }
    }

    @Test
    void cursorWalkListsEachTaskOnceWhileTheCursorTaskIsCancelledBetweenPages() {
        for (int limit : new int[] {1, 2, 4}) {
            int walkQueue = store.queueId("walk" + limit);
            List<Long> all = new ArrayList<>();
            for (int i = 0; i < 9; i++) {
                all.add(put(task(walkQueue, event)));
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
                    assertThat(store.cancel(last.id(), last.revision(), "between pages"))
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
        int other = store.eventId("other");
        for (int limit : new int[] {1, 3, 4}) {
            int walkQueue = store.queueId("defer" + limit);
            List<Long> all = new ArrayList<>();
            long deferred = put(task(walkQueue, other).due(millis(now().minusSeconds(60))));
            all.add(deferred);
            for (int i = 0; i < 9; i++) {
                all.add(put(task(walkQueue, event)));
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
                    List<TaskRecord> claimed = claim(3, LEASE, Set.of(event), walkQueue);
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
        assertThat(group.stream().map(t -> t.stateChanged().toEpochMilli()).distinct())
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

        for (String bad : new String[] {"garbage", "1", ":5", "5:", "a:b", "1:2:3", ""}) {
            assertThatThrownBy(() -> store.tasks(TaskState.QUEUED, null, bad, 10))
                    .as("cursor '%s'", bad)
                    .isInstanceOf(IllegalArgumentException.class);
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

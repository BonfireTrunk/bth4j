package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * Queue slots under {@link QueuePolicy#maxConcurrent(int)}, driven through {@code processOnce} with a manual
 * executor so that no service thread runs.
 */
@Timeout(5)
class TaskProcessorCapacityTest {
    private static final TaskType<Void> TYPE = TaskType.of("capacity-test");
    private static final TaskType<Void> FAILING = TaskType.of("capacity-test-failing");
    private static final Duration LEASE = Duration.ofSeconds(1);
    /** past the class timeout minus the waits below, so only a wake-up ends the wait in time */
    private static final Duration IDLE_LONGER_THAN_THE_TEST = Duration.ofSeconds(4);
    /** generous for a wake-up; a full idle sleep of {@link #IDLE_LONGER_THAN_THE_TEST} cannot fit */
    private static final Duration WOKEN_WITHIN = Duration.ofSeconds(1);

    private final MutableClock clock = new MutableClock();
    private final InMemoryStore store = new InMemoryStore(clock);
    private final Names names = new Names(store);
    private final TaskClient client = TaskClient.builder()
            .store(store)
            .clock(clock)
            .queues(
                    List.of(
                            "Q:DEF",
                            "Q:HEAVY",
                            "Q:LIGHT",
                            "Q:CAPPED",
                            "Q:OPEN",
                            "Q:EMPTY",
                            "Q:BUSY",
                            "Q:PAUSED",
                            "Q:FIRST",
                            "Q:SECOND"),
                    "Q:DEF")
            .build();
    private final int queueId = names.queueId("Q:DEF");
    private final int typeId = names.typeId(TYPE.name());
    private final int failingTypeId = names.typeId(FAILING.name());
    private final Map<Long, TaskExecution> running = new ConcurrentHashMap<>();
    private final QueueReservations reservations = new QueueReservations();
    private final RunnerStatus status = new RunnerStatus(clock);
    private final TaskProcessorRegistry registry = new TaskProcessorRegistry()
            .register(TYPE, (context, payload) -> {})
            .register(FAILING, (context, payload) -> {
                throw new IllegalStateException("failed by the test");
            });
    private final TaskExecution.Settings settings = settingsOn(store);

    @Test
    void replacementProcessorClaimsAfterInheritedExecutionFinishes() throws Exception {
        var executor = new ManualExecutor(1);
        var predecessor = processor(executor, 1);
        long inheritedId = enqueue();

        processOnce(predecessor);
        assertThat(running).containsKey(inheritedId);
        var replacement = processor(executor, 1);
        executor.runNext();
        assertThat(running).isEmpty();
        assertThat(store.getTask(inheritedId).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(reservations.running(queueId)).isZero();

        long nextId = enqueue();
        processOnce(replacement);

        assertThat(store.getTask(nextId).orElseThrow().state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void reclaimedAttemptAndItsStaleAttemptEachReleaseTheirSlot() throws Exception {
        var executor = new ManualExecutor(2);
        var processor = processor(executor, 2);
        long id = enqueue();

        processOnce(processor);
        var stale = running.get(id);
        clock.advance(LEASE.plusMillis(1));
        var failure = new FailureInfo(1, clock.instant(), "lease-expired", "lease expired", null);
        assertThat(store.failExpired(id, stale.revision(), failure, clock.instant(), true, null))
                .isEqualTo(FailOutcome.RETRY);
        store.promote(10);

        processOnce(processor);
        var current = running.get(id);
        assertThat(current).isNotSameAs(stale);
        assertThat(reservations.running(queueId)).isEqualTo(2);

        executor.runNext();

        assertThat(running).containsEntry(id, current);
        assertThat(reservations.running(queueId)).isEqualTo(1);
        var leased = store.getTask(id).orElseThrow();
        assertThat(leased.state()).isEqualTo(TaskState.LEASED);
        assertThat(leased.revision()).isEqualTo(current.revision());
        clock.advance(Duration.ofMillis(100));
        processor.renewOnce();
        assertThat(store.getTask(id).orElseThrow().leaseUntil())
                .isEqualTo(clock.instant().plus(LEASE));

        executor.runNext();
        assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(running).isEmpty();
        assertThat(reservations.running(queueId)).isZero();

        long first = enqueue();
        long second = enqueue();
        processOnce(processor);
        assertThat(store.getTask(first).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(store.getTask(second).orElseThrow().state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void theQueueCapHoldsWhenTheExecutorHasMoreCapacityAndTheClaimBatchIsLarger() throws Exception {
        var executor = new ManualExecutor(10);
        var processor = processor(executor, 5, Map.of(queueId, QueuePolicy.maxConcurrent(2)));
        var ids = enqueue(5, "Q:DEF");

        processOnce(processor);

        assertThat(leased(ids)).isEqualTo(2);
        assertThat(reservations.running(queueId)).isEqualTo(2);
        assertThat(executor.available()).isEqualTo(8);
        assertThat(executor.claims).containsExactly(new Claim(5, 2));
    }

    @Test
    void aQuietProcessorReservesNoSlotAndClaimsNothingUntilItIsResumed() throws Exception {
        var executor = new ManualExecutor(10);
        var processor = processor(executor, 5, Map.of(queueId, QueuePolicy.maxConcurrent(5)));
        var ids = enqueue(3, "Q:DEF");
        processor.quiet(true);

        processOnce(processor);
        processOnce(processor);

        assertThat(ids).allSatisfy(id -> assertThat(state(id)).isEqualTo(TaskState.QUEUED));
        assertThat(executor.available()).isEqualTo(10);
        assertThat(reservations.running(queueId)).isZero();
        assertThat(status.lastClaimAt()).isNull();

        processor.quiet(false);
        processOnce(processor);

        assertThat(leased(ids)).isEqualTo(3);
        assertThat(executor.available()).isEqualTo(7);
    }

    @Test
    void anotherIterationWhileTheAttemptsStayBlockedClaimsNothingUntilOneFinishes() throws Exception {
        var executor = new ManualExecutor(10);
        var processor = processor(executor, 5, Map.of(queueId, QueuePolicy.maxConcurrent(2)));
        var ids = enqueue(5, "Q:DEF");
        processOnce(processor);

        processOnce(processor);
        processOnce(processor);

        assertThat(leased(ids)).isEqualTo(2);
        assertThat(reservations.running(queueId)).isEqualTo(2);
        assertThat(executor.available()).isEqualTo(8);

        executor.runNext();
        processOnce(processor);

        assertThat(reservations.running(queueId)).isEqualTo(2);
        assertThat(ids.stream().filter(id -> state(id) == TaskState.SUCCEEDED)).hasSize(1);
        assertThat(leased(ids)).isEqualTo(2);
        assertThat(ids.stream().filter(id -> state(id) == TaskState.QUEUED)).hasSize(2);
    }

    @Test
    void freeSlotsAreSharedByWeightBetweenQueues() throws Exception {
        int heavy = names.queueId("Q:HEAVY");
        int light = names.queueId("Q:LIGHT");
        var executor = new ManualExecutor(4);
        var processor = processor(
                executor, 4, List.of(heavy, light), Map.of(heavy, QueuePolicy.weight(3), light, QueuePolicy.weight(1)));
        var heavyIds = enqueue(6, "Q:HEAVY");
        var lightIds = enqueue(6, "Q:LIGHT");

        processOnce(processor);

        assertThat(leased(heavyIds)).isEqualTo(3);
        assertThat(leased(lightIds)).isEqualTo(1);
    }

    @Test
    void weightsWhoseSumExceedsAnIntShareTheSlotsInTheirRatio() throws Exception {
        int heavy = names.queueId("Q:HEAVY");
        int light = names.queueId("Q:LIGHT");
        var executor = new ManualExecutor(4);
        var processor = processor(
                executor,
                4,
                List.of(heavy, light),
                Map.of(heavy, QueuePolicy.weight(3 << 29), light, QueuePolicy.weight(1 << 29)));
        var heavyIds = enqueue(6, "Q:HEAVY");
        var lightIds = enqueue(6, "Q:LIGHT");

        processOnce(processor);

        assertThat(leased(heavyIds)).isEqualTo(3);
        assertThat(leased(lightIds)).isEqualTo(1);
    }

    @Test
    void aQueueCapGivesItsUnusedShareToTheOtherQueues() throws Exception {
        int capped = names.queueId("Q:CAPPED");
        int open = names.queueId("Q:OPEN");
        var executor = new ManualExecutor(4);
        var processor = processor(
                executor,
                4,
                List.of(capped, open),
                Map.of(capped, QueuePolicy.maxConcurrent(1), open, QueuePolicy.DEFAULT));
        var cappedIds = enqueue(6, "Q:CAPPED");
        var openIds = enqueue(6, "Q:OPEN");

        processOnce(processor);

        assertThat(leased(cappedIds)).isEqualTo(1);
        assertThat(leased(openIds)).isEqualTo(3);
        assertThat(reservations.running(capped)).isEqualTo(1);
        assertThat(reservations.running(open)).isEqualTo(3);
    }

    @Test
    void anEmptyQueueGivesItsShareToTheQueueThatHasWork() throws Exception {
        int empty = names.queueId("Q:EMPTY");
        int busy = names.queueId("Q:BUSY");
        var executor = new ManualExecutor(4);
        var processor = processor(
                executor,
                4,
                List.of(empty, busy),
                Map.of(empty, QueuePolicy.maxConcurrent(5), busy, QueuePolicy.maxConcurrent(5)));
        var busyIds = enqueue(10, "Q:BUSY");

        processOnce(processor);

        assertThat(leased(busyIds)).isEqualTo(4);
        assertThat(reservations.running(busy)).isEqualTo(4);
        assertThat(reservations.running(empty)).isZero();
    }

    @Test
    void aPausedQueueGivesItsShareToTheOtherQueueUntilItIsResumed() throws Exception {
        int paused = names.queueId("Q:PAUSED");
        int busy = names.queueId("Q:BUSY");
        var executor = new ManualExecutor(4);
        var processor = processor(
                executor,
                4,
                List.of(paused, busy),
                Map.of(paused, QueuePolicy.maxConcurrent(5), busy, QueuePolicy.maxConcurrent(5)));
        var pausedIds = enqueue(3, "Q:PAUSED");
        var busyIds = enqueue(10, "Q:BUSY");
        store.pauseQueue(paused, true);

        processOnce(processor);

        assertThat(leased(pausedIds)).isZero();
        assertThat(leased(busyIds)).isEqualTo(4);

        store.pauseQueue(paused, false);
        while (executor.pending() > 0) executor.runNext();
        processOnce(processor);

        assertThat(leased(pausedIds)).isEqualTo(2);
        assertThat(leased(busyIds)).isEqualTo(2);
    }

    @Test
    void anAbandonedAttemptKeepsItsSlotUntilItsThreadReturns() throws Exception {
        var executor = new ManualExecutor(2);
        var processor = processor(executor, 1);
        long id = enqueue();
        processOnce(processor);
        var abandoned = running.get(id);
        assertThat(reservations.abandon(abandoned)).isTrue();
        clock.advance(LEASE.plusMillis(1));
        var failure = new FailureInfo(1, clock.instant(), "lease-expired", "lease expired", null);
        assertThat(store.failExpired(id, abandoned.revision(), failure, clock.instant(), true, null))
                .isEqualTo(FailOutcome.RETRY);
        store.promote(10);

        processOnce(processor);

        assertThat(state(id)).isEqualTo(TaskState.QUEUED);
        assertThat(reservations.running(queueId)).isEqualTo(1);
        assertThat(reservations.abandoned()).isEqualTo(1);

        executor.runNext();

        assertThat(reservations.running(queueId)).isZero();
        assertThat(reservations.abandoned()).isZero();
        assertThat(state(id)).isEqualTo(TaskState.QUEUED);
        processOnce(processor);
        assertThat(state(id)).isEqualTo(TaskState.LEASED);
    }

    @Test
    void aClaimThatThrowsReleasesTheSlotsAndTheNextIterationClaims() throws Exception {
        var executor = new ManualExecutor(2);
        var fail = new AtomicInteger(1);
        var processor = processorOn(
                StoreInterceptor.around(store, (method, args, real) -> {
                    if (method.equals("claim") && fail.getAndDecrement() > 0) {
                        throw new TaskStoreException("claim failed");
                    }
                    return real.proceed();
                }),
                executor,
                2);
        long id = enqueue();

        assertThatThrownBy(() -> processOnce(processor)).isInstanceOf(TaskStoreException.class);

        assertThat(executor.claims).isEmpty();
        assertThat(state(id)).isEqualTo(TaskState.QUEUED);
        assertThat(executor.available()).isEqualTo(2);
        assertThat(reservations.running(queueId)).isZero();
        assertThat(running).isEmpty();
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("store-error"));

        processOnce(processor);

        assertThat(state(id)).isEqualTo(TaskState.LEASED);
        assertThat(reservations.running(queueId)).isEqualTo(1);
    }

    @Test
    void aClaimThatCommittedBeforeItsReplyWasLostLeavesTheTaskToExpiryRecovery() throws Exception {
        var executor = new ManualExecutor(2);
        var fail = new AtomicInteger(1);
        var processor = processorOn(
                StoreInterceptor.around(store, (method, args, real) -> {
                    var result = real.proceed();
                    if (method.equals("claim") && fail.getAndDecrement() > 0) {
                        throw new TaskStoreException("reply lost");
                    }
                    return result;
                }),
                executor,
                2);
        long id = enqueue();

        assertThatThrownBy(() -> processOnce(processor)).isInstanceOf(TaskStoreException.class);

        assertThat(state(id)).isEqualTo(TaskState.LEASED);
        assertThat(running).isEmpty();
        assertThat(executor.available()).isEqualTo(2);
        assertThat(reservations.running(queueId)).isZero();

        clock.advance(LEASE.plusMillis(1));
        var expired = store.reclaim(10);
        assertThat(expired).singleElement().satisfies(e -> assertThat(e.id()).isEqualTo(id));
        var failure = new FailureInfo(1, clock.instant(), "lease-expired", "lease expired", null);
        assertThat(store.failExpired(id, expired.getFirst().revision(), failure, clock.instant(), true, null))
                .isEqualTo(FailOutcome.RETRY);
        store.promote(10);

        processOnce(processor);

        assertThat(state(id)).isEqualTo(TaskState.LEASED);
        assertThat(running).containsKey(id);
        assertThat(reservations.running(queueId)).isEqualTo(1);
    }

    @Test
    void weightedQueuesAreClaimedInOneStoreCall() throws Exception {
        int first = names.queueId("Q:FIRST");
        int second = names.queueId("Q:SECOND");
        var executor = new ManualExecutor(4);
        var claims = new AtomicInteger();
        var processor = processorOn(
                StoreInterceptor.around(store, (method, args, real) -> {
                    if (method.equals("claim") && claims.incrementAndGet() == 2) {
                        throw new TaskStoreException("second queue failed");
                    }
                    return real.proceed();
                }),
                executor,
                4,
                List.of(first, second),
                Map.of(first, QueuePolicy.maxConcurrent(5), second, QueuePolicy.maxConcurrent(5)));
        var firstIds = enqueue(3, "Q:FIRST");
        var secondIds = enqueue(3, "Q:SECOND");

        processOnce(processor);

        assertThat(claims).hasValue(1);
        assertThat(leased(firstIds)).isEqualTo(2);
        assertThat(leased(secondIds)).isEqualTo(2);
        assertThat(running).hasSize(4);
        assertThat(executor.available()).isZero();
        assertThat(reservations.running(first)).isEqualTo(2);
        assertThat(reservations.running(second)).isEqualTo(2);
    }

    @Test
    void aFailedRenewalRoundKeepsTheExecutionAndTheNextRoundRenewsIt() throws Exception {
        var executor = new ManualExecutor(1);
        var fail = new AtomicInteger(1);
        var processor = processorOn(
                StoreInterceptor.around(store, (method, args, real) -> {
                    if (method.equals("renew") && fail.getAndDecrement() > 0) {
                        throw new TaskStoreException("renew failed");
                    }
                    return real.proceed();
                }),
                executor,
                1);
        long id = enqueue();
        processOnce(processor);
        var leaseBefore = store.getTask(id).orElseThrow().leaseUntil();
        clock.advance(Duration.ofMillis(300));

        processor.renewOnce();

        assertThat(running).containsKey(id);
        assertThat(store.getTask(id).orElseThrow().leaseUntil()).isEqualTo(leaseBefore);
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("store-error"));
        assertThat(reservations.running(queueId)).isEqualTo(1);

        processor.renewOnce();

        assertThat(store.getTask(id).orElseThrow().leaseUntil())
                .isEqualTo(clock.instant().plus(LEASE));
        executor.runNext();
        assertThat(state(id)).isEqualTo(TaskState.SUCCEEDED);
        assertThat(reservations.running(queueId)).isZero();
    }

    @Test
    void aRenewalRoundBetweenTheAckAndTheReleaseReportsNoLostLease() throws Exception {
        var executor = new ManualExecutor(1);
        var processor = new AtomicReference<TaskProcessorService>();
        processor.set(processorOn(
                StoreInterceptor.around(store, (method, args, real) -> {
                    var result = real.proceed();
                    if (method.equals("ack")) processor.get().renewOnce();
                    return result;
                }),
                executor,
                1));
        long id = enqueue();
        processOnce(processor.get());

        executor.runNext();

        assertThat(state(id)).isEqualTo(TaskState.SUCCEEDED);
        assertThat(status.issues()).noneMatch(i -> i.kind().equals("lease-lost"));
        assertThat(running).isEmpty();
    }

    @Test
    void anUnclaimThatFailsAfterAnExecutorRejectionReleasesTheSlotAndLeavesTheTaskToExpiryRecovery() throws Exception {
        var executor = new ManualExecutor(1);
        executor.reject = true;
        var processor = processorOn(
                StoreInterceptor.around(store, (method, args, real) -> {
                    if (method.equals("unclaim")) throw new TaskStoreException("unclaim failed");
                    return real.proceed();
                }),
                executor,
                1);
        long id = enqueue();

        processOnce(processor);

        assertThat(state(id)).isEqualTo(TaskState.LEASED);
        assertThat(running).isEmpty();
        assertThat(executor.available()).isEqualTo(1);
        assertThat(reservations.running(queueId)).isZero();
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("executor-rejected"));
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("store-error"));

        clock.advance(LEASE.plusMillis(1));
        assertThat(store.reclaim(10))
                .singleElement()
                .satisfies(e -> assertThat(e.id()).isEqualTo(id));
    }

    @Test
    void executorRejectionReleasesTheSlotOnce() throws Exception {
        var executor = new ManualExecutor(1);
        executor.reject = true;
        var processor = processor(executor, 1);
        long id = enqueue();

        processOnce(processor);

        assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(running).isEmpty();
        assertThat(reservations.running(queueId)).isZero();

        executor.reject = false;
        processOnce(processor);
        assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(reservations.running(queueId)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aClaimHeldBackByAQueueCapWakesWhenAnInheritedTaskFinishes(boolean restart) throws Exception {
        var executor = new ManualExecutor(10);
        var processor = processor(
                settings,
                executor,
                5,
                List.of(queueId),
                Map.of(queueId, QueuePolicy.maxConcurrent(1)),
                IDLE_LONGER_THAN_THE_TEST);
        var ids = enqueue(2, "Q:DEF");
        processOnce(processor);
        assertThat(leased(ids)).isEqualTo(1);

        var waitingProcessor = restart
                ? processor(
                        settings,
                        executor,
                        5,
                        List.of(queueId),
                        Map.of(queueId, QueuePolicy.maxConcurrent(1)),
                        IDLE_LONGER_THAN_THE_TEST)
                : processor;
        var waiting = Thread.ofPlatform().start(() -> {
            try {
                waitingProcessor.processOnce();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            awaitState(waiting, Thread.State.TIMED_WAITING);
            executor.runNext();

            waiting.join(WOKEN_WITHIN.toMillis());
            assertThat(waiting.isAlive()).as("woken by the finished task").isFalse();
        } finally {
            waiting.interrupt();
            waiting.join();
        }
        processOnce(waitingProcessor);
        assertThat(leased(ids)).isEqualTo(1);
        assertThat(ids.stream().filter(id -> state(id) == TaskState.SUCCEEDED)).hasSize(1);
    }

    @Test
    void aClaimThatFoundNoWorkUnderItsCapWaitsTheFullIntervalWhenATaskFinishes() throws Exception {
        var executor = new ManualExecutor(10);
        var processor = processor(
                settings,
                executor,
                5,
                List.of(queueId),
                Map.of(queueId, QueuePolicy.maxConcurrent(2)),
                IDLE_LONGER_THAN_THE_TEST);
        enqueue(1, "Q:DEF");
        processOnce(processor);

        var waiting = Thread.ofPlatform().start(() -> {
            try {
                processor.processOnce();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            awaitState(waiting, Thread.State.TIMED_WAITING);
            executor.runNext();

            waiting.join(WOKEN_WITHIN.toMillis());
            assertThat(waiting.isAlive())
                    .as("still sleeping: nothing was held back")
                    .isTrue();
        } finally {
            waiting.interrupt();
            waiting.join();
        }
    }

    @Test
    void reportsAFullClaimAndAnEmptyClaim() throws Exception {
        var executor = new ManualExecutor(2);
        var processor = processor(executor, 2, Map.of());
        enqueue(2, "Q:DEF");
        processOnce(processor);
        assertThat(executor.claims).containsExactly(new Claim(2, 2));
        executor.runNext();
        executor.runNext();
        processOnce(processor);
        assertThat(executor.claims).containsExactly(new Claim(2, 2), new Claim(2, 0));
    }

    private static void awaitState(Thread thread, Thread.State state) throws InterruptedException {
        while (thread.getState() != state) {
            if (!thread.isAlive()) throw new AssertionError("thread ended before reaching " + state);
            Thread.sleep(1);
        }
    }

    private long enqueue() {
        return client.enqueue(TaskRequest.of(TYPE).build()).id();
    }

    private List<Long> enqueue(int count, String queue) {
        var ids = new ArrayList<Long>();
        for (int i = 0; i < count; i++) {
            ids.add(client.enqueue(TaskRequest.of(TYPE).queue(queue).build()).id());
        }
        return ids;
    }

    private TaskState state(long id) {
        return store.getTask(id).orElseThrow().state();
    }

    private long leased(List<Long> ids) {
        return ids.stream().filter(id -> state(id) == TaskState.LEASED).count();
    }

    private TaskExecution.Settings settingsOn(FullStore executionStore) {
        return new TaskExecution.Settings(
                executionStore,
                executionStore,
                names,
                new Codecs(null),
                registry,
                RetryPolicies.of(RetryPolicy.defaultPolicy()),
                new Observers(null),
                clock,
                status,
                true,
                true,
                true,
                FailureDetail.FULL);
    }

    private TaskProcessorService processor(BackgroundExecutor executor, int maxConcurrent) {
        return processor(executor, maxConcurrent, Map.of(queueId, QueuePolicy.maxConcurrent(maxConcurrent)));
    }

    private TaskProcessorService processor(
            BackgroundExecutor executor, int claimBatchSize, Map<Integer, QueuePolicy> policies) {
        return processor(executor, claimBatchSize, List.of(queueId), policies);
    }

    private TaskProcessorService processor(
            BackgroundExecutor executor,
            int claimBatchSize,
            List<Integer> queueIds,
            Map<Integer, QueuePolicy> policies) {
        return processor(settings, executor, claimBatchSize, queueIds, policies);
    }

    private TaskProcessorService processorOn(FullStore executionStore, BackgroundExecutor executor, int maxConcurrent) {
        return processor(
                settingsOn(executionStore),
                executor,
                maxConcurrent,
                List.of(queueId),
                Map.of(queueId, QueuePolicy.maxConcurrent(maxConcurrent)));
    }

    private TaskProcessorService processorOn(
            FullStore executionStore,
            BackgroundExecutor executor,
            int claimBatchSize,
            List<Integer> queueIds,
            Map<Integer, QueuePolicy> policies) {
        return processor(settingsOn(executionStore), executor, claimBatchSize, queueIds, policies);
    }

    private TaskProcessorService processor(
            TaskExecution.Settings processorSettings,
            BackgroundExecutor executor,
            int claimBatchSize,
            List<Integer> queueIds,
            Map<Integer, QueuePolicy> policies) {
        return processor(processorSettings, executor, claimBatchSize, queueIds, policies, Duration.ofMillis(1));
    }

    private TaskProcessorService processor(
            TaskExecution.Settings processorSettings,
            BackgroundExecutor executor,
            int claimBatchSize,
            List<Integer> queueIds,
            Map<Integer, QueuePolicy> policies,
            Duration idlePollInterval) {
        return new TaskProcessorService.Builder()
                .group(new ThreadGroup("capacity-test"))
                .threadName("capacity-test")
                .settings(processorSettings)
                .running(running)
                .reservations(reservations)
                .executor(executor)
                .queueIds(queueIds)
                .knownTypeIds(Set.of(typeId, failingTypeId))
                .queuePolicies(policies)
                .claimBatchSize(claimBatchSize)
                .idlePollInterval(idlePollInterval)
                .lease(LEASE)
                .workerId(1)
                .create();
    }

    private static void processOnce(TaskProcessorService processor) throws Exception {
        processor.processOnce();
    }

    private record Claim(int reserved, int claimed) {}

    private static final class ManualExecutor implements BackgroundExecutor {
        private final Deque<Runnable> tasks = new ArrayDeque<>();
        private int available;
        boolean reject;
        final List<Claim> claims = new ArrayList<>();

        ManualExecutor(int slots) {
            this.available = slots;
        }

        @Override
        public synchronized int reserve(int max, Duration wait) {
            int given = Math.min(max, available);
            available -= given;
            return given;
        }

        @Override
        public synchronized void release(int slots) {
            available += slots;
        }

        @Override
        public synchronized void execute(Runnable task) {
            if (reject) {
                available++;
                throw new RejectedExecutionException("rejected by the test");
            }
            tasks.add(task);
        }

        synchronized void runNext() {
            var next = tasks.removeFirst();
            try {
                next.run();
            } finally {
                available++;
            }
        }

        synchronized int available() {
            return available;
        }

        @Override
        public void claimed(int reserved, int claimed) {
            claims.add(new Claim(reserved, claimed));
        }

        int pending() {
            return tasks.size();
        }

        @Override
        public int activeCount() {
            return tasks.size();
        }

        @Override
        public void shutdown() {}

        @Override
        public boolean awaitTermination(Duration timeout) {
            return tasks.isEmpty();
        }
    }
}

package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * One maintenance cycle or purge at a time, run on the test thread against an in-memory store and a manual
 * clock, so that expiry, retention and the events they emit are exact.
 */
@Timeout(10)
class MaintenanceServiceTest {
    private static final TaskType<Void> TYPE = TaskType.of("maintenance-test");
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(30);

    private final MutableClock clock = new MutableClock();
    private final InMemoryStore store = new InMemoryStore(clock);
    private final Names names = new Names(store);
    private final TaskClient client =
            TaskClient.builder().store(store).clock(clock).build();
    private final RunnerStatus status = new RunnerStatus(clock);
    private final List<TaskEvent> events = new ArrayList<>();
    private final int queueId = store.queueId("Q:DEF");
    private FullStore serviceStore = store;
    private RetryPolicies retryPolicies = RetryPolicies.of(RetryPolicy.fixed(RETRY_DELAY));
    private Map<TaskState, Duration> retention = Map.of();
    private boolean keepDead = true;
    private int batchSize = 100;
    private int purgeBatchSize = 500;

    // ---- expired leases -------------------------------------------------------------------------------

    @Test
    void anExpiredAttemptWithAttemptsLeftIsRetriedAfterThePolicyDelay() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.getTask(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(task.due()).isEqualTo(clock.instant().plus(RETRY_DELAY));
        assertThat(task.failures()).singleElement().satisfies(f -> {
            assertThat(f.type()).isEqualTo("lease-expired");
            assertThat(f.attempt()).isEqualTo(1);
            assertThat(f.message()).isEqualTo("lease expired after attempt 1");
        });
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(id, 1, 3, false));
    }

    @Test
    void anAttemptThatIsStillWithinItsLeaseIsLeftAlone() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.minusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.LEASED);
        assertThat(events).isEmpty();
    }

    @Test
    void anExpiredLastAttemptDiesWithoutAskingThePolicyAndIsKept() throws Exception {
        var asked = new AtomicInteger();
        retryPolicies = RetryPolicies.of((task, error) -> {
            asked.incrementAndGet();
            return RetryPolicy.Decision.retryAfter(Duration.ZERO);
        });
        long id = leased(request().maxAttempts(1).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.getTask(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.DEAD);
        assertThat(task.failures())
                .singleElement()
                .satisfies(f -> assertThat(f.type()).isEqualTo("lease-expired"));
        assertThat(asked).hasValue(0);
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(id, 1, 1, true));
    }

    @Test
    void anExpiredLastAttemptIsRemovedWhenDeadTasksAreNotKept() throws Exception {
        keepDead = false;
        long id = leased(request().maxAttempts(1).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(store.getTask(id)).isEmpty();
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(id, 1, 1, true));
    }

    @Test
    void theTasksNamedRetryPolicyDecidesAndSeesNoError() throws Exception {
        var errors = new ArrayList<Throwable>();
        var views = new ArrayList<TaskView>();
        retryPolicies = new RetryPolicies(RetryPolicy.fixed(RETRY_DELAY), Map.of("slow", (task, error) -> {
            views.add(task);
            errors.add(error);
            return RetryPolicy.Decision.retryAfter(Duration.ofHours(1));
        }));
        long id = leased(request().maxAttempts(3).retryPolicy("slow").build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(store.getTask(id).orElseThrow().due())
                .isEqualTo(clock.instant().plus(Duration.ofHours(1)));
        assertThat(errors).containsExactly((Throwable) null);
        assertThat(views).singleElement().satisfies(v -> assertThat(v.id()).isEqualTo(id));
    }

    @Test
    void aNamedPolicyThatSaysDeadEndsTheTaskThoughAttemptsAreLeft() throws Exception {
        retryPolicies = new RetryPolicies(
                RetryPolicy.fixed(RETRY_DELAY), Map.of("give-up", (task, error) -> RetryPolicy.Decision.dead("no")));
        long id = leased(request().maxAttempts(3).retryPolicy("give-up").build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.DEAD);
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(id, 1, 3, true));
    }

    @Test
    void anUnknownPolicyNameFallsBackToTheDefaultPolicy() throws Exception {
        long id = leased(request().maxAttempts(3).retryPolicy("missing").build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(store.getTask(id).orElseThrow().due())
                .isEqualTo(clock.instant().plus(RETRY_DELAY));
    }

    @Test
    void aNullDecisionRetriesAtOnce() throws Exception {
        retryPolicies = RetryPolicies.of((task, error) -> null);
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.getTask(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(task.due()).isEqualTo(clock.instant());
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(id, 1, 3, false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"runtime", "assertion", "fatal"})
    void aPolicyThatThrowsRetriesAtOnceAndRaisesAnIssue(String kind) throws Exception {
        retryPolicies = RetryPolicies.of((task, error) -> {
            switch (kind) {
                case "runtime" -> throw new IllegalStateException("policy broke");
                case "assertion" -> throw new AssertionError("policy broke");
                default -> throw new OutOfMemoryError("policy broke");
            }
        });
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.getTask(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(task.due()).isEqualTo(clock.instant());
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo("retry-policy");
            assertThat(i.taskId()).isEqualTo(id);
        });
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(id, 1, 3, false));
    }

    @Test
    void anExpiredAttemptWithACancelRequestEndsCancelledAndIsNotRetried() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        store.cancel(id, store.getTask(id).orElseThrow().revision(), "operator stop", null);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.getTask(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(task.cancelReason()).isEqualTo("operator stop");
        assertThat(task.attempts()).isEqualTo(1);
        assertThat(task.failures()).isNull();
        assertThat(events)
                .singleElement()
                .satisfies(e -> assertThat(e).isInstanceOfSatisfying(TaskEvent.Cancelled.class, c -> {
                    assertThat(c.reason()).isEqualTo("operator stop");
                    assertThat(c.outcome()).isEqualTo(TaskEvent.Outcome.APPLIED);
                    assertThat(c.task().id()).isEqualTo(id);
                }));
    }

    @Test
    void aCancelRequestWithoutAReasonEndsWithTheDefaultReason() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        store.cancel(id, store.getTask(id).orElseThrow().revision(), null, null);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(events)
                .singleElement()
                .satisfies(e -> assertThat(((TaskEvent.Cancelled) e).reason()).isEqualTo("cancel requested"));
    }

    @Test
    void aFixedDelayOccurrenceThatDiesOnExpiryPausesItsScheduleAndRaisesAnIssue() throws Exception {
        long id = leasedOccurrence(1);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.DEAD);
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isTrue();
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo("schedule-paused");
            assertThat(i.taskId()).isEqualTo(id);
            assertThat(i.detail()).isEqualTo("schedule sched paused: occurrence ended DEAD");
        });
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(id, 1, 1, true));
    }

    @Test
    void aFixedDelayOccurrenceThatIsRetriedOnExpiryKeepsItsScheduleRunning() throws Exception {
        long id = leasedOccurrence(3);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.SCHEDULED);
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isFalse();
        assertThat(status.issues()).isEmpty();
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(id, 1, 3, false));
    }

    @Test
    void aCancelledFixedDelayOccurrencePausesItsScheduleAndRaisesAnIssue() throws Exception {
        long id = leasedOccurrence(3);
        store.cancel(id, store.getTask(id).orElseThrow().revision(), "stop", null);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.CANCELLED);
        assertThat(store.recurringDefinitions().getFirst().definition().paused())
                .isTrue();
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo("schedule-paused");
            assertThat(i.detail()).isEqualTo("schedule sched paused: occurrence ended CANCELLED");
        });
    }

    @Test
    void aStaleSweeperTransitionIsRejectedAndEmitsNoLeaseExpired() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));
        serviceStore = takenOverAfterReclaim();

        cycle();

        var task = store.getTask(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.QUEUED);
        assertThat(task.failures()).isNull();
        assertThat(events).isEmpty();
    }

    @Test
    void aStaleSweeperCancellationIsRejectedAndEmitsNoCancelled() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        store.cancel(id, store.getTask(id).orElseThrow().revision(), "stop", null);
        clock.advance(LEASE.plusSeconds(1));
        serviceStore = takenOverAfterReclaim();

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.QUEUED);
        assertThat(events).isEmpty();
    }

    @Test
    void aFailureHandlingOneExpiredTaskDoesNotStopTheOthers() throws Exception {
        long first = leased(request().maxAttempts(3).build());
        long second = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("failExpired") && (long) args[0] == first) throw new TaskStoreException("store down");
            return real.proceed();
        });

        cycle();

        assertThat(state(first)).isEqualTo(TaskState.LEASED);
        assertThat(state(second)).isEqualTo(TaskState.SCHEDULED);
        assertThat(events).containsExactly(new TaskEvent.LeaseExpired(second, 1, 3, false));
    }

    @Test
    void moreExpiredLeasesThanOneBatchAreAllHandled() throws Exception {
        batchSize = 2;
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) ids.add(leased(request().maxAttempts(3).build()));
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(ids).allSatisfy(id -> assertThat(state(id)).isEqualTo(TaskState.SCHEDULED));
        assertThat(events).hasSize(5).allMatch(e -> e instanceof TaskEvent.LeaseExpired);
    }

    @Test
    void tasksThatMissedTheirStartByAreExpiredInBatchesWithAnEventEach() throws Exception {
        batchSize = 2;
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 3; i++) {
            ids.add(client.enqueue(
                            request().startBy(clock.instant().plusSeconds(10)).build())
                    .id());
        }
        clock.advance(Duration.ofSeconds(11));

        cycle();

        assertThat(ids).allSatisfy(id -> assertThat(state(id)).isEqualTo(TaskState.EXPIRED));
        assertThat(events)
                .containsExactlyInAnyOrderElementsOf(
                        ids.stream().map(TaskEvent.Expired::new).toList());
    }

    @Test
    void aFailedReclaimStillExpiresMissedStartsAndTheNextCycleRecoversTheLease() throws Exception {
        long leasedId = leased(request().maxAttempts(3).build());
        long late = client.enqueue(
                        request().startBy(clock.instant().plusSeconds(10)).build())
                .id();
        clock.advance(LEASE.plusSeconds(1));
        var failing = new AtomicInteger(1);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("reclaim") && failing.getAndDecrement() > 0) throw new TaskStoreException("store down");
            return real.proceed();
        });

        cycle();

        assertThat(state(leasedId)).isEqualTo(TaskState.LEASED);
        assertThat(state(late)).isEqualTo(TaskState.EXPIRED);
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("store-error"));

        cycle();

        assertThat(state(leasedId)).isEqualTo(TaskState.SCHEDULED);
        assertThat(events).contains(new TaskEvent.LeaseExpired(leasedId, 1, 3, false));
    }

    // ---- purge ----------------------------------------------------------------------------------------

    @Test
    void aFinishedTaskIsPurgedOnlyOnceItIsOlderThanTheRetention() throws Exception {
        retention = Map.of(TaskState.SUCCEEDED, Duration.ofHours(1));
        long old = succeeded(request().build());
        clock.advance(Duration.ofMillis(1));
        long boundary = succeeded(request().build());
        clock.advance(Duration.ofHours(1));

        purge();

        assertThat(store.getTask(old)).isEmpty();
        assertThat(store.getTask(boundary)).isPresent();
        assertThat(events).containsExactly(new TaskEvent.Purged(TaskState.SUCCEEDED, 1));
    }

    @Test
    void eachStateIsPurgedByItsOwnRetentionAndStatesWithoutOneAreKept() throws Exception {
        retention = Map.of(
                TaskState.SUCCEEDED, Duration.ofHours(1),
                TaskState.DEAD, Duration.ofDays(1),
                TaskState.CANCELLED, Duration.ofMinutes(1));
        long succeeded = succeeded(request().build());
        long dead = dead(request().maxAttempts(1).build());
        long cancelled = cancelled(request().build());
        long expired = expired();
        clock.advance(Duration.ofHours(2));

        purge();

        assertThat(store.getTask(succeeded)).isEmpty();
        assertThat(store.getTask(cancelled)).isEmpty();
        assertThat(store.getTask(dead)).isPresent();
        assertThat(store.getTask(expired)).isPresent();
        assertThat(events)
                .containsExactlyInAnyOrder(
                        new TaskEvent.Purged(TaskState.SUCCEEDED, 1), new TaskEvent.Purged(TaskState.CANCELLED, 1));

        events.clear();
        clock.advance(Duration.ofDays(1));

        purge();

        assertThat(store.getTask(dead)).isEmpty();
        assertThat(store.getTask(expired)).isPresent();
        assertThat(events).containsExactly(new TaskEvent.Purged(TaskState.DEAD, 1));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 5})
    void aPurgeLargerThanOneBatchDrainsEverythingAndEmitsOneAggregatedEvent(int count) throws Exception {
        purgeBatchSize = 2;
        retention = Map.of(TaskState.SUCCEEDED, Duration.ofMinutes(1));
        var ids = new ArrayList<Long>();
        for (int i = 0; i < count; i++) ids.add(succeeded(request().build()));
        clock.advance(Duration.ofMinutes(2));
        long young = succeeded(request().build());

        purge();

        assertThat(ids).allSatisfy(id -> assertThat(store.getTask(id)).isEmpty());
        assertThat(store.getTask(young)).isPresent();
        assertThat(events).containsExactly(new TaskEvent.Purged(TaskState.SUCCEEDED, count));
    }

    @Test
    void aTaskHoldingItsUniqueKeyIsNotPurgedBeforeTheKeyWindowEnds() throws Exception {
        retention = Map.of(TaskState.SUCCEEDED, Duration.ofMinutes(1));
        long id = succeeded(request()
                .uniqueKey("k", Uniqueness.keepFor(Duration.ofHours(1)))
                .build());
        clock.advance(Duration.ofMinutes(2));

        purge();

        assertThat(store.getTask(id)).isPresent();
        assertThat(events).isEmpty();

        clock.advance(Duration.ofHours(1));
        purge();

        assertThat(store.getTask(id)).isEmpty();
        assertThat(events).containsExactly(new TaskEvent.Purged(TaskState.SUCCEEDED, 1));
    }

    @Test
    void aUniqueKeyIsReleasedWhenItsWindowPassedEvenWithoutRetention() throws Exception {
        long id = succeeded(request()
                .uniqueKey("k", Uniqueness.keepFor(Duration.ofHours(1)))
                .build());
        assertThat(client.enqueue(request().uniqueKey("k").build()).accepted()).isFalse();
        clock.advance(Duration.ofMinutes(30));

        purge();

        assertThat(store.getTask(id).orElseThrow().uniqueKey()).isEqualTo("k");

        clock.advance(Duration.ofMinutes(31));
        purge();

        assertThat(store.getTask(id).orElseThrow().uniqueKey()).isNull();
        assertThat(events).isEmpty();
    }

    @Test
    void moreKeysThanOneBatchAreAllReleased() throws Exception {
        purgeBatchSize = 2;
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) {
            ids.add(succeeded(request()
                    .uniqueKey("k" + i, Uniqueness.keepFor(Duration.ofHours(1)))
                    .build()));
        }
        clock.advance(Duration.ofHours(1).plusSeconds(1));

        purge();

        assertThat(ids)
                .allSatisfy(id ->
                        assertThat(store.getTask(id).orElseThrow().uniqueKey()).isNull());
    }

    @Test
    void aPurgeThatFailedIsRepeatedByTheNextCycle() throws Exception {
        retention = Map.of(TaskState.SUCCEEDED, Duration.ofMinutes(1));
        long id = succeeded(request().build());
        clock.advance(Duration.ofMinutes(2));
        var failing = new AtomicInteger(1);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("purge") && failing.getAndDecrement() > 0) throw new TaskStoreException("store down");
            return real.proceed();
        });

        purge();

        assertThat(store.getTask(id)).isPresent();
        assertThat(events).isEmpty();
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("store-error"));

        purge();

        assertThat(store.getTask(id)).isEmpty();
        assertThat(events).containsExactly(new TaskEvent.Purged(TaskState.SUCCEEDED, 1));
    }

    @Test
    void aFailedKeyReleaseIsRepeatedByTheNextCycle() throws Exception {
        long id = succeeded(request()
                .uniqueKey("k", Uniqueness.keepFor(Duration.ofHours(1)))
                .build());
        clock.advance(Duration.ofHours(1).plusSeconds(1));
        var failing = new AtomicInteger(1);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("releaseKeys") && failing.getAndDecrement() > 0) {
                throw new TaskStoreException("store down");
            }
            return real.proceed();
        });

        purge();

        assertThat(store.getTask(id).orElseThrow().uniqueKey()).isEqualTo("k");
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("store-error"));

        purge();

        assertThat(store.getTask(id).orElseThrow().uniqueKey()).isNull();
    }

    // ---- fixtures -------------------------------------------------------------------------------------

    private TaskRequest.Builder<Void> request() {
        return TaskRequest.of(TYPE);
    }

    private long leased(TaskRequest<Void> request) {
        long id = client.enqueue(request).id();
        var claimed = store.claim(new ClaimRequest(List.of(queueId), 1, LEASE, 1, null));
        assertThat(claimed).singleElement().satisfies(r -> assertThat(r.id()).isEqualTo(id));
        return id;
    }

    private long leasedOccurrence(int maxAttempts) {
        long id = ScheduleFixtures.fixedDelayOccurrence(
                store, client, clock.instant(), store.typeId(TYPE.name()), queueId, maxAttempts);
        var claimed = store.claim(new ClaimRequest(List.of(queueId), 1, LEASE, 1, null));
        assertThat(claimed).singleElement().satisfies(r -> assertThat(r.id()).isEqualTo(id));
        return id;
    }

    private long succeeded(TaskRequest<Void> request) {
        long id = leased(request);
        assertThat(store.ack(id, store.getTask(id).orElseThrow().revision(), true, true, null))
                .isTrue();
        return id;
    }

    private long dead(TaskRequest<Void> request) {
        long id = leased(request);
        var failure = new FailureInfo(1, clock.instant(), "test", "failed", null);
        store.fail(id, store.getTask(id).orElseThrow().revision(), failure, null, true, null);
        assertThat(state(id)).isEqualTo(TaskState.DEAD);
        return id;
    }

    private long cancelled(TaskRequest<Void> request) {
        long id = client.enqueue(request).id();
        store.cancel(id, store.getTask(id).orElseThrow().revision(), "stop", null);
        assertThat(state(id)).isEqualTo(TaskState.CANCELLED);
        return id;
    }

    private long expired() {
        long id = client.enqueue(
                        request().startBy(clock.instant().plusSeconds(1)).build())
                .id();
        clock.advance(Duration.ofSeconds(2));
        assertThat(store.expire(10)).containsExactly(id);
        return id;
    }

    private TaskState state(long id) {
        return store.getTask(id).orElseThrow().state();
    }

    /** Another worker takes the tasks back after the sweeper listed them, before it can transition them. */
    private FullStore takenOverAfterReclaim() {
        return StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("reclaim")) {
                for (var e : (List<ExpiredLease>) result) store.unclaim(e.id(), e.revision());
            }
            return result;
        });
    }

    private void cycle() throws Exception {
        service().cycle();
    }

    private void purge() throws Exception {
        service().purge();
    }

    private MaintenanceService service() {
        return new MaintenanceService.Builder()
                .group(new ThreadGroup("maintenance-test"))
                .threadName("maintenance-test")
                .store(serviceStore)
                .names(names)
                .retryPolicies(retryPolicies)
                .observer(new Observers(events::add))
                .clock(clock)
                .status(status)
                .retention(retention)
                .keepDead(keepDead)
                .batchSize(batchSize)
                .purgeBatchSize(purgeBatchSize)
                .create();
    }
}

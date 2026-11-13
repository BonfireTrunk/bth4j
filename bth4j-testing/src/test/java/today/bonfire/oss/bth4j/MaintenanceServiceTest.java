package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.ExpiredLease;
import today.bonfire.oss.bth4j.spi.ExpiredTask;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.QueueGrant;
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
    private final InMemoryStore store = InMemoryStore.builder().clock(clock).build();
    private final Names names = new Names(store);
    private final TaskClient client = TaskClient.builder()
            .queues(Queues.of("Q:DEF"))
            .store(store)
            .clock(clock)
            .build();
    private final RunnerStatus status = new RunnerStatus(clock);
    private final List<TaskEvent> events = new ArrayList<>();
    private final int queueId = store.queueId("Q:DEF");
    private FullStore serviceStore = store;
    private RetryPolicies retryPolicies = RetryPolicies.of(RetryPolicy.fixed(RETRY_DELAY));
    private Map<TaskState, Duration> retention = Map.of();
    private boolean keepDead = true;
    private int batchSize = 100;
    private int purgeBatchSize = 500;
    private int runnerId;
    private Set<Integer> runnerQueueIds = Set.of();
    private Set<Integer> runnerTypeIds = Set.of();
    private int runningTasks;
    private boolean accepting = true;
    private Duration runnerStaleTimeout = Duration.ofMinutes(10);
    private Duration checkInterval = Duration.ofSeconds(5);
    private final AtomicLong nanos = new AtomicLong();

    private TaskEvent.LeaseExpired leaseExpired(long id, int attempts, int maxAttempts, boolean dead) {
        return new TaskEvent.LeaseExpired(id, "Q:DEF", TYPE.name(), attempts, maxAttempts, dead);
    }

    private TaskEvent.Expired expired(long id) {
        return new TaskEvent.Expired(id, "Q:DEF", TYPE.name());
    }

    // ---- expired leases -------------------------------------------------------------------------------

    @Test
    void anExpiredAttemptWithAttemptsLeftIsRetriedAfterThePolicyDelay() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.task(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(task.dueAt()).isEqualTo(clock.instant().plus(RETRY_DELAY));
        assertThat(task.failures()).singleElement().satisfies(f -> {
            assertThat(f.type()).isEqualTo("lease-expired");
            assertThat(f.attempt()).isEqualTo(1);
            assertThat(f.message()).isEqualTo("lease expired after attempt 1");
        });
        assertThat(events).containsExactly(leaseExpired(id, 1, 3, false));
    }

    @Test
    void anExpiredTaskWhoseRecordCannotBeReadEndsDeadWithAttemptsLeftAndIsAnIssue() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        serviceStore = unreadable(id);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.task(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.DEAD);
        assertThat(task.failures()).singleElement().satisfies(f -> {
            assertThat(f.type()).isEqualTo("unreadable-record");
            assertThat(f.attempt()).isEqualTo(1);
            assertThat(f.message()).isEqualTo("task " + id + " has a stored value that cannot be read");
        });
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.CORRUPT_TASK);
            assertThat(i.taskId()).isEqualTo(id);
        });
        assertThat(events).containsExactly(leaseExpired(id, 1, 3, true));
    }

    @Test
    void anUnreadableTaskWithACancelRequestEndsDeadToo() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        store.cancel(id, store.task(id).orElseThrow().revision(), "stop", null);
        serviceStore = unreadable(id);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.DEAD);
        assertThat(status.issues()).extracting(Issue::kind).containsExactly(Issue.Kind.CORRUPT_TASK);
    }

    @Test
    void aFullBatchOfExpiredLeasesThatAllFailIsNotReadAgainInTheSameCycle() throws Exception {
        batchSize = 2;
        leased(request().build());
        leased(request().build());
        var reclaims = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("reclaim")) reclaims.incrementAndGet();
            if (method.equals("failExpired")) throw new TaskStoreException("rejected");
            return real.proceed();
        });
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(reclaims).hasValue(1);
    }

    @Test
    void aCycleThatThrowsAnErrorRunsAgainAtItsNextInterval() throws Exception {
        var reclaims = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("reclaim") && reclaims.incrementAndGet() == 1) throw new AssertionError("first cycle");
            return real.proceed();
        });
        checkInterval = Duration.ofMillis(10);
        var service = service();

        service.start();
        try {
            var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (reclaims.get() < 3 && System.nanoTime() < deadline) Thread.sleep(5);
        } finally {
            service.stopThread();
            service.waitTillDone(Duration.ofSeconds(5));
        }

        assertThat(reclaims.get()).isGreaterThanOrEqualTo(3);
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

        var task = store.task(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.DEAD);
        assertThat(task.failures())
                .singleElement()
                .satisfies(f -> assertThat(f.type()).isEqualTo("lease-expired"));
        assertThat(asked).hasValue(0);
        assertThat(events).containsExactly(leaseExpired(id, 1, 1, true));
    }

    @Test
    void anExpiredLastAttemptIsRemovedWhenDeadTasksAreNotKept() throws Exception {
        keepDead = false;
        long id = leased(request().maxAttempts(1).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(store.task(id)).isEmpty();
        assertThat(events).containsExactly(leaseExpired(id, 1, 1, true));
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

        assertThat(store.task(id).orElseThrow().dueAt())
                .isEqualTo(clock.instant().plus(Duration.ofHours(1)));
        assertThat(errors).containsExactly((Throwable) null);
        assertThat(views).singleElement().satisfies(v -> assertThat(v.id()).isEqualTo(id));
    }

    @Test
    void aRetryDelayLongerThanOneHundredYearsRetriesInOneHundredYears() throws Exception {
        retryPolicies = RetryPolicies.of(RetryPolicy.fixed(ChronoUnit.FOREVER.getDuration()));
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.task(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(task.dueAt()).isEqualTo(clock.instant().plus(TaskRequest.MAX_TIME_FROM_NOW));
    }

    @Test
    void aNamedPolicyThatSaysDeadEndsTheTaskThoughAttemptsAreLeft() throws Exception {
        retryPolicies = new RetryPolicies(
                RetryPolicy.fixed(RETRY_DELAY), Map.of("give-up", (task, error) -> RetryPolicy.Decision.dead("no")));
        long id = leased(request().maxAttempts(3).retryPolicy("give-up").build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.DEAD);
        assertThat(events).containsExactly(leaseExpired(id, 1, 3, true));
    }

    @Test
    void anUnknownPolicyNameFallsBackToTheDefaultPolicyAndRaisesAnIssue() throws Exception {
        long id = leased(request().maxAttempts(3).retryPolicy("missing").build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(store.task(id).orElseThrow().dueAt())
                .isEqualTo(clock.instant().plus(RETRY_DELAY));
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.RETRY_POLICY);
            assertThat(i.detail()).isEqualTo("unknown retry policy missing");
            assertThat(i.taskId()).isEqualTo(id);
        });
    }

    @Test
    void aNullDecisionRetriesAtOnce() throws Exception {
        retryPolicies = RetryPolicies.of((task, error) -> null);
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.task(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(task.dueAt()).isEqualTo(clock.instant());
        assertThat(events).containsExactly(leaseExpired(id, 1, 3, false));
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

        var task = store.task(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(task.dueAt()).isEqualTo(clock.instant());
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.RETRY_POLICY);
            assertThat(i.taskId()).isEqualTo(id);
        });
        assertThat(events).containsExactly(leaseExpired(id, 1, 3, false));
    }

    @Test
    void anExpiredAttemptWithACancelRequestEndsCancelledAndIsNotRetried() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        store.cancel(id, store.task(id).orElseThrow().revision(), "operator stop", null);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        var task = store.task(id).orElseThrow();
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
        store.cancel(id, store.task(id).orElseThrow().revision(), null, null);
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
        assertThat(store.schedules().getFirst().definition().paused()).isTrue();
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_PAUSED);
            assertThat(i.taskId()).isEqualTo(id);
            assertThat(i.detail()).isEqualTo("schedule sched paused: occurrence ended DEAD");
        });
        assertThat(events).containsExactly(leaseExpired(id, 1, 1, true));
    }

    @Test
    void aFixedDelayOccurrenceThatIsRetriedOnExpiryKeepsItsScheduleRunning() throws Exception {
        long id = leasedOccurrence(3);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.SCHEDULED);
        assertThat(store.schedules().getFirst().definition().paused()).isFalse();
        assertThat(status.issues()).isEmpty();
        assertThat(events).containsExactly(leaseExpired(id, 1, 3, false));
    }

    @Test
    void aCancelledFixedDelayOccurrencePausesItsScheduleAndRaisesAnIssue() throws Exception {
        long id = leasedOccurrence(3);
        store.cancel(id, store.task(id).orElseThrow().revision(), "stop", null);
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(state(id)).isEqualTo(TaskState.CANCELLED);
        assertThat(store.schedules().getFirst().definition().paused()).isTrue();
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.SCHEDULE_PAUSED);
            assertThat(i.detail()).isEqualTo("schedule sched paused: occurrence ended CANCELLED");
        });
    }

    @Test
    void aStaleSweeperTransitionIsRejectedAndEmitsNoLeaseExpired() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        clock.advance(LEASE.plusSeconds(1));
        serviceStore = takenOverAfterReclaim();

        cycle();

        var task = store.task(id).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.QUEUED);
        assertThat(task.failures()).isNull();
        assertThat(events).isEmpty();
    }

    @Test
    void aStaleSweeperCancellationIsRejectedAndEmitsNoCancelled() throws Exception {
        long id = leased(request().maxAttempts(3).build());
        store.cancel(id, store.task(id).orElseThrow().revision(), "stop", null);
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
        assertThat(events).containsExactly(leaseExpired(second, 1, 3, false));
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
    void aQueueOrTypeThatCannotBeNamedIsReportedAsUnknownOnAnExpiredTaskAndOnAnExpiredLease() throws Exception {
        long leased = leased(request().maxAttempts(3).build());
        long late = client.enqueue(
                        request().startBy(clock.instant().plusSeconds(1)).build())
                .id();
        serviceStore = withoutQueueAndType();
        clock.advance(LEASE.plusSeconds(1));

        cycle();

        assertThat(events)
                .containsExactlyInAnyOrder(
                        new TaskEvent.LeaseExpired(leased, "unknown", "unknown", 1, 3, false),
                        new TaskEvent.Expired(late, "unknown", "unknown"));
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
                        ids.stream().map(this::expired).toList());
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
        assertThat(status.issues()).anyMatch(i -> i.kind() == Issue.Kind.STORE_ERROR);

        cycle();

        assertThat(state(leasedId)).isEqualTo(TaskState.SCHEDULED);
        assertThat(events).contains(leaseExpired(leasedId, 1, 3, false));
    }

    // ---- purge ----------------------------------------------------------------------------------------

    @Test
    void aFinishedTaskIsPurgedOnceItIsAsOldAsTheRetention() throws Exception {
        retention = Map.of(TaskState.SUCCEEDED, Duration.ofHours(1));
        long boundary = succeeded(request().build());
        clock.advance(Duration.ofMillis(1));
        long younger = succeeded(request().build());
        clock.advance(Duration.ofHours(1).minusMillis(1));

        purge();

        assertThat(store.task(boundary)).isEmpty();
        assertThat(store.task(younger)).isPresent();
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

        assertThat(store.task(succeeded)).isEmpty();
        assertThat(store.task(cancelled)).isEmpty();
        assertThat(store.task(dead)).isPresent();
        assertThat(store.task(expired)).isPresent();
        assertThat(events)
                .containsExactlyInAnyOrder(
                        new TaskEvent.Purged(TaskState.SUCCEEDED, 1), new TaskEvent.Purged(TaskState.CANCELLED, 1));

        events.clear();
        clock.advance(Duration.ofDays(1));

        purge();

        assertThat(store.task(dead)).isEmpty();
        assertThat(store.task(expired)).isPresent();
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

        assertThat(ids).allSatisfy(id -> assertThat(store.task(id)).isEmpty());
        assertThat(store.task(young)).isPresent();
        assertThat(events).containsExactly(new TaskEvent.Purged(TaskState.SUCCEEDED, count));
    }

    @Test
    void aTaskPastRetentionIsPurgedWhileItsKeyStaysTakenUntilTheWindowEnds() throws Exception {
        retention = Map.of(TaskState.SUCCEEDED, Duration.ofMinutes(1));
        long id = succeeded(request()
                .uniqueKey("k", Uniqueness.keepAfterDone(Duration.ofHours(1)))
                .build());
        clock.advance(Duration.ofMinutes(2));

        purge();

        assertThat(store.task(id)).isEmpty();
        assertThat(events).containsExactly(new TaskEvent.Purged(TaskState.SUCCEEDED, 1));
        assertThat(client.enqueue(request().uniqueKey("k").build()).isAccepted())
                .isFalse();

        clock.advance(Duration.ofHours(1));
        purge();

        assertThat(client.enqueue(request().uniqueKey("k").build()).isAccepted())
                .isTrue();
    }

    @Test
    void aUniqueKeyIsReleasedWhenItsWindowPassedEvenWithoutRetention() throws Exception {
        long id = succeeded(request()
                .uniqueKey("k", Uniqueness.keepAfterDone(Duration.ofHours(1)))
                .build());
        assertThat(client.enqueue(request().uniqueKey("k").build()).isAccepted())
                .isFalse();
        clock.advance(Duration.ofMinutes(30));

        purge();

        assertThat(store.task(id).orElseThrow().uniqueKey()).isEqualTo("k");

        clock.advance(Duration.ofMinutes(31));
        purge();

        assertThat(store.task(id).orElseThrow().uniqueKey()).isNull();
        assertThat(events).isEmpty();
    }

    @Test
    void moreKeysThanOneBatchAreAllReleased() throws Exception {
        purgeBatchSize = 2;
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) {
            ids.add(succeeded(request()
                    .uniqueKey("k" + i, Uniqueness.keepAfterDone(Duration.ofHours(1)))
                    .build()));
        }
        clock.advance(Duration.ofHours(1).plusSeconds(1));

        purge();

        assertThat(ids)
                .allSatisfy(id ->
                        assertThat(store.task(id).orElseThrow().uniqueKey()).isNull());
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

        assertThat(store.task(id)).isPresent();
        assertThat(events).isEmpty();
        assertThat(status.issues()).anyMatch(i -> i.kind() == Issue.Kind.STORE_ERROR);

        purge();

        assertThat(store.task(id)).isEmpty();
        assertThat(events).containsExactly(new TaskEvent.Purged(TaskState.SUCCEEDED, 1));
    }

    @Test
    void aFailedKeyReleaseIsRepeatedByTheNextCycle() throws Exception {
        long id = succeeded(request()
                .uniqueKey("k", Uniqueness.keepAfterDone(Duration.ofHours(1)))
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

        assertThat(store.task(id).orElseThrow().uniqueKey()).isEqualTo("k");
        assertThat(status.issues()).anyMatch(i -> i.kind() == Issue.Kind.STORE_ERROR);

        purge();

        assertThat(store.task(id).orElseThrow().uniqueKey()).isNull();
    }

    // ---- runner heartbeat -----------------------------------------------------------------------------

    @Test
    void theHeartbeatIsRecordedBeforeTheExpiredLeasesAreWorkedOff() throws Exception {
        runnerId = store.allocateRunnerId("runner-a", Set.of(), Set.of());
        var order = new ArrayList<String>();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("heartbeat") || method.equals("reclaim") || method.equals("expire")) order.add(method);
            return real.proceed();
        });

        cycle();

        assertThat(order).containsExactly("heartbeat", "reclaim", "expire");
    }

    @Test
    void aServiceStoppedBeforeItsThreadRanSchedulesNothing() {
        var service = service();
        service.stopThread();

        service.run();

        assertThat(service.isRunning()).isFalse();
    }

    @Test
    void aCycleRecordsTheRunnersHeartbeatWithItsRunningCount() throws Exception {
        runnerId = store.allocateRunnerId("runner-a", Set.of(), Set.of());
        var startedAt = clock.instant();
        runningTasks = 3;
        clock.advance(Duration.ofMinutes(5));

        cycle();

        assertThat(store.runners()).singleElement().satisfies(w -> {
            assertThat(w.id()).isEqualTo(runnerId);
            assertThat(w.label()).isEqualTo("runner-a");
            assertThat(w.startedAt()).isEqualTo(startedAt);
            assertThat(w.lastSeenAt()).isEqualTo(clock.instant());
            assertThat(w.running()).isEqualTo(3);
        });
        assertThat(status.lastMaintenanceAt()).isEqualTo(clock.instant());
        assertThat(status.issues()).isEmpty();
    }

    @Test
    void eachCycleReportsTheRunningCountAtThatMoment() throws Exception {
        runnerId = store.allocateRunnerId("runner-a", Set.of(), Set.of());
        runningTasks = 3;
        cycle();
        clock.advance(Duration.ofSeconds(5));
        runningTasks = 1;

        cycle();

        assertThat(store.runners()).singleElement().satisfies(w -> {
            assertThat(w.lastSeenAt()).isEqualTo(clock.instant());
            assertThat(w.running()).isEqualTo(1);
        });
    }

    @Test
    void eachCycleReportsWhetherTheRunnerAcceptsWork() throws Exception {
        runnerId = store.allocateRunnerId("runner-a", Set.of(), Set.of());
        accepting = false;
        cycle();
        assertThat(store.runners().getFirst().accepting()).isFalse();
        accepting = true;
        cycle();
        assertThat(store.runners().getFirst().accepting()).isTrue();
    }

    @Test
    void aServiceWithoutARunnerRecordsNoHeartbeatButStillMarksMaintenance() throws Exception {
        store.allocateRunnerId("other", Set.of(), Set.of());
        var before = store.runners();
        clock.advance(Duration.ofMinutes(5));

        cycle();

        assertThat(store.runners()).isEqualTo(before);
        assertThat(status.lastMaintenanceAt()).isEqualTo(clock.instant());
    }

    @Test
    void aFailedHeartbeatIsRecordedWithoutMarkingMaintenanceAndTheNextCycleRecovers() throws Exception {
        runnerId = store.allocateRunnerId("runner-a", Set.of(), Set.of());
        runningTasks = 2;
        clock.advance(Duration.ofSeconds(5));
        var failing = new AtomicInteger(1);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("heartbeat") && failing.getAndDecrement() > 0) throw new TaskStoreException("store down");
            return real.proceed();
        });

        cycle();

        assertThat(store.runners().getFirst().running()).isZero();
        assertThat(status.lastMaintenanceAt()).isNull();
        assertThat(status.lastStoreErrorAt()).isEqualTo(clock.instant());
        assertThat(status.lastStoreError()).isEqualTo(new TaskStoreException("store down").toString());
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.STORE_ERROR);
            assertThat(i.detail()).isEqualTo(new TaskStoreException("store down").toString());
        });

        clock.advance(Duration.ofSeconds(5));
        cycle();

        assertThat(store.runners().getFirst().running()).isEqualTo(2);
        assertThat(store.runners().getFirst().lastSeenAt()).isEqualTo(clock.instant());
        assertThat(status.lastMaintenanceAt()).isEqualTo(clock.instant());
        assertThat(status.issues()).hasSize(1);
    }

    @Test
    void aCycleRestoresTheRunnersRowAfterAnotherRunnerPurgedItUnderTheSameId() throws Exception {
        int typeId = store.typeId("work");
        runnerQueueIds = Set.of(queueId);
        runnerTypeIds = Set.of(typeId);
        runnerId = store.allocateRunnerId("runner-a", runnerQueueIds, runnerTypeIds);
        clock.advance(runnerStaleTimeout.plusSeconds(1));
        purge();
        assertThat(store.runners()).isEmpty();
        runningTasks = 2;

        cycle();

        assertThat(store.runners()).singleElement().satisfies(w -> {
            assertThat(w.id()).isEqualTo(runnerId);
            assertThat(w.label()).isEqualTo("runner-a");
            assertThat(w.queueIds()).containsExactly(queueId);
            assertThat(w.typeIds()).containsExactly(typeId);
            assertThat(w.startedAt()).isEqualTo(clock.instant());
            assertThat(w.lastSeenAt()).isEqualTo(clock.instant());
            assertThat(w.running()).isEqualTo(2);
            assertThat(w.accepting()).isTrue();
        });
    }

    // ---- stale runners --------------------------------------------------------------------------------

    @Test
    void aRunnerSilentForLongerThanTheThresholdIsRemovedAndAFreshOneIsKept() throws Exception {
        int stale = store.allocateRunnerId("stale", Set.of(), Set.of());
        clock.advance(Duration.ofMinutes(6));
        int fresh = store.allocateRunnerId("fresh", Set.of(), Set.of());
        clock.advance(Duration.ofMinutes(5));

        purge();

        assertThat(store.runners()).extracting(w -> w.id()).containsExactly(fresh);
        assertThat(store.runners()).extracting(w -> w.id()).doesNotContain(stale);
    }

    @Test
    void aRunnerExactlyAtTheThresholdIsKeptAndOneMillisecondOlderIsRemoved() throws Exception {
        int id = store.allocateRunnerId("edge", Set.of(), Set.of());
        clock.advance(runnerStaleTimeout);

        purge();

        assertThat(store.runners()).extracting(w -> w.id()).containsExactly(id);

        clock.advance(Duration.ofMillis(1));
        purge();

        assertThat(store.runners()).isEmpty();
    }

    @Test
    void aHeartbeatKeepsARunnerFromBeingPurged() throws Exception {
        runnerId = store.allocateRunnerId("beating", Set.of(), Set.of());
        clock.advance(Duration.ofMinutes(9));
        cycle();
        clock.advance(Duration.ofMinutes(9));

        purge();

        assertThat(store.runners()).extracting(w -> w.id()).containsExactly(runnerId);
    }

    @Test
    void theStaleThresholdIsTheConfiguredOne() throws Exception {
        runnerStaleTimeout = Duration.ofSeconds(30);
        store.allocateRunnerId("short", Set.of(), Set.of());
        clock.advance(Duration.ofSeconds(31));

        purge();

        assertThat(store.runners()).isEmpty();
    }

    @Test
    void aFailedRunnerPurgeIsRecordedAndTheNextPurgeRemovesTheStaleRunner() throws Exception {
        store.allocateRunnerId("stale", Set.of(), Set.of());
        clock.advance(runnerStaleTimeout.plusSeconds(1));
        var failing = new AtomicInteger(1);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("purgeRunners") && failing.getAndDecrement() > 0) {
                throw new TaskStoreException("store down");
            }
            return real.proceed();
        });

        purge();

        assertThat(store.runners()).hasSize(1);
        assertThat(status.issues()).singleElement().satisfies(i -> {
            assertThat(i.kind()).isEqualTo(Issue.Kind.STORE_ERROR);
            assertThat(i.detail()).isEqualTo(new TaskStoreException("store down").toString());
        });

        purge();

        assertThat(store.runners()).isEmpty();
        assertThat(status.issues()).hasSize(1);
    }

    // ---- fixtures -------------------------------------------------------------------------------------

    private TaskRequest.Builder<Void> request() {
        return TaskRequest.of(TYPE);
    }

    private long leased(TaskRequest<Void> request) {
        long id = client.enqueue(request).id();
        var claimed = store.claim(new ClaimRequest(List.of(new QueueGrant(queueId, 1, 1)), 1, LEASE, 1, null));
        assertThat(claimed).singleElement().satisfies(r -> assertThat(r.id()).isEqualTo(id));
        return id;
    }

    private long leasedOccurrence(int maxAttempts) {
        long id = ScheduleFixtures.fixedDelayOccurrence(
                store, client, clock.instant(), store.typeId(TYPE.name()), queueId, maxAttempts);
        var claimed = store.claim(new ClaimRequest(List.of(new QueueGrant(queueId, 1, 1)), 1, LEASE, 1, null));
        assertThat(claimed).singleElement().satisfies(r -> assertThat(r.id()).isEqualTo(id));
        return id;
    }

    private long succeeded(TaskRequest<Void> request) {
        long id = leased(request);
        assertThat(store.ack(id, store.task(id).orElseThrow().revision(), true, true, null))
                .isTrue();
        return id;
    }

    private long dead(TaskRequest<Void> request) {
        long id = leased(request);
        var failure = new FailureInfo(1, clock.instant(), "test", "failed", null);
        store.fail(id, store.task(id).orElseThrow().revision(), failure, null, true, null);
        assertThat(state(id)).isEqualTo(TaskState.DEAD);
        return id;
    }

    private long cancelled(TaskRequest<Void> request) {
        long id = client.enqueue(request).id();
        store.cancel(id, store.task(id).orElseThrow().revision(), "stop", null);
        assertThat(state(id)).isEqualTo(TaskState.CANCELLED);
        return id;
    }

    private long expired() {
        long id = client.enqueue(
                        request().startBy(clock.instant().plusSeconds(1)).build())
                .id();
        clock.advance(Duration.ofSeconds(2));
        assertThat(store.expire(10)).extracting(ExpiredTask::id).containsExactly(id);
        return id;
    }

    private TaskState state(long id) {
        return store.task(id).orElseThrow().state();
    }

    /** The store as maintenance sees it when it cannot say which queue and type a task has: id 0 and an id nobody registered. */
    private FullStore withoutQueueAndType() {
        return StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("reclaim")) {
                return ((List<ExpiredLease>) result)
                        .stream()
                                .map(e -> new ExpiredLease(
                                        e.id(), e.revision(), e.attempts(), e.maxAttempts(), 0, 9999, false))
                                .toList();
            }
            if (method.equals("expire")) {
                return ((List<ExpiredTask>) result)
                        .stream().map(e -> new ExpiredTask(e.id(), 0, 9999)).toList();
            }
            return result;
        });
    }

    /** Another runner takes the tasks back after the sweeper listed them, before it can transition them. */
    private FullStore takenOverAfterReclaim() {
        return StoreInterceptor.around(store, (method, args, real) -> {
            var result = real.proceed();
            if (method.equals("reclaim")) {
                for (var e : (List<ExpiredLease>) result) store.unclaim(e.id(), e.revision());
            }
            return result;
        });
    }

    /** The store as maintenance sees it when a stored value of the task cannot be read. */
    private FullStore unreadable(long id) {
        return StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("task") && (long) args[0] == id) {
                throw new UnreadableTaskException(id, "task " + id + " has a stored value that cannot be read", null);
            }
            return real.proceed();
        });
    }

    /**
     * A store that answers full batches of one call for a number of calls, then an empty batch, and moves the
     * fake elapsed time on by a step at every call of it. Every call is logged by name.
     */
    private List<String> scriptedStore(String full, int fullBatches, Duration step) {
        var calls = new ArrayList<String>();
        var seen = new AtomicInteger();
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            calls.add(method);
            boolean scripted = method.equals(full);
            if (scripted) nanos.addAndGet(step.toNanos());
            boolean batch = scripted && seen.incrementAndGet() <= fullBatches;
            return switch (method) {
                case "purge" -> batch ? (long) purgeBatchSize : 0L;
                case "releaseKeys" -> batch ? purgeBatchSize : 0;
                case "reclaim" ->
                    batch
                            ? List.of(
                                    new ExpiredLease(1, 1, 1, 1, queueId, 0, false),
                                    new ExpiredLease(2, 1, 1, 1, queueId, 0, false))
                            : List.of();
                case "expire" -> List.of();
                case "purgeRunners" -> 0;
                case "heartbeat" -> null;
                case "task" -> java.util.Optional.empty();
                case "failExpired" -> FailOutcome.LOST;
                default -> real.proceed();
            };
        });
        return calls;
    }

    // ---- long jobs do not hold the others back --------------------------------------------------------

    @Test
    void aLongPurgeRunsACycleBetweenBatchesWheneverACheckIntervalHasPassed() throws Exception {
        purgeBatchSize = 2;
        retention = Map.of(TaskState.SUCCEEDED, Duration.ZERO);
        runnerId = 1;
        var calls = scriptedStore("purge", 5, Duration.ofSeconds(3));

        purge();

        assertThat(calls)
                .containsExactly(
                        "purge",
                        "purge",
                        "heartbeat",
                        "reclaim",
                        "expire",
                        "purge",
                        "purge",
                        "heartbeat",
                        "reclaim",
                        "expire",
                        "purge",
                        "purge",
                        "releaseKeys",
                        "purgeRunners");
    }

    @Test
    void aShortPurgeRunsNoCycleEvenWhenACheckIntervalHasPassed() throws Exception {
        purgeBatchSize = 2;
        retention = Map.of(TaskState.SUCCEEDED, Duration.ZERO);
        runnerId = 1;
        var calls = scriptedStore("purge", 0, Duration.ofSeconds(30));

        purge();

        assertThat(calls).containsExactly("purge", "releaseKeys", "purgeRunners");
    }

    @Test
    void aLongKeyReleaseRunsACycleBetweenBatchesWheneverACheckIntervalHasPassed() throws Exception {
        purgeBatchSize = 2;
        runnerId = 1;
        var calls = scriptedStore("releaseKeys", 5, Duration.ofSeconds(3));

        purge();

        assertThat(calls)
                .containsExactly(
                        "releaseKeys",
                        "releaseKeys",
                        "heartbeat",
                        "reclaim",
                        "expire",
                        "releaseKeys",
                        "releaseKeys",
                        "heartbeat",
                        "reclaim",
                        "expire",
                        "releaseKeys",
                        "releaseKeys",
                        "purgeRunners");
    }

    @Test
    void aShortKeyReleaseRunsNoCycleEvenWhenACheckIntervalHasPassed() throws Exception {
        purgeBatchSize = 2;
        runnerId = 1;
        var calls = scriptedStore("releaseKeys", 0, Duration.ofSeconds(30));

        purge();

        assertThat(calls).containsExactly("releaseKeys", "purgeRunners");
    }

    @Test
    void aBacklogOfExpiredLeasesWritesTheHeartbeatAgainWheneverACheckIntervalHasPassed() throws Exception {
        batchSize = 2;
        runnerId = 1;
        var calls = scriptedStore("reclaim", 5, Duration.ofSeconds(3));

        cycle();

        assertThat(calls.stream().filter("heartbeat"::equals)).hasSize(3);
        assertThat(calls.indexOf("expire")).isGreaterThan(calls.lastIndexOf("reclaim"));
        assertThat(status.lastMaintenanceAt()).isNotNull();
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                "heartbeat|cycle|Heartbeat of runner %d failed|Maintenance heartbeat",
                "reclaim|cycle|Error in reclaiming expired leases|Maintenance lease reclaim",
                "expire|cycle|Error in expiring tasks|Maintenance expiry of missed starts",
                "purgeRunners|purge|Error in purging finished tasks|Maintenance purge",
            })
    void anOutageOfAMaintenanceJobLogsOneStackTraceThenOneLinePerFailureAndTheRecovery(
            String failingCall, String job, String text, String loop) throws Exception {
        runnerId = store.allocateRunnerId("runner-a", Set.of(), Set.of());
        var failures = new AtomicInteger(3);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals(failingCall) && failures.getAndDecrement() > 0)
                throw new TaskStoreException("store down");
            return real.proceed();
        });
        var service = service();
        var message = text.formatted(runnerId);

        try (var logs = new LogCapture(Loggers.MAINTENANCE)) {
            for (int i = 0; i < 5; i++) {
                if (job.equals("cycle")) service.cycle();
                else service.purge();
            }

            var lines = logs.lines();
            assertThat(lines).hasSize(4);
            assertThat(lines.get(0)).isEqualTo("ERROR " + message + " [trace]");
            assertThat(lines.get(1))
                    .isEqualTo("ERROR " + message + " (failure 2 in a row: "
                            + "today.bonfire.oss.bth4j.exceptions.TaskStoreException: store down)");
            assertThat(lines.get(2))
                    .isEqualTo("ERROR " + message + " (failure 3 in a row: "
                            + "today.bonfire.oss.bth4j.exceptions.TaskStoreException: store down)");
            assertThat(lines.get(3))
                    .startsWith("INFO " + loop + ": the store answers again after 3 failed attempts over ")
                    .endsWith(" seconds");
        }
    }

    @Test
    void theRecoveryLineOfAMaintenanceJobCountsTheSecondsOfItsOwnClock() throws Exception {
        runnerId = store.allocateRunnerId("runner-a", Set.of(), Set.of());
        var failures = new AtomicInteger(2);
        serviceStore = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("heartbeat") && failures.getAndDecrement() > 0) {
                nanos.addAndGet(Duration.ofMillis(2500).toNanos());
                throw new TaskStoreException("store down");
            }
            return real.proceed();
        });
        var service = service();

        try (var logs = new LogCapture(Loggers.MAINTENANCE)) {
            service.cycle();
            service.cycle();
            nanos.addAndGet(Duration.ofMillis(3700).toNanos());
            service.cycle();

            assertThat(logs.lines().getLast())
                    .isEqualTo(
                            "INFO Maintenance heartbeat: the store answers again after 2 failed attempts over 6.2 seconds");
        }
    }

    @Test
    void aServiceWithoutARunnerMakesNoHeartbeatCallSoItNeverReportsAHeartbeatRecovery() throws Exception {
        try (var logs = new LogCapture(Loggers.MAINTENANCE)) {
            var service = service();
            service.cycle();
            service.cycle();

            assertThat(logs.lines()).isEmpty();
        }
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
                .runner(runnerId, "runner-a", runnerQueueIds, runnerTypeIds, () -> runningTasks)
                .accepting(() -> accepting)
                .runnerStaleTimeout(runnerStaleTimeout)
                .checkInterval(checkInterval)
                .nanoTime(nanos::get)
                .create();
    }
}

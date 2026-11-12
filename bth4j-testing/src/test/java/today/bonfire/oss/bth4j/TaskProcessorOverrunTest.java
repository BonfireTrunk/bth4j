package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * The maximum run time of an attempt, driven one renewal round at a time through {@code renewOnce} against a
 * processor that blocks and ignores the cancel request, the way a call without a timeout does.
 */
@Timeout(10)
class TaskProcessorOverrunTest {
    private static final TaskType<Void> TYPE = TaskType.of("overrun-test");
    private static final Duration LEASE = Duration.ofSeconds(3);
    private static final Duration RENEW_INTERVAL = Duration.ofSeconds(1);
    private static final Duration DEFAULT_MAX_RUN_TIME = Duration.ofSeconds(2);

    private final MutableClock clock = new MutableClock();
    private final InMemoryStore store = new InMemoryStore(clock);
    private final Names names = new Names(store);
    private final TaskClient client = TaskClient.builder()
            .queues(Queues.of("Q:DEF"))
            .store(store)
            .clock(clock)
            .build();
    private final int queueId = names.queueId("Q:DEF");
    private final int typeId = names.typeId(TYPE.name());
    private final Admission admission = new Admission();
    private final QueueReservations reservations = new QueueReservations();
    private final RunnerStatus status = new RunnerStatus(clock);
    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch replacementStarted = new CountDownLatch(1);
    private final CountDownLatch replacementRelease = new CountDownLatch(1);
    private final AtomicReference<TaskContext> context = new AtomicReference<>();
    private final List<TaskEvent> events = new CopyOnWriteArrayList<>();
    private final CapturingExecutor executor = new CapturingExecutor();
    private volatile boolean staleBodyThrows;
    private Thread worker;
    private Thread replacementWorker;

    private final TaskExecution.Settings settings = new TaskExecution.Settings(
            store,
            store,
            names,
            new Codecs(null),
            new TaskProcessorRegistry().register(TYPE, (ctx, payload) -> {
                boolean replacement = ctx.attemptNumber() > 1;
                if (replacement) {
                    replacementStarted.countDown();
                } else {
                    context.set(ctx);
                    started.countDown();
                }
                var gate = replacement ? replacementRelease : release;
                while (true) {
                    try {
                        if (gate.await(10, TimeUnit.SECONDS)) break;
                    } catch (InterruptedException ignored) {
                        // a call without a timeout does not stop for an interrupt either
                    }
                }
                if (!replacement && staleBodyThrows) throw new IllegalStateException("late failure");
            }),
            RetryPolicies.of(RetryPolicy.defaultPolicy()),
            new Observers(events::add),
            clock,
            status,
            true,
            true,
            true,
            FailureDetail.FULL);

    @AfterEach
    void releaseWorker() throws InterruptedException {
        release.countDown();
        replacementRelease.countDown();
        if (worker != null) worker.join(5_000);
        if (replacementWorker != null) replacementWorker.join(5_000);
    }

    @Test
    void anAttemptPastTheDefaultMaxRunTimeIsCancelledThenAbandonedOnce() throws Exception {
        var processor = processor(DEFAULT_MAX_RUN_TIME);
        long id = startStuckAttempt(processor, TaskRequest.of(TYPE).build());

        clock.advance(DEFAULT_MAX_RUN_TIME.plusMillis(1));
        processor.renewOnce();

        assertThat(context.get().isCancelRequested()).isTrue();
        var renewedUntil = store.task(id).orElseThrow().leaseUntil();
        assertThat(renewedUntil).isEqualTo(clock.instant().plus(LEASE));
        assertThat(reservations.abandoned()).isZero();

        clock.advance(RENEW_INTERVAL);
        processor.renewOnce();
        clock.advance(RENEW_INTERVAL);
        processor.renewOnce();

        assertThat(store.task(id).orElseThrow().leaseUntil()).isEqualTo(renewedUntil);
        assertThat(reservations.abandoned()).isEqualTo(1);
        assertThat(status.issues())
                .filteredOn(i -> i.kind() == Issue.Kind.ATTEMPT_ABANDONED)
                .singleElement()
                .satisfies(i -> {
                    assertThat(i.taskId()).isEqualTo(id);
                    assertThat(i.detail()).contains("1 abandoned attempts hold slots");
                });

        release.countDown();
        worker.join(5_000);

        assertThat(reservations.abandoned()).isZero();
        assertThat(admission.attempts()).isEmpty();
    }

    @Test
    void theTasksOwnMaxRunTimeWinsOverTheDefault() throws Exception {
        var processor = processor(Duration.ofMinutes(1));
        long id = startStuckAttempt(
                processor,
                TaskRequest.of(TYPE).maxRunTime(Duration.ofSeconds(1)).build());

        clock.advance(Duration.ofMillis(1_001));
        processor.renewOnce();

        assertThat(context.get().isCancelRequested()).isTrue();

        clock.advance(RENEW_INTERVAL);
        processor.renewOnce();

        assertThat(reservations.abandoned()).isEqualTo(1);
        assertThat(store.task(id).orElseThrow().state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void anAttemptWithinItsMaxRunTimeKeepsItsLease() throws Exception {
        var processor = processor(DEFAULT_MAX_RUN_TIME);
        long id = startStuckAttempt(processor, TaskRequest.of(TYPE).build());

        clock.advance(DEFAULT_MAX_RUN_TIME.minusMillis(1));
        processor.renewOnce();

        assertThat(context.get().isCancelRequested()).isFalse();
        assertThat(store.task(id).orElseThrow().leaseUntil())
                .isEqualTo(clock.instant().plus(LEASE));
        assertThat(reservations.abandoned()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anAbandonedBodyFinishingAfterTakeoverChangesNothingAndTheReplacementKeepsRenewing(boolean staleBodyFails)
            throws Exception {
        staleBodyThrows = staleBodyFails;
        var processor = processor(DEFAULT_MAX_RUN_TIME);
        long id = startStuckAttempt(processor, TaskRequest.of(TYPE).build());
        var stale = attempt(id);
        abandon(processor);
        expireAndRecover(id, stale);

        var replacement = startReplacement(processor, id);

        assertThat(replacement).isNotSameAs(stale);
        assertThat(replacement.revision()).isGreaterThan(stale.revision());
        assertThat(reservations.running(queueId)).isEqualTo(2);
        var before = store.task(id).orElseThrow();
        assertThat(before.state()).isEqualTo(TaskState.LEASED);
        assertThat(before.revision()).isEqualTo(replacement.revision());
        assertThat(before.attempts()).isEqualTo(2);
        assertThat(before.failures()).hasSize(1);

        release.countDown();
        worker.join(5_000);

        assertThat(worker.isAlive()).isFalse();
        var after = store.task(id).orElseThrow();
        assertThat(after.state()).isEqualTo(TaskState.LEASED);
        assertThat(after.revision()).isEqualTo(replacement.revision());
        assertThat(after.failures()).hasSize(1);
        assertThat(after.leaseUntil()).isEqualTo(before.leaseUntil());
        if (staleBodyFails) {
            assertThat(events.getLast())
                    .isInstanceOfSatisfying(
                            TaskEvent.Cancelled.class,
                            e -> assertThat(e.outcome()).isEqualTo(TaskEvent.Outcome.LOST));
        } else {
            assertThat(events.getLast())
                    .isInstanceOfSatisfying(
                            TaskEvent.Succeeded.class,
                            e -> assertThat(e.outcome()).isEqualTo(TaskEvent.Outcome.LOST));
        }
        assertThat(attempt(id)).isSameAs(replacement);
        assertThat(reservations.abandoned()).isZero();
        assertThat(reservations.running(queueId)).isEqualTo(1);

        clock.advance(Duration.ofMillis(100));
        processor.renewOnce();

        assertThat(store.task(id).orElseThrow().leaseUntil())
                .isEqualTo(clock.instant().plus(LEASE));

        replacementRelease.countDown();
        replacementWorker.join(5_000);

        assertThat(store.task(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(admission.attempts()).isEmpty();
        assertThat(reservations.running(queueId)).isZero();
        assertThat(reservations.abandoned()).isZero();
    }

    @Test
    void anOperatorCancelledAttemptThatIgnoresItUntilAbandonmentEndsCancelledAndIsNotRetried() throws Exception {
        var processor = processor(DEFAULT_MAX_RUN_TIME);
        long id = startStuckAttempt(processor, TaskRequest.of(TYPE).build());
        var attempt = attempt(id);
        assertThat(store.cancel(id, attempt.revision(), "operator stop", null)).isEqualTo(CancelOutcome.REQUESTED);

        processor.renewOnce();

        assertThat(context.get().isCancelRequested()).isTrue();
        abandon(processor);

        clock.advance(LEASE);
        maintenance().cycle();

        var ended = store.task(id).orElseThrow();
        assertThat(ended.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(ended.cancelReason()).isEqualTo("operator stop");
        assertThat(ended.attempts()).isEqualTo(1);
        assertThat(ended.failures()).isNull();
        assertThat(events)
                .filteredOn(e -> e instanceof TaskEvent.Cancelled)
                .singleElement()
                .satisfies(e -> assertThat(((TaskEvent.Cancelled) e).outcome()).isEqualTo(TaskEvent.Outcome.APPLIED));
        assertThat(events).noneMatch(e -> e instanceof TaskEvent.LeaseExpired);

        clock.advance(Duration.ofHours(1));
        store.promote(10);
        processor.processOnce();
        assertThat(executor.tasks).hasSize(1);
        assertThat(store.task(id).orElseThrow().state()).isEqualTo(TaskState.CANCELLED);

        release.countDown();
        worker.join(5_000);

        var finalTask = store.task(id).orElseThrow();
        assertThat(finalTask.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(finalTask.revision()).isEqualTo(ended.revision());
        assertThat(admission.attempts()).isEmpty();
        assertThat(reservations.abandoned()).isZero();
        assertThat(reservations.running(queueId)).isZero();
    }

    @Test
    void anAttemptWhoseProcessorReturnedIsRenewedWhileItsOutcomeIsWrittenAndNeverCancelledOrAbandoned()
            throws Exception {
        var ackEntered = new CountDownLatch(1);
        var ackRelease = new CountDownLatch(1);
        var blocking = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.startsWith("ack")) {
                ackEntered.countDown();
                if (!ackRelease.await(5, TimeUnit.SECONDS)) throw new AssertionError("ack not released");
            }
            return real.proceed();
        });
        var quick = new TaskExecution.Settings(
                blocking,
                blocking,
                names,
                new Codecs(null),
                new TaskProcessorRegistry().register(TYPE, (ctx, payload) -> {}),
                RetryPolicies.of(RetryPolicy.defaultPolicy()),
                new Observers(events::add),
                clock,
                status,
                true,
                true,
                true,
                FailureDetail.FULL);
        var processor = processor(DEFAULT_MAX_RUN_TIME, quick);
        long id = client.enqueue(TaskRequest.of(TYPE).build()).id();
        processor.processOnce();
        worker = new Thread(executor.tasks.getFirst(), "overrun-test-worker");
        worker.start();
        try {
            assertThat(ackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            var attempt = attempt(id);
            assertThat(attempt.reporting()).isTrue();

            clock.advance(DEFAULT_MAX_RUN_TIME.plusMillis(1));
            processor.renewOnce();
            clock.advance(RENEW_INTERVAL);
            processor.renewOnce();
            clock.advance(RENEW_INTERVAL);
            processor.renewOnce();

            assertThat(attempt.overrunSince()).isNull();
            assertThat(attempt.isCancelRequested()).isFalse();
            assertThat(reservations.abandoned()).isZero();
            assertThat(status.issues()).noneMatch(i -> i.kind() == Issue.Kind.ATTEMPT_ABANDONED);
            assertThat(store.task(id).orElseThrow().leaseUntil())
                    .isEqualTo(clock.instant().plus(LEASE));
        } finally {
            ackRelease.countDown();
            worker.join(5_000);
        }
        assertThat(store.task(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(admission.attempts()).isEmpty();
    }

    @Test
    void aRenewalRoundThatThrowsAnErrorDoesNotEndTheRenewalThread() throws Exception {
        var renewals = new java.util.concurrent.atomic.AtomicInteger();
        var survived = new CountDownLatch(1);
        var failing = StoreInterceptor.around(store, (method, args, real) -> {
            if (method.equals("renew")) {
                if (renewals.incrementAndGet() == 1) throw new AssertionError("renew");
                survived.countDown();
            }
            return real.proceed();
        });
        var processor = processor(
                DEFAULT_MAX_RUN_TIME,
                new TaskExecution.Settings(
                        failing,
                        failing,
                        names,
                        new Codecs(null),
                        new TaskProcessorRegistry().register(TYPE, (ctx, payload) -> {}),
                        RetryPolicies.of(RetryPolicy.defaultPolicy()),
                        new Observers(events::add),
                        clock,
                        status,
                        true,
                        true,
                        true,
                        FailureDetail.FULL),
                Duration.ofMillis(300));
        client.enqueue(TaskRequest.of(TYPE).build());
        processor.processOnce();
        admission.quiet(true);
        processor.start();
        try {
            assertThat(survived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(processor.ensureRenewer()).isFalse();
        } finally {
            processor.stopThread();
            processor.stopLeaseRenewal();
            processor.waitTillDone(Duration.ofSeconds(5));
        }
    }

    @Test
    void aDeadRenewalThreadIsReplacedUnlessRenewalWasStopped() throws Exception {
        var processor = processor(DEFAULT_MAX_RUN_TIME);

        assertThat(processor.ensureRenewer()).isTrue();
        assertThat(processor.ensureRenewer()).isFalse();

        processor.stopLeaseRenewal();
        assertThat(processor.ensureRenewer()).isFalse();
    }

    private long startStuckAttempt(TaskProcessorService processor, TaskRequest<Void> request) throws Exception {
        long id = client.enqueue(request).id();
        processor.processOnce();
        worker = new Thread(executor.tasks.getFirst(), "overrun-test-worker");
        worker.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        return id;
    }

    /** Past the maximum run time and one renewal period after it, so the attempt is cancelled then abandoned. */
    private void abandon(TaskProcessorService processor) {
        clock.advance(DEFAULT_MAX_RUN_TIME.plusMillis(1));
        processor.renewOnce();
        clock.advance(RENEW_INTERVAL);
        processor.renewOnce();
        assertThat(reservations.abandoned()).isEqualTo(1);
    }

    /** Lets the abandoned attempt's lease run out and maintenance end it, then makes the retry claimable. */
    private void expireAndRecover(long id, TaskExecution stale) throws Exception {
        clock.advance(LEASE);
        maintenance().cycle();
        assertThat(events)
                .anySatisfy(e -> assertThat(e)
                        .isInstanceOfSatisfying(
                                TaskEvent.LeaseExpired.class,
                                x -> assertThat(x.id()).isEqualTo(id)));
        assertThat(store.task(id).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        store.promote(10);
        assertThat(attempt(id)).isSameAs(stale);
    }

    private TaskExecution startReplacement(TaskProcessorService processor, long id) throws Exception {
        processor.processOnce();
        replacementWorker = new Thread(executor.tasks.getLast(), "overrun-test-replacement");
        replacementWorker.start();
        assertThat(replacementStarted.await(5, TimeUnit.SECONDS)).isTrue();
        return attempt(id);
    }

    private MaintenanceService maintenance() {
        return new MaintenanceService.Builder()
                .group(new ThreadGroup("overrun-test"))
                .threadName("overrun-test-maintenance")
                .store(store)
                .names(names)
                .retryPolicies(RetryPolicies.of(RetryPolicy.fixed(Duration.ZERO)))
                .observer(new Observers(events::add))
                .clock(clock)
                .status(status)
                .create();
    }

    private TaskProcessorService processor(Duration defaultMaxRunTime) {
        return processor(defaultMaxRunTime, settings);
    }

    private TaskProcessorService processor(Duration defaultMaxRunTime, TaskExecution.Settings processorSettings) {
        return processor(defaultMaxRunTime, processorSettings, LEASE);
    }

    private TaskProcessorService processor(
            Duration defaultMaxRunTime, TaskExecution.Settings processorSettings, Duration lease) {
        return new TaskProcessorService.Builder()
                .group(new ThreadGroup("overrun-test"))
                .threadName("overrun-test")
                .settings(processorSettings)
                .admission(admission)
                .reservations(reservations)
                .executor(executor)
                .queueIds(List.of(queueId))
                .knownTypeIds(Set.of(typeId))
                .claimBatchSize(1)
                .idlePollInterval(Duration.ofMillis(1))
                .lease(lease)
                .defaultMaxRunTime(defaultMaxRunTime)
                .runnerId(1)
                .create();
    }

    private static final class CapturingExecutor implements BackgroundExecutor {
        private final List<Runnable> tasks = new ArrayList<>();
        private int available = 2;

        @Override
        public int reserve(int max, Duration wait) {
            int given = Math.min(max, available);
            available -= given;
            return given;
        }

        @Override
        public void release(int slots) {
            available += slots;
        }

        @Override
        public void execute(Runnable next) {
            tasks.add(next);
        }

        @Override
        public int activeCount() {
            return tasks.size();
        }

        @Override
        public void shutdown() {}

        @Override
        public boolean awaitTermination(Duration timeout) {
            return true;
        }
    }

    /** The latest attempt of the task still tracked. */
    private TaskExecution attempt(long id) {
        return admission.attempts().stream()
                .filter(a -> a.taskId() == id)
                .max(java.util.Comparator.comparingInt(TaskExecution::revision))
                .orElse(null);
    }
}

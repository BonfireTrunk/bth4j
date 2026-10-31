package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * Behaviour every store must give a {@link BackgroundRunner}, checked end to end through the public API. A store
 * module subclasses it and supplies a fresh, isolated store per test through {@link #newStore(String)}. The
 * runners use short intervals so a test finishes in seconds; waiting is done by polling with generous
 * timeouts, and no assertion depends on a fixed sleep.
 */
@Timeout(60)
public abstract class RunnerContract {

    public record Payload(String name, int number) {}

    public record StepData(String value, int number) {}

    private static final TaskType<Payload> PAYLOAD_TYPE = TaskType.of("rc-payload", Payload.class);
    private static final TaskType<Void> VOID_TYPE = TaskType.of("rc-void");
    private static final TaskType<Void> OTHER_TYPE = TaskType.of("rc-other");
    private static final TaskType<Void> UNREGISTERED_TYPE = TaskType.of("rc-unregistered");

    private static final Duration AWAIT_LIMIT = Duration.ofSeconds(20);
    private static final AtomicInteger NAMESPACES = new AtomicInteger();

    protected FullStore store;
    protected TaskClient client;
    private final List<BackgroundRunner> runners = new ArrayList<>();

    /** @return a store with no state, isolated from every other namespace */
    protected abstract FullStore newStore(String namespace);

    @BeforeEach
    void openStore() {
        store = newStore("rc" + Long.toString(System.currentTimeMillis(), 36) + NAMESPACES.incrementAndGet());
        client = TaskClient.builder()
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .build();
    }

    @AfterEach
    void stopRunners() {
        for (var runner : runners) runner.stop();
        runners.clear();
    }

    /** A runner builder with short intervals; the lease cannot go below one second. */
    protected BackgroundRunner.Builder runnerBuilder() {
        return BackgroundRunner.builder()
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .concurrency(Concurrency.fixed(8))
                .leaseDuration(Duration.ofSeconds(1))
                .idlePollInterval(Duration.ofMillis(20))
                .maintenanceCheckInterval(Duration.ofSeconds(1))
                .scheduledTasksCheckInterval(Duration.ofMillis(50))
                .recurringTasksCheckInterval(Duration.ofMillis(200))
                .purgeInterval(Duration.ofSeconds(1))
                .shutdownTimeout(Duration.ofSeconds(5))
                .threadGroupName("rc-" + NAMESPACES.get())
                .retryPolicy(RetryPolicy.fixed(Duration.ofMillis(100)));
    }

    protected BackgroundRunner buildRunner(
            Consumer<BackgroundRunner.Builder> customize, Consumer<TaskProcessorRegistry> register) {
        var builder = runnerBuilder();
        customize.accept(builder);
        var runner = builder.build();
        register.accept(runner.registry());
        runners.add(runner);
        return runner;
    }

    protected BackgroundRunner startRunner(Consumer<TaskProcessorRegistry> register) {
        return startRunner(b -> {}, register);
    }

    protected BackgroundRunner startRunner(
            Consumer<BackgroundRunner.Builder> customize, Consumer<TaskProcessorRegistry> register) {
        var runner = buildRunner(customize, register);
        runner.start();
        return runner;
    }

    protected static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + AWAIT_LIMIT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            pause(10);
        }
        if (!condition.getAsBoolean()) fail("Timed out after %s waiting for %s", AWAIT_LIMIT, what);
    }

    protected TaskView awaitTask(long id, String what, Predicate<TaskView> condition) {
        var found = new AtomicReference<TaskView>();
        await("task " + id + " " + what, () -> {
            var view = client.task(id).filter(condition).orElse(null);
            found.set(view);
            return view != null;
        });
        return found.get();
    }

    protected TaskView awaitState(long id, TaskState state) {
        return awaitTask(id, "to be " + state, v -> v.state() == state);
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private long enqueue(TaskRequest<?> request) {
        var result = client.enqueue(request);
        assertThat(result).isInstanceOf(EnqueueResult.Accepted.class);
        return result.id();
    }

    @Test
    void typedPayloadTaskRunsWithItsDecodedPayloadAndIsLeasedWhileRunning() {
        var seen = new AtomicReference<Payload>();
        var stateInside = new AtomicReference<TaskState>();
        var viewInside = new AtomicReference<TaskView>();
        startRunner(r -> r.register(PAYLOAD_TYPE, (ctx, payload) -> {
            seen.set(payload);
            stateInside.set(client.task(ctx.id()).orElseThrow().state());
            viewInside.set(ctx.task());
        }));

        long id = enqueue(TaskRequest.of(PAYLOAD_TYPE, new Payload("alpha", 7))
                .header("trace", "t-1")
                .build());

        var done = awaitState(id, TaskState.SUCCEEDED);
        assertThat(seen.get()).isEqualTo(new Payload("alpha", 7));
        assertThat(stateInside.get()).isEqualTo(TaskState.LEASED);
        assertThat(viewInside.get().event()).isEqualTo("rc-payload");
        assertThat(viewInside.get().queue()).isEqualTo("Q:DEF");
        assertThat(viewInside.get().headers()).containsEntry("trace", "t-1");
        assertThat(done.attempts()).isEqualTo(1);
    }

    @Test
    void voidTaskRuns() {
        var ran = new CountDownLatch(1);
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            assertThat(payload).isNull();
            ran.countDown();
        }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());

        awaitState(id, TaskState.SUCCEEDED);
        assertThat(ran.getCount()).isZero();
    }

    @Test
    void successLeavesASucceededRecordWithoutItsPayload() {
        startRunner(r -> r.register(PAYLOAD_TYPE, (ctx, payload) -> {}));

        long id = enqueue(TaskRequest.of(PAYLOAD_TYPE, new Payload("gone", 1)).build());

        var done = awaitState(id, TaskState.SUCCEEDED);
        assertThat(done.data()).isNull();
        assertThat(done.leaseUntil()).isNull();
    }

    @Test
    void keepSucceededPayloadKeepsThePayload() {
        startRunner(b -> b.keepSucceededPayload(true), r -> r.register(PAYLOAD_TYPE, (ctx, payload) -> {}));

        long id = enqueue(TaskRequest.of(PAYLOAD_TYPE, new Payload("kept", 2)).build());

        var done = awaitState(id, TaskState.SUCCEEDED);
        assertThat(done.data()).contains("kept");
    }

    @Test
    void zeroSucceededRetentionRemovesTheTaskOnSuccess() {
        var ran = new CountDownLatch(1);
        startRunner(
                b -> b.succeededRetention(Duration.ZERO),
                r -> r.register(VOID_TYPE, (ctx, payload) -> ran.countDown()));

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());

        await("the task to run", () -> ran.getCount() == 0);
        await("task " + id + " to be removed", () -> client.task(id).isEmpty());
    }

    @Test
    void failingTaskIsRetriedAfterThePolicyDelayWithIncreasingAttempts() {
        var attempts = new CopyOnWriteArrayList<Integer>();
        var startedAt = new CopyOnWriteArrayList<Long>();
        startRunner(
                b -> b.retryPolicy(RetryPolicy.fixed(Duration.ofMillis(400))),
                r -> r.register(VOID_TYPE, (ctx, payload) -> {
                    attempts.add(ctx.attempt());
                    startedAt.add(System.nanoTime());
                    if (ctx.attempt() < 3) throw new IllegalStateException("not yet " + ctx.attempt());
                }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());

        var done = awaitState(id, TaskState.SUCCEEDED);
        assertThat(attempts).containsExactly(1, 2, 3);
        assertThat(done.attempts()).isEqualTo(3);
        for (int i = 1; i < startedAt.size(); i++) {
            long gap = TimeUnit.NANOSECONDS.toMillis(startedAt.get(i) - startedAt.get(i - 1));
            assertThat(gap).isGreaterThanOrEqualTo(350);
        }
    }

    @Test
    void taskDiesAfterMaxAttemptsWithItsFailuresRecorded() {
        var runs = new AtomicInteger();
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            throw new IllegalStateException("failure " + runs.incrementAndGet());
        }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).maxAttempts(3).build());

        var dead = awaitState(id, TaskState.DEAD);
        assertThat(runs).hasValue(3);
        assertThat(dead.attempts()).isEqualTo(3);
        assertThat(dead.failures()).hasSize(3);
        assertThat(dead.failures()).extracting(f -> f.attempt()).containsExactly(1, 2, 3);
        assertThat(dead.lastFailure().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(dead.lastFailure().message()).isEqualTo("failure 3");
        assertThat(dead.lastFailure().stack()).contains("failure 3");
    }

    @Test
    void unrecoverableExceptionKillsTheTaskAtOnce() {
        var runs = new AtomicInteger();
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            runs.incrementAndGet();
            throw new TaskUnrecoverableException("cannot be fixed");
        }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).maxAttempts(5).build());

        var dead = awaitState(id, TaskState.DEAD);
        assertThat(dead.attempts()).isEqualTo(1);
        assertThat(dead.lastFailure().type()).isEqualTo(TaskUnrecoverableException.class.getName());
        assertThat(dead.lastFailure().message()).isEqualTo("cannot be fixed");
        assertThat(runs).hasValue(1);
    }

    @Test
    void rescheduleRunsTheTaskAgainLaterWithoutConsumingAnAttempt() {
        var attempts = new CopyOnWriteArrayList<Integer>();
        var startedAt = new CopyOnWriteArrayList<Long>();
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            attempts.add(ctx.attempt());
            startedAt.add(System.nanoTime());
            if (attempts.size() == 1) throw new TaskRescheduleException(Duration.ofMillis(400));
        }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).maxAttempts(1).build());

        var done = awaitState(id, TaskState.SUCCEEDED);
        assertThat(attempts).containsExactly(1, 1);
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(done.failures()).isEmpty();
        long gap = TimeUnit.NANOSECONDS.toMillis(startedAt.get(1) - startedAt.get(0));
        assertThat(gap).isGreaterThanOrEqualTo(350);
    }

    @Test
    void completedStepsAreNotRunAgainOnRetryAndTheirResultsComeFromStorage() {
        var runs = new ConcurrentHashMap<String, AtomicInteger>();
        var firstResults = new CopyOnWriteArrayList<StepData>();
        var secondResults = new CopyOnWriteArrayList<String>();
        var stepsAtStart = new CopyOnWriteArrayList<Map<String, Instant>>();
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            var known = new java.util.LinkedHashMap<String, Instant>();
            ctx.task().steps().forEach((name, step) -> known.put(name, step.at()));
            stepsAtStart.add(known);
            var first = ctx.step("one", StepData.class, () -> {
                runs.computeIfAbsent("one", k -> new AtomicInteger()).incrementAndGet();
                return new StepData("first", 1);
            });
            var second = ctx.step("two", String.class, () -> {
                runs.computeIfAbsent("two", k -> new AtomicInteger()).incrementAndGet();
                return "second";
            });
            firstResults.add(first);
            secondResults.add(second);
            if (ctx.attempt() == 1) throw new IllegalStateException("after the second step");
            ctx.step(
                    "three",
                    () -> runs.computeIfAbsent("three", k -> new AtomicInteger())
                            .incrementAndGet());
        }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());

        var done = awaitState(id, TaskState.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(2);
        assertThat(runs.get("one")).hasValue(1);
        assertThat(runs.get("two")).hasValue(1);
        assertThat(runs.get("three")).hasValue(1);
        assertThat(firstResults).containsExactly(new StepData("first", 1), new StepData("first", 1));
        assertThat(firstResults.get(1)).isNotSameAs(firstResults.get(0));
        assertThat(secondResults).containsExactly("second", "second");
        assertThat(stepsAtStart.get(0)).isEmpty();
        assertThat(stepsAtStart.get(1)).containsOnlyKeys("one", "two");
        assertThat(done.steps()).containsOnlyKeys("one", "two", "three");
        assertThat(done.steps().get("one").at()).isEqualTo(stepsAtStart.get(1).get("one"));
        assertThat(done.steps().get("two").at()).isEqualTo(stepsAtStart.get(1).get("two"));
        assertThat(done.steps().get("one").result()).contains("first");
    }

    @Test
    void progressIsVisibleToOthersWhileTheTaskRuns() {
        var reported = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            ctx.progress("halfway");
            reported.countDown();
            release.await();
        }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());

        try {
            await("progress to be reported", () -> reported.getCount() == 0);
            var running = awaitTask(id, "to show progress", v -> "halfway".equals(v.progress()));
            assertThat(running.state()).isEqualTo(TaskState.LEASED);
        } finally {
            release.countDown();
        }
        awaitState(id, TaskState.SUCCEEDED);
    }

    @Test
    void uniqueKeyAnswersDuplicateWhilePendingAndAcceptsAgainAfterTheTaskFinished() {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            started.countDown();
            release.await();
        }));

        long first = enqueue(TaskRequest.of(VOID_TYPE)
                .uniqueKey("only-one", Uniqueness.UNTIL_DONE)
                .build());
        await("the first task to start", () -> started.getCount() == 0);
        var whileRunning =
                client.enqueue(TaskRequest.of(VOID_TYPE).uniqueKey("only-one").build());

        assertThat(whileRunning).isEqualTo(new EnqueueResult.Duplicate(first));

        release.countDown();
        awaitState(first, TaskState.SUCCEEDED);
        var afterwards =
                client.enqueue(TaskRequest.of(VOID_TYPE).uniqueKey("only-one").build());

        assertThat(afterwards).isInstanceOf(EnqueueResult.Accepted.class);
        assertThat(afterwards.id()).isNotEqualTo(first);
        awaitState(afterwards.id(), TaskState.SUCCEEDED);
    }

    @Test
    void uniqueKeyWithKeepForStaysDuplicateAfterSuccess() {
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {}));

        long first = enqueue(TaskRequest.of(VOID_TYPE)
                .uniqueKey("kept", Uniqueness.keepFor(Duration.ofMinutes(5)))
                .build());
        awaitState(first, TaskState.SUCCEEDED);

        var again = client.enqueue(TaskRequest.of(VOID_TYPE)
                .uniqueKey("kept", Uniqueness.keepFor(Duration.ofMinutes(5)))
                .build());

        assertThat(again).isEqualTo(new EnqueueResult.Duplicate(first));
    }

    @Test
    void delayedTaskRunsAfterItsDelayAndNotBefore() {
        var startedAt = new AtomicLong();
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> startedAt.set(System.nanoTime())));

        long enqueuedAt = System.nanoTime();
        long id =
                enqueue(TaskRequest.of(VOID_TYPE).delay(Duration.ofMillis(300)).build());

        assertThat(client.task(id).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
        awaitState(id, TaskState.SUCCEEDED);
        long elapsed = TimeUnit.NANOSECONDS.toMillis(startedAt.get() - enqueuedAt);
        assertThat(elapsed).isGreaterThanOrEqualTo(250);
    }

    @Test
    void recurringScheduleHasAFutureFireTimeAndKeepsItWhenRegisteredAgainWithTheSameCron() {
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {}));
        var before = Instant.now();

        var next = client.putSchedule(ScheduleDefinition.of("every-minute", VOID_TYPE, "* * * * *"));

        assertThat(next)
                .isAfter(before.minusSeconds(1))
                .isBeforeOrEqualTo(Instant.now().plusSeconds(61));
        assertThat(client.schedules()).containsKey("every-minute");
        assertThat(client.schedules().get("every-minute")).isAfter(before.minusSeconds(1));

        var yearly = ScheduleDefinition.of("yearly", VOID_TYPE, "0 0 1 1 *");
        var yearlyNext = client.putSchedule(yearly);
        assertThat(client.putSchedule(yearly)).isEqualTo(yearlyNext);
        assertThat(client.schedules().get("yearly")).isEqualTo(yearlyNext);

        assertThat(client.deleteSchedule("yearly")).isTrue();
        assertThat(client.deleteSchedule("yearly")).isFalse();
        assertThat(client.schedules()).doesNotContainKey("yearly");
    }

    @Test
    void quietRunnerClaimsNothingUntilResumed() {
        var ran = new CountDownLatch(1);
        var runner = buildRunner(b -> {}, r -> r.register(VOID_TYPE, (ctx, payload) -> ran.countDown()));
        runner.quiet();
        runner.start();
        assertThat(runner.isQuiet()).isTrue();

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());
        pause(500);

        assertThat(ran.getCount()).isEqualTo(1);
        var waiting = client.task(id).orElseThrow();
        assertThat(waiting.state()).isEqualTo(TaskState.QUEUED);
        assertThat(waiting.attempts()).isZero();

        runner.resume();

        assertThat(runner.isQuiet()).isFalse();
        awaitState(id, TaskState.SUCCEEDED);
        assertThat(ran.getCount()).isZero();
    }

    @Test
    void stopWaitsForARunningTaskToFinish() {
        var started = new CountDownLatch(1);
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            started.countDown();
            Thread.sleep(500);
        }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());
        await("the task to start", () -> started.getCount() == 0);
        runner.stop();

        assertThat(client.task(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(runner.runningTasks()).isZero();
    }

    @Test
    void observerSeesStartedAndSucceededForASuccess() {
        var events = new CopyOnWriteArrayList<TaskEvent>();
        startRunner(b -> b.observer(events::add), r -> r.register(VOID_TYPE, (ctx, payload) -> {}));

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());

        await("the success events", () -> eventNames(events, id).size() >= 2);
        assertThat(eventNames(events, id)).containsExactly("Started", "Succeeded");
        var succeeded = events.stream()
                .filter(TaskEvent.Succeeded.class::isInstance)
                .map(TaskEvent.Succeeded.class::cast)
                .findFirst()
                .orElseThrow();
        assertThat(succeeded.outcome()).isEqualTo(TaskEvent.Outcome.APPLIED);
    }

    @Test
    void observerSeesStartedRetryingAndDiedForAFailingTask() {
        var events = new CopyOnWriteArrayList<TaskEvent>();
        startRunner(
                b -> b.observer(events::add),
                r -> r.register(VOID_TYPE, (ctx, payload) -> {
                    throw new IllegalStateException("always");
                }));

        long id = enqueue(TaskRequest.of(VOID_TYPE).maxAttempts(2).build());

        await("the failure events", () -> eventNames(events, id).contains("Died"));
        assertThat(eventNames(events, id)).containsExactly("Started", "Retrying", "Started", "Died");
        var died = events.stream()
                .filter(TaskEvent.Died.class::isInstance)
                .map(TaskEvent.Died.class::cast)
                .findFirst()
                .orElseThrow();
        assertThat(died.error()).isInstanceOf(IllegalStateException.class);
        assertThat(died.outcome()).isEqualTo(TaskEvent.Outcome.APPLIED);
    }

    @Test
    void eventWithoutARegisteredProcessorIsNeverClaimedByThisRunner() {
        var ran = new CountDownLatch(1);
        startRunner(r -> r.register(OTHER_TYPE, (ctx, payload) -> ran.countDown()));

        long unknown = enqueue(TaskRequest.of(UNREGISTERED_TYPE).build());
        long known = enqueue(TaskRequest.of(OTHER_TYPE).build());

        awaitState(known, TaskState.SUCCEEDED);
        pause(1000);
        var untouched = client.task(unknown).orElseThrow();
        assertThat(untouched.attempts()).isZero();
        assertThat(untouched.state()).isIn(TaskState.QUEUED, TaskState.SCHEDULED);
        assertThat(untouched.workerId()).isNull();
    }

    @Test
    void processorsCannotBeRegisteredAfterStart() {
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {}));

        assertThatThrownBy(() -> runner.registry().register(OTHER_TYPE, (ctx, payload) -> {}))
                .isInstanceOf(TaskConfigurationError.class);
    }

    @Test
    void twoRunnersOnTheSameStoreNeverRunTheSameTaskTwice() {
        var executions = new ConcurrentHashMap<Long, AtomicInteger>();
        Consumer<TaskProcessorRegistry> register = r -> r.register(PAYLOAD_TYPE, (ctx, payload) -> {
            executions.computeIfAbsent(ctx.id(), k -> new AtomicInteger()).incrementAndGet();
            Thread.sleep(5);
        });
        startRunner(b -> b.concurrency(Concurrency.fixed(4)).threadGroupName("rc-a"), register);
        startRunner(b -> b.concurrency(Concurrency.fixed(4)).threadGroupName("rc-b"), register);

        var requests = new ArrayList<TaskRequest<Payload>>();
        for (int i = 0; i < 100; i++) {
            requests.add(TaskRequest.of(PAYLOAD_TYPE, new Payload("n" + i, i)).build());
        }
        var ids = client.enqueueAll(requests).stream().map(EnqueueResult::id).toList();

        for (long id : ids) awaitState(id, TaskState.SUCCEEDED);
        assertThat(executions).hasSize(100);
        assertThat(executions.values()).allSatisfy(count -> assertThat(count).hasValue(1));
        assertThat(Collections.unmodifiableSet(executions.keySet())).containsExactlyInAnyOrderElementsOf(ids);
    }

    // ---- wave 2: cancellation, expiry, policies, admin, health ------------------------------------------

    @Test
    void cancelRequestWhileRunningEndsTheTaskAsCancelled() throws Exception {
        var started = new CountDownLatch(1);
        var events = new CopyOnWriteArrayList<TaskEvent>();
        var runner = startRunner(
                b -> b.observer(events::add),
                r -> r.register(VOID_TYPE, (ctx, payload) -> {
                    started.countDown();
                    while (true) {
                        ctx.checkCancelled();
                        pause(10);
                    }
                }));
        long id = enqueue(TaskRequest.of(VOID_TYPE).build());
        assertThat(started.await(20, TimeUnit.SECONDS)).isTrue();
        var running = awaitState(id, TaskState.LEASED);

        var outcome = runner.admin().cancel(id, running.revision(), "operator says stop");

        assertThat(outcome).isEqualTo(today.bonfire.oss.bth4j.spi.CancelOutcome.REQUESTED);
        var done = awaitState(id, TaskState.CANCELLED);
        assertThat(done.cancelReason()).isEqualTo("operator says stop");
        assertThat(done.revision()).isEqualTo(running.revision() + 1);
        assertThat(done.data()).isNull();
        await("the cancelled event", () -> eventNames(events, id).contains("Cancelled"));
        assertThat(eventNames(events, id)).containsExactly("Started", "Cancelled");
    }

    @Test
    void cancelOfAQueuedTaskIsImmediateAndRetryBringsItBack() throws Exception {
        var ran = new CountDownLatch(1);
        var runner = buildRunner(b -> {}, r -> r.register(VOID_TYPE, (ctx, payload) -> ran.countDown()));
        runner.quiet();
        runner.start();
        long id = enqueue(TaskRequest.of(VOID_TYPE).build());
        var queued = client.task(id).orElseThrow();

        assertThat(runner.admin().cancel(id, queued.revision(), null))
                .isEqualTo(today.bonfire.oss.bth4j.spi.CancelOutcome.CANCELLED);
        var cancelled = client.task(id).orElseThrow();
        assertThat(cancelled.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(cancelled.cancelReason()).isEqualTo("cancelled by operator");
        assertThat(runner.admin().cancel(id, queued.revision(), null))
                .isEqualTo(today.bonfire.oss.bth4j.spi.CancelOutcome.STALE);

        assertThat(runner.admin().retry(id, cancelled.revision())).isTrue();
        runner.resume();
        awaitState(id, TaskState.SUCCEEDED);
        assertThat(ran.await(20, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void maxRunCancelsATaskThatRunsTooLong() {
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            while (true) {
                ctx.checkCancelled();
                pause(10);
            }
        }));

        long id =
                enqueue(TaskRequest.of(VOID_TYPE).maxRun(Duration.ofMillis(200)).build());

        var done = awaitState(id, TaskState.CANCELLED);
        assertThat(done.cancelReason()).contains("max run time");
    }

    @Test
    void interruptOnCancelWakesABlockedProcessor() throws Exception {
        var interruptible = TaskType.of("rc-interruptible").interruptOnCancel();
        var started = new CountDownLatch(1);
        var runner = startRunner(r -> r.register(interruptible, (ctx, payload) -> {
            started.countDown();
            Thread.sleep(30_000);
        }));
        long id = enqueue(TaskRequest.of(interruptible).build());
        assertThat(started.await(20, TimeUnit.SECONDS)).isTrue();
        var running = awaitState(id, TaskState.LEASED);

        runner.admin().cancel(id, running.revision(), "wake up");

        var done = awaitState(id, TaskState.CANCELLED);
        assertThat(done.cancelReason()).isEqualTo("wake up");
    }

    @Test
    void taskThatMissesItsStartByTimeExpiresWithoutRunning() throws Exception {
        var ran = new CountDownLatch(1);
        var events = new CopyOnWriteArrayList<TaskEvent>();
        var runner = buildRunner(
                b -> b.observer(events::add), r -> r.register(VOID_TYPE, (ctx, payload) -> ran.countDown()));
        runner.quiet();
        runner.start();

        long id = enqueue(
                TaskRequest.of(VOID_TYPE).startBy(Instant.now().plusMillis(300)).build());

        var expired = awaitState(id, TaskState.EXPIRED);
        assertThat(expired.attempts()).isZero();
        runner.resume();
        pause(300);
        assertThat(ran.getCount()).isEqualTo(1);
        await("the expired event", () -> eventNames(events, id).contains("Expired"));
    }

    @Test
    void namedRetryPolicyDecidesAndAnUnknownNameFallsBackToTheDefault() {
        var calls = new AtomicInteger();
        var runner = startRunner(
                b -> b.retryPolicy("never", (task, error) -> RetryPolicy.Decision.dead("policy says no")),
                r -> r.register(VOID_TYPE, (ctx, payload) -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("boom");
                }));

        long never = enqueue(TaskRequest.of(VOID_TYPE).retryPolicy("never").build());
        long unknown = enqueue(TaskRequest.of(VOID_TYPE)
                .retryPolicy("no-such-policy")
                .maxAttempts(2)
                .build());

        var dead = awaitState(never, TaskState.DEAD);
        assertThat(dead.attempts()).isEqualTo(1);
        var retried = awaitTask(unknown, "to reach attempt 2", v -> v.attempts() >= 2);
        assertThat(retried.attempts()).isEqualTo(2);
        await(
                "the retry-policy issue",
                () -> runner.issues().stream().anyMatch(i -> i.kind().equals("retry-policy")));
    }

    @Test
    void pausedQueueIsNotClaimedUntilResumed() {
        var ran = new CountDownLatch(1);
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> ran.countDown()));
        runner.admin().pauseQueue("Q:DEF", true);
        assertThat(runner.admin().queues()).anyMatch(q -> q.name().equals("Q:DEF") && q.paused());

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());
        pause(400);
        assertThat(client.task(id).orElseThrow().state()).isEqualTo(TaskState.QUEUED);

        runner.admin().pauseQueue("Q:DEF", false);

        awaitState(id, TaskState.SUCCEEDED);
        assertThat(ran.getCount()).isZero();
    }

    @Test
    void drainWaitsForRunningTasksAndHealthReportsTheRunner() throws Exception {
        var started = new CountDownLatch(1);
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            started.countDown();
            pause(400);
        }));
        assertThat(runner.health().status()).isEqualTo(RunnerHealth.Status.RUNNING);
        assertThat(runner.health().workerId()).isPositive();

        long id = enqueue(TaskRequest.of(VOID_TYPE).build());
        assertThat(started.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(runner.health().running()).isEqualTo(1);

        assertThat(runner.drain(Duration.ofSeconds(10))).isTrue();

        assertThat(runner.runningTasks()).isZero();
        assertThat(client.task(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        var health = runner.health();
        assertThat(health.status()).isEqualTo(RunnerHealth.Status.QUIET);
        assertThat(health.lastClaimAt()).isNotNull();
        await(
                "the worker heartbeat",
                () -> runner.admin().workers().stream()
                        .anyMatch(w -> w.id() == runner.workerId() && w.lastSeen() != null));
        assertThat(runner.admin().health()).isPresent();
    }

    @Test
    void fixedDelayScheduleReArmsAfterSuccessAndPausesAfterADeadOccurrence() {
        var okRuns = new AtomicInteger();
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> okRuns.incrementAndGet())
                .register(OTHER_TYPE, (ctx, payload) -> {
                    throw new TaskUnrecoverableException("always");
                }));

        client.putSchedule(ScheduleDefinition.every("tick", VOID_TYPE, Duration.ofSeconds(1)));
        client.putSchedule(ScheduleDefinition.every("broken", OTHER_TYPE, Duration.ofSeconds(1)));

        await("two ticks", () -> okRuns.get() >= 2);
        await(
                "the broken schedule to pause",
                () -> runner.admin().schedules().stream().anyMatch(s -> s.id().equals("broken") && s.paused()));
        var broken = runner.admin().schedules().stream()
                .filter(s -> s.id().equals("broken"))
                .findFirst()
                .orElseThrow();
        assertThat(broken.fixedDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(runner.issues()).anyMatch(i -> i.kind().equals("schedule-paused"));
        var tick = runner.admin().schedules().stream()
                .filter(s -> s.id().equals("tick"))
                .findFirst()
                .orElseThrow();
        assertThat(tick.paused()).isFalse();
        assertThat(tick.nextFire()).isAfter(Instant.now().minusSeconds(2));
    }

    @Test
    void misfireFireOnceNowCreatesOneOccurrenceAndMovesOn() {
        var runs = new AtomicInteger();
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> runs.incrementAndGet()));
        registerLate(
                "late-once",
                "* * * * *",
                today.bonfire.oss.bth4j.spi.RecurringDefinition.MISFIRE_FIRE_ONCE_NOW,
                0,
                Duration.ofMinutes(10));

        await("one catch-up run", () -> runs.get() == 1);
        await("a future fire time", () -> client.schedules().get("late-once").isAfter(Instant.now()));
        pause(300);
        assertThat(runs.get()).isEqualTo(1);
        assertThat(runner.issues()).anyMatch(i -> i.kind().equals("schedule-misfire"));
    }

    @Test
    void misfireSkipCreatesNothing() {
        var runs = new AtomicInteger();
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> runs.incrementAndGet()));
        registerLate(
                "late-skip",
                "* * * * *",
                today.bonfire.oss.bth4j.spi.RecurringDefinition.MISFIRE_SKIP,
                0,
                Duration.ofMinutes(10));

        await("a future fire time", () -> client.schedules().get("late-skip").isAfter(Instant.now()));
        pause(300);
        assertThat(runs.get()).isZero();
        assertThat(client.tasks(TaskState.QUEUED, null, 0, 10)).isEmpty();
        assertThat(runner.issues()).anyMatch(i -> i.kind().equals("schedule-misfire"));
    }

    @Test
    void misfireCatchUpCreatesAtMostTheConfiguredOccurrences() {
        var runs = new AtomicInteger();
        startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> runs.incrementAndGet()));
        registerLate(
                "late-catch",
                "* * * * *",
                today.bonfire.oss.bth4j.spi.RecurringDefinition.MISFIRE_CATCH_UP,
                2,
                Duration.ofMinutes(10));

        await("two catch-up runs", () -> runs.get() == 2);
        await("a future fire time", () -> client.schedules().get("late-catch").isAfter(Instant.now()));
        pause(300);
        assertThat(runs.get()).isEqualTo(2);
    }

    @Test
    void overlapSkipDoesNotStartASecondOccurrenceWhileTheFirstRuns() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var runs = new AtomicInteger();
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {
            runs.incrementAndGet();
            started.countDown();
            release.await();
        }));
        client.putSchedule(
                ScheduleDefinition.of("guarded", VOID_TYPE, "0 0 1 1 *").overlap(Overlap.SKIP));

        assertThat(runner.admin().triggerSchedule("guarded")).isTrue();
        assertThat(started.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(runner.admin().triggerSchedule("guarded")).isTrue();
        await(
                "the overlap issue",
                () -> runner.issues().stream().anyMatch(i -> i.kind().equals("schedule-overlap")));
        assertThat(runs.get()).isEqualTo(1);
        var guarded = runner.admin().schedules().stream()
                .filter(s -> s.id().equals("guarded"))
                .findFirst()
                .orElseThrow();
        assertThat(guarded.overlapSkip()).isTrue();
        assertThat(guarded.nextFire()).isAfter(Instant.now());

        release.countDown();
        await(
                "the first occurrence to finish",
                () -> client.tasks(TaskState.LEASED, null, 0, 10).isEmpty());
        assertThat(runner.admin().triggerSchedule("guarded")).isTrue();
        await("a second run", () -> runs.get() == 2);
    }

    @Test
    void scheduleZoneAndPauseAreStoredAndVisible() {
        var runner = startRunner(r -> r.register(VOID_TYPE, (ctx, payload) -> {}));
        client.putSchedule(ScheduleDefinition.of("kolkata", VOID_TYPE, "30 9 * * *")
                .zone(java.time.ZoneId.of("Asia/Kolkata"))
                .misfire(Misfire.SKIP));

        var view = runner.admin().schedules().stream()
                .filter(s -> s.id().equals("kolkata"))
                .findFirst()
                .orElseThrow();
        assertThat(view.zone()).isEqualTo("Asia/Kolkata");
        assertThat(view.misfire()).isEqualTo("SKIP");
        assertThat(view.cron()).isEqualTo("30 9 * * *");
        assertThat(view.nextFire().atZone(java.time.ZoneId.of("Asia/Kolkata")).getHour())
                .isEqualTo(9);

        assertThat(runner.admin().pauseSchedule("kolkata", true)).isTrue();
        assertThat(runner.admin().schedules().stream()
                        .filter(s -> s.id().equals("kolkata"))
                        .findFirst()
                        .orElseThrow()
                        .paused())
                .isTrue();
        assertThat(runner.admin().pauseSchedule("kolkata", false)).isTrue();
        assertThat(runner.admin().pauseSchedule("missing", true)).isFalse();
        assertThat(runner.admin().deleteSchedule("kolkata")).isTrue();
    }

    /** Stores a cron definition whose next fire is {@code late} in the past, as a runner that was down would find it. */
    private void registerLate(String id, String cron, String misfire, int catchUpMax, Duration late) {
        var def = new today.bonfire.oss.bth4j.spi.RecurringDefinition(
                id,
                store.eventId(VOID_TYPE.name()),
                1,
                store.queueId("Q:DEF"),
                null,
                cron,
                null,
                misfire,
                catchUpMax,
                null,
                false,
                null,
                3,
                false,
                0);
        store.putRecurring(def, Instant.now().minus(late));
    }

    private static List<String> eventNames(List<TaskEvent> events, long id) {
        var names = new ArrayList<String>();
        for (var event : List.copyOf(events)) {
            if (taskId(event) == id) names.add(event.getClass().getSimpleName());
        }
        return names;
    }

    private static long taskId(TaskEvent event) {
        return switch (event) {
            case TaskEvent.Started e -> e.task().id();
            case TaskEvent.Succeeded e -> e.task().id();
            case TaskEvent.Retrying e -> e.task().id();
            case TaskEvent.Died e -> e.task().id();
            case TaskEvent.Rescheduled e -> e.task().id();
            case TaskEvent.Cancelled e -> e.task().id();
            case TaskEvent.LeaseExpired e -> e.id();
            case TaskEvent.Deferred e -> e.id();
            case TaskEvent.Expired e -> e.id();
            case TaskEvent.ScheduleFired e -> e.taskId();
            case TaskEvent.Purged e -> -1L;
        };
    }
}

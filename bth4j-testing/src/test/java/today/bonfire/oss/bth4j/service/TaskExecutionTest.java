package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.FailureDetail;
import today.bonfire.oss.bth4j.RetryPolicy;
import today.bonfire.oss.bth4j.TaskClient;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.TaskProcessor;
import today.bonfire.oss.bth4j.TaskProcessorRegistry;
import today.bonfire.oss.bth4j.TaskRequest;
import today.bonfire.oss.bth4j.TaskType;
import today.bonfire.oss.bth4j.exceptions.TaskCancelledException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

@Timeout(15)
class TaskExecutionTest {
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final TaskType<Void> TYPE = TaskType.of("lease-loss").interruptOnCancel();

    private final MutableClock clock = new MutableClock();
    private final InMemoryStore store = new InMemoryStore(clock);
    private final TaskClient client =
            TaskClient.builder().store(store).clock(clock).build();
    private final TaskProcessorRegistry registry = new TaskProcessorRegistry();
    private final List<TaskEvent> events = new ArrayList<>();
    private final AtomicBoolean interruptedAtFinish = new AtomicBoolean();
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private RetryPolicy retryPolicy = RetryPolicy.fixed(Duration.ZERO);

    @BeforeEach
    void captureLogs() {
        logs.start();
        ((Logger) LoggerFactory.getLogger(TaskExecution.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(TaskExecution.class)).detachAppender(logs);
        logs.stop();
    }

    enum BodyExit {
        RETURN,
        THROW,
        CANCEL,
        RESCHEDULE,
        FATAL
    }

    @ParameterizedTest
    @EnumSource(BodyExit.class)
    void leaseLossInterruptOverridesTheBodyOutcomeAndClearsTheInterruptBeforeReuse(BodyExit exit) throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var fatal = new InternalError("processor failed");
        var execution = execution(TYPE, (ctx, payload) -> {
            started.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted.countDown();
                if (exit != BodyExit.RETURN) Thread.currentThread().interrupt();
                switch (exit) {
                    case RETURN -> {}
                    case THROW -> throw new IllegalStateException("interrupted", e);
                    case CANCEL -> throw new TaskCancelledException("interrupted");
                    case RESCHEDULE -> throw new TaskRescheduleException(Duration.ofSeconds(1));
                    case FATAL -> throw fatal;
                }
            }
        });
        var nextInterrupted = new AtomicReference<Boolean>();
        var next = execution(
                TaskType.of("next-task"),
                (ctx, payload) -> nextInterrupted.set(Thread.currentThread().isInterrupted()));
        var thrown = new AtomicReference<Throwable>();
        var worker = new Thread(() -> {
            try {
                execution.run();
            } catch (Throwable e) {
                thrown.set(e);
            }
            execution.leaseLost();
            execution.requestCancel("late cancel");
            next.run();
        });
        worker.start();
        try {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            clock.advance(LEASE.plusSeconds(1));
            var status = store.renew(List.of(new LeaseRef(execution.taskId(), execution.revision())), LEASE);
            assertThat(status.getFirst().owned()).isFalse();

            execution.leaseLost();

            assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
            worker.join(5_000);
            assertThat(worker.isAlive()).isFalse();
            assertThat(thrown.get()).isSameAs(exit == BodyExit.FATAL ? fatal : null);
            assertThat(interruptedAtFinish).isFalse();
            assertThat(nextInterrupted.get()).isFalse();
            var task = store.getTask(execution.taskId()).orElseThrow();
            assertThat(task.state()).isEqualTo(TaskState.LEASED);
            assertThat(task.revision()).isEqualTo(execution.revision());
            assertThat(task.failures()).isNull();
            assertThat(events).hasSize(3);
            assertThat(events.get(0)).isInstanceOf(TaskEvent.Started.class);
            assertThat(events.get(1)).isInstanceOf(TaskEvent.Started.class);
            assertThat(events.get(2)).isInstanceOf(TaskEvent.Succeeded.class);
            assertThat(logs.list)
                    .anySatisfy(event -> assertThat(event.getFormattedMessage())
                            .contains("Task " + execution.taskId() + " stopped: lease")
                            .contains("was lost"));
        } finally {
            release.countDown();
            worker.join(5_000);
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void nonOptedBodyOutcomeUsesTheStoreFenceAfterRenewalRefusesOwnership(boolean fails, boolean takenOver) {
        var runs = new AtomicInteger();
        var execution = execution(TaskType.of("non-interruptible"), (ctx, payload) -> {
            runs.incrementAndGet();
            clock.advance(LEASE.plusSeconds(1));
            var status = store.renew(List.of(new LeaseRef(ctx.id(), ctx.task().revision())), LEASE);
            assertThat(status.getFirst().owned()).isFalse();
            ((TaskExecution) ctx).leaseLost();
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            if (takenOver)
                assertThat(store.unclaim(ctx.id(), ctx.task().revision())).isTrue();
            if (fails) throw new IllegalStateException("body failed");
        });

        execution.run();

        assertThat(runs).hasValue(1);
        var task = store.getTask(execution.taskId()).orElseThrow();
        var expectedOutcome = takenOver ? TaskEvent.Outcome.LOST : TaskEvent.Outcome.APPLIED;
        if (fails) {
            assertThat(events.getLast()).isInstanceOfSatisfying(TaskEvent.Retrying.class, event -> {
                assertThat(event.outcome()).isEqualTo(expectedOutcome);
                assertThat(event.error()).isInstanceOf(IllegalStateException.class);
            });
            assertThat(task.state()).isEqualTo(takenOver ? TaskState.QUEUED : TaskState.SCHEDULED);
            if (!takenOver) assertThat(task.failures()).hasSize(1);
        } else {
            assertThat(events.getLast())
                    .isInstanceOfSatisfying(
                            TaskEvent.Succeeded.class,
                            event -> assertThat(event.outcome()).isEqualTo(expectedOutcome));
            assertThat(task.state()).isEqualTo(takenOver ? TaskState.QUEUED : TaskState.SUCCEEDED);
            assertThat(task.failures()).isNull();
            if (!takenOver) {
                assertThat(store.claim(new ClaimRequest(List.of(store.queueId("Q:DEF")), 1, LEASE, 1, null)))
                        .isEmpty();
            }
        }
        assertThat(task.attempts()).isEqualTo(takenOver ? 0 : 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leaseLossAfterTheBodyStopsStillLetsTheStoreDecideTheFailure(boolean optedIn) {
        var current = new AtomicReference<TaskExecution>();
        var owned = new AtomicReference<Boolean>();
        retryPolicy = (task, failure) -> {
            clock.advance(LEASE.plusSeconds(1));
            owned.set(store.renew(List.of(new LeaseRef(task.id(), task.revision())), LEASE)
                    .getFirst()
                    .owned());
            current.get().leaseLost();
            return RetryPolicy.Decision.retryAfter(Duration.ZERO);
        };
        var execution = execution(optedIn ? TYPE : TaskType.of("non-interruptible"), (ctx, payload) -> {
            throw new IllegalStateException("body failed");
        });
        current.set(execution);

        execution.run();

        assertThat(owned.get()).isFalse();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        assertThat(events.getLast())
                .isInstanceOfSatisfying(
                        TaskEvent.Retrying.class,
                        event -> assertThat(event.outcome()).isEqualTo(TaskEvent.Outcome.APPLIED));
        var task = store.getTask(execution.taskId()).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.SCHEDULED);
        assertThat(task.failures()).hasSize(1);
    }

    @Test
    void leaseLostBeforeStartSkipsTheBodyAndDoesNotInterruptTheThread() {
        var ran = new AtomicBoolean();
        var execution = execution(TYPE, (ctx, payload) -> ran.set(true));

        execution.leaseLost();
        execution.run();
        execution.leaseLost();

        assertThat(ran).isFalse();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        assertThat(interruptedAtFinish).isFalse();
        assertThat(store.getTask(execution.taskId()).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(events).singleElement().isInstanceOf(TaskEvent.Started.class);
    }

    @Test
    void cancelInterruptIsClearedBeforeTheThreadIsReused() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var execution = execution(TYPE, (ctx, payload) -> {
            started.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        });
        var after = new AtomicReference<Boolean>();
        var worker = new Thread(() -> {
            execution.run();
            execution.requestCancel("late cancel");
            execution.leaseLost();
            after.set(Thread.currentThread().isInterrupted());
        });
        worker.start();
        try {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            execution.requestCancel("stop");
            worker.join(5_000);

            assertThat(worker.isAlive()).isFalse();
            assertThat(after.get()).isFalse();
            assertThat(interruptedAtFinish).isFalse();
            assertThat(store.getTask(execution.taskId()).orElseThrow().state()).isEqualTo(TaskState.CANCELLED);
            assertThat(events.getLast()).isInstanceOf(TaskEvent.Cancelled.class);
        } finally {
            release.countDown();
            worker.join(5_000);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anInterruptAlreadyInFlightCannotReachTheNextTask(boolean cancel) throws Exception {
        var started = new CountDownLatch(1);
        var releaseBody = new CountDownLatch(1);
        var returning = new CountDownLatch(1);
        var interruptEntered = new CountDownLatch(1);
        var deliverInterrupt = new CountDownLatch(1);
        var nextStarted = new CountDownLatch(1);
        var releaseNext = new CountDownLatch(1);
        var nextInterrupted = new AtomicBoolean();
        var execution = execution(TYPE, (ctx, payload) -> {
            started.countDown();
            releaseBody.await(10, TimeUnit.SECONDS);
            returning.countDown();
        });
        var worker =
                new Thread(() -> {
                    execution.run();
                    nextStarted.countDown();
                    try {
                        releaseNext.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        nextInterrupted.set(true);
                    }
                }) {
                    @Override
                    public void interrupt() {
                        interruptEntered.countDown();
                        try {
                            if (!deliverInterrupt.await(10, TimeUnit.SECONDS))
                                throw new AssertionError("interrupt stalled");
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        super.interrupt();
                    }
                };
        var signaller = new Thread(() -> {
            if (cancel) execution.requestCancel("stop");
            else execution.leaseLost();
        });
        worker.start();
        try {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            signaller.start();
            assertThat(interruptEntered.await(5, TimeUnit.SECONDS)).isTrue();
            releaseBody.countDown();
            assertThat(returning.await(5, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (worker.getState() != Thread.State.WAITING
                    && worker.getState() != Thread.State.TIMED_WAITING
                    && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            assertThat(worker.getState()).isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING);
            assertThat(nextStarted.getCount()).isEqualTo(1);

            deliverInterrupt.countDown();
            assertThat(nextStarted.await(5, TimeUnit.SECONDS)).isTrue();
            releaseNext.countDown();
            worker.join(5_000);
            signaller.join(5_000);

            assertThat(worker.isAlive()).isFalse();
            assertThat(signaller.isAlive()).isFalse();
            assertThat(nextInterrupted).isFalse();
            assertThat(interruptedAtFinish).isFalse();
        } finally {
            releaseBody.countDown();
            deliverInterrupt.countDown();
            releaseNext.countDown();
            worker.join(5_000);
            signaller.join(5_000);
        }
    }

    private TaskExecution execution(TaskType<Void> type, TaskProcessor<Void> processor) {
        registry.register(type, processor);
        long id = client.enqueue(TaskRequest.of(type).build()).id();
        var record = store.claim(new ClaimRequest(List.of(store.queueId("Q:DEF")), 1, LEASE, 1, null))
                .getFirst();
        assertThat(record.id()).isEqualTo(id);
        var settings = new TaskExecution.Settings(
                store,
                store,
                new Names(store),
                new Codecs(null),
                registry,
                RetryPolicies.of(retryPolicy),
                new Observers(events::add),
                clock,
                new RunnerStatus(clock),
                true,
                false,
                true,
                FailureDetail.FULL);
        return new TaskExecution(record, settings, e -> {
            if (Thread.currentThread().isInterrupted()) interruptedAtFinish.set(true);
        });
    }
}

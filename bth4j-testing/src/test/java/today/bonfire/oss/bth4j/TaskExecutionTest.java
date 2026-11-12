package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
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
import today.bonfire.oss.bth4j.exceptions.TaskCancelledException;
import today.bonfire.oss.bth4j.exceptions.TaskLeaseLostException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.spi.TaskStoreException;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

@Timeout(15)
class TaskExecutionTest {
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final TaskType<Void> TYPE = TaskType.of("lease-loss").interruptOnCancel();

    private final MutableClock clock = new MutableClock();
    private final InMemoryStore store = new InMemoryStore(clock);
    private final TaskClient client = TaskClient.builder()
            .queues(Queues.of("Q:DEF"))
            .store(store)
            .clock(clock)
            .build();
    private final TaskProcessorRegistry registry = new TaskProcessorRegistry();
    private final List<TaskEvent> events = new ArrayList<>();
    private final AtomicBoolean interruptedAtFinish = new AtomicBoolean();
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final List<String> storeCalls = new ArrayList<>();
    private RetryPolicy retryPolicy = RetryPolicy.fixed(Duration.ZERO);
    private FullStore executionStore = store;

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
            assertThat(events).hasSize(4);
            assertThat(events.get(0)).isInstanceOf(TaskEvent.Started.class);
            assertThat(events.get(1)).isInstanceOfSatisfying(TaskEvent.Retrying.class, event -> {
                assertThat(event.outcome()).isEqualTo(TaskEvent.Outcome.LOST);
                assertThat(event.error()).isInstanceOf(TaskLeaseLostException.class);
                assertThat(event.retryAt()).isEqualTo(clock.instant());
            });
            assertThat(events.get(2)).isInstanceOf(TaskEvent.Started.class);
            assertThat(events.get(3)).isInstanceOf(TaskEvent.Succeeded.class);
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
                assertThat(store.claim(new ClaimRequest(
                                List.of(new QueueGrant(store.queueId("Q:DEF"), 1, 1)), 1, LEASE, 1, null)))
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
        assertThat(events).hasSize(2);
        assertThat(events.getFirst()).isInstanceOf(TaskEvent.Started.class);
        assertThat(events.getLast())
                .isInstanceOfSatisfying(
                        TaskEvent.Retrying.class,
                        event -> assertThat(event.outcome()).isEqualTo(TaskEvent.Outcome.LOST));
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

    @Test
    void aCancelRequestPassedOnAgainDoesNotInterruptTheBodyAgain() throws Exception {
        var started = new CountDownLatch(1);
        var firstInterrupt = new CountDownLatch(1);
        var cleanup = new CountDownLatch(1);
        var interrupts = new AtomicInteger();
        var execution = execution(TYPE, (ctx, payload) -> {
            started.countDown();
            try {
                cleanup.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupts.incrementAndGet();
                firstInterrupt.countDown();
            }
            try {
                cleanup.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupts.incrementAndGet();
            }
        });
        var worker = new Thread(execution::run);
        worker.start();
        try {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            execution.requestCancel("stop");
            assertThat(firstInterrupt.await(5, TimeUnit.SECONDS)).isTrue();

            execution.requestCancel(null);
            execution.requestCancel(null);
            cleanup.countDown();
            worker.join(5_000);

            assertThat(worker.isAlive()).isFalse();
            assertThat(interrupts).hasValue(1);
            assertThat(interruptedAtFinish).isFalse();
        } finally {
            cleanup.countDown();
            worker.join(5_000);
        }
    }

    @Test
    void aBodyThatReschedulesItselfWhileACancelIsRequestedEndsCancelled() {
        var execution = execution(TaskType.of("reschedule-cancel"), (ctx, payload) -> {
            throw new TaskRescheduleException(Duration.ofMinutes(5));
        });
        execution.requestCancel("operator stop");

        execution.run();

        var task = store.getTask(execution.taskId()).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(task.cancelReason()).isEqualTo("operator stop");
        assertThat(events.getLast()).isInstanceOf(TaskEvent.Cancelled.class);
        assertThat(events).noneMatch(e -> e instanceof TaskEvent.Rescheduled);
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

    enum Transition {
        ACK("ack", TaskState.SUCCEEDED),
        RETRY("fail", TaskState.SCHEDULED),
        DIE("fail", TaskState.DEAD),
        RESCHEDULE("reschedule", TaskState.SCHEDULED),
        CANCEL("finishCancelled", TaskState.CANCELLED);

        final String storeMethod;
        final TaskState committedState;

        Transition(String storeMethod, TaskState committedState) {
            this.storeMethod = storeMethod;
            this.committedState = committedState;
        }

        void exit() throws Exception {
            switch (this) {
                case ACK -> {}
                case RETRY -> throw new IllegalStateException("body failed");
                case DIE -> throw new TaskUnrecoverableException("never again");
                case RESCHEDULE -> throw new TaskRescheduleException(Duration.ofSeconds(30));
                case CANCEL -> throw new TaskCancelledException("stop");
            }
        }
    }

    enum Fault {
        BEFORE_COMMIT,
        AFTER_COMMIT
    }

    @ParameterizedTest
    @EnumSource(Transition.class)
    void aTransitionTheStoreRefusesIsLostAndChangesNothingMore(Transition transition) {
        var execution = execution(TaskType.of("refused"), (ctx, payload) -> {
            assertThat(store.unclaim(ctx.id(), ctx.task().revision())).isTrue();
            transition.exit();
        });

        execution.run();

        assertThat(lastOutcome()).isEqualTo(TaskEvent.Outcome.LOST);
        var task = store.getTask(execution.taskId()).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.QUEUED);
        assertThat(task.failures()).isNull();
    }

    @ParameterizedTest
    @EnumSource(Transition.class)
    void aTransitionThatFailsBeforeItCommitsIsUnknownAndExpiryRecoversTheTask(Transition transition) {
        executionStore = faulty(transition.storeMethod, Fault.BEFORE_COMMIT);
        var execution = execution(TaskType.of("lost-before"), (ctx, payload) -> transition.exit());

        execution.run();

        assertThat(lastOutcome()).isEqualTo(TaskEvent.Outcome.UNKNOWN);
        var task = store.getTask(execution.taskId()).orElseThrow();
        assertThat(task.state()).isEqualTo(TaskState.LEASED);
        assertThat(task.revision()).isEqualTo(execution.revision());
        assertThat(task.failures()).isNull();
        assertThat(storeCalls).doesNotContain(transition.storeMethod);

        clock.advance(LEASE.plusSeconds(1));
        var expired = store.reclaim(10);
        assertThat(expired).singleElement().satisfies(e -> {
            assertThat(e.id()).isEqualTo(execution.taskId());
            assertThat(e.revision()).isEqualTo(execution.revision());
        });
        var failure = new FailureInfo(1, clock.instant(), "lease-expired", "lease expired", null);
        assertThat(store.failExpired(execution.taskId(), execution.revision(), failure, clock.instant(), true, null))
                .isEqualTo(FailOutcome.RETRY);
        store.promote(10);
        assertThat(store.claim(
                        new ClaimRequest(List.of(new QueueGrant(store.queueId("Q:DEF"), 1, 1)), 1, LEASE, 1, null)))
                .singleElement()
                .satisfies(r -> assertThat(r.id()).isEqualTo(execution.taskId()));
    }

    @ParameterizedTest
    @EnumSource(Transition.class)
    void aTransitionCommittedWithALostResponseIsUnknownAndKeepsItsStateWithoutACompensatingWrite(
            Transition transition) {
        executionStore = faulty(transition.storeMethod, Fault.AFTER_COMMIT);
        var execution = execution(TaskType.of("lost-after"), (ctx, payload) -> transition.exit());

        execution.run();

        assertThat(lastOutcome()).isEqualTo(TaskEvent.Outcome.UNKNOWN);
        var task = store.getTask(execution.taskId()).orElseThrow();
        assertThat(task.state()).isEqualTo(transition.committedState);
        assertThat(task.revision()).isEqualTo(execution.revision() + 1);
        assertThat(storeCalls).filteredOn(c -> !c.equals("getTask")).containsExactly(transition.storeMethod);
        if (transition == Transition.RETRY || transition == Transition.DIE)
            assertThat(task.failures()).hasSize(1);
        if (transition == Transition.RESCHEDULE) {
            assertThat(task.due()).isEqualTo(clock.instant().plusSeconds(30));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = Transition.class,
            names = {"ACK", "DIE", "CANCEL"})
    void aFixedDelayOccurrenceStaysParkedWhenItsTransitionFailedBeforeCommitting(Transition transition) {
        executionStore = faulty(transition.storeMethod, Fault.BEFORE_COMMIT);
        var execution = fixedDelayExecution(TaskType.of("parked"), (ctx, payload) -> transition.exit());

        execution.run();

        assertThat(lastOutcome()).isEqualTo(TaskEvent.Outcome.UNKNOWN);
        var schedule = store.recurringDefinitions().getFirst();
        assertThat(schedule.definition().paused()).isFalse();
        assertThat(schedule.nextFire()).isAfter(clock.instant().plus(Duration.ofDays(365)));
        assertThat(storeCalls).filteredOn(c -> !c.equals("getTask")).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(
            value = Transition.class,
            names = {"ACK", "DIE", "CANCEL"})
    void aFixedDelayOccurrenceKeepsItsScheduleEffectWhenTheResponseWasLost(Transition transition) {
        executionStore = faulty(transition.storeMethod, Fault.AFTER_COMMIT);
        var execution = fixedDelayExecution(TaskType.of("effect-kept"), (ctx, payload) -> transition.exit());

        execution.run();

        assertThat(lastOutcome()).isEqualTo(TaskEvent.Outcome.UNKNOWN);
        var schedule = store.recurringDefinitions().getFirst();
        if (transition == Transition.ACK) {
            assertThat(schedule.definition().paused()).isFalse();
            assertThat(schedule.nextFire()).isEqualTo(clock.instant().plus(Duration.ofMinutes(1)));
        } else {
            assertThat(schedule.definition().paused()).isTrue();
        }
        assertThat(storeCalls).filteredOn(c -> !c.equals("getTask")).containsExactly(transition.storeMethod);
        assertThat(store.getTask(execution.taskId()).orElseThrow().state()).isEqualTo(transition.committedState);
    }

    @ParameterizedTest
    @EnumSource(Transition.class)
    void theSlotIsFreedOnceTheProcessorReturnsAndBeforeTheOutcomeIsWritten(Transition transition) {
        var order = new ArrayList<String>();
        executionStore = recording(order);
        var execution = execution(TaskType.of("slot-" + transition), (ctx, payload) -> {
            order.add("processor");
            transition.exit();
        });

        execution.run(() -> order.add("slot freed"));

        assertThat(order).containsSubsequence("processor", "slot freed", transition.storeMethod);
        assertThat(order).containsOnlyOnce("slot freed");
        assertThat(store.getTask(execution.taskId()).orElseThrow().state()).isEqualTo(transition.committedState);
    }

    @Test
    void aTaskWithoutAProcessorFreesItsSlotBeforeItIsDeferred() {
        var order = new ArrayList<String>();
        executionStore = recording(order);
        client.enqueue(TaskRequest.of(TaskType.of("unregistered")).build());
        var record = store.claim(
                        new ClaimRequest(List.of(new QueueGrant(store.queueId("Q:DEF"), 1, 1)), 1, LEASE, 1, null))
                .getFirst();

        new TaskExecution(record, settings(), e -> {}).run(() -> order.add("slot freed"));

        assertThat(order).containsSubsequence("slot freed", "reschedule");
        assertThat(store.getTask(record.id()).orElseThrow().state()).isEqualTo(TaskState.SCHEDULED);
    }

    private TaskEvent.Outcome lastOutcome() {
        return switch (events.getLast()) {
            case TaskEvent.Succeeded e -> e.outcome();
            case TaskEvent.Retrying e -> e.outcome();
            case TaskEvent.Died e -> e.outcome();
            case TaskEvent.Rescheduled e -> e.outcome();
            case TaskEvent.Cancelled e -> e.outcome();
            default -> throw new AssertionError("no outcome on " + events.getLast());
        };
    }

    /** A store that fails the named call before it reaches the real store, or after the real store applied it. */
    private FullStore faulty(String method, Fault fault) {
        return (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, m, args) -> {
                    // an acknowledgement without a schedule effect goes through ackAll
                    boolean target = m.getName().equals(method)
                            || (method.equals("ack") && m.getName().equals("ackAll"));
                    if (target && fault == Fault.BEFORE_COMMIT) throw new TaskStoreException("lost before commit");
                    Object result;
                    try {
                        result = m.invoke(store, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    storeCalls.add(target ? method : m.getName());
                    if (target) throw new TaskStoreException("reply lost");
                    return result;
                });
    }

    /** The real store, noting each outcome call in {@code order}; an acknowledgement goes through ackAll. */
    private FullStore recording(List<String> order) {
        return (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, m, args) -> {
                    if (!m.getName().equals("getTask")) order.add(m.getName().equals("ackAll") ? "ack" : m.getName());
                    try {
                        return m.invoke(store, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private TaskExecution fixedDelayExecution(TaskType<Void> type, TaskProcessor<Void> processor) {
        registry.register(type, processor);
        ScheduleFixtures.fixedDelayOccurrence(
                store, client, clock.instant(), store.typeId(type.name()), store.queueId("Q:DEF"), 3);
        var record = store.claim(
                        new ClaimRequest(List.of(new QueueGrant(store.queueId("Q:DEF"), 1, 1)), 1, LEASE, 1, null))
                .getFirst();
        return new TaskExecution(record, settings(), e -> {});
    }

    private TaskExecution execution(TaskType<Void> type, TaskProcessor<Void> processor) {
        registry.register(type, processor);
        long id = client.enqueue(TaskRequest.of(type).build()).id();
        var record = store.claim(
                        new ClaimRequest(List.of(new QueueGrant(store.queueId("Q:DEF"), 1, 1)), 1, LEASE, 1, null))
                .getFirst();
        assertThat(record.id()).isEqualTo(id);
        return new TaskExecution(record, settings(), e -> {
            if (Thread.currentThread().isInterrupted()) interruptedAtFinish.set(true);
        });
    }

    private TaskExecution.Settings settings() {
        return new TaskExecution.Settings(
                executionStore,
                executionStore,
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
    }
}

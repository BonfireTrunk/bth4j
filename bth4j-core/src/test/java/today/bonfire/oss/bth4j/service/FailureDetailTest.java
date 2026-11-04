package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.FailureDetail;
import today.bonfire.oss.bth4j.RetryPolicy;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.TaskProcessor;
import today.bonfire.oss.bth4j.TaskProcessorRegistry;
import today.bonfire.oss.bth4j.TaskType;
import today.bonfire.oss.bth4j.exceptions.RedactedFailure;
import today.bonfire.oss.bth4j.exceptions.TaskCancelledException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

class FailureDetailTest {

    private static final String SENTINEL = "customer-4711@example.invalid";
    private static final Instant NOW = Instant.parse("2026-06-01T12:00:00Z");

    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final List<TaskEvent> events = new ArrayList<>();
    private final AtomicReference<FailureInfo> stored = new AtomicReference<>();
    private final AtomicReference<String> cancelledReason = new AtomicReference<>();
    private final List<Throwable> policyCalls = new ArrayList<>();
    private Logger executionLogger;
    private boolean cancelRequested;
    private int finishedCalls;
    private RetryPolicy policy;
    private Error storeFailError;
    private RunnerStatus status;
    private Class<? extends TaskEvent> observerFailsOn;
    private Error observerError;
    private Error scheduleError;
    private boolean fixedDelay;
    private Map<String, String> headers;
    private boolean acked;
    private final AtomicReference<Instant> nextFire = new AtomicReference<>();
    private final AtomicReference<Boolean> paused = new AtomicReference<>();

    @BeforeEach
    void attachAppender() {
        executionLogger = (Logger) LoggerFactory.getLogger(TaskExecution.class);
        logs.start();
        executionLogger.addAppender(logs);
    }

    @AfterEach
    void detachAppender() {
        executionLogger.detachAppender(logs);
    }

    @Test
    void classNameOnlyStripsMessageAndStackFromTheRecord() {
        var info = Failures.info(FailureDetail.CLASS_NAME_ONLY, 2, NOW, new IllegalStateException(SENTINEL));

        assertThat(info.type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(info.attempt()).isEqualTo(2);
        assertThat(info.at()).isEqualTo(NOW);
        assertThat(info.message()).isNull();
        assertThat(info.stack()).isNull();
    }

    @Test
    void fullKeepsMessageAndStackInTheRecord() {
        var info = Failures.info(FailureDetail.FULL, 1, NOW, new IllegalStateException(SENTINEL));

        assertThat(info.message()).isEqualTo(SENTINEL);
        assertThat(info.stack()).contains(SENTINEL).contains("FailureDetailTest");
    }

    @Test
    void classNameOnlyEventCarriesTheClassNameOnly() {
        var event = Failures.forEvent(FailureDetail.CLASS_NAME_ONLY, new IllegalStateException(SENTINEL));

        assertThat(event).isInstanceOf(RedactedFailure.class);
        assertThat(event.getMessage()).isNull();
        assertThat(event.getStackTrace()).isEmpty();
        assertThat(event.toString()).isEqualTo(IllegalStateException.class.getName());
    }

    @Test
    void retryInClassNameOnlyModeKeepsTheSentinelOutOfRecordEventAndLog() {
        run(FailureDetail.CLASS_NAME_ONLY, new IllegalStateException(SENTINEL));

        assertThat(stored.get().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(stored.get().message()).isNull();
        assertThat(stored.get().stack()).isNull();
        var retrying = (TaskEvent.Retrying) events.get(1);
        assertThat(retrying.error()).isInstanceOf(RedactedFailure.class);
        assertThat(retrying.error().toString()).doesNotContain(SENTINEL);
        assertThat(logText()).doesNotContain(SENTINEL).contains(IllegalStateException.class.getName());
    }

    @Test
    void unrecoverableInClassNameOnlyModeKeepsTheSentinelOutOfEventAndLog() {
        run(FailureDetail.CLASS_NAME_ONLY, new TaskUnrecoverableException(SENTINEL));

        var died = (TaskEvent.Died) events.get(1);
        assertThat(died.reason()).doesNotContain(SENTINEL);
        assertThat(died.error().toString()).doesNotContain(SENTINEL);
        assertThat(stored.get().message()).isNull();
        assertThat(logText()).doesNotContain(SENTINEL).contains(TaskUnrecoverableException.class.getName());
    }

    @Test
    void fullModeKeepsTheSentinelInRecordEventAndLog() {
        run(FailureDetail.FULL, new IllegalStateException(SENTINEL));

        assertThat(stored.get().message()).isEqualTo(SENTINEL);
        assertThat(stored.get().stack()).contains(SENTINEL);
        assertThat(((TaskEvent.Retrying) events.get(1)).error()).hasMessage(SENTINEL);
        assertThat(logText()).contains(SENTINEL);
    }

    @Test
    void unrecoverableInFullModeKeepsTheMessageInTheReason() {
        run(FailureDetail.FULL, new TaskUnrecoverableException(SENTINEL));

        assertThat(((TaskEvent.Died) events.get(1)).reason()).contains(SENTINEL);
        assertThat(logText()).contains(SENTINEL);
    }

    @Test
    void cancelledByTheProcessorInClassNameOnlyModeDropsItsMessage() {
        run(FailureDetail.CLASS_NAME_ONLY, new TaskCancelledException(SENTINEL));

        assertThat(cancelledReason.get()).isEqualTo("cancel requested");
        assertThat(((TaskEvent.Cancelled) events.get(1)).reason()).doesNotContain(SENTINEL);
        assertThat(logText()).doesNotContain(SENTINEL);
    }

    @Test
    void cancelledByTheProcessorInFullModeKeepsItsMessage() {
        run(FailureDetail.FULL, new TaskCancelledException(SENTINEL));

        assertThat(cancelledReason.get()).isEqualTo(SENTINEL);
        assertThat(logText()).contains(SENTINEL);
    }

    private String logText() {
        var text = new StringBuilder();
        for (var event : logs.list) {
            text.append(event.getFormattedMessage()).append('\n');
            if (event.getThrowableProxy() != null) {
                text.append(ThrowableProxyUtil.asString(event.getThrowableProxy()))
                        .append('\n');
            }
        }
        return text.toString();
    }

    @Test
    void anErrorGoesThroughTheRetryPolicyAndIsRecorded() {
        run(FailureDetail.FULL, new AssertionError("boom"));

        assertThat(policyCalls).hasSize(1);
        assertThat(policyCalls.get(0)).isInstanceOf(AssertionError.class);
        assertThat(stored.get().type()).isEqualTo(AssertionError.class.getName());
        assertThat(stored.get().message()).isEqualTo("boom");
        assertThat(events.get(1)).isInstanceOf(TaskEvent.Retrying.class);
    }

    @Test
    void anErrorInClassNameOnlyModeKeepsItsMessageOutOfRecordEventAndLog() {
        run(FailureDetail.CLASS_NAME_ONLY, new AssertionError("boom"));

        assertThat(stored.get().type()).isEqualTo(AssertionError.class.getName());
        assertThat(stored.get().message()).isNull();
        assertThat(stored.get().stack()).isNull();
        var retrying = (TaskEvent.Retrying) events.get(1);
        assertThat(retrying.error()).isInstanceOf(RedactedFailure.class);
        assertThat(retrying.error().toString()).doesNotContain("boom");
        assertThat(logText()).doesNotContain("boom").contains(AssertionError.class.getName());
    }

    @Test
    void aStackOverflowErrorIsRecordedAndNotRethrown() {
        run(FailureDetail.FULL, new StackOverflowError());

        assertThat(stored.get().type()).isEqualTo(StackOverflowError.class.getName());
        assertThat(events.get(1)).isInstanceOf(TaskEvent.Retrying.class);
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void anOutOfMemoryErrorIsRecordedThenRethrown() {
        var error = new OutOfMemoryError("heap");

        assertThatThrownBy(() -> run(FailureDetail.FULL, error)).isSameAs(error);

        assertThat(stored.get().type()).isEqualTo(OutOfMemoryError.class.getName());
        assertThat(events.get(1)).isInstanceOf(TaskEvent.Retrying.class);
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void anErrorWhileACancelIsRequestedEndsCancelled() {
        cancelRequested = true;

        run(FailureDetail.FULL, new AssertionError("boom"));

        assertThat(events.get(1)).isInstanceOf(TaskEvent.Cancelled.class);
        assertThat(cancelledReason.get()).isEqualTo("cancel requested");
        assertThat(stored.get()).isNull();
        assertThat(policyCalls).isEmpty();
    }

    @Test
    void anOutOfMemoryErrorInClassNameOnlyModeIsRedactedAndRethrown() {
        var error = new OutOfMemoryError("boom");

        assertThatThrownBy(() -> run(FailureDetail.CLASS_NAME_ONLY, error)).isSameAs(error);

        assertThat(stored.get().message()).isNull();
        assertThat(stored.get().stack()).isNull();
        assertThat(((TaskEvent.Retrying) events.get(1)).error()).isInstanceOf(RedactedFailure.class);
        assertThat(logText()).doesNotContain("boom");
    }

    @Test
    void anInternalErrorIsRethrown() {
        var error = new InternalError("boom");

        assertThatThrownBy(() -> run(FailureDetail.FULL, error)).isSameAs(error);

        assertThat(stored.get().type()).isEqualTo(InternalError.class.getName());
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void aNonFatalErrorFromTheRetryPolicyRetriesAtOnceAndIsAnIssue() {
        policy = (task, error) -> {
            throw new AssertionError("policy");
        };

        run(FailureDetail.FULL, new IllegalStateException("x"));

        assertThat(((TaskEvent.Retrying) events.get(1)).retryAt()).isEqualTo(NOW);
        assertThat(stored.get().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("retry-policy"));
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void aFatalErrorFromTheRetryPolicyIsRecordedThenRethrown() {
        var first = new IllegalStateException("x");
        var fatal = new OutOfMemoryError("policy");
        policy = (task, error) -> {
            throw fatal;
        };

        assertThatThrownBy(() -> run(FailureDetail.FULL, first)).isSameAs(fatal);

        assertThat(stored.get().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(events.get(1)).isInstanceOf(TaskEvent.Retrying.class);
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void aLaterFatalErrorCarriesTheProcessorErrorAsSuppressed() {
        var first = new InternalError("first");
        var later = new OutOfMemoryError("later");
        policy = (task, error) -> {
            throw later;
        };

        assertThatThrownBy(() -> run(FailureDetail.FULL, first)).isSameAs(later);

        assertThat(later.getSuppressed()).containsExactly(first);
    }

    @Test
    void anErrorFromTheStoreLeavesTheTaskLeasedAndDoesNotEscape() {
        storeFailError = new AssertionError("driver");

        run(FailureDetail.FULL, new IllegalStateException("x"));

        assertThat(stored.get()).isNull();
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("store-error"));
        assertThat(logText()).contains("Unable to update the state of task 9");
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void aStoreErrorOnFailReportsTheRetryAsUnknownNotLost() {
        storeFailError = new AssertionError("reply lost");

        run(FailureDetail.FULL, new IllegalStateException("x"));

        assertThat(events.getLast())
                .isInstanceOfSatisfying(
                        TaskEvent.Retrying.class, e -> assertThat(e.outcome()).isEqualTo(TaskEvent.Outcome.UNKNOWN));
        assertThat(status.issues()).noneMatch(i -> i.kind().equals("lease-lost"));
        assertThat(logText()).contains("the outcome is unknown");
    }

    @Test
    void aStoreErrorOnFailOfADyingTaskReportsDiedUnknown() {
        storeFailError = new AssertionError("reply lost");
        policy = (task, error) -> RetryPolicy.Decision.dead("no more");

        run(FailureDetail.FULL, new IllegalStateException("x"));

        assertThat(events.getLast())
                .isInstanceOfSatisfying(
                        TaskEvent.Died.class, e -> assertThat(e.outcome()).isEqualTo(TaskEvent.Outcome.UNKNOWN));
    }

    @Test
    void aStoreErrorOnAckReportsSucceededUnknownNotLost() {
        scheduleError = new AssertionError("reply lost");

        run(FailureDetail.FULL, null);

        assertThat(events.getLast())
                .isInstanceOfSatisfying(
                        TaskEvent.Succeeded.class, e -> assertThat(e.outcome()).isEqualTo(TaskEvent.Outcome.UNKNOWN));
        assertThat(status.issues()).noneMatch(i -> i.kind().equals("lease-lost"));
    }

    @Test
    void anErrorTheRetryPolicyKillsTheTaskWithIsRecordedAsDied() {
        policy = (task, error) -> RetryPolicy.Decision.dead("no more");

        run(FailureDetail.CLASS_NAME_ONLY, new AssertionError("boom"));

        var died = (TaskEvent.Died) events.get(1);
        assertThat(died.error()).isInstanceOf(RedactedFailure.class);
        assertThat(stored.get().type()).isEqualTo(AssertionError.class.getName());
        assertThat(logText()).doesNotContain("boom");
    }

    @Test
    void aPolicyDecisionWithoutAReasonStillRecordsTheFailureInClassNameOnlyMode() {
        policy = (task, error) -> RetryPolicy.Decision.dead(null);

        run(FailureDetail.CLASS_NAME_ONLY, new IllegalStateException());

        var died = (TaskEvent.Died) events.get(1);
        assertThat(died.reason()).isEqualTo("no reason given");
        assertThat(stored.get().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(logText()).contains("no reason given").contains(IllegalStateException.class.getName());
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void aPolicyDecisionWithoutAReasonStillRecordsTheFailureInFullMode() {
        policy = (task, error) -> RetryPolicy.Decision.dead(null);

        run(FailureDetail.FULL, new IllegalStateException());

        var died = (TaskEvent.Died) events.get(1);
        assertThat(died.reason()).isEqualTo("no reason given");
        assertThat(stored.get().type()).isEqualTo(IllegalStateException.class.getName());
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void aFatalErrorFromTheStoreFailStillEmitsRetryingUnknownOnceAndIsRethrown() {
        var error = new OutOfMemoryError("store");
        storeFailError = error;

        assertThatThrownBy(() -> run(FailureDetail.FULL, new IllegalStateException("x")))
                .isSameAs(error);

        assertThat(events.stream().filter(TaskEvent.Retrying.class::isInstance))
                .singleElement()
                .satisfies(e -> assertThat(((TaskEvent.Retrying) e).outcome()).isEqualTo(TaskEvent.Outcome.UNKNOWN));
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void aNonFatalErrorFromTheObserverIsSwallowedAndTheScheduleIsRearmed() {
        observerFailsOn = TaskEvent.Succeeded.class;
        observerError = new AssertionError("observer");
        fixedDelay = true;

        run(FailureDetail.FULL, null);

        assertThat(nextFire.get()).isEqualTo(NOW.plus(Duration.ofMinutes(1)));
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void aFatalErrorFromTheObserverIsRethrownAfterTheScheduleIsPaused() {
        observerFailsOn = TaskEvent.Died.class;
        var error = new OutOfMemoryError("observer");
        observerError = error;
        fixedDelay = true;
        policy = (task, e) -> RetryPolicy.Decision.dead("no more");

        assertThatThrownBy(() -> run(FailureDetail.FULL, new IllegalStateException("x")))
                .isSameAs(error);

        assertThat(paused.get()).isTrue();
        assertThat(finishedCalls).isEqualTo(1);
    }

    @Test
    void anOrdinaryTaskWithAnInvalidFixedDelayHeaderIsStillAcknowledged() {
        headers = Map.of(RecurringTaskService.FIXED_DELAY_HEADER, "invalid");

        run(FailureDetail.FULL, null);

        assertThat(acked).isTrue();
        assertThat(nextFire.get()).isNull();
        assertThat(events.get(1)).isInstanceOf(TaskEvent.Succeeded.class);
        assertThat(status.issues()).noneMatch(i -> i.kind().equals("schedule-rearm"));
    }

    @Test
    void anOccurrenceWithAnUnreadableFixedDelayHeaderIsAcknowledgedWithoutARearmAndRaisesAnIssue() {
        fixedDelay = true;
        headers = Map.of(RecurringTaskService.FIXED_DELAY_HEADER, "invalid");

        run(FailureDetail.FULL, null);

        assertThat(acked).isTrue();
        assertThat(nextFire.get()).isNull();
        assertThat(events.get(1)).isInstanceOf(TaskEvent.Succeeded.class);
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("schedule-rearm"));
        assertThat(logText()).contains("not re-armed");
    }

    @Test
    void anErrorFromTheScheduleUpdateLeavesTheTaskSucceededAndIsAStoreError() {
        scheduleError = new NoClassDefFoundError("x");
        fixedDelay = true;

        run(FailureDetail.FULL, null);

        assertThat(events.get(1)).isInstanceOf(TaskEvent.Succeeded.class);
        assertThat(status.issues()).anyMatch(i -> i.kind().equals("store-error"));
        assertThat(finishedCalls).isEqualTo(1);
    }

    private void run(FailureDetail detail, Throwable failure) {
        var type = TaskType.of("boom");
        TaskProcessor<Void> processor = (ctx, payload) -> {
            if (failure instanceof Error error) throw error;
            if (failure != null) throw (RuntimeException) failure;
        };
        var registry = new TaskProcessorRegistry().register(type, processor);
        var store = (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, method, args) -> {
                    return switch (method.getName()) {
                        case "queueNames" -> Map.of(1, "Q:DEF");
                        case "eventNames" -> Map.of(1, "boom");
                        case "ack" -> {
                            if (scheduleError != null) throw scheduleError;
                            acked = true;
                            var effect = (ScheduleEffect) args[4];
                            if (effect != null && !effect.pauses()) {
                                nextFire.set(effect.nextFire());
                                effect.markApplied();
                            }
                            yield true;
                        }
                        case "getTask" -> java.util.Optional.empty();
                        case "finishCancelled" -> {
                            cancelledReason.set((String) args[2]);
                            yield true;
                        }
                        case "fail" -> {
                            if (storeFailError != null) throw storeFailError;
                            stored.set((FailureInfo) args[2]);
                            var dead = args[3] == null;
                            var effect = (ScheduleEffect) args[5];
                            if (dead && effect != null && effect.pauses()) {
                                paused.set(true);
                                effect.markApplied();
                            }
                            yield dead ? FailOutcome.DEAD : FailOutcome.RETRY;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
        var clock = Clock.fixed(NOW, ZoneOffset.UTC);
        status = new RunnerStatus(clock);
        var settings = new TaskExecution.Settings(
                store,
                store,
                new Names(store),
                new Codecs(null),
                registry,
                RetryPolicies.of((task, error) -> {
                    policyCalls.add(error);
                    return policy == null
                            ? RetryPolicy.Decision.retryAfter(Duration.ofSeconds(1))
                            : policy.decide(task, error);
                }),
                new Observers(event -> {
                    events.add(event);
                    if (observerError != null && observerFailsOn.isInstance(event)) throw observerError;
                }),
                clock,
                status,
                true,
                false,
                true,
                detail);
        var record = TaskRecord.builder()
                .id(9)
                .revision(1)
                .queueId(1)
                .eventId(1)
                .state(TaskState.LEASED)
                .attempts(1)
                .scheduleId(fixedDelay ? "sched" : null)
                .headers(
                        headers != null
                                ? headers
                                : fixedDelay ? Map.of(RecurringTaskService.FIXED_DELAY_HEADER, "PT1M") : null)
                .cancelRequestedAt(cancelRequested ? NOW : null)
                .maxAttempts(3)
                .build();
        new TaskExecution(record, settings, execution -> finishedCalls++).run();
    }
}

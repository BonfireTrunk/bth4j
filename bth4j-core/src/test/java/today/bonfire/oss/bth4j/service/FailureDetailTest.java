package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;

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
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

class FailureDetailTest {

    private static final String SENTINEL = "customer-4711@example.invalid";
    private static final Instant NOW = Instant.parse("2026-06-01T12:00:00Z");

    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final List<TaskEvent> events = new ArrayList<>();
    private final AtomicReference<FailureInfo> stored = new AtomicReference<>();
    private final AtomicReference<String> cancelledReason = new AtomicReference<>();
    private Logger executionLogger;

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

    private void run(FailureDetail detail, RuntimeException failure) {
        var type = TaskType.of("boom");
        TaskProcessor<Void> processor = (ctx, payload) -> {
            throw failure;
        };
        var registry = new TaskProcessorRegistry().register(type, processor);
        var store = (FullStore) Proxy.newProxyInstance(
                FullStore.class.getClassLoader(), new Class<?>[] {FullStore.class}, (proxy, method, args) -> {
                    return switch (method.getName()) {
                        case "queueNames" -> Map.of(1, "Q:DEF");
                        case "eventNames" -> Map.of(1, "boom");
                        case "getTask" -> java.util.Optional.empty();
                        case "finishCancelled" -> {
                            cancelledReason.set((String) args[2]);
                            yield true;
                        }
                        case "fail" -> {
                            stored.set((FailureInfo) args[2]);
                            yield args[3] == null ? FailOutcome.DEAD : FailOutcome.RETRY;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
        var clock = Clock.fixed(NOW, ZoneOffset.UTC);
        var settings = new TaskExecution.Settings(
                store,
                store,
                store,
                new Names(store),
                new Codecs(null),
                registry,
                RetryPolicies.of(RetryPolicy.fixed(Duration.ofSeconds(1))),
                new Observers(events::add),
                clock,
                new RunnerStatus(clock),
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
                .maxAttempts(3)
                .build();
        new TaskExecution(record, settings, execution -> {}).run();
    }
}

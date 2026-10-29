package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.common.Keys;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;

class TaskRunnerWrapperTest {

    /** Records the transitions in the list shared with the callbacks, so the order of both can be asserted. */
    private static class FakeTaskOps extends TaskOps {
        private final List<String> events;
        boolean ackResult = true;
        RuntimeException ackError = null;
        final List<Duration> failDelays = new ArrayList<>();
        final List<String> failMessages = new ArrayList<>();
        final List<Instant> rescheduledTo = new ArrayList<>();

        FakeTaskOps(List<String> events) {
            super(
                    new Keys("T"),
                    null,
                    null,
                    TestEvents.UNKNOWN::from,
                    new QueuesHolder(Set.of("Q:DEF"), List.of("Q:DEF"), "Q:DEF"),
                    Duration.ofDays(1));
            this.events = events;
        }

        @Override
        boolean ack(Task task) {
            events.add("ack");
            if (ackError != null) throw ackError;
            return ackResult;
        }

        @Override
        FailOutcome fail(Task task, Duration retryDelay, String error) {
            events.add("fail");
            failDelays.add(retryDelay);
            failMessages.add(error);
            return FailOutcome.RETRY;
        }

        @Override
        boolean reschedule(Task task, Instant due) {
            events.add("reschedule");
            rescheduledTo.add(due);
            return true;
        }
    }

    private final List<String> events = new ArrayList<>();
    private FakeTaskOps ops;
    private Task task;

    private Consumer<Task> beforeStart = t -> events.add("before");
    private Consumer<Task> onSuccess = t -> events.add("success");
    private BiConsumer<Task, Exception> onError = (t, e) -> events.add("error");
    private Consumer<Task> afterTask = t -> events.add("after");
    private Function<Integer, Duration> retryDelay = n -> Duration.ofSeconds(7);
    private final List<Integer> retryDelayArgs = new ArrayList<>();

    @BeforeEach
    void setUp() {
        events.clear();
        ops = new FakeTaskOps(events);
        task = Task.Builder.newTask("id")
                .event(TestEvents.REGULAR_TASK)
                .accountId("a")
                .build();
    }

    private void run(Consumer<Task> handler) {
        var callbacks = new TaskCallbacks(beforeStart, onSuccess, onError, afterTask);
        Function<Integer, Duration> recording = n -> {
            retryDelayArgs.add(n);
            return retryDelay.apply(n);
        };
        new TaskRunnerWrapper(task, handler, callbacks, ops, recording, t -> events.add("finished")).run();
    }

    @Test
    void successAcksThenCallsOnSuccessAfterTaskAndOnFinished() {
        run(t -> events.add("handler"));
        assertThat(events).containsExactly("before", "handler", "ack", "success", "finished", "after");
    }

    @Test
    void ackReturningFalseIsOnlyLogged() {
        ops.ackResult = false;
        assertThatCode(() -> run(t -> {})).doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "ack", "success", "finished", "after");
    }

    @Test
    void runtimeExceptionFromAckIsSwallowedAndAfterTaskStillRuns() {
        ops.ackError = new IllegalStateException("redis down");
        assertThatCode(() -> run(t -> {})).doesNotThrowAnyException();
        assertThat(events).contains("ack", "finished", "after");
    }

    @Test
    void failingOnSuccessCallbackDoesNotChangeTheOutcome() {
        onSuccess = t -> {
            events.add("success");
            throw new IllegalStateException("callback bug");
        };
        assertThatCode(() -> run(t -> {})).doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "ack", "success", "finished", "after");
    }

    @Test
    void unrecoverableExceptionFailsWithoutDelay() {
        run(t -> {
            throw new TaskUnrecoverableException("fatal");
        });
        assertThat(events).containsExactly("before", "fail", "error", "finished", "after");
        assertThat(ops.failDelays).containsExactly((Duration) null);
        assertThat(ops.failMessages.getFirst()).contains("fatal");
    }

    @Test
    void dataExceptionFailsWithoutDelay() {
        run(t -> {
            throw new TaskDataException("bad data");
        });
        assertThat(events).containsExactly("before", "fail", "error", "finished", "after");
        assertThat(ops.failDelays).containsExactly((Duration) null);
    }

    @Test
    void rescheduleExceptionReschedulesAboutDelaySecondsFromNow() {
        var before = Instant.now();
        run(t -> {
            throw new TaskRescheduleException(30);
        });
        var after = Instant.now();

        assertThat(events).containsExactly("before", "reschedule", "error", "finished", "after");
        assertThat(ops.rescheduledTo).hasSize(1);
        assertThat(ops.rescheduledTo.getFirst()).isBetween(before.plusSeconds(30), after.plusSeconds(30));
    }

    @Test
    void genericExceptionFailsWithTheRetryDelayOfTheAttempt() {
        run(t -> {
            throw new IllegalStateException("boom");
        });
        assertThat(events).containsExactly("before", "fail", "error", "finished", "after");
        assertThat(retryDelayArgs).containsExactly(task.attempts());
        assertThat(ops.failDelays).containsExactly(Duration.ofSeconds(7));
        assertThat(ops.failMessages.getFirst())
                .contains("IllegalStateException")
                .contains("boom");
    }

    @Test
    void nullRetryDelayMeansNoDelay() {
        retryDelay = n -> null;
        run(t -> {
            throw new IllegalStateException("boom");
        });
        assertThat(ops.failDelays).containsExactly(Duration.ZERO);
    }

    @Test
    void failingOnErrorCallbackDoesNotChangeTheOutcome() {
        onError = (t, e) -> {
            events.add("error");
            throw new IllegalStateException("callback bug");
        };
        assertThatCode(() -> run(t -> {
                    throw new TaskUnrecoverableException("fatal");
                }))
                .doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "fail", "error", "finished", "after");
    }

    @Test
    void errorCallbackReceivesTheOriginalException() {
        var seen = new ArrayList<Exception>();
        onError = (t, e) -> seen.add(e);
        var failure = new IllegalStateException("boom");
        run(t -> {
            throw failure;
        });
        assertThat(seen).containsExactly(failure);
    }

    @Test
    void exceptionFromBeforeStartCountsAsFailureAndSkipsTheHandler() {
        beforeStart = t -> {
            events.add("before");
            throw new IllegalStateException("callback bug");
        };
        run(t -> events.add("handler"));
        assertThat(events).containsExactly("before", "fail", "error", "finished", "after");
    }

    @Test
    void afterTaskRunsEvenWhenItFails() {
        afterTask = t -> {
            events.add("after");
            throw new IllegalStateException("callback bug");
        };
        assertThatCode(() -> run(t -> {})).doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "ack", "success", "finished", "after");
    }

    @Test
    void runtimeExceptionFromFailIsSwallowed() {
        var failing = new FakeTaskOps(events) {
            @Override
            FailOutcome fail(Task task, Duration retryDelay, String error) {
                throw new IllegalStateException("redis down");
            }
        };
        ops = failing;
        assertThatCode(() -> run(t -> {
                    throw new IllegalStateException("boom");
                }))
                .doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "error", "finished", "after");
    }
}

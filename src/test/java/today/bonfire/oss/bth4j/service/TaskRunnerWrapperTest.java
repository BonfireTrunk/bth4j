package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;

class TaskRunnerWrapperTest {

    /** Records Redis operations in the list shared with the callbacks, so the order of both can be asserted. */
    private static class FakeTaskOps extends TaskOps {
        private final List<String> events;
        final List<Task> addedTasks = new ArrayList<>();
        final List<Object> addedData = new ArrayList<>();
        final List<Boolean> addedDataRaw = new ArrayList<>();
        String storedData = null;
        boolean failOnDelete = false;
        boolean failOnDeadMove = false;

        FakeTaskOps(List<String> events) {
            super(null, null, null, null, null);
            this.events = events;
        }

        @Override
        void deleteTaskFromInProgressQueue(Task task) {
            if (failOnDelete) throw new IllegalStateException("redis down");
            events.add("delete:" + task.uniqueId());
        }

        @Override
        void moveToDeadQueue(Task task) {
            if (failOnDeadMove) throw new IllegalStateException("redis down");
            events.add("dead:" + task.uniqueId());
        }

        @Override
        void addTaskToQueue(Task task, Object data, boolean isDataRaw) {
            events.add("add");
            addedTasks.add(task);
            addedData.add(data);
            addedDataRaw.add(isDataRaw);
        }

        @Override
        public String getDataForTask(String uniqueId) {
            return storedData;
        }
    }

    private final List<String> events = new ArrayList<>();
    private FakeTaskOps ops;
    private Task task;

    private Consumer<Task> beforeStart = t -> events.add("before");
    private Consumer<Task> onSuccess = t -> events.add("success");
    private BiConsumer<Task, Exception> onError = (t, e) -> events.add("error");
    private Consumer<Task> afterTask = t -> events.add("after");

    @BeforeEach
    void setUp() {
        events.clear();
        ops = new FakeTaskOps(events);
        task = Task.Builder.newTask("t1")
                .accountId("acct")
                .event(TestEvents.REGULAR_TASK)
                .build();
    }

    private void run(Consumer<Task> handler) {
        var callbacks = new TaskCallbacks(beforeStart, onSuccess, onError, afterTask);
        new TaskRunnerWrapper(task, handler, callbacks, ops).run();
    }

    @Test
    void successDeletesTaskThenCallsOnSuccess() {
        run(t -> events.add("handler"));
        assertThat(events).containsExactly("before", "handler", "delete:t1", "success", "after");
    }

    @Test
    void failingOnSuccessCallbackDoesNotMoveCompletedTaskToDeadQueue() {
        onSuccess = t -> {
            events.add("success");
            throw new IllegalStateException("callback bug");
        };
        assertThatCode(() -> run(t -> {})).doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "delete:t1", "success", "after");
    }

    @Test
    void unrecoverableExceptionMovesTaskToDeadQueue() {
        run(t -> {
            throw new TaskUnrecoverableException("fatal");
        });
        assertThat(events).containsExactly("before", "dead:t1", "error", "after");
    }

    @Test
    void dataExceptionMovesTaskToDeadQueue() {
        run(t -> {
            throw new TaskDataException("bad data");
        });
        assertThat(events).containsExactly("before", "dead:t1", "error", "after");
    }

    @Test
    void rescheduleExceptionQueuesNewTaskAndDeletesOldOne() {
        ops.storedData = "{\"k\":1}";
        var before = Instant.now();
        run(t -> {
            throw new TaskRescheduleException(30);
        });

        assertThat(events).containsExactly("before", "add", "delete:t1", "error", "after");
        assertThat(ops.addedTasks).hasSize(1);
        var newTask = ops.addedTasks.getFirst();
        assertThat(newTask.uniqueId()).isNotEqualTo("t1");
        assertThat(newTask.event()).isEqualTo(TestEvents.REGULAR_TASK);
        assertThat(newTask.accountId()).isEqualTo("acct");
        assertThat(newTask.executeAtTimeStamp()).isAfter(before.plusSeconds(29));
        assertThat(ops.addedData).containsExactly("{\"k\":1}");
        assertThat(ops.addedDataRaw).containsExactly(true);
    }

    @Test
    void rescheduleKeepsQueueName() {
        task = Task.Builder.newTask("t1")
                .accountId("acct")
                .queueName("Q:LOW")
                .event(TestEvents.REGULAR_TASK)
                .build();
        run(t -> {
            throw new TaskRescheduleException(1);
        });
        assertThat(ops.addedTasks.getFirst().queueName()).isEqualTo("Q:LOW");
    }

    @Test
    void genericExceptionLeavesTaskInProgressForRetry() {
        run(t -> {
            throw new IllegalStateException("boom");
        });
        assertThat(events).containsExactly("before", "error", "after");
    }

    @Test
    void taskErrorExceptionLeavesTaskInProgressForRetry() {
        run(t -> {
            throw new TaskErrorException("retry me");
        });
        assertThat(events).containsExactly("before", "error", "after");
    }

    @Test
    void failingOnErrorCallbackStillMovesTaskToDeadQueue() {
        onError = (t, e) -> {
            events.add("error");
            throw new IllegalStateException("callback bug");
        };
        assertThatCode(() -> run(t -> {
                    throw new TaskUnrecoverableException("fatal");
                }))
                .doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "dead:t1", "error", "after");
    }

    @Test
    void failingOnErrorCallbackStillReschedulesTask() {
        onError = (t, e) -> {
            throw new IllegalStateException("callback bug");
        };
        run(t -> {
            throw new TaskRescheduleException(5);
        });
        assertThat(events).containsExactly("before", "add", "delete:t1", "after");
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
    void failingBeforeStartCallbackSkipsHandlerAndRunsErrorHandling() {
        beforeStart = t -> {
            events.add("before");
            throw new IllegalStateException("callback bug");
        };
        run(t -> events.add("handler"));
        assertThat(events).containsExactly("before", "error", "after");
    }

    @Test
    void afterTaskRunsEvenWhenItFails() {
        afterTask = t -> {
            events.add("after");
            throw new IllegalStateException("callback bug");
        };
        assertThatCode(() -> run(t -> {})).doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "delete:t1", "success", "after");
    }

    @Test
    void failureToDeleteCompletedTaskIsTreatedAsFailure() {
        ops.failOnDelete = true;
        run(t -> {});
        assertThat(events).containsExactly("before", "error", "after");
    }

    @Test
    void failureToMoveToDeadQueueStillCallsOnError() {
        ops.failOnDeadMove = true;
        assertThatCode(() -> run(t -> {
                    throw new TaskUnrecoverableException("fatal");
                }))
                .doesNotThrowAnyException();
        assertThat(events).containsExactly("before", "error", "after");
    }
}

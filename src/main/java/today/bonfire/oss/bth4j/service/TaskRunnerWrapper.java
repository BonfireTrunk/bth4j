package today.bonfire.oss.bth4j.service;

import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;

/**
 * Runs a single task and applies the outcome to Redis.
 * <p>
 * Callbacks ({@link TaskCallbacks}) are user code. The state change of the task (deleted, moved to the dead
 * queue, rescheduled) is always applied before the matching callback is invoked, and an exception thrown
 * by a callback is only logged. It can neither undo a completed task nor skip the failure handling.
 */
@Slf4j
public class TaskRunnerWrapper implements Runnable {

    private final Task task;
    private final Consumer<Task> taskHandler;
    private final TaskCallbacks callbacks;
    private final TaskOps taskOps;

    TaskRunnerWrapper(Task task, Consumer<Task> taskHandler, TaskCallbacks callbacks, TaskOps taskOps) {
        this.task = task;
        this.taskHandler = taskHandler;
        this.callbacks = callbacks;
        this.taskOps = taskOps;
    }

    private void rescheduleTask(Task task, long delay) {
        var newTask = Task.Builder.newTask()
                .event(task.event())
                .accountId(task.accountId())
                .queueName(task.queueName())
                .executeAfter(delay)
                .build();
        taskOps.addTaskToQueue(newTask, taskOps.getDataForTask(task.uniqueId()), true);
        log.info(
                "Rescheduling old task {} to run after {} seconds, new task id {}",
                task.uniqueId(),
                delay,
                newTask.uniqueId());
        deleteTask(task);
    }

    private void deleteTask(Task task) {
        // delete the task and data if present
        taskOps.deleteTaskFromInProgressQueue(task);
    }

    private void moveToDeadQueue(Task task) {
        // move to dead list if task failed because of BGTaskUnrecoverableException
        taskOps.moveToDeadQueue(task);
    }

    /**
     * If the task fails it may be retried automatically by the maintenance service
     * unless the exception is {@link TaskUnrecoverableException} or {@link TaskDataException},
     * in which case the task is moved to the dead queue, or {@link TaskRescheduleException},
     * in which case the task is replaced by a new delayed task.
     */
    @Override
    public void run() {
        try {
            Exception failure = null;
            try {
                callbacks.beforeStart().accept(task);
                taskHandler.accept(task);
                deleteTask(task);
            } catch (Exception e) {
                failure = e;
            }

            if (failure == null) {
                invokeCallback("onSuccess", () -> callbacks.onSuccess().accept(task));
            } else {
                handleFailure(failure);
            }
        } finally {
            invokeCallback("afterTask", () -> callbacks.afterTask().accept(task));
        }
    }

    private void handleFailure(Exception e) {
        try {
            switch (e) {
                case TaskUnrecoverableException ex -> {
                    log.error("Task {} failed", task.taskString(), e);
                    // task will not be retried
                    moveToDeadQueue(task);
                }
                case TaskDataException ex -> {
                    log.error("Task {} failed", task.taskString(), e);
                    // task will not be retried
                    moveToDeadQueue(task);
                }
                case TaskRescheduleException ex -> rescheduleTask(task, ex.delay());
                case TaskErrorException ex ->
                    // task can be retried
                    log.warn("Task {} failed", task.taskString(), e);
                default ->
                    // unknown exception task may be retried
                    log.warn("Unhandled exception. Task will be retried", e);
            }
        } catch (RuntimeException stateError) {
            // typically a redis error. The task stays in progress and the maintenance service retries it.
            log.error(
                    "Unable to update state of failed task {}. It will be retried by maintenance",
                    task.taskString(),
                    stateError);
        }
        invokeCallback("onError", () -> callbacks.onError().accept(task, e));
    }

    private void invokeCallback(String name, Runnable callback) {
        try {
            callback.run();
        } catch (Exception callbackError) {
            log.error("Callback {} failed for task {}", name, task.taskString(), callbackError);
        }
    }
}

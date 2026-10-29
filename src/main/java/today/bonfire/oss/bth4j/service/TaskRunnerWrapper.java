package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;

/**
 * Runs one claimed task and applies the outcome to Redis with the task's lease token.
 * <p>
 * The state change (completed, scheduled for retry, dead, rescheduled) is applied before the matching
 * callback runs. Every transition is a no-op when the lease was lost in the meantime, in which case the
 * task has already been handed to another worker by maintenance. If Redis is unreachable when the outcome
 * is applied, the lease simply expires and the task is retried.
 */
public class TaskRunnerWrapper implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TaskRunnerWrapper.class);

    private final Task task;
    private final Consumer<Task> taskHandler;
    private final TaskCallbacks callbacks;
    private final TaskOps taskOps;
    private final Function<Integer, Duration> retryDelay;
    private final Consumer<Task> onFinished;

    TaskRunnerWrapper(
            Task task,
            Consumer<Task> taskHandler,
            TaskCallbacks callbacks,
            TaskOps taskOps,
            Function<Integer, Duration> retryDelay,
            Consumer<Task> onFinished) {
        this.task = task;
        this.taskHandler = taskHandler;
        this.callbacks = callbacks;
        this.taskOps = taskOps;
        this.retryDelay = retryDelay;
        this.onFinished = onFinished;
    }

    @Override
    public void run() {
        try {
            Exception failure = null;
            try {
                callbacks.beforeStart().accept(task);
                taskHandler.accept(task);
            } catch (Exception e) {
                failure = e;
            }

            if (failure == null) {
                applyState(() -> {
                    if (!taskOps.ack(task)) {
                        log.warn("Task {} completed but its lease was lost, it may run again", task.uniqueId());
                    }
                });
                invokeCallback("onSuccess", () -> callbacks.onSuccess().accept(task));
            } else {
                handleFailure(failure);
            }
        } finally {
            onFinished.accept(task);
            invokeCallback("afterTask", () -> callbacks.afterTask().accept(task));
        }
    }

    private void handleFailure(Exception e) {
        switch (e) {
            case TaskUnrecoverableException ex -> {
                log.error("Task {} failed and will not be retried", task, e);
                applyState(() -> logOutcome(taskOps.fail(task, null, message(e))));
            }
            case TaskDataException ex -> {
                log.error("Task {} failed on its data and will not be retried", task, e);
                applyState(() -> logOutcome(taskOps.fail(task, null, message(e))));
            }
            case TaskRescheduleException ex -> {
                var due = Instant.now().plusSeconds(ex.delay());
                log.info("Rescheduling task {} to {}", task.uniqueId(), due);
                applyState(() -> {
                    if (!taskOps.reschedule(task, due)) {
                        log.warn("Task {} could not be rescheduled, its lease was lost", task.uniqueId());
                    }
                });
            }
            default -> {
                log.warn("Task {} failed on attempt {} of {}", task.uniqueId(), task.attempts(), task.maxAttempts(), e);
                applyState(() -> logOutcome(taskOps.fail(task, delayFor(task.attempts()), message(e))));
            }
        }
        invokeCallback("onError", () -> callbacks.onError().accept(task, e));
    }

    private Duration delayFor(int attempts) {
        var delay = retryDelay.apply(attempts);
        return delay == null || delay.isNegative() ? Duration.ZERO : delay;
    }

    private void logOutcome(TaskOps.FailOutcome outcome) {
        switch (outcome) {
            case RETRY -> log.info("Task {} will be retried", task.uniqueId());
            case DEAD -> log.error("Task {} moved to the dead set", task.uniqueId());
            case LOST -> log.warn("Task {} failed but its lease was lost, another worker owns it", task.uniqueId());
        }
    }

    private static String message(Exception e) {
        return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }

    private void applyState(Runnable transition) {
        try {
            transition.run();
        } catch (RuntimeException redisError) {
            log.error(
                    "Unable to update the state of task {}. Its lease expires and maintenance retries it",
                    task.uniqueId(),
                    redisError);
        }
    }

    private void invokeCallback(String name, Runnable callback) {
        try {
            callback.run();
        } catch (Exception callbackError) {
            log.error("Callback {} failed for task {}", name, task.uniqueId(), callbackError);
        }
    }
}

package today.bonfire.oss.bth4j.service;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Encapsulates all callback functions for task lifecycle events.
 * <p>
 * Callbacks are user code. Exceptions thrown by a callback are logged and never change the outcome of the task,
 * except that an exception from {@code beforeStart} skips the processor and counts as a failed attempt.
 *
 * @param beforeStart called before the task processor runs
 * @param onSuccess   called after the task completed successfully
 * @param onError     called after a task failed, once the failure has been handled
 * @param afterTask   called after every run, successful or not
 */
public record TaskCallbacks(
        Consumer<Task> beforeStart,
        Consumer<Task> onSuccess,
        BiConsumer<Task, Exception> onError,
        Consumer<Task> afterTask) {

    public static final Consumer<Task> EMPTY_CONSUMER = t -> {};
    public static final BiConsumer<Task, Exception> EMPTY_BICONSUMER = (t, e) -> {};

    public TaskCallbacks {
        if (beforeStart == null) beforeStart = EMPTY_CONSUMER;
        if (onSuccess == null) onSuccess = EMPTY_CONSUMER;
        if (onError == null) onError = EMPTY_BICONSUMER;
        if (afterTask == null) afterTask = EMPTY_CONSUMER;
    }

    public static TaskCallbacks noOp() {
        return new TaskCallbacks(null, null, null, null);
    }
}

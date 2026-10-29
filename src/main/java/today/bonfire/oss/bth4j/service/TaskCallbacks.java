package today.bonfire.oss.bth4j.service;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import today.bonfire.oss.bth4j.common.THC;

/**
 * Encapsulates all callback functions for task lifecycle events.
 * <p>
 * Callbacks are user code. Exceptions thrown by a callback are logged and never change the outcome of the task.
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

    static TaskCallbacks noOp() {
        return new TaskCallbacks(THC.EMPTY_CONSUMER, THC.EMPTY_CONSUMER, THC.EMPTY_BICONSUMER, THC.EMPTY_CONSUMER);
    }
}

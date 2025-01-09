package today.bonfire.oss.bth4j.service;

import today.bonfire.oss.bth4j.Task;
import today.bonfire.oss.bth4j.common.THC;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Encapsulates all callback functions for task lifecycle events.
 */
record TaskCallbacks(Consumer<Task> beforeStart, Consumer<Task> onSuccess, BiConsumer<Task, Exception> onError, Consumer<Task> afterTask) {

  static TaskCallbacks noOp() {
    return new TaskCallbacks(
        THC.EMPTY_CONSUMER,
        THC.EMPTY_CONSUMER,
        THC.EMPTY_BICONSUMER,
        THC.EMPTY_CONSUMER);
  }
}

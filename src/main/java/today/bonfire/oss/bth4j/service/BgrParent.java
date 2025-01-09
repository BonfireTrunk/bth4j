package today.bonfire.oss.bth4j.service;

import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;

public abstract class BgrParent {

  protected static List<String>       AVAILABLE_QUEUES  = List.of("BTH:Q:DEF", "BTH:Q:LOW", "BTH:Q:MED", "BTH:Q:HIGH");
  protected static List<String>       QUEUES_TO_PROCESS = AVAILABLE_QUEUES;
  protected static String             DEFAULT_QUEUE     = QUEUES_TO_PROCESS.getFirst();
  protected static AtomicIntegerArray QUEUE_PROCESSING_STATUS;
  protected static boolean            queuesConfigured  = false;

  public static void setQueueProcessingStatus() {
    BgrParent.QUEUE_PROCESSING_STATUS = new AtomicIntegerArray(AVAILABLE_QUEUES.size());
    for (var i = 0; i < BgrParent.QUEUE_PROCESSING_STATUS.length(); i++) {
      BgrParent.QUEUE_PROCESSING_STATUS.set(i, 1);
    }
  }

  /**
   * do  not call this method once the runner has started.
   * keep the queue name as small as possible to save space in redis data structures.
   */
  protected void updateQueues(List<String> availableQueues,
                              List<String> queuesToProcess,
                              String defaultQueue) {
    if (queuesConfigured)
      throw new TaskConfigurationError("Queues already configured");

    queuesToProcess.forEach(s -> {
      if (!availableQueues.contains(s))
        throw new TaskConfigurationError("queues to process not found in available queues set");
    });
    if (!availableQueues.contains(defaultQueue))
      throw new TaskConfigurationError("default queue not found in available queues set");

    AVAILABLE_QUEUES  = availableQueues;
    QUEUES_TO_PROCESS = queuesToProcess;
    DEFAULT_QUEUE     = defaultQueue;
    queuesConfigured = true;
  }
}

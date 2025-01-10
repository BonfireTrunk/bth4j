package today.bonfire.oss.bth4j.common;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class QueuesHolder {
  public final String                             defaultQueue;
  public final List<String>                       availableQueues;
  public final List<String>                       queuesToProcess;
  public final ConcurrentHashMap<String, Boolean> queueProcessingStatus;

  public QueuesHolder(List<String> availableQueues, List<String> queuesToProcess, String defaultQueue) {

    this.availableQueues       = availableQueues;
    this.queuesToProcess       = queuesToProcess;
    this.defaultQueue          = defaultQueue;
    this.queueProcessingStatus = new ConcurrentHashMap<>();
    for (var availableQueue : availableQueues) {
      this.queueProcessingStatus.put(availableQueue, true);
    }
  }

  @Override
  public String toString() {
    return "QueuesHolder[" +
           "availableQueues=" + availableQueues + ", " +
           "queuesToProcess=" + queuesToProcess + ", " +
           "defaultQueue=" + defaultQueue + ", " +
           "queueProcessingStatus=" + queueProcessingStatus + ']';
  }

}

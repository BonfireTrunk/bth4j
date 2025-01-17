package today.bonfire.oss.bth4j.common;

import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class QueuesHolder {
  public final String                             defaultQueue;
  public final Set<String>                        availableQueues;
  public final List<String>                       queuesToProcess;
  public final ConcurrentHashMap<String, Boolean> queueProcessingStatus;
  public final String                             namespace;

  public QueuesHolder(String namespace, Set<String> availableQueues, List<String> queuesToProcess, String defaultQueue) {
    this.namespace             = namespace;
    this.availableQueues       = availableQueues;
    this.queuesToProcess       = queuesToProcess;
    this.defaultQueue          = defaultQueue;
    this.queueProcessingStatus = new ConcurrentHashMap<>();
    for (var availableQueue : availableQueues) {
      this.queueProcessingStatus.put(availableQueue, true);
    }

  }

  public String getValidQueue(String queueName) {
    if (StringUtils.isBlank(queueName)) {
      return defaultQueue;
    } else {
      // queue name would not have the namespace since it might come from
      // userspace.
      queueName = namespace + ":" + queueName;
      if (availableQueues.contains(queueName)) {
        return queueName;
      } else {
        throw new TaskErrorException("Queue name is not in available list: " + queueName);
      }
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

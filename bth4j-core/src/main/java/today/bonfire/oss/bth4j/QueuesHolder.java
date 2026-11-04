package today.bonfire.oss.bth4j;

import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;

/**
 * Queue configuration of a runner, with logical queue names (without namespace). Immutable.
 */
final class QueuesHolder {
    final String defaultQueue;
    final Set<String> availableQueues;
    final List<String> queuesToProcess;

    QueuesHolder(Set<String> availableQueues, List<String> queuesToProcess, String defaultQueue) {
        this.availableQueues = Set.copyOf(availableQueues);
        this.queuesToProcess = List.copyOf(queuesToProcess);
        this.defaultQueue = defaultQueue;
    }

    /**
     * @return the given queue name, or the default queue when it is blank
     * @throws TaskValidationException if the queue is not one of the available queues
     */
    String validQueue(String queueName) {
        if (StringUtils.isBlank(queueName)) {
            return defaultQueue;
        }
        if (availableQueues.contains(queueName)) {
            return queueName;
        }
        throw new TaskValidationException("Queue name is not in available list: " + queueName);
    }

    @Override
    public String toString() {
        return "QueuesHolder[availableQueues=" + availableQueues + ", queuesToProcess=" + queuesToProcess
                + ", defaultQueue=" + defaultQueue + ']';
    }
}

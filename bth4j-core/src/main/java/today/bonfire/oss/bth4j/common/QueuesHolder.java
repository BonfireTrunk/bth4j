package today.bonfire.oss.bth4j.common;

import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.exceptions.TaskErrorException;

/**
 * Queue configuration of a runner, with logical queue names (without namespace). Immutable.
 */
public final class QueuesHolder {
    public final String defaultQueue;
    public final Set<String> availableQueues;
    public final List<String> queuesToProcess;

    public QueuesHolder(Set<String> availableQueues, List<String> queuesToProcess, String defaultQueue) {
        this.availableQueues = Set.copyOf(availableQueues);
        this.queuesToProcess = List.copyOf(queuesToProcess);
        this.defaultQueue = defaultQueue;
    }

    /**
     * @return the given queue name, or the default queue when it is blank
     * @throws TaskErrorException if the queue is not one of the available queues
     */
    public String getValidQueue(String queueName) {
        if (StringUtils.isBlank(queueName)) {
            return defaultQueue;
        }
        if (availableQueues.contains(queueName)) {
            return queueName;
        }
        throw new TaskErrorException("Queue name is not in available list: " + queueName);
    }

    @Override
    public String toString() {
        return "QueuesHolder[availableQueues=" + availableQueues + ", queuesToProcess=" + queuesToProcess
                + ", defaultQueue=" + defaultQueue + ']';
    }
}

package today.bonfire.oss.bth4j.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The executions holding a slot of their queue's {@link today.bonfire.oss.bth4j.QueuePolicy#maxConcurrent()}.
 * One instance is shared by every processor generation of a runner, so an execution started by a processor
 * that died is released against the same count its replacement reads. Each execution is released once,
 * whether or not it is still the running map's entry for its id: a retry of the same id claimed while an
 * earlier attempt was still finishing holds a slot of its own.
 */
public final class QueueReservations {
    private final Map<TaskExecution, Integer> held = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> perQueue = new ConcurrentHashMap<>();

    void reserve(TaskExecution execution) {
        if (held.putIfAbsent(execution, execution.queueId()) == null) {
            count(execution.queueId()).incrementAndGet();
        }
    }

    void release(TaskExecution execution) {
        var queueId = held.remove(execution);
        if (queueId != null) count(queueId).decrementAndGet();
    }

    /** @return the executions of that queue holding a slot */
    public int running(int queueId) {
        var count = perQueue.get(queueId);
        return count == null ? 0 : count.get();
    }

    private AtomicInteger count(int queueId) {
        return perQueue.computeIfAbsent(queueId, q -> new AtomicInteger());
    }
}

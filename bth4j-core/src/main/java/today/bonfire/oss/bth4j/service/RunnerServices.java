package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import today.bonfire.oss.bth4j.QueuePolicy;
import today.bonfire.oss.bth4j.TaskProcessorRegistry;
import today.bonfire.oss.bth4j.spi.FullStore;

/** Everything the task processor needs, resolved once at start. */
public final class RunnerServices {

    private final TaskExecution.Settings settings;
    private final List<Integer> queueIds;
    private final Map<Integer, QueuePolicy> queuePolicies;
    private final Set<Integer> knownEventIds;
    private final int workerId;

    public RunnerServices(
            FullStore store,
            Names names,
            Codecs codecs,
            IdAllocator ids,
            TaskProcessorRegistry registry,
            RetryPolicies retryPolicies,
            Observers observer,
            Clock clock,
            RunnerStatus status,
            boolean keepSucceeded,
            boolean keepPayload,
            boolean keepDead,
            List<Integer> queueIds,
            Map<Integer, QueuePolicy> queuePolicies,
            Set<Integer> knownEventIds,
            int workerId) {
        this.settings = new TaskExecution.Settings(
                store,
                store,
                names,
                codecs,
                registry,
                retryPolicies,
                observer,
                clock,
                status,
                keepSucceeded,
                keepPayload,
                keepDead);
        this.queueIds = queueIds;
        this.queuePolicies = Map.copyOf(queuePolicies);
        this.knownEventIds = knownEventIds;
        this.workerId = workerId;
    }

    public TaskProcessorService.Builder processorBuilder() {
        return new TaskProcessorService.Builder()
                .settings(settings)
                .queueIds(queueIds)
                .queuePolicies(queuePolicies)
                .knownEventIds(knownEventIds)
                .workerId(workerId);
    }
}

package today.bonfire.oss.bth4j.service;

import java.time.Clock;
import java.util.List;
import java.util.Set;
import today.bonfire.oss.bth4j.RetryPolicy;
import today.bonfire.oss.bth4j.TaskProcessorRegistry;
import today.bonfire.oss.bth4j.spi.FullStore;

/** Everything the task processor needs, resolved once at start. */
public final class RunnerServices {

    private final TaskExecution.Settings settings;
    private final List<Integer> queueIds;
    private final Set<Integer> knownEventIds;
    private final int workerId;

    public RunnerServices(
            FullStore store,
            Names names,
            Codecs codecs,
            IdAllocator ids,
            TaskProcessorRegistry registry,
            RetryPolicy retryPolicy,
            Observers observer,
            Clock clock,
            boolean keepSucceeded,
            boolean keepPayload,
            boolean keepDead,
            List<Integer> queueIds,
            Set<Integer> knownEventIds,
            int workerId) {
        this.settings = new TaskExecution.Settings(
                store, names, codecs, registry, retryPolicy, observer, clock, keepSucceeded, keepPayload, keepDead);
        this.queueIds = queueIds;
        this.knownEventIds = knownEventIds;
        this.workerId = workerId;
    }

    public TaskProcessorService.Builder processorBuilder() {
        return new TaskProcessorService.Builder()
                .settings(settings)
                .queueIds(queueIds)
                .knownEventIds(knownEventIds)
                .workerId(workerId);
    }
}

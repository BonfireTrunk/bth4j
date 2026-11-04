package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.FailureDetail;
import today.bonfire.oss.bth4j.QueuePolicy;
import today.bonfire.oss.bth4j.RetryPolicy;
import today.bonfire.oss.bth4j.TaskClient;
import today.bonfire.oss.bth4j.TaskProcessorRegistry;
import today.bonfire.oss.bth4j.TaskRequest;
import today.bonfire.oss.bth4j.TaskType;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.TaskState;
import today.bonfire.oss.bth4j.testing.InMemoryStore;
import today.bonfire.oss.bth4j.testing.MutableClock;

/**
 * Queue slots under {@link QueuePolicy#maxConcurrent(int)}, driven through {@code processOnce} with a manual
 * executor so that no service thread runs.
 */
@Timeout(5)
class TaskProcessorCapacityTest {
    private static final TaskType<Void> TYPE = TaskType.of("capacity-test");
    private static final Duration LEASE = Duration.ofSeconds(1);

    private final MutableClock clock = new MutableClock();
    private final InMemoryStore store = new InMemoryStore(clock);
    private final Names names = new Names(store);
    private final TaskClient client =
            TaskClient.builder().store(store).clock(clock).build();
    private final int queueId = names.queueId("Q:DEF");
    private final int eventId = names.eventId(TYPE.name());
    private final Map<Long, TaskExecution> running = new ConcurrentHashMap<>();
    private final QueueReservations reservations = new QueueReservations();
    private final TaskExecution.Settings settings = new TaskExecution.Settings(
            store,
            store,
            names,
            new Codecs(null),
            new TaskProcessorRegistry().register(TYPE, (context, payload) -> {}),
            RetryPolicies.of(RetryPolicy.defaultPolicy()),
            new Observers(null),
            clock,
            new RunnerStatus(clock),
            true,
            true,
            true,
            FailureDetail.FULL);

    @Test
    void replacementProcessorClaimsAfterInheritedExecutionFinishes() throws Exception {
        var executor = new ManualExecutor(1);
        var predecessor = processor(executor, 1);
        long inheritedId = enqueue();

        processOnce(predecessor);
        assertThat(running).containsKey(inheritedId);
        var replacement = processor(executor, 1);
        executor.runNext();
        assertThat(running).isEmpty();
        assertThat(store.getTask(inheritedId).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(reservations.running(queueId)).isZero();

        long nextId = enqueue();
        processOnce(replacement);

        assertThat(store.getTask(nextId).orElseThrow().state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void reclaimedAttemptAndItsStaleAttemptEachReleaseTheirSlot() throws Exception {
        var executor = new ManualExecutor(2);
        var processor = processor(executor, 2);
        long id = enqueue();

        processOnce(processor);
        var stale = running.get(id);
        clock.advance(LEASE.plusMillis(1));
        var failure = new FailureInfo(1, clock.instant(), "lease-expired", "lease expired", null);
        assertThat(store.failExpired(id, stale.revision(), failure, clock.instant(), true, null))
                .isEqualTo(FailOutcome.RETRY);
        store.promote(10);

        processOnce(processor);
        var current = running.get(id);
        assertThat(current).isNotSameAs(stale);
        assertThat(reservations.running(queueId)).isEqualTo(2);

        executor.runNext();
        executor.runNext();
        assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.SUCCEEDED);
        assertThat(running).isEmpty();
        assertThat(reservations.running(queueId)).isZero();

        long first = enqueue();
        long second = enqueue();
        processOnce(processor);
        assertThat(store.getTask(first).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(store.getTask(second).orElseThrow().state()).isEqualTo(TaskState.LEASED);
    }

    @Test
    void executorRejectionReleasesTheSlotOnce() throws Exception {
        var executor = new ManualExecutor(1);
        executor.reject = true;
        var processor = processor(executor, 1);
        long id = enqueue();

        processOnce(processor);

        assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.QUEUED);
        assertThat(running).isEmpty();
        assertThat(reservations.running(queueId)).isZero();

        executor.reject = false;
        processOnce(processor);
        assertThat(store.getTask(id).orElseThrow().state()).isEqualTo(TaskState.LEASED);
        assertThat(reservations.running(queueId)).isEqualTo(1);
    }

    private long enqueue() {
        return client.enqueue(TaskRequest.of(TYPE).build()).id();
    }

    private TaskProcessorService processor(BackgroundExecutor executor, int maxConcurrent) {
        return new TaskProcessorService.Builder()
                .group(new ThreadGroup("capacity-test"))
                .threadName("capacity-test")
                .settings(settings)
                .running(running)
                .reservations(reservations)
                .executor(executor)
                .queueIds(List.of(queueId))
                .knownEventIds(Set.of(eventId))
                .queuePolicies(Map.of(queueId, QueuePolicy.maxConcurrent(maxConcurrent)))
                .claimBatchSize(maxConcurrent)
                .idlePollInterval(Duration.ofMillis(1))
                .lease(LEASE)
                .workerId(1)
                .create();
    }

    private static void processOnce(TaskProcessorService processor) throws Exception {
        var method = TaskProcessorService.class.getDeclaredMethod("processOnce");
        method.setAccessible(true);
        method.invoke(processor);
    }

    private static final class ManualExecutor implements BackgroundExecutor {
        private final Deque<Runnable> tasks = new ArrayDeque<>();
        private int available;
        boolean reject;

        ManualExecutor(int slots) {
            this.available = slots;
        }

        @Override
        public int reserve(int max, Duration wait) {
            int given = Math.min(max, available);
            available -= given;
            return given;
        }

        @Override
        public void release(int slots) {
            available += slots;
        }

        @Override
        public void execute(Runnable task) {
            if (reject) {
                available++;
                throw new RejectedExecutionException("rejected by the test");
            }
            tasks.add(task);
        }

        void runNext() {
            var next = tasks.removeFirst();
            try {
                next.run();
            } finally {
                available++;
            }
        }

        @Override
        public int activeCount() {
            return tasks.size();
        }

        @Override
        public void shutdown() {}

        @Override
        public boolean awaitTermination(Duration timeout) {
            return tasks.isEmpty();
        }
    }
}

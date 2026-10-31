package today.bonfire.oss.bth4j.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.TestEvents;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.store.TaskRecord;
import today.bonfire.oss.bth4j.store.TaskStore;

/** The processor loop against a mocked store: slot handling around the executor and the running-task map. */
class TaskProcessorServiceTest {

    /** Hands out one slot per claim and keeps the runnables so the test decides when each attempt finishes. */
    private static class CapturingExecutor implements BackgroundExecutor {
        final BlockingQueue<Runnable> submitted = new LinkedBlockingQueue<>();
        volatile RuntimeException failWith;

        @Override
        public int reserve(int max, Duration wait) {
            return 1;
        }

        @Override
        public void release(int slots) {}

        @Override
        public void execute(Runnable task) {
            if (failWith != null) throw failWith;
            submitted.add(task);
        }

        @Override
        public int activeCount() {
            return 0;
        }

        @Override
        public void shutdown() {}

        @Override
        public boolean awaitTermination(Duration timeout) {
            return true;
        }
    }

    private final Map<String, Task> running = new ConcurrentHashMap<>();
    private TaskStore store;
    private CapturingExecutor executor;
    private TaskProcessorService service;

    @BeforeEach
    void setUp() {
        store = mock(TaskStore.class);
        executor = new CapturingExecutor();
        var ops = new TaskOps(
                store,
                null,
                TestEvents.UNKNOWN::from,
                new QueuesHolder(Set.of("def"), List.of("def"), "def"),
                Duration.ofDays(1));
        var registry = new TaskProcessorRegistry();
        registry.register(TestEvents.REGULAR_TASK, (task, data) -> {});
        service = new TaskProcessorService.Builder()
                .setGroup(new ThreadGroup("TaskProcessorServiceTest"))
                .setThreadName("processor")
                .setExecutor(executor)
                .setTaskOps(ops)
                .setTaskProcessorRegistry(registry)
                .setLease(Duration.ofSeconds(60))
                .setIdlePollInterval(Duration.ofMillis(20))
                .setClaimBatchSize(1)
                .setRunning(running)
                .create();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        service.stopThread();
        service.stopLeaseRenewal();
        service.waitTillDone(Duration.ofSeconds(5));
    }

    private static TaskRecord leased(String id, String token) {
        return new TaskRecord(
                id,
                TestEvents.REGULAR_TASK.value(),
                "1",
                "def",
                null,
                1,
                3,
                Instant.now(),
                null,
                token,
                Instant.now().plusSeconds(60),
                null,
                null,
                null);
    }

    @Test
    void executorFailureReturnsTheTaskToItsQueueAndForgetsIt() {
        when(store.claim(anyList(), anyInt(), anyString(), any()))
                .thenReturn(List.of(leased("a", "t1")))
                .thenReturn(List.of());
        when(store.unclaim("a", "t1")).thenReturn(true);
        executor.failWith = new IllegalStateException("executor broken");

        service.start();

        verify(store, timeout(5000)).unclaim("a", "t1");
        assertThat(running).isEmpty();
        assertThat(service.runningCount()).isZero();
    }

    @Test
    void finishingAnOlderAttemptKeepsTheNewerAttemptRunning() throws InterruptedException {
        when(store.claim(anyList(), anyInt(), anyString(), any()))
                .thenReturn(List.of(leased("a", "t1")))
                .thenReturn(List.of(leased("a", "t2")))
                .thenReturn(List.of());
        when(store.ack(anyString(), anyString())).thenReturn(true);

        service.start();
        var first = executor.submitted.poll(5, TimeUnit.SECONDS);
        var second = executor.submitted.poll(5, TimeUnit.SECONDS);
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(running.get("a").token()).isEqualTo("t2");

        // the retried attempt was claimed while the first one was still finishing
        first.run();

        assertThat(running.get("a")).isNotNull();
        assertThat(running.get("a").token()).isEqualTo("t2");
        verify(store).ack("a", "t1");

        second.run();

        assertThat(running).isEmpty();
    }
}

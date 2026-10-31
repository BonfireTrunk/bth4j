package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.spi.FullStore;

/**
 * Measures how fast a store enqueues and executes a large batch of tasks and checks that each one ran exactly
 * once. The rates are only printed: they depend on the machine, so no assertion is made on them.
 */
@Timeout(300)
public abstract class ThroughputContract {

    private static final int TASKS = 10_000;
    private static final Duration EXECUTE_LIMIT = Duration.ofSeconds(120);
    private static final AtomicInteger NAMESPACES = new AtomicInteger();
    private static final TaskType<Void> TYPE = TaskType.of("tp-void");

    /** @return a store with no state, isolated from every other namespace */
    protected abstract FullStore newStore(String namespace);

    @Test
    void everyTaskRunsExactlyOnce() throws InterruptedException {
        var store = newStore("tp" + Long.toString(System.currentTimeMillis(), 36) + NAMESPACES.incrementAndGet());
        var client = TaskClient.builder()
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .build();

        var requests = new ArrayList<TaskRequest<Void>>(TASKS);
        for (int i = 0; i < TASKS; i++) requests.add(TaskRequest.of(TYPE).build());
        long enqueueStart = System.nanoTime();
        var results = client.enqueueAll(requests);
        long enqueueNanos = System.nanoTime() - enqueueStart;
        assertThat(results).hasSize(TASKS).allSatisfy(r -> assertThat(r).isInstanceOf(EnqueueResult.Accepted.class));

        var executions = new ConcurrentHashMap<Long, AtomicInteger>();
        var executed = new AtomicLong();
        var runner = BackgroundRunner.builder()
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .concurrency(Concurrency.fixed(64))
                .claimBatchSize(64)
                .idlePollInterval(Duration.ofMillis(20))
                .succeededRetention(Duration.ZERO)
                .shutdownTimeout(Duration.ofSeconds(10))
                .threadGroupName("tp-" + NAMESPACES.get())
                .build();
        runner.registry().register(TYPE, (ctx, payload) -> {
            executions.computeIfAbsent(ctx.id(), k -> new AtomicInteger()).incrementAndGet();
            executed.incrementAndGet();
        });

        long executeNanos;
        try {
            long executeStart = System.nanoTime();
            runner.start();
            long deadline = executeStart + EXECUTE_LIMIT.toNanos();
            while (executed.get() < TASKS && System.nanoTime() < deadline) Thread.sleep(10);
            executeNanos = System.nanoTime() - executeStart;
            if (executed.get() < TASKS) fail("Only %d of %d tasks ran within %s", executed.get(), TASKS, EXECUTE_LIMIT);
        } finally {
            runner.stop();
        }

        System.out.printf(
                "THROUGHPUT [%s] tasks=%d enqueue=%.0f tasks/s (%d ms) execute=%.0f tasks/s (%d ms)%n",
                store.getClass().getSimpleName(),
                TASKS,
                TASKS / (enqueueNanos / 1e9),
                enqueueNanos / 1_000_000,
                TASKS / (executeNanos / 1e9),
                executeNanos / 1_000_000);

        assertThat(executed).hasValue(TASKS);
        assertThat(executions).hasSize(TASKS);
        assertThat(executions.values()).allSatisfy(count -> assertThat(count).hasValue(1));
    }
}

package today.bonfire.oss.bth4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import today.bonfire.oss.bth4j.spi.FullStore;

/**
 * Happy-path, empty-payload batch baseline, with separate processor-callback and confirmed terminal
 * acknowledgment rates. Success retention stays zero; synchronous APPLIED observer events confirm store
 * acknowledgments without inspection I/O. Both rates include runner startup, ID/count bookkeeping and
 * observer overhead. Store durability settings still determine persistence guarantees; this does not
 * establish exactly-once execution or external effects under crashes.
 *
 * <p>All intervals use System.nanoTime in this JVM. Start latency is measured from the batch enqueue
 * call's return to Started, before decoding/processing. It excludes enqueue time and is a lower bound on
 * full queue residence because individual insertion times are unavailable. Percentiles use nearest rank.
 *
 * <p>System properties: {@code bth4j.benchmark.tasks} (1..100000, default 10000),
 * {@code bth4j.benchmark.warmups} (0..10, default 0), and {@code bth4j.benchmark.rounds} (1..10,
 * default 1). Every round uses a fresh namespace and is checked; only measured rounds print rates.
 * The overall test timeout still applies. Rates have no machine-dependent pass threshold.
 */
@Timeout(300)
public abstract class ThroughputContract {

    private static final Duration EXECUTE_LIMIT = Duration.ofSeconds(120);
    private static final AtomicInteger NAMESPACES = new AtomicInteger();
    private static final TaskType<Void> TYPE = TaskType.of("tp-void");

    /** @return a store with no state, isolated from every other namespace */
    protected abstract FullStore newStore(String namespace);

    @Test
    void everyAcceptedTaskHasOneConfirmedSuccessInThisRun() throws InterruptedException {
        int taskCount = setting("tasks", 10_000, 1, 100_000);
        int warmups = setting("warmups", 0, 0, 10);
        int rounds = setting("rounds", 1, 1, 10);
        for (int round = 0; round < warmups + rounds; round++) {
            runRound(taskCount, round - warmups + 1, rounds, warmups);
        }
    }

    private void runRound(int taskCount, int round, int rounds, int warmups) throws InterruptedException {
        int namespace = NAMESPACES.incrementAndGet();
        var store = newStore("tp" + Long.toString(System.currentTimeMillis(), 36) + namespace);
        var client = TaskClient.builder()
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .build();

        var requests = new ArrayList<TaskRequest<Void>>(taskCount);
        for (int i = 0; i < taskCount; i++) requests.add(TaskRequest.of(TYPE).build());
        long enqueueStart = System.nanoTime();
        var results = client.enqueueAll(requests);
        long enqueueEnd = System.nanoTime();
        long enqueueNanos = enqueueEnd - enqueueStart;
        assertThat(results)
                .hasSize(taskCount)
                .allSatisfy(r -> assertThat(r).isInstanceOf(EnqueueResult.Accepted.class));
        Set<Long> ids = results.stream().map(EnqueueResult::id).collect(Collectors.toSet());
        assertThat(ids).hasSize(taskCount);

        var measurement = new Measurement(ids, enqueueEnd, System::nanoTime);
        var runner = BackgroundRunner.builder()
                .store(store)
                .jsonMapper(new JsonMapperTest())
                .concurrency(Concurrency.fixed(64))
                .claimBatchSize(64)
                .idlePollInterval(Duration.ofMillis(20))
                .succeededRetention(Duration.ZERO)
                .shutdownTimeout(Duration.ofSeconds(10))
                .observer(measurement)
                .threadGroupName("tp-" + namespace)
                .build();
        runner.registry().register(TYPE, (ctx, payload) -> measurement.processorCallback(ctx.id()));

        try {
            measurement.start();
            runner.start();
            while (measurement.applied.get() < taskCount
                    && measurement.unexpected.isEmpty()
                    && System.nanoTime() - measurement.executeStart < EXECUTE_LIMIT.toNanos()) {
                Thread.sleep(10);
            }
            if (measurement.applied.get() < taskCount && measurement.unexpected.isEmpty()) {
                fail(
                        "Only %d of %d successes were acknowledged within %s",
                        measurement.applied.get(), taskCount, EXECUTE_LIMIT);
            }
        } finally {
            runner.stop();
        }
        measurement.assertComplete();

        if (round > 0) {
            System.out.printf(
                    "THROUGHPUT [%s] round=%d/%d warmups=%d tasks=%d concurrency=64 claimBatch=64 idlePollMs=20 "
                            + "successRetentionMs=0 instrumentation=synchronous-APPLIED-observer+ID-counts+nanoTime "
                            + "enqueue=%.0f tasks/s (%d ms) processorCallbacks=%.0f tasks/s (%d ms) "
                            + "confirmedTerminalAcks=%.0f tasks/s (%d ms) "
                            + "batchEnqueueReturnToStartMs[p50=%.3f p95=%.3f p99=%.3f] unexpectedEvents=0%n",
                    store.getClass().getSimpleName(),
                    round,
                    rounds,
                    warmups,
                    taskCount,
                    taskCount / (enqueueNanos / 1e9),
                    enqueueNanos / 1_000_000,
                    taskCount / (measurement.processorNanos.get() / 1e9),
                    measurement.processorNanos.get() / 1_000_000,
                    taskCount / (measurement.completionNanos.get() / 1e9),
                    measurement.completionNanos.get() / 1_000_000,
                    measurement.percentileMillis(50),
                    measurement.percentileMillis(95),
                    measurement.percentileMillis(99));
        }
    }

    private static int setting(String name, int fallback, int min, int max) {
        String property = "bth4j.benchmark." + name;
        int value = Integer.parseInt(System.getProperty(property, Integer.toString(fallback)));
        if (value < min || value > max) throw new IllegalArgumentException(property + " must be " + min + ".." + max);
        return value;
    }

    static final class Measurement implements TaskObserver {
        private final Set<Long> expected;
        private final long enqueueEnd;
        private final LongSupplier ticker;
        private final ConcurrentHashMap<Long, AtomicInteger> executions = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, AtomicInteger> successes = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, Long> startLatencies = new ConcurrentHashMap<>();
        private final ConcurrentLinkedQueue<TaskEvent> unexpected = new ConcurrentLinkedQueue<>();
        private final AtomicInteger applied = new AtomicInteger();
        final AtomicLong processorNanos = new AtomicLong();
        final AtomicLong completionNanos = new AtomicLong();
        private long executeStart;

        Measurement(Set<Long> expected, long enqueueEnd, LongSupplier ticker) {
            this.expected = Set.copyOf(expected);
            this.enqueueEnd = enqueueEnd;
            this.ticker = ticker;
        }

        void start() {
            executeStart = ticker.getAsLong();
        }

        void processorCallback(long id) {
            executions.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            processorNanos.accumulateAndGet(ticker.getAsLong() - executeStart, Math::max);
        }

        @Override
        public void onEvent(TaskEvent event) {
            switch (event) {
                case TaskEvent.Started e -> startLatencies.putIfAbsent(e.task().id(), ticker.getAsLong() - enqueueEnd);
                case TaskEvent.Succeeded e
                when e.outcome() == TaskEvent.Outcome.APPLIED -> {
                    successes
                            .computeIfAbsent(e.task().id(), k -> new AtomicInteger())
                            .incrementAndGet();
                    completionNanos.accumulateAndGet(ticker.getAsLong() - executeStart, Math::max);
                    applied.incrementAndGet();
                }
                case TaskEvent.Purged ignored -> {}
                default -> unexpected.add(event);
            }
        }

        void assertComplete() {
            assertThat(unexpected)
                    .as("no lost, uncertain, failure, deferred or other unexpected events")
                    .isEmpty();
            assertThat(executions.keySet()).as("processor IDs").containsExactlyInAnyOrderElementsOf(expected);
            assertThat(executions.values())
                    .allSatisfy(count -> assertThat(count).hasValue(1));
            assertThat(successes.keySet())
                    .as("confirmed terminal acknowledgment IDs")
                    .containsExactlyInAnyOrderElementsOf(expected);
            assertThat(successes.values()).allSatisfy(count -> assertThat(count).hasValue(1));
            assertThat(applied).hasValue(expected.size());
            assertThat(startLatencies.keySet()).as("started IDs").containsExactlyInAnyOrderElementsOf(expected);
        }

        double percentileMillis(int percentile) {
            long[] sorted =
                    startLatencies.values().stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(sorted);
            return sorted[(int) Math.ceil(sorted.length * percentile / 100.0) - 1] / 1e6;
        }
    }
}

package today.bonfire.oss.bth4j.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.spi.StateCounts;

class MicrometerRunnerMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final AtomicReference<Map<String, StateCounts>> counts = new AtomicReference<>(Map.of());
    private final AtomicReference<RunnerHealth.Status> status = new AtomicReference<>(RunnerHealth.Status.RUNNING);
    private final AtomicInteger abandoned = new AtomicInteger();
    private final AtomicInteger storeCalls = new AtomicInteger();
    private final AtomicReference<RuntimeException> countsFailure = new AtomicReference<>();
    private final AtomicReference<RuntimeException> healthFailure = new AtomicReference<>();
    private volatile Runnable beforeCountsReturn = () -> {};

    private final AdminOps admin = (AdminOps) Proxy.newProxyInstance(
            AdminOps.class.getClassLoader(), new Class<?>[] {AdminOps.class}, (proxy, method, args) -> {
                storeCalls.incrementAndGet();
                return switch (method.getName()) {
                    case "counts" -> {
                        if (countsFailure.get() != null) throw countsFailure.get();
                        var snapshot = counts.get();
                        beforeCountsReturn.run();
                        yield snapshot;
                    }
                    case "health" -> {
                        if (healthFailure.get() != null) throw healthFailure.get();
                        yield Optional.of(new RunnerHealth(
                                status.get(), 1, 0, abandoned.get(), Instant.now(), null, null, null, 0));
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            });

    private double gauge(String name, String... tags) {
        return registry.get(name).tags(tags).gauge().value();
    }

    private double state(String queue, String state) {
        return gauge("bth4j.tasks.state", "queue", queue, "state", state);
    }

    @Test
    void abandonedAttemptsComeFromHealth() {
        try (var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1))) {
            assertThat(gauge("bth4j.runner.abandoned")).isEqualTo(0);

            abandoned.set(2);
            metrics.refresh();

            assertThat(gauge("bth4j.runner.abandoned")).isEqualTo(2);
        }
    }

    @Test
    void pollerPublishesStateCountsPerQueue() {
        counts.set(Map.of("emails", new StateCounts(3, 2, 1, 10, 4, 5, 6)));

        try (var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1))) {
            assertThat(state("emails", "queued")).isEqualTo(3);
            assertThat(state("emails", "scheduled")).isEqualTo(2);
            assertThat(state("emails", "leased")).isEqualTo(1);
            assertThat(state("emails", "succeeded")).isEqualTo(10);
            assertThat(state("emails", "dead")).isEqualTo(4);
            assertThat(state("emails", "cancelled")).isEqualTo(5);
            assertThat(state("emails", "expired")).isEqualTo(6);

            counts.set(Map.of("emails", new StateCounts(8, 0, 0, 0, 0, 0, 0)));
            metrics.refresh();

            assertThat(state("emails", "queued")).isEqualTo(8);
            assertThat(state("emails", "succeeded")).isEqualTo(0);
        }
    }

    @Test
    void goneQueueDropsToZero() {
        counts.set(Map.of("a", new StateCounts(4, 0, 0, 0, 0, 0, 0)));
        try (var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1))) {
            counts.set(Map.of());
            metrics.refresh();

            assertThat(state("a", "queued")).isEqualTo(0);
        }
    }

    @Test
    void backgroundThreadRefreshesWithoutManualCalls() throws Exception {
        try (var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofMillis(10))) {
            counts.set(Map.of("q", new StateCounts(5, 0, 0, 0, 0, 0, 0)));

            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (registry.find("bth4j.tasks.state").tag("queue", "q").gauge() == null
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            while (state("q", "queued") != 5 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(state("q", "queued")).isEqualTo(5);
        }
    }

    @Test
    void runningAndUpGauges() {
        var running = new AtomicInteger(2);
        try (var metrics = MicrometerRunnerMetrics.bind(admin, running::get, registry, Duration.ofHours(1))) {
            assertThat(gauge("bth4j.runner.running")).isEqualTo(2);
            assertThat(gauge("bth4j.runner.up")).isEqualTo(1);

            running.set(5);
            status.set(RunnerHealth.Status.QUIET);
            metrics.refresh();

            assertThat(gauge("bth4j.runner.running")).isEqualTo(5);
            assertThat(gauge("bth4j.runner.up")).isEqualTo(0);
        }
    }

    @Test
    void closeRemovesAllMeters() {
        counts.set(Map.of("q", new StateCounts(1, 0, 0, 0, 0, 0, 0)));
        var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1));
        assertThat(registry.getMeters()).isNotEmpty();

        metrics.close();

        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void countsFailureKeepsTheLastValuesThenRecovers() {
        counts.set(Map.of("q", new StateCounts(5, 0, 0, 0, 0, 0, 0)));
        try (var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1))) {
            countsFailure.set(new IllegalStateException("store down"));
            abandoned.set(3);
            metrics.refresh();

            assertThat(state("q", "queued")).isEqualTo(5);
            assertThat(gauge("bth4j.runner.up")).isEqualTo(1);
            assertThat(gauge("bth4j.runner.abandoned")).isEqualTo(3);

            countsFailure.set(null);
            counts.set(Map.of("q", new StateCounts(9, 0, 0, 0, 0, 0, 0)));
            metrics.refresh();

            assertThat(state("q", "queued")).isEqualTo(9);
        }
    }

    @Test
    void healthFailureSetsUpToZeroThenRecovers() {
        counts.set(Map.of("q", new StateCounts(5, 0, 0, 0, 0, 0, 0)));
        try (var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1))) {
            assertThat(gauge("bth4j.runner.up")).isEqualTo(1);

            healthFailure.set(new IllegalStateException("store down"));
            metrics.refresh();

            assertThat(gauge("bth4j.runner.up")).isEqualTo(0);
            assertThat(state("q", "queued")).isEqualTo(5);

            healthFailure.set(null);
            metrics.refresh();

            assertThat(gauge("bth4j.runner.up")).isEqualTo(1);
        }
    }

    @Test
    void scrapingTheGaugesMakesNoStoreCalls() {
        counts.set(Map.of("q", new StateCounts(5, 0, 0, 0, 0, 0, 0)));
        try (var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1))) {
            var before = storeCalls.get();

            registry.getMeters().forEach(m -> m.measure().forEach(measurement -> measurement.getValue()));
            gauge("bth4j.runner.up");
            state("q", "queued");

            assertThat(before).isPositive();
            assertThat(storeCalls.get()).isEqualTo(before);
        }
    }

    @Test
    void aRefreshInFlightWhenCloseIsCalledDoesNotRecreateMeters() throws Exception {
        counts.set(Map.of("q", new StateCounts(5, 0, 0, 0, 0, 0, 0)));
        var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1));
        var inFlight = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        beforeCountsReturn = () -> {
            inFlight.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        counts.set(Map.of("q", new StateCounts(5, 0, 0, 0, 0, 0, 0), "other", new StateCounts(1, 0, 0, 0, 0, 0, 0)));
        var refresh = new Thread(metrics::refresh);
        refresh.start();
        assertThat(inFlight.await(10, TimeUnit.SECONDS)).isTrue();

        metrics.close();
        release.countDown();
        refresh.join(10_000);

        assertThat(refresh.isAlive()).isFalse();
        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void refreshMustBePositive() {
        assertThatThrownBy(() -> MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

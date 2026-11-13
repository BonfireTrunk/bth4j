package today.bonfire.oss.bth4j.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.BackgroundRunner;
import today.bonfire.oss.bth4j.Concurrency;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.StateCounts;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.testing.InMemoryStore;

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
    void aScrapeDuringARefreshNeverReadsAZeroForAQueueThatHasTasks() {
        counts.set(Map.of("q", new StateCounts(5, 0, 0, 0, 0, 0, 0)));
        try (var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofHours(1))) {
            var readWhileRefreshing = new ArrayList<Double>();
            // the refresh walks the entries of the counts it was given: that moment stands for a scrape in between
            counts.set(new LinkedHashMap<>(Map.of("q", new StateCounts(7, 0, 0, 0, 0, 0, 0))) {
                @Override
                public Set<Map.Entry<String, StateCounts>> entrySet() {
                    readWhileRefreshing.add(state("q", "queued"));
                    return super.entrySet();
                }
            });
            metrics.refresh();

            assertThat(readWhileRefreshing).isNotEmpty().containsOnly(5.0);
            assertThat(state("q", "queued")).isEqualTo(7);
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

    private MicrometerRunnerMetrics bindWithClock(AtomicLong nanos) {
        return MicrometerRunnerMetrics.bind(
                admin, () -> 0, OptionalInt::empty, registry, Tags.empty(), Duration.ofHours(1), nanos::get);
    }

    @Test
    void countsAgeIsAboutZeroAfterAReadThatWorkedAndGrowsWithTheClock() {
        var nanos = new AtomicLong(5_000_000_000L);
        try (var metrics = bindWithClock(nanos)) {
            assertThat(gauge("bth4j.runner.counts.age")).isEqualTo(0);
            assertThat(registry.get("bth4j.runner.counts.age").gauge().getId().getBaseUnit())
                    .isEqualTo("seconds");

            nanos.addAndGet(Duration.ofSeconds(90).toNanos());
            assertThat(gauge("bth4j.runner.counts.age")).isEqualTo(90);

            metrics.refresh();
            assertThat(gauge("bth4j.runner.counts.age")).isEqualTo(0);
        }
    }

    @Test
    void countsAgeKeepsGrowingOverFailingReadsAndDropsBackAfterOneThatWorks() {
        var nanos = new AtomicLong();
        try (var metrics = bindWithClock(nanos)) {
            countsFailure.set(new IllegalStateException("store down"));
            nanos.addAndGet(Duration.ofSeconds(30).toNanos());
            metrics.refresh();
            nanos.addAndGet(Duration.ofSeconds(30).toNanos());
            metrics.refresh();

            assertThat(gauge("bth4j.runner.counts.age")).isEqualTo(60);

            countsFailure.set(null);
            metrics.refresh();

            assertThat(gauge("bth4j.runner.counts.age")).isEqualTo(0);
        }
    }

    @Test
    void countsAgeCountsFromBindingWhenTheFirstReadFails() {
        var nanos = new AtomicLong(1_000L);
        countsFailure.set(new IllegalStateException("store down"));
        try (var metrics = bindWithClock(nanos)) {
            assertThat(gauge("bth4j.runner.counts.age")).isEqualTo(0);

            nanos.addAndGet(Duration.ofSeconds(10).toNanos());

            assertThat(gauge("bth4j.runner.counts.age")).isEqualTo(10);
        }
    }

    @Test
    void closeRemovesTheCountsAgeGaugeAndItHasTheCommonTags() {
        var metrics = MicrometerRunnerMetrics.bind(
                admin, () -> 0, OptionalInt::empty, registry, Tags.of("runner", "a"), Duration.ofHours(1), () -> 0L);
        assertThat(gauge("bth4j.runner.counts.age", "runner", "a")).isEqualTo(0);

        metrics.close();

        assertThat(registry.find("bth4j.runner.counts.age").gauge()).isNull();
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
    void limitGaugeTracksChangesUsesTagsAndIsRemovedOnClose() {
        var limit = new AtomicReference<>(OptionalInt.empty());
        var metrics = MicrometerRunnerMetrics.bind(
                admin, () -> 0, limit::get, registry, Tags.of("runner", "one"), Duration.ofHours(1));
        assertThat(registry.find(MicrometerRunnerMetrics.CONCURRENCY_LIMIT).gauge())
                .isNull();
        limit.set(OptionalInt.of(8));
        metrics.refresh();
        assertThat(gauge(MicrometerRunnerMetrics.CONCURRENCY_LIMIT, "runner", "one"))
                .isEqualTo(8);
        limit.set(OptionalInt.of(16));
        assertThat(gauge(MicrometerRunnerMetrics.CONCURRENCY_LIMIT, "runner", "one"))
                .isEqualTo(16);
        metrics.close();
        metrics.refresh();
        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void aThrowingLimitSupplierDoesNotStopHealthAndCountPolling() {
        counts.set(Map.of("emails", new StateCounts(3, 0, 0, 0, 0, 0, 0)));
        var failing = new AtomicReference<RuntimeException>(new IllegalStateException("limit unavailable"));
        Supplier<OptionalInt> limit = () -> {
            if (failing.get() != null) throw failing.get();
            return OptionalInt.of(4);
        };
        try (var metrics =
                MicrometerRunnerMetrics.bind(admin, () -> 0, limit, registry, Tags.empty(), Duration.ofHours(1))) {
            abandoned.set(5);
            metrics.refresh();

            assertThat(gauge("bth4j.runner.abandoned")).isEqualTo(5);
            assertThat(state("emails", "queued")).isEqualTo(3);

            failing.set(null);
            metrics.refresh();
            assertThat(gauge(MicrometerRunnerMetrics.CONCURRENCY_LIMIT)).isEqualTo(4);
        }
    }

    @Test
    void bindingARunnerIncludesItsExecutorLimit() {
        var runner = BackgroundRunner.builder()
                .store(InMemoryStore.builder().build())
                .concurrency(Concurrency.fixed(7))
                .build();
        try (var metrics = MicrometerRunnerMetrics.bind(runner, registry, Duration.ofHours(1))) {
            assertThat(gauge(MicrometerRunnerMetrics.CONCURRENCY_LIMIT)).isEqualTo(7);
        } finally {
            runner.stop();
        }
        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void aSecondBindingWithTheSameTagsOnOneRegistryIsRefusedUntilTheFirstIsClosed() {
        var tags = Tags.of("runner", "a");
        var first = MicrometerRunnerMetrics.bind(admin, () -> 1, registry, tags, Duration.ofHours(1));
        try {
            assertThatThrownBy(() -> MicrometerRunnerMetrics.bind(
                            admin, () -> 2, registry, Tags.of("runner", "a"), Duration.ofHours(1)))
                    .isInstanceOf(TaskConfigurationException.class)
                    .hasMessageContaining("tags");
            assertThat(gauge("bth4j.runner.running", "runner", "a")).isEqualTo(1);

            try (var other = MicrometerRunnerMetrics.bind(
                            admin, () -> 2, registry, Tags.of("runner", "b"), Duration.ofHours(1));
                    var untagged = MicrometerRunnerMetrics.bind(admin, () -> 3, registry, null, Duration.ofHours(1));
                    var elsewhere = MicrometerRunnerMetrics.bind(
                            admin, () -> 4, new SimpleMeterRegistry(), tags, Duration.ofHours(1))) {
                assertThat(gauge("bth4j.runner.running", "runner", "b")).isEqualTo(2);
            }
        } finally {
            first.close();
        }

        try (var again = MicrometerRunnerMetrics.bind(admin, () -> 5, registry, tags, Duration.ofHours(1))) {
            assertThat(gauge("bth4j.runner.running", "runner", "a")).isEqualTo(5);
        }
    }

    @Test
    void theTagsStayTakenUntilCloseHasRemovedTheMeters() {
        var tags = Tags.of("runner", "a");
        var duringRemoval = new ArrayList<Throwable>();
        var watched = new SimpleMeterRegistry() {
            @Override
            public Meter remove(Meter meter) {
                if (duringRemoval.isEmpty()) {
                    duringRemoval.add(catchThrowable(
                            () -> MicrometerRunnerMetrics.bind(admin, () -> 2, this, tags, Duration.ofHours(1))
                                    .close()));
                }
                return super.remove(meter);
            }
        };

        MicrometerRunnerMetrics.bind(admin, () -> 1, watched, tags, Duration.ofHours(1))
                .close();

        assertThat(duringRemoval).singleElement().isInstanceOf(TaskConfigurationException.class);
        try (var again = MicrometerRunnerMetrics.bind(admin, () -> 3, watched, tags, Duration.ofHours(1))) {
            assertThat(watched.get("bth4j.runner.running").tags(tags).gauge().value())
                    .isEqualTo(3);
        }
    }

    @Test
    void aRefreshTooLargeForMillisecondsStillBindsAndCloseRemovesTheGauges() {
        var metrics = MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ofSeconds(Long.MAX_VALUE));

        assertThat(gauge("bth4j.runner.up")).isEqualTo(1);
        metrics.close();

        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void aBindingThatFailsWhileItRegistersLeavesNoMeterAndItsTagsCanBeBoundAgain() {
        var failing = new SimpleMeterRegistry();
        var registered = new AtomicInteger();
        failing.config().meterFilter(new io.micrometer.core.instrument.config.MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                if (id.getName().startsWith("bth4j.") && registered.incrementAndGet() == 3) {
                    throw new IllegalStateException("registry refuses the third gauge");
                }
                return id;
            }
        });

        assertThatThrownBy(() -> MicrometerRunnerMetrics.bind(admin, () -> 0, failing, Duration.ofHours(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("third gauge");
        assertThat(failing.getMeters()).isEmpty();

        try (var again = MicrometerRunnerMetrics.bind(admin, () -> 0, failing, Duration.ofHours(1))) {
            assertThat(failing.get("bth4j.runner.up").gauge().value()).isEqualTo(1);
        }
        assertThat(failing.getMeters()).isEmpty();
    }

    @Test
    void refreshMustBePositive() {
        assertThatThrownBy(() -> MicrometerRunnerMetrics.bind(admin, () -> 0, registry, Duration.ZERO))
                .isInstanceOf(TaskConfigurationException.class);
    }
}

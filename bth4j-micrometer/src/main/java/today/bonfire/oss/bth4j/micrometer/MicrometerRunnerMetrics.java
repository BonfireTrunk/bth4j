package today.bonfire.oss.bth4j.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.BackgroundRunner;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;

/**
 * Gauges for one runner: running tasks, health, abandoned attempts, concurrency limit when available, and
 * task counts by state and queue. Health and counts are read by a single daemon every {@code refresh}, so
 * scraping never touches the store. Call {@link #close()} to stop polling and remove the meters.
 */
public final class MicrometerRunnerMetrics implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MicrometerRunnerMetrics.class);

    static final String CONCURRENCY_LIMIT = "bth4j.runner.concurrency.limit";
    static final String RUNNING = "bth4j.runner.running";
    static final String UP = "bth4j.runner.up";
    static final String ABANDONED = "bth4j.runner.abandoned";
    static final String TASKS_STATE = "bth4j.tasks.state";

    /**
     * The tag sets bound per registry. A registry hands a second gauge with the same name and tags the first
     * one back, so a second binding would show the first runner's values and its close would remove them.
     */
    private static final Map<MeterRegistry, Set<Tags>> BOUND = new WeakHashMap<>();

    private final MeterRegistry registry;
    private final AdminOps admin;
    private final Tags commonTags;
    private final Supplier<OptionalInt> concurrencyLimit;
    private boolean limitRegistered;
    private boolean limitFailureLogged;
    private final AtomicInteger up = new AtomicInteger();
    private final AtomicInteger abandoned = new AtomicInteger();
    private final Map<StateKey, AtomicLong> stateValues = new ConcurrentHashMap<>();
    private final List<Meter> meters = new ArrayList<>();
    private final ReentrantLock metersLock = new ReentrantLock();
    private final ScheduledExecutorService poller;
    private boolean closed;

    private record StateKey(String queue, TaskState state) {}

    private MicrometerRunnerMetrics(
            AdminOps admin,
            Supplier<Integer> running,
            Supplier<OptionalInt> concurrencyLimit,
            MeterRegistry registry,
            Tags commonTags,
            Duration refresh) {
        this.concurrencyLimit = concurrencyLimit;
        this.admin = admin;
        this.registry = registry;
        this.commonTags = commonTags;
        meters.add(Gauge.builder(RUNNING, running, Supplier::get)
                .tags(commonTags)
                .description("Tasks currently running in this runner")
                .strongReference(true)
                .register(registry));
        meters.add(Gauge.builder(UP, up, AtomicInteger::get)
                .tags(commonTags)
                .description("1 while the runner reports status RUNNING, otherwise 0")
                .strongReference(true)
                .register(registry));
        meters.add(Gauge.builder(ABANDONED, abandoned, AtomicInteger::get)
                .tags(commonTags)
                .description("Attempts past their maximum run time that still hold a slot")
                .strongReference(true)
                .register(registry));
        poller = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(r, "bth4j-micrometer-poller");
            thread.setDaemon(true);
            return thread;
        });
        refresh();
        long millis = Math.max(1, refresh.toMillis());
        poller.scheduleWithFixedDelay(this::refresh, millis, millis, TimeUnit.MILLISECONDS);
    }

    /**
     * Registers the gauges and starts polling.
     *
     * @param runner supplies the admin view, running tasks and concurrency limit
     * @param registry where the gauges are registered
     * @param refresh how often health and counts are read; must be positive
     * @throws TaskConfigurationException when the refresh is not positive, or the registry already has runner
     *     metrics with the same tags that are not closed
     */
    public static MicrometerRunnerMetrics bind(BackgroundRunner runner, MeterRegistry registry, Duration refresh) {
        return bind(runner, registry, Tags.empty(), refresh);
    }

    /** Binds a runner, including its concurrency limit when available, with common tags. */
    public static MicrometerRunnerMetrics bind(
            BackgroundRunner runner, MeterRegistry registry, Tags commonTags, Duration refresh) {
        return bind(runner.admin(), runner::runningTasks, runner::concurrencyLimit, registry, commonTags, refresh);
    }

    public static MicrometerRunnerMetrics bind(
            AdminOps admin, Supplier<Integer> running, MeterRegistry registry, Duration refresh) {
        return bind(admin, running, registry, Tags.empty(), refresh);
    }

    /** Same as {@link #bind(AdminOps, Supplier, MeterRegistry, Duration)} with common tags on every gauge. */
    public static MicrometerRunnerMetrics bind(
            AdminOps admin, Supplier<Integer> running, MeterRegistry registry, Tags commonTags, Duration refresh) {
        return bind(admin, running, OptionalInt::empty, registry, commonTags, refresh);
    }

    static MicrometerRunnerMetrics bind(
            AdminOps admin,
            Supplier<Integer> running,
            Supplier<OptionalInt> limit,
            MeterRegistry registry,
            Tags commonTags,
            Duration refresh) {
        if (refresh.isZero() || refresh.isNegative()) {
            throw new TaskConfigurationException("refresh must be positive");
        }
        var tags = commonTags == null ? Tags.empty() : commonTags;
        synchronized (BOUND) {
            if (!BOUND.computeIfAbsent(registry, r -> new HashSet<>()).add(tags)) {
                throw new TaskConfigurationException(
                        "This registry already has runner metrics with these tags; give each runner its own tags");
            }
        }
        try {
            return new MicrometerRunnerMetrics(admin, running, limit, registry, tags, refresh);
        } catch (RuntimeException | Error e) {
            unbind(registry, tags);
            throw e;
        }
    }

    /** Reads health and counts once; also called by the poller. A failing store leaves the last counts in place. */
    void refresh() {
        metersLock.lock();
        try {
            if (closed) return;
            if (!limitRegistered) {
                try {
                    registerLimitGauge();
                } catch (RuntimeException e) {
                    if (!limitFailureLogged) {
                        limitFailureLogged = true;
                        log.warn("Cannot read or register the concurrency limit gauge; retrying on each refresh", e);
                    }
                }
            }
        } finally {
            metersLock.unlock();
        }
        try {
            var health = admin.health();
            this.up.set(
                    health.map(RunnerHealth::status)
                                    .filter(s -> s == RunnerHealth.Status.RUNNING)
                                    .isPresent()
                            ? 1
                            : 0);
            this.abandoned.set(health.map(RunnerHealth::abandonedAttempts).orElse(0));
        } catch (RuntimeException e) {
            this.up.set(0);
        }
        try {
            var counts = admin.counts();
            metersLock.lock();
            try {
                if (closed) {
                    return;
                }
                stateValues.values().forEach(v -> v.set(0));
                for (var entry : counts.entrySet()) {
                    for (var state : TaskState.values()) {
                        stateGauge(entry.getKey(), state).set(entry.getValue().of(state));
                    }
                }
            } finally {
                metersLock.unlock();
            }
        } catch (RuntimeException e) {
            // keep the last known counts; the next tick retries
        }
    }

    private void registerLimitGauge() {
        if (concurrencyLimit.get().isPresent()) {
            meters.add(Gauge.builder(CONCURRENCY_LIMIT, concurrencyLimit, source -> {
                        var value = source.get();
                        return value.isPresent() ? value.getAsInt() : Double.NaN;
                    })
                    .tags(commonTags)
                    .description("Current executor concurrency limit")
                    .strongReference(true)
                    .register(registry));
            limitRegistered = true;
        }
    }

    private AtomicLong stateGauge(String queue, TaskState state) {
        return stateValues.computeIfAbsent(new StateKey(queue, state), key -> {
            var value = new AtomicLong();
            meters.add(Gauge.builder(TASKS_STATE, value, AtomicLong::get)
                    .tags(commonTags.and("state", state.name().toLowerCase(Locale.ROOT), "queue", queue))
                    .description("Tasks per state and queue as of the last refresh")
                    .strongReference(true)
                    .register(registry));
            return value;
        });
    }

    private static void unbind(MeterRegistry registry, Tags tags) {
        synchronized (BOUND) {
            var bound = BOUND.get(registry);
            if (bound != null) bound.remove(tags);
        }
    }

    /** Stops the polling thread and removes every meter this instance registered. */
    @Override
    public void close() {
        poller.shutdownNow();
        metersLock.lock();
        try {
            var wasOpen = !closed;
            closed = true;
            try {
                meters.forEach(registry::remove);
                meters.clear();
                stateValues.clear();
            } finally {
                // only now may another binding take these tags: one made sooner would get this one's meters,
                // which are removed here
                if (wasOpen) unbind(registry, commonTags);
            }
        } finally {
            metersLock.unlock();
        }
    }
}

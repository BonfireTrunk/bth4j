package today.bonfire.oss.bth4j.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import today.bonfire.oss.bth4j.AdminOps;
import today.bonfire.oss.bth4j.RunnerHealth;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * Gauges for one runner: {@code bth4j.runner.running}, {@code bth4j.runner.up} and {@code bth4j.tasks.state}
 * (tags state, queue). Health and task counts are read by a single daemon thread every {@code refresh}, so
 * scraping the registry never touches the store. Call {@link #close()} to stop the thread and remove the meters.
 */
public final class MicrometerRunnerMetrics implements AutoCloseable {

    static final String RUNNING = "bth4j.runner.running";
    static final String UP = "bth4j.runner.up";
    static final String TASKS_STATE = "bth4j.tasks.state";

    private final MeterRegistry registry;
    private final AdminOps admin;
    private final Tags commonTags;
    private final AtomicInteger up = new AtomicInteger();
    private final Map<StateKey, AtomicLong> stateValues = new ConcurrentHashMap<>();
    private final List<Meter> meters = new ArrayList<>();
    private final ScheduledExecutorService poller;
    private boolean closed;

    private record StateKey(String queue, TaskState state) {}

    private MicrometerRunnerMetrics(
            AdminOps admin, Supplier<Integer> running, MeterRegistry registry, Tags commonTags, Duration refresh) {
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
     * @param admin the runner's admin view; supplies health and the task counts
     * @param running current number of running tasks, normally {@code runner::runningTasks}
     * @param registry where the gauges are registered
     * @param refresh how often health and counts are read; must be positive
     */
    public static MicrometerRunnerMetrics bind(
            AdminOps admin, Supplier<Integer> running, MeterRegistry registry, Duration refresh) {
        return bind(admin, running, registry, Tags.empty(), refresh);
    }

    /** Same as {@link #bind(AdminOps, Supplier, MeterRegistry, Duration)} with common tags on every gauge. */
    public static MicrometerRunnerMetrics bind(
            AdminOps admin, Supplier<Integer> running, MeterRegistry registry, Tags commonTags, Duration refresh) {
        if (refresh.isZero() || refresh.isNegative()) {
            throw new IllegalArgumentException("refresh must be positive");
        }
        return new MicrometerRunnerMetrics(admin, running, registry, commonTags, refresh);
    }

    /** Reads health and counts once; also called by the poller. A failing store leaves the last counts in place. */
    void refresh() {
        try {
            var up = admin.health()
                    .map(RunnerHealth::status)
                    .filter(s -> s == RunnerHealth.Status.RUNNING)
                    .isPresent();
            this.up.set(up ? 1 : 0);
        } catch (RuntimeException e) {
            this.up.set(0);
        }
        try {
            var counts = admin.counts();
            synchronized (meters) {
                if (closed) {
                    return;
                }
                stateValues.values().forEach(v -> v.set(0));
                for (var entry : counts.entrySet()) {
                    for (var state : TaskState.values()) {
                        stateGauge(entry.getKey(), state).set(entry.getValue().of(state));
                    }
                }
            }
        } catch (RuntimeException e) {
            // keep the last known counts; the next tick retries
        }
    }

    private AtomicLong stateGauge(String queue, TaskState state) {
        return stateValues.computeIfAbsent(new StateKey(queue, state), key -> {
            var value = new AtomicLong();
            meters.add(Gauge.builder(TASKS_STATE, value, AtomicLong::get)
                    .tags(commonTags.and("state", state.name().toLowerCase(), "queue", queue))
                    .description("Tasks per state and queue as of the last refresh")
                    .strongReference(true)
                    .register(registry));
            return value;
        });
    }

    /** Stops the polling thread and removes every meter this instance registered. */
    @Override
    public void close() {
        poller.shutdownNow();
        synchronized (meters) {
            closed = true;
            meters.forEach(registry::remove);
            meters.clear();
            stateValues.clear();
        }
    }
}

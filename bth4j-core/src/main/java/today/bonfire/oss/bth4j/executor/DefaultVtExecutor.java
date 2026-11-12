package today.bonfire.oss.bth4j.executor;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.Concurrency;
import today.bonfire.oss.bth4j.Loggers;

/** Runs tasks on virtual threads, adapting a non-blocking semaphore to CPU and memory targets. */
public class DefaultVtExecutor implements BackgroundExecutor {
    private static final Logger log = LoggerFactory.getLogger(Loggers.EXECUTOR);
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger maxConcurrentTasks;
    private final SlotGate gate;
    private final AtomicInteger activeTaskCount = new AtomicInteger();
    private final ResourceSampler sampler;
    private final LongSupplier clock;
    private final ResourceController controller;
    private final Consumer<String> issues;
    private final ScheduledExecutorService monitor;

    public DefaultVtExecutor() {
        this(AVAILABLE_PROCESSORS, Math.max(256, AVAILABLE_PROCESSORS * 8));
    }

    /** Bounds are normalized to {@code 1 <= minConcurrency <= maxConcurrency}. */
    public DefaultVtExecutor(int minConcurrency, int maxConcurrency) {
        this(new Concurrency.Adaptive(
                Math.max(1, minConcurrency), Math.max(Math.max(1, minConcurrency), maxConcurrency)));
    }

    public DefaultVtExecutor(Concurrency.Adaptive settings) {
        this(settings, detail -> {});
    }

    /**
     * @param issues told once per signal, besides a WARN log, when the CPU or a configured memory reading goes 5
     *     seconds without a valid value: the limit cannot grow and decays to {@code min} while it lasts
     */
    public DefaultVtExecutor(Concurrency.Adaptive settings, Consumer<String> issues) {
        this(settings, new MxBeanResourceSampler(settings.memorySignal()), System::nanoTime, true, issues);
    }

    /** No scheduler: tests advance the clock and call {@link #adjustThreadCount()} explicitly. */
    DefaultVtExecutor(Concurrency.Adaptive settings, ResourceSampler sampler, LongSupplier clock) {
        this(settings, sampler, clock, detail -> {});
    }

    DefaultVtExecutor(
            Concurrency.Adaptive settings, ResourceSampler sampler, LongSupplier clock, Consumer<String> issues) {
        this(settings, sampler, clock, false, issues);
    }

    private DefaultVtExecutor(
            Concurrency.Adaptive settings,
            ResourceSampler sampler,
            LongSupplier clock,
            boolean schedule,
            Consumer<String> issues) {
        this.maxConcurrentTasks = new AtomicInteger(settings.min());
        this.gate = new SlotGate(settings.min());
        this.sampler = sampler;
        this.clock = clock;
        this.controller = new ResourceController(settings, clock.getAsLong());
        this.issues = issues;
        this.monitor = schedule
                ? Executors.newSingleThreadScheduledExecutor(r -> {
                    var thread = new Thread(r, "bth4j-vt-monitor");
                    thread.setDaemon(true);
                    return thread;
                })
                : null;
        if (monitor != null) {
            monitor.scheduleWithFixedDelay(
                    () -> {
                        try {
                            adjustThreadCount();
                        } catch (RuntimeException e) {
                            log.debug("Error in resource monitoring", e);
                        } catch (Error e) {
                            // a periodic task that throws is never run again, which would freeze the limit
                            log.error("Error in resource monitoring, the next sample runs as scheduled", e);
                        }
                    },
                    500,
                    500,
                    TimeUnit.MILLISECONDS);
        }
    }

    synchronized void adjustThreadCount() {
        long now = clock.getAsLong();
        var sample = sampler.sample(now);
        int old = maxConcurrentTasks.get();
        // slots the in-flight bound keeps a claim from using count as occupied
        int occupied = old - gate.usable();
        var decision = controller.tick(sample, occupied, activeCount(), now);
        for (var signal : decision.unavailable()) {
            var detail = "The " + signal + " reading is unavailable: adaptive concurrency cannot grow and decays to"
                    + " its minimum while it lasts";
            log.warn(detail);
            issues.accept(detail);
        }
        int next = decision.limit();
        if (next != old) {
            maxConcurrentTasks.set(next);
            gate.resize(old, next);
            log.debug(
                    "Concurrency {} -> {}, reason={}, CPU raw={}, smoothed={}, memory={}, occupied={}",
                    old,
                    next,
                    decision.reason(),
                    sample.cpu(),
                    decision.smoothedCpu(),
                    sample.memory(),
                    occupied);
        } else {
            log.trace(
                    "Concurrency hold {}, reason={}, CPU raw={}, smoothed={}, memory={}, occupied={}",
                    old,
                    decision.reason(),
                    sample.cpu(),
                    decision.smoothedCpu(),
                    sample.memory(),
                    occupied);
        }
    }

    @Override
    public synchronized void claimed(int reserved, int claimed) {
        controller.claimed(reserved, claimed, clock.getAsLong());
    }

    @Override
    public OptionalInt limit() {
        return OptionalInt.of(currentMaxThreads());
    }

    @Override
    public int reserve(int max, Duration wait) throws InterruptedException {
        return gate.reserve(max, wait);
    }

    @Override
    public void release(int slots) {
        gate.release(slots);
    }

    /** Runs the task in a reserved slot; the slot is released when the task finishes or if it is rejected. */
    @Override
    public void execute(Runnable task) {
        executeWithSlot(freeSlot -> task.run());
    }

    /** The slot may be freed early; at most twice the current limit run or write their outcome at once. */
    @Override
    public void executeWithSlot(SlotTask task) {
        try {
            executor.execute(() -> {
                activeTaskCount.incrementAndGet();
                try {
                    gate.run(task);
                } finally {
                    activeTaskCount.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            gate.release(1);
            throw e;
        }
    }

    @Override
    public int activeCount() {
        return activeTaskCount.get();
    }

    @Override
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        return executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void shutdown() {
        if (monitor != null) monitor.shutdownNow();
        executor.shutdown();
    }

    public int activeTaskCount() {
        return activeTaskCount.get();
    }

    public int currentMaxThreads() {
        return maxConcurrentTasks.get();
    }

    public int availablePermits() {
        return gate.availableSlots();
    }
}

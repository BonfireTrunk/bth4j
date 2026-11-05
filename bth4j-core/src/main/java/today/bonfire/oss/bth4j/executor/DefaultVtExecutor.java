package today.bonfire.oss.bth4j.executor;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final AdjustableSemaphore concurrencyLimiter;
    private final AtomicInteger activeTaskCount = new AtomicInteger();
    private final ResourceSampler sampler;
    private final LongSupplier clock;
    private final ResourceController controller;
    private final ScheduledExecutorService monitor;
    /** written on every reserve, release and finish without the monitor lock, read by the next tick */
    private volatile long lastActivity;

    public DefaultVtExecutor() {
        this(AVAILABLE_PROCESSORS, Math.max(256, AVAILABLE_PROCESSORS * 8));
    }

    /** Bounds are normalized to {@code 1 <= minConcurrency <= maxConcurrency}. */
    public DefaultVtExecutor(int minConcurrency, int maxConcurrency) {
        this(new Concurrency.Adaptive(
                Math.max(1, minConcurrency), Math.max(Math.max(1, minConcurrency), maxConcurrency)));
    }

    public DefaultVtExecutor(Concurrency.Adaptive settings) {
        this(settings, new MxBeanResourceSampler(settings.memorySignal()), System::nanoTime, true);
    }

    /** No scheduler: tests advance the clock and call {@link #adjustThreadCount()} explicitly. */
    DefaultVtExecutor(Concurrency.Adaptive settings, ResourceSampler sampler, LongSupplier clock) {
        this(settings, sampler, clock, false);
    }

    private DefaultVtExecutor(
            Concurrency.Adaptive settings, ResourceSampler sampler, LongSupplier clock, boolean schedule) {
        this.maxConcurrentTasks = new AtomicInteger(settings.min());
        this.concurrencyLimiter = new AdjustableSemaphore(settings.min());
        this.sampler = sampler;
        this.clock = clock;
        this.lastActivity = clock.getAsLong();
        this.controller = new ResourceController(settings, lastActivity);
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
        int occupied = old - concurrencyLimiter.availablePermits();
        controller.occupied(lastActivity);
        var decision = controller.tick(sample, occupied, now);
        int next = decision.limit();
        if (next != old) {
            maxConcurrentTasks.set(next);
            if (next > old) concurrencyLimiter.release(next - old);
            else concurrencyLimiter.shrink(old - next);
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

    private void occupied() {
        lastActivity = clock.getAsLong();
    }

    @Override
    public synchronized void claimed(int reserved, int claimed) {
        controller.claimed(reserved, claimed);
    }

    @Override
    public OptionalInt limit() {
        return OptionalInt.of(currentMaxThreads());
    }

    @Override
    public int reserve(int max, Duration wait) throws InterruptedException {
        if (!concurrencyLimiter.tryAcquire(wait.toNanos(), TimeUnit.NANOSECONDS)) {
            return 0;
        }
        int reserved = 1;
        while (reserved < max && concurrencyLimiter.tryAcquire()) {
            reserved++;
        }
        occupied();
        return reserved;
    }

    @Override
    public void release(int slots) {
        if (slots > 0) {
            occupied();
            concurrencyLimiter.release(slots);
        }
    }

    /** Runs the task in a reserved slot; the slot is released when the task finishes or if it is rejected. */
    @Override
    public void execute(Runnable task) {
        try {
            executor.execute(() -> {
                activeTaskCount.incrementAndGet();
                try {
                    task.run();
                } finally {
                    activeTaskCount.decrementAndGet();
                    occupied();
                    concurrencyLimiter.release();
                }
            });
        } catch (RuntimeException e) {
            occupied();
            concurrencyLimiter.release();
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

    /** Held slots remain occupied after a shrink; available permits can be negative until tasks finish. */
    private static final class AdjustableSemaphore extends Semaphore {
        AdjustableSemaphore(int permits) {
            super(permits);
        }

        void shrink(int permits) {
            reducePermits(permits);
        }
    }

    public int getActiveTaskCount() {
        return activeTaskCount.get();
    }

    public int currentMaxThreads() {
        return maxConcurrentTasks.get();
    }

    public int availablePermits() {
        return concurrencyLimiter.availablePermits();
    }
}

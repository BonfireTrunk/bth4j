package today.bonfire.oss.bth4j.executor;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.DoubleSupplier;
import org.slf4j.Logger;

/**
 * Default executor that runs every task on its own virtual thread.
 * <p>
 * The number of concurrently running tasks is limited by a semaphore. The limit starts at the number
 * of available processors and is tuned once per second based on CPU load and throughput, between the
 * number of processors and {@code maxConcurrency}. Throughput counts only the tasks that succeeded (see
 * {@link #succeeded()}), so fast failures do not look like progress. While throughput stays flat the limit
 * probes upwards for at most three intervals, and again only after throughput has changed.
 */
public class DefaultVtExecutor implements BackgroundExecutor {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(today.bonfire.oss.bth4j.Loggers.EXECUTOR);

    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final double CPU_THRESHOLD_HIGH = 0.8; // 80% CPU threshold
    private static final long MONITOR_INTERVAL_MS = 1000; // 1 second

    private final ExecutorService executor;
    private final AtomicInteger maxConcurrentTasks;
    private final AdjustableSemaphore concurrencyLimiter;
    private final AtomicInteger activeTaskCount;
    private final DoubleSupplier cpuLoad;
    private final Timer monitor;
    private final int changingOutputDampenLimit = 1;
    private final int constantOutputDampeningLimit = 3;

    private long lastCompletedCount;
    private long lastThroughput;
    private int consecutiveIncreases = 0;
    private int consecutiveDecreases = 0;
    private long consecutiveZeroThroughput = 0;
    /** number of tasks that succeeded. Written by many threads, so not a plain long. */
    private final LongAdder totalTasksSucceeded = new LongAdder();

    private int consecutiveStableThroughput = 0;

    private final int maxConcurrency;
    private final int minConcurrency;

    public DefaultVtExecutor() {
        this(AVAILABLE_PROCESSORS, Math.max(256, AVAILABLE_PROCESSORS * 8));
    }

    /**
     * @param minConcurrency lower bound and starting value of the adaptive limit, at least 1
     * @param maxConcurrency upper bound of the adaptive limit, at least {@code minConcurrency}
     */
    public DefaultVtExecutor(int minConcurrency, int maxConcurrency) {
        this(minConcurrency, maxConcurrency, DefaultVtExecutor::systemCpuLoad, true);
    }

    /** With {@code monitor} false nothing tunes the limit unless {@link #adjustThreadCount()} is called. */
    DefaultVtExecutor(int minConcurrency, int maxConcurrency, DoubleSupplier cpuLoad, boolean monitor) {
        this.minConcurrency = Math.max(1, minConcurrency);
        this.maxConcurrency = Math.max(this.minConcurrency, maxConcurrency);
        this.maxConcurrentTasks = new AtomicInteger(this.minConcurrency);
        this.concurrencyLimiter = new AdjustableSemaphore(this.minConcurrency);
        this.activeTaskCount = new AtomicInteger(0);
        this.cpuLoad = cpuLoad;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.monitor = monitor ? new Timer("bth4j-vt-monitor", true) : null; // daemon timer

        if (monitor) startMonitoring();
    }

    private void startMonitoring() {
        monitor.scheduleAtFixedRate(
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            adjustThreadCount();
                        } catch (Exception e) {
                            // Log error but don't stop monitoring
                            log.debug("Error in CPU monitoring", e);
                        }
                    }
                },
                MONITOR_INTERVAL_MS,
                MONITOR_INTERVAL_MS);
    }

    void adjustThreadCount() {
        double cpuLoad = this.cpuLoad.getAsDouble();
        int currentMax = maxConcurrentTasks.get();

        // Calculate throughput metrics
        long completed = totalTasksSucceeded.sum();
        long throughput = completed - lastCompletedCount;
        long throughputChange = throughput - lastThroughput;

        lastCompletedCount = completed;
        lastThroughput = throughput;

        int newMax = currentMax;
        String reason = "";

        if (cpuLoad > CPU_THRESHOLD_HIGH) {
            if (currentMax > minConcurrency) {
                newMax = Math.max(minConcurrency, currentMax - 1);
                reason = "high CPU load";
            }
            consecutiveIncreases = 0;
            consecutiveStableThroughput = 0;
        } else {
            if (throughput == 0 && activeTaskCount.get() == 0) {
                consecutiveZeroThroughput++;
                log.trace(
                        "Zero throughput detected ({} consecutive intervals). CPU: {}, currentMax: {}",
                        consecutiveZeroThroughput,
                        String.format("%.3f", cpuLoad),
                        currentMax);

                consecutiveStableThroughput = 0;
                if (consecutiveZeroThroughput >= constantOutputDampeningLimit) {
                    if (currentMax > minConcurrency) {
                        newMax = minConcurrency;
                        reason = "sustained zero throughput";
                    }
                }
                consecutiveIncreases = 0;
            } else {
                consecutiveZeroThroughput = 0;
                if (throughputChange != 0) consecutiveStableThroughput = 0;
                if (throughputChange > 0) {
                    newMax = Math.min(maxConcurrency, currentMax + 1);
                    reason = "increasing throughput";
                } else if (throughputChange < 0 && currentMax > minConcurrency) {
                    newMax = Math.max(minConcurrency, currentMax - 1);
                    reason = "decreasing throughput";
                } else if (throughputChange == 0 && consecutiveStableThroughput < constantOutputDampeningLimit) {
                    newMax = Math.min(maxConcurrency, currentMax + 1);
                    consecutiveStableThroughput++;
                    reason = "stable throughput, will probe for more";
                }
            }
        }

        if (throughput > 0) {
            if (newMax > currentMax) {
                consecutiveIncreases++;
                consecutiveDecreases = 0;
                if (consecutiveIncreases > changingOutputDampenLimit) {
                    updateConcurrencyLimit(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
                    consecutiveIncreases = 0;
                } else {
                    log.trace(
                            "Ignoring potential increase, recent increases: {}, throughput: {}",
                            consecutiveIncreases,
                            throughput);
                }
            } else if (newMax < currentMax) {
                consecutiveDecreases++;
                consecutiveIncreases = 0;
                if (consecutiveDecreases > changingOutputDampenLimit) {
                    updateConcurrencyLimit(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
                    consecutiveDecreases = 0;
                } else {
                    log.debug(
                            "Ignoring potential decrease, recent decreases: {}, throughput: {}, Change: {}",
                            consecutiveDecreases,
                            throughput,
                            throughputChange);
                }
            } else {
                consecutiveIncreases = 0;
                consecutiveDecreases = 0;
                log.trace(
                        "CPU: {}, Throughput: {}, Change: {}, currentMax: {} ",
                        String.format("%.3f", cpuLoad),
                        throughput,
                        throughputChange,
                        currentMax);
            }
        } else if (newMax != currentMax) {
            updateConcurrencyLimit(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
        }
    }

    private void updateConcurrencyLimit(
            int currentMax, int newMax, String reason, double cpuLoad, long throughput, long throughputChange) {
        maxConcurrentTasks.set(newMax);
        // Adjust semaphore permits
        if (newMax > currentMax) {
            concurrencyLimiter.release(newMax - currentMax);
        } else {
            // never blocks: permits held by running tasks are given back and simply not reused,
            // the available count may go negative until enough tasks finish
            concurrencyLimiter.shrink(currentMax - newMax);
        }

        log.debug(
                "{} threads: {} -> {} due to {}. CPU: {}, Throughput: {}, Change: {}",
                newMax > currentMax ? "Increased" : "Decreased",
                currentMax,
                newMax,
                reason,
                String.format("%.2f", cpuLoad),
                throughput,
                throughputChange);
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
        return reserved;
    }

    @Override
    public void release(int slots) {
        if (slots > 0) concurrencyLimiter.release(slots);
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
                    concurrencyLimiter.release();
                }
            });
        } catch (RuntimeException e) {
            concurrencyLimiter.release();
            throw e;
        }
    }

    /** Counts towards the throughput that tunes the limit. */
    @Override
    public void succeeded() {
        totalTasksSucceeded.increment();
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
        if (monitor != null) monitor.cancel();
        executor.shutdown();
    }

    private static double systemCpuLoad() {
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        double load;
        if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
            load = sunOsBean.getCpuLoad();
        } else {
            // Fallback to the system load average if system-wide CPU load is not available
            load = osBean.getSystemLoadAverage() / AVAILABLE_PROCESSORS;
        }
        // the JDK returns a negative value when the load is not available
        return Double.isNaN(load) ? 0.0 : Math.max(0.0, load);
    }

    /**
     * Semaphore that can lower its permits without blocking.
     * {@link Semaphore#reducePermits(int)} is protected, so it is exposed here.
     */
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

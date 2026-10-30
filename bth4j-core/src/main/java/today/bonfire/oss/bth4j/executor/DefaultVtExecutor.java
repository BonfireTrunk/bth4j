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
import org.slf4j.Logger;

/**
 * Default executor that runs every task on its own virtual thread.
 * <p>
 * The number of concurrently running tasks is limited by a semaphore. The limit starts at the number
 * of available processors and is tuned once per second based on CPU load and throughput, between the
 * number of processors and {@code maxConcurrency}.
 */
public class DefaultVtExecutor implements BackgroundExecutor {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(DefaultVtExecutor.class);

    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final double CPU_THRESHOLD_HIGH = 0.8; // 80% CPU threshold
    private static final long MONITOR_INTERVAL_MS = 1000; // 1 second

    private final ExecutorService executor;
    private final AtomicInteger maxConcurrentTasks;
    private final AdjustableSemaphore concurrencyLimiter;
    private final AtomicInteger activeTaskCount;
    private final OperatingSystemMXBean osBean;
    private final Timer monitor;
    private final int changingOutputDampenLimit = 1;
    private final int constantOutputDampeningLimit = 3;

    private long lastCompletedCount;
    private long lastThroughput;
    private int consecutiveIncreases = 0;
    private int consecutiveDecreases = 0;
    private long consecutiveZeroThroughput = 0;
    /** number of tasks that finished running, successfully or not. Written by many threads, so not a plain long. */
    private final LongAdder totalTasksCompleted = new LongAdder();

    private int consecutiveStableThroughput = 0;

    private final int maxConcurrency;

    public DefaultVtExecutor() {
        this(Math.max(256, AVAILABLE_PROCESSORS * 8));
    }

    /**
     * @param maxConcurrency upper bound of the adaptive concurrency limit, at least the number of processors
     */
    public DefaultVtExecutor(int maxConcurrency) {
        this.maxConcurrency = Math.max(AVAILABLE_PROCESSORS, maxConcurrency);
        this.maxConcurrentTasks = new AtomicInteger(AVAILABLE_PROCESSORS);
        this.concurrencyLimiter = new AdjustableSemaphore(AVAILABLE_PROCESSORS);
        this.activeTaskCount = new AtomicInteger(0);
        this.osBean = ManagementFactory.getOperatingSystemMXBean();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.monitor = new Timer("bth4j-vt-monitor", true); // daemon timer

        startMonitoring();
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

    private void adjustThreadCount() {
        double cpuLoad = getCpuLoad();
        int currentMax = maxConcurrentTasks.get();

        // Calculate throughput metrics
        long completed = totalTasksCompleted.sum();
        long throughput = completed - lastCompletedCount;
        long throughputChange = throughput - lastThroughput;

        lastCompletedCount = completed;
        lastThroughput = throughput;

        int newMax = currentMax;
        String reason = "";

        if (cpuLoad > CPU_THRESHOLD_HIGH) {
            if (currentMax > AVAILABLE_PROCESSORS) {
                newMax = Math.max(AVAILABLE_PROCESSORS, currentMax - 1);
                reason = "high CPU load";
            }
            consecutiveIncreases = 0;
        } else {
            if (throughput == 0 && activeTaskCount.get() == 0) {
                consecutiveZeroThroughput++;
                log.trace(
                        "Zero throughput detected ({} consecutive intervals). CPU: {}, currentMax: {}",
                        consecutiveZeroThroughput,
                        String.format("%.3f", cpuLoad),
                        currentMax);

                if (consecutiveZeroThroughput >= constantOutputDampeningLimit) {
                    if (currentMax > AVAILABLE_PROCESSORS) {
                        newMax = AVAILABLE_PROCESSORS;
                        reason = "sustained zero throughput";
                    }
                }
                consecutiveIncreases = 0;
            } else {
                consecutiveZeroThroughput = 0;
                if (throughputChange > 0) {
                    newMax = Math.min(maxConcurrency, currentMax + 1);
                    reason = "increasing throughput";
                } else if (throughputChange < 0 && currentMax > AVAILABLE_PROCESSORS) {
                    newMax = Math.max(AVAILABLE_PROCESSORS, currentMax - 1);
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
                consecutiveStableThroughput = 0;
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
                consecutiveStableThroughput = 0;
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
                    // counts finished tasks, including failed ones, to measure throughput
                    totalTasksCompleted.increment();
                    activeTaskCount.decrementAndGet();
                    concurrencyLimiter.release();
                }
            });
        } catch (RuntimeException e) {
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
        monitor.cancel();
        executor.shutdown();
    }

    private double getCpuLoad() {
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

    public int getCurrentMaxThreads() {
        return maxConcurrentTasks.get();
    }

    public int getAvailablePermits() {
        return concurrencyLimiter.availablePermits();
    }
}

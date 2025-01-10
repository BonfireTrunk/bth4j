package today.bonfire.oss.bth4j.executor;

import org.slf4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultVtExecutor implements BackgroundExecutor {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(DefaultVtExecutor.class);

  private static final int    AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
  private static final double CPU_THRESHOLD_HIGH   = 0.8; // 80% CPU threshold
  private static final long   MONITOR_INTERVAL_MS  = 1000; // 1 second


  private final ExecutorService       executor;
  private final AtomicInteger         maxConcurrentTasks;
  private final Semaphore             concurrencyLimiter;
  private final AtomicInteger         activeTaskCount;
  private final OperatingSystemMXBean osBean;
  private final Timer                 monitor;
  private final int                   changingOutputDampenLimit    = 1;
  private final int                   constantOutputDampeningLimit = 3;

  private long lastCompletedCount;
  private long lastThroughput;
  private int  consecutiveIncreases        = 0;
  private int  consecutiveDecreases        = 0;
  private long consecutiveZeroThroughput   = 0;
  private long totalTasksCompleted         = 0;
  private int  consecutiveStableThroughput = 0;

  public DefaultVtExecutor() {
    this.maxConcurrentTasks = new AtomicInteger(AVAILABLE_PROCESSORS);
    this.concurrencyLimiter = new Semaphore(AVAILABLE_PROCESSORS);
    this.activeTaskCount    = new AtomicInteger(0);
    this.osBean             = ManagementFactory.getOperatingSystemMXBean();
    this.executor           = Executors.newVirtualThreadPerTaskExecutor();
    this.monitor            = new Timer("CPU-Monitor", true); // daemon timer

    startMonitoring();
  }

  private void startMonitoring() {
    monitor.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        try {
          adjustThreadCount();
        } catch (Exception e) {
          // Log error but don't stop monitoring
        }
      }
    }, MONITOR_INTERVAL_MS, MONITOR_INTERVAL_MS);
  }

  private void adjustThreadCount() {
    double cpuLoad    = getCpuLoad();
    int    currentMax = maxConcurrentTasks.get();

    // Calculate throughput metrics
    long throughput       = totalTasksCompleted - lastCompletedCount;
    long throughputChange = throughput - lastThroughput;

    lastCompletedCount = totalTasksCompleted;
    lastThroughput     = throughput;

    int    newMax = currentMax;
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
        log.trace("Zero throughput detected ({} consecutive intervals). CPU: {}, currentMax: {}",
                  consecutiveZeroThroughput, String.format("%.3f", cpuLoad), currentMax);

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
          newMax = currentMax + 1;
          reason = "increasing throughput";
        } else if (throughputChange < 0 && currentMax > AVAILABLE_PROCESSORS) {
          newMax = Math.max(AVAILABLE_PROCESSORS, currentMax - 1);
          reason = "decreasing throughput";
        } else if (throughputChange == 0 && consecutiveStableThroughput < constantOutputDampeningLimit) {
          newMax = currentMax + 1;
          consecutiveStableThroughput++;
          reason = "stable throughput, will probe for more";
        }
      }
    }

    if (throughput > 0) {
      if (newMax > currentMax) {
        consecutiveIncreases++;
        consecutiveDecreases        = 0;
        consecutiveStableThroughput = 0;
        if (consecutiveIncreases > changingOutputDampenLimit) {
          updateConcurrencyLimit(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
          consecutiveIncreases = 0;
          // TODO: maybe need max limiter?
        } else {
          log.trace("Ignoring potential increase, recent increases: {}, throughput: {}",
                    consecutiveIncreases,
                    throughput);
        }
      } else if (newMax < currentMax) {
        consecutiveDecreases++;
        consecutiveIncreases        = 0;
        consecutiveStableThroughput = 0;
        if (consecutiveDecreases > changingOutputDampenLimit) {
          updateConcurrencyLimit(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
          consecutiveDecreases = 0;
        } else {
          log.debug("Ignoring potential decrease, recent decreases: {}, throughput: {}",
                    consecutiveDecreases,
                    throughput);
        }
      } else {
        consecutiveIncreases = 0;
        consecutiveDecreases = 0;
        log.trace("CPU: {}, Throughput: {}, Change: {}, currentMax: {} ",
                  String.format("%.3f", cpuLoad),
                  throughput,
                  throughputChange,
                  currentMax
        );
      }
    } else if (newMax != currentMax) {
      updateConcurrencyLimit(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
    }
  }

  private void updateConcurrencyLimit(int currentMax, int newMax, String reason, double cpuLoad, long throughput, long throughputChange) {
    maxConcurrentTasks.set(newMax);
    // Adjust semaphore permits
    if (newMax > currentMax) {
      concurrencyLimiter.release(newMax - currentMax);
    } else {
      int permitsToRemove = currentMax - newMax;
      concurrencyLimiter.acquireUninterruptibly(permitsToRemove);
    }

    log.debug("{} threads: {} -> {} due to {}. CPU: {}, Throughput: {}, Change: {}",
              newMax > currentMax ? "Increased" : "Decreased",
              currentMax,
              newMax,
              reason,
              String.format("%.2f", cpuLoad),
              throughput,
              throughputChange);
  }

  @Override
  public ExecutorService getExecutor() {
    return executor;
  }

  @Override
  public boolean isPoolFull() {
    return concurrencyLimiter.availablePermits() <= 0;
  }

  @Override
  public void execute(Runnable task) {
    if (!concurrencyLimiter.tryAcquire()) {
      throw new RejectedExecutionException(
          String.format("Executor at capacity with CPU usage: %.2f%%",
                        getCpuLoad() * 100));
    }

    executor.execute(() -> {
      try {
        activeTaskCount.incrementAndGet();
        task.run();
        totalTasksCompleted++;
      } finally {
        activeTaskCount.decrementAndGet();
        concurrencyLimiter.release();
      }
    });
  }

  @Override
  public void shutdown() {
    monitor.cancel();
    executor.shutdown();
  }

  private double getCpuLoad() {
    if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
      return sunOsBean.getCpuLoad();
    }
    // Fallback to process CPU load if system-wide CPU load is not available
    return osBean.getSystemLoadAverage() / AVAILABLE_PROCESSORS;
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

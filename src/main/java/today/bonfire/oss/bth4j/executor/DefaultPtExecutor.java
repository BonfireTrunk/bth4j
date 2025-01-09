package today.bonfire.oss.bth4j.executor;

import org.slf4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultPtExecutor implements BackgroundExecutor {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(DefaultPtExecutor.class);

  private static final int    AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
  private static final int    MAX_POOL_SIZE        = Math.max(AVAILABLE_PROCESSORS * 2, 256);
  private static final double CPU_THRESHOLD_HIGH   = 0.9;
  private static final long   MONITOR_INTERVAL_MS  = 1000; // 1 second

  private final ExecutorService       executor;
  private final AtomicInteger         maxConcurrentTasks;
  private final OperatingSystemMXBean osBean;
  private final Timer                 monitor;
  private final ThreadPoolExecutor    threadPool;

  private long lastCompletedCount;
  private long lastThroughput; // Track last throughput


  private int  consecutiveIncreases      = 0;
  private int  consecutiveDecreases      = 0;
  private long consecutiveZeroThroughput = 0;

  public DefaultPtExecutor() {
    this.maxConcurrentTasks = new AtomicInteger(AVAILABLE_PROCESSORS);
    this.osBean             = ManagementFactory.getOperatingSystemMXBean();
    this.threadPool         = new ThreadPoolExecutor(
        AVAILABLE_PROCESSORS,    // core pool size
        MAX_POOL_SIZE,       // max pool size
        0L,                    // keep alive time
        TimeUnit.SECONDS,       // time unit
        new LinkedBlockingQueue<>(),
        new ThreadFactory() {
          private final AtomicInteger counter = new AtomicInteger();

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("PtExecutor-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
          }
        }
    );
    this.executor           = threadPool;
    this.monitor            = new Timer("CPU-Monitor", true); // daemon timer
    this.lastCompletedCount = threadPool.getCompletedTaskCount();

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
          log.error("Error in CPU monitoring", e);
        }
      }
    }, MONITOR_INTERVAL_MS, MONITOR_INTERVAL_MS);
  }

  private void adjustThreadCount() {
    double cpuLoad    = getCpuLoad();
    int    currentMax = maxConcurrentTasks.get();
    long   completed  = threadPool.getCompletedTaskCount();

    long throughput       = completed - lastCompletedCount;
    long throughputChange = throughput - lastThroughput; // Calculate change based on last throughput

    lastCompletedCount = completed;
    lastThroughput     = throughput; // Store current throughput as last

    int    newMax = currentMax;
    String reason = "";

    if (cpuLoad > CPU_THRESHOLD_HIGH) {
      if (currentMax > AVAILABLE_PROCESSORS) {
        newMax = Math.max(AVAILABLE_PROCESSORS, currentMax - 1);
        reason = "high CPU load";
      }
      consecutiveIncreases = 0;
    } else {
      if (throughput == 0) {
        consecutiveZeroThroughput++;
        log.debug("Zero throughput detected ({} consecutive intervals). CPU: {}, currentMax: {}",
                  consecutiveZeroThroughput, String.format("%.3f", cpuLoad), currentMax);

        if (consecutiveZeroThroughput >= 3) {
          if (currentMax > AVAILABLE_PROCESSORS) {
            newMax = AVAILABLE_PROCESSORS;
            reason = "sustained zero throughput";
          }
        }
        consecutiveIncreases = 0;

      } else {
        consecutiveZeroThroughput = 0;
        if (throughputChange > 0) {
          newMax = Math.min(MAX_POOL_SIZE, currentMax + 1);
          reason = "increasing throughput";
        } else if (throughputChange < 0 && currentMax > AVAILABLE_PROCESSORS) {
          newMax = Math.max(AVAILABLE_PROCESSORS, currentMax - 1);
          reason = "decreasing throughput";
        } else if (throughputChange == 0) {
          newMax = Math.min(MAX_POOL_SIZE, currentMax + 1);
          reason = "stable throughput, will probe for more";
        }
      }
    }


    if (throughput > 0) {
      if (newMax > currentMax) {
        consecutiveIncreases++;
        consecutiveDecreases = 0;
        if (consecutiveIncreases > 2) {
          updateThreadPoolSize(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
          consecutiveIncreases = 0;
        } else {
          log.debug("Ignoring potential increase, recent increases: {}", consecutiveIncreases);
        }
      } else if (newMax < currentMax) {
        consecutiveDecreases++;
        consecutiveIncreases = 0;
        if (consecutiveDecreases > 2) {
          updateThreadPoolSize(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
          consecutiveDecreases = 0;
        } else {
          log.debug("Ignoring potential decrease, recent decreases: {}", consecutiveDecreases);
        }
      } else {
        consecutiveIncreases = 0;
        consecutiveDecreases = 0;
        log.debug("CPU: {}, Throughput: {}, Change: {}, currentMax: {} ",
                  String.format("%.3f", cpuLoad),
                  throughput,
                  throughputChange,
                  currentMax
        );
      }
    } else if (newMax != currentMax) {
      updateThreadPoolSize(currentMax, newMax, reason, cpuLoad, throughput, throughputChange);
    }
  }

  private void updateThreadPoolSize(int currentMax, int newMax, String reason, double cpuLoad, long throughput, long throughputChange) {
    maxConcurrentTasks.set(newMax);
    threadPool.setCorePoolSize(newMax);
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
    return threadPool.getActiveCount() >= threadPool.getMaximumPoolSize();
  }

  @Override
  public void execute(Runnable task) {
    try {
      executor.execute(task);
    } catch (RejectedExecutionException e) {
      log.error("Task rejected", e);
      throw e;
    }
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
    return threadPool.getActiveCount();
  }

  public int getCurrentMaxThreads() {
    return maxConcurrentTasks.get();
  }
}

package today.bonfire.oss.bth4j.service;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import today.bonfire.oss.bth4j.Task;
import today.bonfire.oss.bth4j.common.THC;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.CustomThread;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Accessors(fluent = true)
public class MaintenanceService extends CustomThread {

  private static final Map<String, Long> LOCK_TIMEOUTS = Map.of(
      THC.Keys.LOCK_SCHEDULED_TASKS_QUEUE, THC.Time.T_5_MINUTES,
      THC.Keys.LOCK_RECURRING_TASKS, THC.Time.T_5_MINUTES,
      THC.Keys.LOCK_ROTATION_LIST, THC.Time.T_2_MINUTES,
      THC.Keys.LOCK_IN_PROGRESS_TASKS, THC.Time.T_2_MINUTES
  );

  private final ScheduledExecutorService executor;
  private final ThreadFactory            virtualThreadFactory;

  private final Map<Runnable, Long>        maintenanceTasks         = new HashMap<>();
  @Getter
  private final Function<Integer, Integer> taskRetryDelay;
  @Getter
  private final long                       staleTaskTimeout;
  private       Set<String>                previousRotationListData = new HashSet<>();
  @Setter @Getter
  private       long                       inProgressCheckInterval;

  MaintenanceService(Builder builder) {
    super(builder.group, builder.threadName);
    this.inProgressCheckInterval = builder.inProgressCheckInterval;
    this.taskRetryDelay          = builder.taskRetryDelay;
    this.staleTaskTimeout        = builder.staleTaskTimeout;
    this.executor                = Executors.newSingleThreadScheduledExecutor();
    this.virtualThreadFactory    = Thread.ofVirtual()
                                         .name(builder.threadName + "-task", 0)
                                         .factory();
    initializeMaintenanceTasks();
  }

  private void initializeMaintenanceTasks() {
    // ideally not required may be removed in later version
    Runnable checkAndRemoveLocks = () -> {
      try {
        log.trace("Maintenance Service: Checking and removing stale locks");
        LOCK_TIMEOUTS.forEach((lockKey, timeout) -> {
          if (checkLockExpired(TaskOps.getLock(lockKey), timeout)) {
            log.warn("Maintenance Service: Removing lock on {}. Ideally this should not occur", lockKey);
            TaskOps.releaseLock(lockKey);
          }
        });
      } catch (Exception exception) {
        log.error("Error in lock checks", exception);
      }
    };

    Runnable updateQueueExecutionStatus = () -> {
      for (var i = 0; i < BgrParent.QUEUE_PROCESSING_STATUS.length(); i++) {
        BgrParent.QUEUE_PROCESSING_STATUS.set(i, 1);
      }
    };

    /*
      this is an extra check to ensure that tasks that have been in the temp rotation list for more than 1 cycle are
      moved back to the queue.
      when a task is taken by a worker it is popped from a queue and added to the temp rotation list and then removed
      once worker is processing the task. This is a check such that in a where a task is not lost due to a crash.
      This case is highly unlikely but still we have a check here.
     */
    Runnable checkAndMoveTaskFromTempRotationListBackToQueue = () -> {
      try {
        if (TaskOps.acquireLock(THC.Keys.LOCK_ROTATION_LIST, THC.Time.T_1_MINUTE)) {
          // current assumptions is the list will be very small and scan is not required.
          var tasks          = TaskOps.getAllItemsFromList(THC.Keys.TEMP_ROTATION_LIST);
          var tasksToBeMoved = new ArrayList<String>();
          if (ObjectUtils.isNotEmpty(tasks)) {
            for (var task : tasks) {
              if (previousRotationListData.contains(task)) {
                tasksToBeMoved.add(task);
              }
            }
          }
          previousRotationListData.addAll(tasks);

          if (!tasksToBeMoved.isEmpty()) {
            // delete items from temp rotation list and add them back to the queue
            log.error(
                "Tasks have been in the temp rotation list for more than 1 cycle. " +
                "This should not have happened. Need to investigate.");
            TaskOps.deleteFromRotationListAndAddToQueue(tasksToBeMoved);
          }
          TaskOps.releaseLock(THC.Keys.LOCK_ROTATION_LIST);
        }
      } catch (Exception e) {
        log.error("Error in checking rotation list", e);
      }
    };

    Runnable checkInProgressTaskAndRetry = () -> {
      try {
        log.trace("Maintenance Service: Checking in progress task and submit for retry if needed");
        if (TaskOps.acquireLock(THC.Keys.LOCK_IN_PROGRESS_TASKS, THC.Time.T_2_MINUTES)) {
          var taskCount = TaskOps.getNumberOfTaskInProgress();
          log.trace("No of task in progress {}", taskCount);
          // scan through all the tasks in the sorted set and queue them again if they have not been
          // removed for more than staleTaskTimeout duration.
          var cursor = "0";
          do {
            var r = TaskOps.scanSortedSet(THC.Keys.IN_PROGRESS_TASKS, cursor);
            cursor = r.getCursor();
            var l = r.getResult();
            if (!l.isEmpty()) {
              var tasksToDelete = new ArrayList<Task>();
              var tasksToRetry  = new ArrayList<Task>(); // task with executionTimeStamp
              l.forEach(item -> {
                var executionQueuedTime = Instant.ofEpochSecond((long) item.getScore());
                if (executionQueuedTime.isBefore(Instant.now()
                                                        .minusMillis(staleTaskTimeout))) {
                  // task may have failed or thrown exception. reschedule the task
                  // data may be present already so not need to modify only the task retry count.
                  var task = new Task(item.getElement());
                  // fetch task retry count
                  var retryCount = TaskOps.incrementRetryCount(task.uniqueId());
                  if (TaskOps.isRetryCountExhausted(task.event(), retryCount)) {
                    // delete items since they can't be we can retry them
                    // task is deleted along with the data
                    tasksToDelete.add(task);
                  } else {
                    tasksToRetry.add(task
                                         .executeAtTimeStamp(Instant.now()
                                                                    .plusSeconds(taskRetryDelay.apply(retryCount))));

                  }
                }
              });
              TaskOps.deleteFromInProgressQueueAndAddToQueue(tasksToRetry, tasksToDelete);
            }
          } while (!"0".equals(cursor));
          TaskOps.releaseLock(THC.Keys.LOCK_IN_PROGRESS_TASKS);
        }
      } catch (Exception e) {
        log.error("Error in checking in progress tasks", e);
      }
    };

    Runnable stopExecutor = () -> {
      if (!canContinueProcessing()) {
        try {
          log.info("Maintenance Service: Stopping executor");
          executor.shutdown();
          if (executor.awaitTermination(THC.Time.T_10_SECONDS, TimeUnit.SECONDS)) {
            log.info("Maintenance Service: Executor stopped");
          } else {
            log.error("Maintenance Service: Executor failed to stop within 10 seconds");
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

    };

    maintenanceTasks.put(stopExecutor, TimeUnit.SECONDS.toMillis(2));
    maintenanceTasks.put(checkAndRemoveLocks, TimeUnit.SECONDS.toMillis(THC.Time.T_5_MINUTES));
    maintenanceTasks.put(updateQueueExecutionStatus, TimeUnit.SECONDS.toMillis(THC.Time.T_3_SECONDS));
    maintenanceTasks.put(checkAndMoveTaskFromTempRotationListBackToQueue, TimeUnit.SECONDS.toMillis(THC.Time.T_30_MINUTES));
    maintenanceTasks.put(checkInProgressTaskAndRetry, inProgressCheckInterval);
  }

  @Override
  public void run() {
    final long initialDelay = TimeUnit.SECONDS.toMillis(5);
    maintenanceTasks.forEach((runnable, delay) ->
                                 executor.scheduleWithFixedDelay(
                                     () -> virtualThreadFactory.newThread(runnable).start(),
                                     initialDelay,
                                     delay,
                                     TimeUnit.MILLISECONDS
                                 )
    );
  }

  private boolean checkLockExpired(String instant, long expiryTimeLimitInSeconds) {
    if (StringUtils.isNotBlank(instant)) {
      var i = Instant.ofEpochSecond(Long.parseLong(instant));
      return i.isBefore(Instant.now()
                               .minusSeconds(expiryTimeLimitInSeconds));
    }
    return false;
  }

  public boolean isRunning() {
    return !executor.isShutdown();
  }

  public static class Builder {
    private ThreadGroup                group;
    private String                     threadName;
    private long                       inProgressCheckInterval = THC.Time.T_1_MINUTE;
    private Function<Integer, Integer> taskRetryDelay          = retryCount -> RandomUtils.insecure().randomInt(1, 5) * retryCount;
    private long                       staleTaskTimeout        = THC.Time.T_1_HOUR;

    public Builder() {}

    public Builder setGroup(ThreadGroup group) {
      this.group = group;
      return this;
    }

    public Builder setThreadName(String threadName) {
      this.threadName = threadName;
      return this;
    }

    public Builder setInProgressCheckInterval(long interval) {
      this.inProgressCheckInterval = interval;
      return this;
    }

    public Builder setTaskRetryDelay(Function<Integer, Integer> taskRetryDelay) {
      this.taskRetryDelay = taskRetryDelay;
      return this;
    }

    public Builder setStaleTaskTimeout(long timeout) {
      this.staleTaskTimeout = timeout;
      return this;
    }

    public MaintenanceService create() {
      if (group == null) {
        throw new TaskConfigurationError("ThreadGroup is required");
      }
      if (threadName == null) {
        throw new TaskConfigurationError("ThreadName is required");
      }
      return new MaintenanceService(this);
    }
  }
}

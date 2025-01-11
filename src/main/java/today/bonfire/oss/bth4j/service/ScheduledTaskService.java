package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.common.THC;
import today.bonfire.oss.bth4j.executor.CustomThread;

import java.time.Instant;
import java.util.ArrayList;

@Slf4j
public class ScheduledTaskService extends CustomThread {

  private final long     delay;
  private final TaskOps  taskOps;
  private final THC.Keys keys;

  public ScheduledTaskService(Builder builder) {
    super(builder.group, builder.threadName);
    this.delay   = builder.delay;
    this.taskOps = builder.taskOps;
    this.keys    = builder.keys;

  }

  @Override
  public void run() {
    while (this.canContinueProcessing()) {
      try {
        // check lock key in redis
        // if lock key does not exist or expired, then set new lock key.
        // if lock key exists, then do return immediately.
        if (taskOps.acquireLock(keys.LOCK_SCHEDULED_TASKS_QUEUE, THC.Time.T_5_MINUTES)) {
          // lock is acquired here
      /*
       peek into the delayed task queue to get one item and check that it is ready to be
       executed. if ready then queue it to the relevant task queue. else exit this function.
       repeat this process until the queue is empty or no task are ready to be executed.
       */
          while (true) {
            var tasks = taskOps.peek(keys.SCHEDULED_TASK_QUEUE, 20);
            if (tasks.isEmpty()) {
              log.trace("Delayed task queue is empty");
              taskOps.releaseLock(keys.LOCK_SCHEDULED_TASKS_QUEUE);
              break;
            }
            var       itemsToQueue = new ArrayList<String>();
            final var epochSecond  = Instant.now().getEpochSecond();
            // check that every item in tasks is ready for execution based on score
            tasks.forEach((k, v) -> {
              if (epochSecond >= v) {
                itemsToQueue.add(k);
              }
            });
            if (!itemsToQueue.isEmpty()) {
              taskOps.addItemsFromDelayedTasksToQueue(itemsToQueue);
            }

            if (itemsToQueue.size() < tasks.size()) {
              log.trace("Selected tasks are not ready for execution. Exiting function");
              // remove lock key
              taskOps.releaseLock(keys.LOCK_SCHEDULED_TASKS_QUEUE);
              break;
            } else {
              // more task may be available for execution
              // refresh lock here
              if (!taskOps.refreshLock(keys.LOCK_SCHEDULED_TASKS_QUEUE, THC.Time.T_5_MINUTES)) {
                log.error("Unable to refresh lock on delayed task queue");
                break;
              }
            }
          }
        } else {
          // lock is not acquired here
          log.trace("Current Run not able to acquire lock on delayed task queue");
        }
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        log.error("Task processor Thread interrupted", e);
      } catch (Exception e) {
        log.error("Task processor Thread error", e);
      }
    }
  }

  public static class Builder {

    private long        delay;
    private ThreadGroup group;
    private String      threadName;
    private TaskOps     taskOps;
    private THC.Keys    keys;


    public Builder() {}

    public Builder setGroup(ThreadGroup group) {
      this.group = group;
      return this;
    }

    public Builder setThreadName(String threadName) {
      this.threadName = threadName;
      return this;
    }

    public Builder setDelay(long delay) {
      this.delay = delay;
      return this;
    }

    public ScheduledTaskService create() {
      return new ScheduledTaskService(this);
    }

    public Builder setTaskOps(TaskOps taskOps) {
      this.taskOps = taskOps;
      return this;
    }

    public Builder setKeys(THC.Keys keys) {
      this.keys = keys;
      return this;
    }
  }
}

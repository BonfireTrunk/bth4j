package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.common.THC;

import java.time.Instant;
import java.util.ArrayList;

@Slf4j
public class ScheduledTaskHandler implements Runnable {

  @Override
  public void run() {
    // check lock key in redis
    // if lock key does not exist or expired, then set new lock key.
    // if lock key exists, then do return immediately.
    if (TaskOps.acquireLock(THC.Keys.LOCK_SCHEDULED_TASKS_QUEUE, THC.Time.T_5_MINUTES)) {
      // lock is acquired here
      /*
       peek into the delayed task queue to get one item and check that it is ready to be
       executed. if ready then queue it to the relevant task queue. else exit this function.
       repeat this process until the queue is empty or no task are ready to be executed.
       */
      while (true) {
        var tasks = TaskOps.peek(THC.Keys.SCHEDULED_TASK_QUEUE, 10);
        if (tasks.isEmpty()) {
          log.trace("Delayed task queue is empty");
          TaskOps.releaseLock(THC.Keys.LOCK_SCHEDULED_TASKS_QUEUE);
          break;
        }
        var itemsToQueue = new ArrayList<String>();
        final var epochSecond = Instant.now().getEpochSecond();
        // check that every item in tasks is ready for execution based on score
        tasks.forEach((k, v) -> {
          if (epochSecond >= v) {
            itemsToQueue.add(k);
          }
        });
        if (!itemsToQueue.isEmpty()) {
          TaskOps.addItemsFromDelayedTasksToQueue(itemsToQueue);
        }

        if (itemsToQueue.size() < tasks.size()) {
          log.trace("Selected tasks are not ready for execution. Exiting function");
          // remove lock key
          TaskOps.releaseLock(THC.Keys.LOCK_SCHEDULED_TASKS_QUEUE);
          break;
        } else {
          // more task may be available for execution
          // refresh lock here
          if (!TaskOps.refreshLock(THC.Keys.LOCK_SCHEDULED_TASKS_QUEUE, THC.Time.T_5_MINUTES)) {
            log.error("Unable to refresh lock on delayed task queue");
            break;
          }
        }
      }
    } else {
      // lock is not acquired here
      log.trace("Current Run not able to acquire lock on delayed task queue");
    }
  }
}

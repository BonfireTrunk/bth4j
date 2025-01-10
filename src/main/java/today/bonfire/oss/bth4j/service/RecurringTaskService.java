package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.common.THC;
import today.bonfire.oss.bth4j.executor.CustomThread;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Slf4j
public class RecurringTaskService extends CustomThread {

  private final long                    checkInterval;
  private final long                    lockTimeout;
  private final long                    queueAheadBy;
  private final TaskOps                 taskOps;
  private final THC.Keys                keys;
  private final Function<String, Event> eventParser;

  RecurringTaskService(Builder builder) {
    super(builder.group, builder.threadName);
    this.checkInterval = builder.checkInterval;
    this.lockTimeout   = builder.lockTimeout;
    this.queueAheadBy  = builder.queueTaskAheadDuration;
    this.taskOps       = builder.taskOps;
    this.keys          = builder.keys;
    this.eventParser   = builder.eventParser;
  }

  @Override
  public void run() {
    while (this.canContinueProcessing()) {
      try {
        if (taskOps.acquireLock(keys.LOCK_RECURRING_TASKS, lockTimeout)) {
          var           cursor      = "0";
          AtomicInteger itemsQueued = new AtomicInteger();
          do {
            var r = taskOps.scanHashSet(keys.RECURRING_TASK_SET, cursor);
            cursor = r.getCursor();
            var l = r.getResult();

            l.forEach((k) -> {
              var lastExecutionTime = Instant.ofEpochSecond(Long.parseLong(k.getValue()));
              var task              = new Task(k.getKey(), eventParser);
              var nextExecutionTime = task.getNextExecutionTime(lastExecutionTime);
              // only queue tasks that will run in the next queueAheadBy seconds
              if (Instant.now()
                         .plusSeconds(queueAheadBy)
                         .isAfter(nextExecutionTime)) {
                // queue the task based on the next execution time
                taskOps.addTaskToQueue(Task.Builder.newTask()
                                                   .event(task.event())
                                                   .queueName(task.queueName())
                                                   .accountId(task.accountId())
                                                   .executeAtTimeStamp(nextExecutionTime)
                                                   .build(), Collections.emptyMap());
                taskOps.updateExecutionTimeForRecurringTasks(k.getKey(), nextExecutionTime);
                itemsQueued.getAndIncrement();
              }

            });
            log.trace("Recurring task service analysed {} tasks", l.size());
          } while (!cursor.equals("0"));
          taskOps.releaseLock(keys.LOCK_RECURRING_TASKS);
          if (itemsQueued.get() > 0) {
            log.info("Recurring task service queued {} tasks", itemsQueued.get());
          }
        }

        Thread.sleep(checkInterval);
      } catch (InterruptedException e) {
        log.error("Task processor Thread interrupted", e);
      } catch (Exception e) {
        log.error("Task processor Thread error", e);
      }
    }
  }

  public static class Builder {

    private long                    checkInterval;
    private long                    queueTaskAheadDuration;
    private ThreadGroup             group;
    private String                  threadName;
    private TaskOps                 taskOps;
    private THC.Keys                keys;
    private Function<String, Event> eventParser;

    private long lockTimeout = THC.Time.T_5_MINUTES;

    public Builder() {}

    public Builder setGroup(ThreadGroup group) {
      this.group = group;
      return this;
    }

    public Builder setThreadName(String threadName) {
      this.threadName = threadName;
      return this;
    }

    public Builder setCheckInterval(long checkInterval) {
      this.checkInterval = checkInterval;
      return this;
    }

    public Builder setLockTimeout(long timeout) {
      this.lockTimeout = timeout;
      return this;
    }

    public Builder setTaskOps(TaskOps taskOps) {
      this.taskOps = taskOps;
      return this;
    }

    public Builder setKeys(THC.Keys keys) {
      this.keys = keys;
      return this;
    }

    public Builder setEventParser(Function<String, Event> eventParser) {
      this.eventParser = eventParser;
      return this;
    }

    public RecurringTaskService create() {
      return new RecurringTaskService(this);
    }

    public Builder setQueueTaskAheadBy(long duration) {
      this.queueTaskAheadDuration = duration;
      return this;
    }
  }
}

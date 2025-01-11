package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.CustomThread;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
public class TaskProcessorService extends CustomThread {

  private final long                  delay;
  private final Consumer<Task>        taskHandler;
  private final BackgroundExecutor    taskExecutor;
  private final int                   queueSize;
  private final TaskCallbacks         regularTaskCallbacks;
  private final TaskCallbacks         recurringTaskCallbacks;
  private final QueuesHolder          queuesHolder;
  private final TaskOps               taskOps;
  private final AtomicInteger         queueIndex = new AtomicInteger(0);
  private final TaskProcessorRegistry taskProcessorRegistry;


  public TaskProcessorService(Builder builder) {
    super(builder.group, builder.threadName);
    this.delay                  = builder.delay;
    this.taskExecutor           = builder.taskExecutor;
    this.regularTaskCallbacks   = builder.regularCallbacks;
    this.recurringTaskCallbacks = builder.recurringCallbacks;
    this.taskOps                = builder.backgroundRunner.taskOps;
    this.queuesHolder           = builder.backgroundRunner.queuesHolder;
    this.queueSize              = this.queuesHolder.queuesToProcess.size();
    this.taskProcessorRegistry  = builder.taskProcessorRegistry;
    this.taskHandler            = task -> {
      taskProcessorRegistry.executeTask(task, taskOps::getDataForTask);
    };
  }

  /**
   * this method is involved in processing the queues as per defined logic
   * currently it is processing the queues in a round-robin fashion
   */
  private String getQueue() {
    // Try up to one full cycle
    for (int attempt = 0; attempt < queueSize; attempt++) {
      // Get and increment index atomically, with wraparound
      int currentIndex = queueIndex.getAndUpdate(idx -> (idx + 1) % queueSize);

      var queue = queuesHolder.queuesToProcess.get(currentIndex);
      if (queuesHolder.queueProcessingStatus.get(queue)) {
        return queue;
      }
    }
    return null;
  }

  @Override
  public void run() {
    while (this.canContinueProcessing()) {
      try {
        var q = getQueue();
        if (taskExecutor.isPoolFull() || q == null) {
          Thread.sleep(delay);
        } else {
          // get a task and process it
          var task = taskOps.getTaskFromQueue(q);
          if (task.isNULL()) {
            // assume the queue is empty for now
            queuesHolder.queueProcessingStatus.put(q, false);
          } else {
            if (task.event().isRecurring()) {
              taskExecutor.execute(new TaskRunnerWrapper(
                  task,
                  taskHandler,
                  recurringTaskCallbacks,
                  taskOps));
            } else {
              taskExecutor.execute(new TaskRunnerWrapper(
                  task,
                  taskHandler,
                  regularTaskCallbacks,
                  taskOps));
            }

          }
        }
      } catch (InterruptedException e) {
        log.error("Task processor Thread interrupted", e);
      } catch (Exception e) {
        log.error("Task processor Thread error", e);
      }
    }
  }

  public static class Builder {
    private long                  delay;
    private TaskProcessorRegistry taskProcessorRegistry;
    private ThreadGroup           group;
    private String                threadName;
    private BackgroundExecutor    taskExecutor;
    private TaskCallbacks         regularCallbacks   = TaskCallbacks.noOp();
    private TaskCallbacks         recurringCallbacks = TaskCallbacks.noOp();
    private BackgroundRunner      backgroundRunner;

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


    /**
     * Configures callback functions for regular tasks.
     *
     * @param beforeTask Function to be executed before a task starts.
     * @param onSuccess  Function to be executed when a task completes successfully.
     * @param onError    Function to be executed when a task encounters an error.
     * @param afterTask  Function to be executed after a task completes, regardless of success or failure.
     * @return This Builder instance for method chaining.
     */
    public Builder wrapTasksWith(
        Consumer<Task> beforeTask,
        Consumer<Task> onSuccess,
        BiConsumer<Task, Exception> onError,
        Consumer<Task> afterTask) {
      this.regularCallbacks = new TaskCallbacks(
          beforeTask,
          onSuccess,
          onError,
          afterTask);
      return this;
    }

    /**
     * Configures callback functions for recurring tasks.
     *
     * @param beforeStart      Function to be executed before a recurring task starts.
     * @param afterSuccess     Function to be executed when a recurring task completes successfully.
     * @param afterError       Function to be executed when a recurring task encounters an error.
     * @param afterTaskFinally Function to be executed after a recurring task completes, regardless of success or failure.
     * @return This Builder instance for method chaining.
     */
    public Builder wrapRecurringTasksWith(
        Consumer<Task> beforeStart,
        Consumer<Task> afterSuccess,
        BiConsumer<Task, Exception> afterError,
        Consumer<Task> afterTaskFinally) {
      this.recurringCallbacks = new TaskCallbacks(
          beforeStart,
          afterSuccess,
          afterError,
          afterTaskFinally);
      return this;
    }

    public Builder setTaskProcessorRegistry(TaskProcessorRegistry registry) {
      this.taskProcessorRegistry = registry;
      return this;
    }

    public Builder setExecutor(BackgroundExecutor taskExecutor) {
      this.taskExecutor = taskExecutor;
      return this;
    }

    /**
     * Create a new task processor service instance.
     */
    public TaskProcessorService create() {
      if (taskProcessorRegistry == null) {
        throw new TaskConfigurationError("TaskProcessorRegistry is required");
      }
      if (taskExecutor == null) {
        throw new TaskConfigurationError("TaskExecutor is required");
      }
      if (backgroundRunner == null) {
        throw new TaskConfigurationError("BackgroundRunner is required");
      }
      return new TaskProcessorService(this);
    }

    public Builder setBackgroundRunner(BackgroundRunner backgroundRunner) {
      this.backgroundRunner = backgroundRunner;
      return this;
    }
  }
}

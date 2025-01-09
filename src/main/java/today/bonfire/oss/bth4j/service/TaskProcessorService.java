package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import today.bonfire.oss.bth4j.Task;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.CustomThread;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
public class TaskProcessorService extends CustomThread {

  private final long               delay;
  private final Consumer<Task>     taskHandler;
  private final BackgroundExecutor taskExecutor;
  private final int                queueSize;
  private final TaskCallbacks      regularTaskCallbacks;
  private final TaskCallbacks      recurringTaskCallbacks;
  private       int                queueIndex    = 0;
  private       int                nullTaskCount = 0;

  public TaskProcessorService(Builder builder) {
    super(builder.group, builder.threadName);
    this.taskHandler            = builder.taskHandler;
    this.delay                  = builder.delay;
    this.taskExecutor           = builder.taskExecutor;
    this.regularTaskCallbacks   = builder.regularCallbacks;
    this.recurringTaskCallbacks = builder.recurringCallbacks;
    this.queueSize              = BgrParent.QUEUES_TO_PROCESS.size();
  }

  /**
   * this method is involved in processing the queues as per defined logic
   * currently it is processing the queues in a round-robin fashion
   */
  private int getIndex() {
    // Reset indices if we've processed all queues
    if (queueIndex >= queueSize) {
      nullTaskCount = 0;
      queueIndex    = 0;
    }

    // Keep track of starting index to prevent infinite loop
    final int startIndex = queueIndex;

    do {
      // Get current index and increment for next iteration
      int currentIndex = queueIndex++;

      // If queue at current index is available for processing, return it
      if (BgrParent.QUEUE_PROCESSING_STATUS.get(currentIndex) == 1) {
        return currentIndex;
      }

      // Count skipped queues
      nullTaskCount++;

      // Wrap around if we reach the end
      if (queueIndex >= queueSize) {
        queueIndex = 0;
      }

    } while (queueIndex != startIndex && nullTaskCount < queueSize);

    // Return -1 if we've checked all queues and none are available
    return -1;
  }

  @Override
  public void run() {
    while (this.canContinueProcessing()) {
      try {
        int index = getIndex();
        if (taskExecutor.isPoolFull() || index == -1) {
          Thread.sleep(delay);
        } else {
          // get a task and process it
          var task = TaskOps.getTaskFromQueue(
              BgrParent.QUEUES_TO_PROCESS.get(index));
          if (task.isNULL()) {
            // assume the queue is empty for now
            BgrParent.QUEUE_PROCESSING_STATUS.set(index, 0);
          } else {
            if (task.event().isRecurring()) {
              taskExecutor.execute(new TaskRunnerWrapper(
                  task,
                  taskHandler,
                  recurringTaskCallbacks));
            } else {
              taskExecutor.execute(new TaskRunnerWrapper(
                  task,
                  taskHandler,
                  regularTaskCallbacks));
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
    private Consumer<Task>        taskHandler;

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
      this.taskHandler = (task) -> taskProcessorRegistry.executeTask(task);
      return new TaskProcessorService(this);
    }
  }
}

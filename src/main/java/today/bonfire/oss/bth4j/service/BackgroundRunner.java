package today.bonfire.oss.bth4j.service;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.Task;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.THC;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * primary class to initialize the background and other
 * thread groups and runners
 * update the AVAILABLE_QUEUES values to suit your needs
 * This class is designed to initialized once per application.
 * Multiple instances of this class will not work as expected.
 */
@Slf4j
public class BackgroundRunner extends BgrParent implements Runnable {

  private static Function<String, Event> eventParser;

  // configurable params with defaults
  private final long                       staleTaskTimeout;
  private final long                       BGThreadCheckInterval;
  private final long                       taskProcessorQueueCheckInterval;
  private final long                       maintenanceCheckInterval;
  private final long                       delayedTasksQueueCheckInterval;
  private final long                       recurringTasksQueueCheckInterval;
  private final long                       queueTaskAheadDurationForRecurringTasks;
  private final BackgroundExecutor         taskExecutor;
  private final ThreadGroup                threadGroup;
  private final TaskProcessorRegistry      taskProcessorRegistry;
  private final TaskCallbacks              taskCallbacks;
  private final TaskCallbacks              recurringTaskCallbacks;
  private final Function<Integer, Integer> taskRetryDelay;
  private       boolean                    stopRunner = false;

  private BackgroundRunner(Builder builder) {
    super();
    this.staleTaskTimeout                   = builder.staleTaskTimeout.toMillis();
    threadGroup                             = new ThreadGroup(builder.threadGroupName);
    maintenanceCheckInterval                = builder.maintenanceCheckInterval.toMillis();
    taskProcessorQueueCheckInterval         = builder.taskProcessorQueueCheckInterval.toMillis();
    BGThreadCheckInterval                   = builder.BGThreadCheckInterval.toMillis();
    delayedTasksQueueCheckInterval          = builder.delayedTasksQueueCheckInterval.toMillis();
    recurringTasksQueueCheckInterval        = builder.recurringTasksQueueCheckInterval.toMillis();
    taskExecutor                            = builder.taskExecutor;
    taskProcessorRegistry                   = builder.taskProcessorRegistry;
    taskCallbacks                           = builder.taskCallbacks;
    recurringTaskCallbacks                  = builder.recurringTaskCallbacks;
    taskRetryDelay                          = builder.taskRetryDelay;
    eventParser                             = builder.eventParser;
    queueTaskAheadDurationForRecurringTasks = builder.recurringTasksQueueAheadDuration.toSeconds();
    if (builder.availableQueues != null) {
      this.updateQueues(builder.availableQueues, builder.queuesToProcess, builder.defaultQueue);
    }
    setQueueProcessingStatus();
    TaskOps.init(builder.jedis, builder.jsonMapper);

  }


  public static Function<String, Event> eventParser() {
    return eventParser;
  }

  public static void setEventParser(Function<String, Event> eventParser) {
    BackgroundRunner.eventParser = eventParser;
  }

  public void run() {
    log.info("Starting background runner");
    var taskProcessorService = getTaskProcessorService();

    var delayedTaskService = getDelayedTaskService();

    var recurringTaskService = getRecurringTaskService();

    MaintenanceService maintenanceService = getMaintenanceService();
    do {
      try {
        if (!taskProcessorService.isAlive()) {
          log.error("PrimaryTaskService thread is not alive!!!");
          taskProcessorService = getTaskProcessorService();
        }
        if (!delayedTaskService.isAlive()) {
          log.error("DelayedTaskService thread is not alive!!!");
          delayedTaskService = getDelayedTaskService();
        }
        if (!recurringTaskService.isAlive()) {
          log.error("RecurringTaskService thread is not alive!!!");
          recurringTaskService = getRecurringTaskService();
        }
        if (!maintenanceService.isRunning()) {
          log.error("MaintenanceService thread is not alive!!!");
          maintenanceService = getMaintenanceService();
        }
        log.trace("Current threads in group {}: {}", threadGroup.getName(),
                  threadGroup.activeCount());

        Thread.sleep(BGThreadCheckInterval);
      } catch (InterruptedException e) {
        log.error("Main loop interrupted ", e);
      } catch (Exception e) {
        log.error("Error in Main loop ", e);
      }
      if (stopRunner) {
        log.info("Stopping background runner");
        taskProcessorService.stopThread();
        delayedTaskService.stopThread();
        recurringTaskService.stopThread();
        maintenanceService.stopThread();
        break;
      }
    } while (true);
  }

  private TaskProcessorService getTaskProcessorService() {
    var taskProcessorService = new TaskProcessorService.Builder()
        .setGroup(threadGroup)
        .setThreadName("taskProcessor")
        .setDelay(taskProcessorQueueCheckInterval)
        .setExecutor(taskExecutor)
        .setTaskProcessorRegistry(taskProcessorRegistry)
        .wrapTasksWith(
            taskCallbacks.beforeStart(),
            taskCallbacks.onSuccess(),
            taskCallbacks.onError(),
            taskCallbacks.afterTask())
        .wrapRecurringTasksWith(
            recurringTaskCallbacks.beforeStart(),
            recurringTaskCallbacks.onSuccess(),
            recurringTaskCallbacks.onError(),
            recurringTaskCallbacks.afterTask())
        .create();
    taskProcessorService.start();
    log.info("Thread {} with id {} started", taskProcessorService.getName(),
             taskProcessorService.threadId());
    return taskProcessorService;
  }

  private ScheduledTaskService getDelayedTaskService() {
    var delayedTaskService = new ScheduledTaskService.Builder().setGroup(threadGroup)
                                                               .setThreadName("DelayedTaskService")
                                                               .setDelay(delayedTasksQueueCheckInterval)
                                                               .setRunnable(new ScheduledTaskHandler())
                                                               .create();
    delayedTaskService.start();
    log.info("Thread {} with id {} started", delayedTaskService.getName(),
             delayedTaskService.threadId());
    return delayedTaskService;
  }

  private RecurringTaskService getRecurringTaskService() {
    var recurringTaskService =
        new RecurringTaskService.Builder().setGroup(threadGroup)
                                          .setThreadName("RecurringTaskService")
                                          .setCheckInterval(recurringTasksQueueCheckInterval)
                                          .setQueueTaskAheadBy(queueTaskAheadDurationForRecurringTasks)
                                          .create();
    recurringTaskService.start();
    log.info("Thread {} with id {} started", recurringTaskService.getName(),
             recurringTaskService.threadId());
    return recurringTaskService;
  }

  private MaintenanceService getMaintenanceService() {
    var maintenanceService = new MaintenanceService.Builder()
        .setGroup(threadGroup)
        .setThreadName("Maintenance")
        .setStaleTaskTimeout(staleTaskTimeout)
        .setInProgressCheckInterval(maintenanceCheckInterval)
        .setTaskRetryDelay(taskRetryDelay)
        .create();
    log.info("Thread {} with id {} started", maintenanceService.getName(),
             maintenanceService.threadId());
    maintenanceService.start();
    return maintenanceService;
  }

  public void stopRunner() {
    this.stopRunner = true;
  }


  public static class Builder {
    private Duration                   recurringTasksQueueAheadDuration = Duration.ofSeconds(THC.Time.T_1_HOUR);
    private Duration                   staleTaskTimeout                 = Duration.ofSeconds(THC.Time.T_30_MINUTES);
    private Duration                   BGThreadCheckInterval            = Duration.ofMillis(10000);
    private Duration                   taskProcessorQueueCheckInterval  = Duration.ofMillis(100);
    private Duration                   maintenanceCheckInterval         = Duration.ofMillis(10000);
    private Duration                   delayedTasksQueueCheckInterval   = Duration.ofMillis(1000);
    private Duration                   recurringTasksQueueCheckInterval = Duration.ofMillis(10000);
    private String                     threadGroupName                  = "bgr";
    private BackgroundExecutor         taskExecutor                     = null;
    private List<String>               availableQueues;
    private List<String>               queuesToProcess;
    private String                     defaultQueue;
    private TaskProcessorRegistry      taskProcessorRegistry;
    private TaskCallbacks              taskCallbacks                    = TaskCallbacks.noOp();
    private TaskCallbacks              recurringTaskCallbacks           = TaskCallbacks.noOp();
    private Function<Integer, Integer> taskRetryDelay;
    private Function<String, Event>    eventParser;
    private JedisPooled                jedis;
    private JsonMapper                 jsonMapper;


    /**
     * @param duration - this duration is used to queue recurring tasks ahead of time to normal queues.
     *                 For example, if the duration is 1 hour, then recurring tasks will be
     *                 queued 1 hour before their scheduled time to a future queue but not run until actual scheduled time.<br>
     *                 The default value is 1 hour. You may not need to change this value.<br>
     *                 NOTE: Actual execution time is the time that the task is scheduled to run.
     * @return current Builder instance for method chaining.
     */
    public Builder setRecurringTasksQueueAheadDuration(Duration duration) {
      this.recurringTasksQueueAheadDuration = duration;
      return this;
    }

    public Builder eventParser(Function<String, Event> eventParser) {
      this.eventParser = eventParser;
      return this;
    }

    /**
     * @param interval - time to wait between thread checks.
     *                 if any of the threads die, then new ones are created
     *                 default is 10 seconds
     */
    public Builder BGThreadCheckInterval(Duration interval) {
      this.BGThreadCheckInterval = interval;
      return this;
    }

    /**
     * @param interval - this delayed the rate at which task get
     *                 picked up and processed. default is 100ms
     */
    public Builder taskProcessorQueueCheckInterval(Duration interval) {
      this.taskProcessorQueueCheckInterval = interval;
      return this;
    }

    /**
     * @param interval - wait time between maintenance checks.
     *                 maintenance is to check if task processing is working fine.
     *                 If stale tasks are found they will be queued for processing again based on retry logic
     *                 default is 10 seconds
     *                 values less than 1 second are rejected
     */
    public Builder maintenanceCheckInterval(Duration interval) {
      if (interval.toMillis() < 1000)
        throw new TaskConfigurationError("Delay cannot be less than 1 second");
      this.maintenanceCheckInterval = interval;
      return this;
    }

    /**
     * @param interval - interval to check for delayed tasks in queue.
     *                 delayed tasks are tasks that are scheduled to run at a future time.
     *                 default is 1-second
     *                 values less than 10ms are rejected
     */
    public Builder delayedTasksQueueCheckInterval(Duration interval) {
      if (interval.toMillis() < 10)
        throw new TaskConfigurationError("Delay cannot be less than 10ms");
      this.delayedTasksQueueCheckInterval = interval;
      return this;
    }

    /**
     * @param interval - interval to check for recurring tasks and queue them accordingly.
     *                 recurring tasks are tasks that run periodically.
     *                 default is 10 seconds
     */
    public Builder recurringTasksQueueCheckInterval(Duration interval) {
      this.recurringTasksQueueCheckInterval = interval;
      return this;
    }


    public Builder threadGroupName(String threadGroupName) {
      this.threadGroupName = threadGroupName;
      return this;
    }


    public Builder taskExecutor(BackgroundExecutor executor) {
      this.taskExecutor = executor;
      return this;
    }

    /**
     * if delay function is not set a default function is used which calculates the value
     * based on the logic (3 ^ retry-count) + (random_delay between 0 and 59)
     *
     * @param taskRetryDelay function which is supplied with the retry count and expects the
     *                       return value as delay in seconds
     */
    public Builder taskRetryDelay(Function<Integer, Integer> taskRetryDelay) {
      this.taskRetryDelay = taskRetryDelay;
      return this;
    }

    /**
     * The stale task timeout is the time after which a task is considered stale.
     * Stale tasks will be retried or marked as dead if retry count is exceeded.
     * This is applicable for all tasks, so set this value larger than the longest running task.
     *
     * @param timeout - stale task timeout
     *                default is 30 minutes
     */
    public Builder staleTaskTimeout(Duration timeout) {
      this.staleTaskTimeout = timeout;
      return this;
    }

    /**
     * Configure callback functions for regular tasks.
     *
     * @param beforeTask Function to be executed before a task starts.
     * @param onSuccess  Function to be executed when a task completes successfully.
     * @param onError    Function to be executed when a task encounters an error.
     * @param afterTask  Function to be executed after a task completes, regardless of success or failure.
     * @return This Builder instance for method chaining.
     */
    public Builder taskHandlerDecorators(
        Consumer<Task> beforeTask,
        Consumer<Task> onSuccess,
        BiConsumer<Task, Exception> onError,
        Consumer<Task> afterTask) {
      this.taskCallbacks = new TaskCallbacks(
          beforeTask == null ? THC.EMPTY_CONSUMER : beforeTask,
          onSuccess == null ? THC.EMPTY_CONSUMER : onSuccess,
          onError == null ? THC.EMPTY_BICONSUMER : onError,
          afterTask == null ? THC.EMPTY_CONSUMER : afterTask);
      return this;
    }

    /**
     * Configure callback functions for recurring tasks.
     *
     * @param beforeTask Function to be executed before a recurring task starts.
     * @param onSuccess  Function to be executed when a recurring task completes successfully.
     * @param onError    Function to be executed when a recurring task encounters an error.
     * @param afterTask  Function to be executed after a recurring task completes, regardless of success or failure.
     * @return This Builder instance for method chaining.
     */
    public Builder recurringTaskHandlerDecorators(
        Consumer<Task> beforeTask,
        Consumer<Task> onSuccess,
        BiConsumer<Task, Exception> onError,
        Consumer<Task> afterTask) {
      this.recurringTaskCallbacks = new TaskCallbacks(
          beforeTask == null ? THC.EMPTY_CONSUMER : beforeTask,
          onSuccess == null ? THC.EMPTY_CONSUMER : onSuccess,
          onError == null ? THC.EMPTY_BICONSUMER : onError,
          afterTask == null ? THC.EMPTY_CONSUMER : afterTask);
      return this;
    }

    /**
     * Configure the queues for task processing. This method should be called before starting the application
     * and should not be changed when the application is running as the behavior would be unknown.
     * Queue names should be kept as small as possible to optimize storage for Redis like data structures.
     * <p>
     * Example queue names: "q:def", "q:low", "q:med", "q:high"
     * <p>
     * In queuesToProcess, only queues that are present in availableQueues should be specified.
     * Also you can use the same name  again for e.g. ", "q:low", "q:med", "q:med", "q:high", "q:high"
     * this is also valid and will result in that queue being processed twice.
     *
     * @param availableQueues - List of all queues visible to the application. Duplicates will be removed.
     *                        Example: List.of("q:def", "q:low", "q:med", "q:high")
     * @param queuesToProcess - List of queues that this instance will process. Must be a subset of availableQueues.
     *                        Example: List.of("q:low", "q:med") to only process low and medium priority queues
     * @param defaultQueue    - Default queue to use when no queue is specified for a task.
     *                        Must be present in availableQueues.
     *                        Example: "q:def" for default queue
     * @return Builder instance for method chaining
     *
     * @throws TaskConfigurationError if queuesToProcess contains queues not in availableQueues,
     *                                or if defaultQueue is not in availableQueues
     */
    public Builder configureQueues(List<String> availableQueues,
                                   List<String> queuesToProcess,
                                   String defaultQueue) {
      this.availableQueues = new HashSet<>(availableQueues).stream().toList();
      this.queuesToProcess = queuesToProcess;
      this.defaultQueue    = defaultQueue;
      return this;
    }

    public Builder jedisClient(JedisPooled jedis) {
      this.jedis = jedis;
      return this;
    }

    public Builder jsonMapper(JsonMapper jsonMapper) {
      this.jsonMapper = jsonMapper;
      return this;
    }

    public BackgroundRunner build() {
      if (taskExecutor == null) taskExecutor = new DefaultVtExecutor();
      if (taskProcessorRegistry == null) throw new TaskConfigurationError("Task processor handler is not set");
      if (eventParser == null) throw new TaskConfigurationError("Cannot build runner without event parser");
      if (jedis == null) throw new TaskConfigurationError("Cannot build runner without jedis client");
      setQueueProcessingStatus();
      return new BackgroundRunner(this);
    }

    public Builder taskProcessorRegistry(TaskProcessorRegistry taskProcessorRegistry) {
      this.taskProcessorRegistry = taskProcessorRegistry;
      return this;
    }
  }
}

package today.bonfire.oss.bth4j.service;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisPooled;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.common.THC;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Accessors(fluent = true)
public class BackgroundRunner implements Runnable {

  @Getter final TaskOps                 taskOps;
  final         QueuesHolder            queuesHolder;
  final         Function<String, Event> eventParser;
  final         THC.Keys                keys;

  private final long                       bgThreadCheckInterval;
  private final long                       staleTaskTimeout;
  private final long                       taskProcessorQueueCheckInterval;
  private final long                       maintenanceCheckInterval;
  private final long                       delayedTasksQueueCheckInterval;
  private final long                       recurringTasksQueueCheckInterval;
  private final long                       queueTaskAheadDurationForRecurringTasks;
  private final long                       cronLockDuration;
  private final BackgroundExecutor         taskExecutor;
  private final ThreadGroup                threadGroup;
  private final TaskProcessorRegistry      taskProcessorRegistry;
  private final TaskCallbacks              taskCallbacks;
  private final TaskCallbacks              recurringTaskCallbacks;
  private final Function<Integer, Integer> taskRetryDelay;
  private final ScheduledExecutorService   scheduler;
  private       boolean                    stopRunner = false;
  private       ScheduledFuture<?>         monitoringTask;

  private volatile CustomThread       taskProcessorService;
  private volatile CustomThread       scheduledTaskService;
  private volatile CustomThread       recurringTaskService;
  private volatile MaintenanceService maintenanceService;


  private BackgroundRunner(Builder builder) {
    this.staleTaskTimeout                   = builder.staleTaskTimeout.toMillis();
    threadGroup                             = new ThreadGroup(builder.threadGroupName);
    maintenanceCheckInterval                = builder.maintenanceCheckInterval.toMillis();
    taskProcessorQueueCheckInterval         = builder.taskProcessorQueueCheckInterval.toMillis();
    bgThreadCheckInterval                   = builder.BGThreadCheckInterval.toMillis();
    delayedTasksQueueCheckInterval          = builder.delayedTasksQueueCheckInterval.toMillis();
    recurringTasksQueueCheckInterval        = builder.recurringTasksQueueCheckInterval.toMillis();
    cronLockDuration                        = builder.cronLockDuration.toSeconds();
    taskExecutor                            = builder.taskExecutor;
    taskProcessorRegistry                   = builder.taskProcessorRegistry;
    taskCallbacks                           = builder.taskCallbacks;
    recurringTaskCallbacks                  = builder.recurringTaskCallbacks;
    taskRetryDelay                          = builder.taskRetryDelay;
    queueTaskAheadDurationForRecurringTasks = builder.recurringTasksQueueAheadDuration.toSeconds();
    eventParser                             = builder.eventParser;
    keys                                    = new THC.Keys(builder.namespacePrefix);
    queuesHolder                            = setupQueues(this.keys.NAMESPACE, builder.availableQueues, builder.queuesToProcess, builder.defaultQueue);
    this.taskOps                            = new TaskOps(this.keys, builder.jedis,
                                                          builder.jsonMapper, eventParser,
                                                          this.queuesHolder);

    scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread thread = new Thread(threadGroup, r, "BackgroundRunner-Monitor");
      thread.setDaemon(true);
      return thread;
    });
  }

  static QueuesHolder setupQueues(String namespace, Set<String> availableQueues,
                                  List<String> queuesToProcess, String defaultQueue) {
    queuesToProcess.forEach(s -> {
      if (!availableQueues.contains(s))
        throw new TaskConfigurationError("Queues to process not found in available queues list. Please check your configuration");
    });

    if (!availableQueues.contains(defaultQueue))
      throw new TaskConfigurationError("Default queue not found in available queues list. Please check your configuration");

    return new QueuesHolder(availableQueues.stream()
                                           .map(s -> namespace + ":" + s)
                                           .collect(Collectors.toUnmodifiableSet()),
                            queuesToProcess.stream()
                                           .map(s -> namespace + ":" + s)
                                           .toList(),
                            namespace + ":" + defaultQueue
    );
  }


  public void run() {
    taskProcessorService = getTaskProcessorService();
    scheduledTaskService = getScheduledTaskService();
    recurringTaskService = getRecurringTaskService();
    maintenanceService   = getMaintenanceService();
    log.info("Background runner - {} with namespace {} started", this.threadGroup.getName(), this.keys.NAMESPACE);
    monitoringTask = scheduler.scheduleWithFixedDelay(() -> {
      try {
        if (stopRunner) {
          log.info("Stopping background runner monitoring task");
          monitoringTask.cancel(false);
          return;
        }

        if (!taskProcessorService.isAlive()) {
          log.error("PrimaryTaskService thread is not alive!!!");
          taskProcessorService = getTaskProcessorService();
        }
        if (!scheduledTaskService.isAlive()) {
          log.error("DelayedTaskService thread is not alive!!!");
          scheduledTaskService = getScheduledTaskService();
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
      } catch (Exception e) {
        log.error("Error in service monitoring", e);
      }
    }, bgThreadCheckInterval, bgThreadCheckInterval, TimeUnit.MILLISECONDS);
  }


  public void stopRunner() {
    stopRunner = true;
    // Stop all services first
    log.info("Stopping background runner services");

    if (taskProcessorService != null) {
      taskProcessorService.stopThread();
    }

    if (scheduledTaskService != null) {
      scheduledTaskService.stopThread();
    }

    if (recurringTaskService != null) {
      recurringTaskService.stopThread();
    }

    if (maintenanceService != null) {
      maintenanceService.stopThread();
    }

    checkServiceStopped(taskProcessorService, "TaskProcessorService");
    checkServiceStopped(scheduledTaskService, "ScheduledTaskService");
    checkServiceStopped(recurringTaskService, "RecurringTaskService");
    checkServiceStopped(maintenanceService, "MaintenanceService");

    if (monitoringTask != null && !monitoringTask.isDone()) {
      monitoringTask.cancel(false);
    }

    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private void checkServiceStopped(CustomThread service, String serviceName) {
    if (service != null) {
      try {
        service.waitTillDone();
        log.info("{} stopped", serviceName);
        Thread.sleep(100);
        if (service.isRunning()) {
          log.warn("{} did not stop within timeout", serviceName);
        }
      } catch (InterruptedException e) {
        log.error("Interrupted while waiting for {} to stop", serviceName, e);
        Thread.currentThread().interrupt();
      }
    }
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
        .setBackgroundRunner(this)
        .create();
    taskProcessorService.start();
    log.info("Thread {} with id {} started", taskProcessorService.getName(),
             taskProcessorService.threadId());
    return taskProcessorService;
  }

  private ScheduledTaskService getScheduledTaskService() {
    var delayedTaskService = new ScheduledTaskService.Builder().setGroup(threadGroup)
                                                               .setThreadName("DelayedTaskService")
                                                               .setDelay(delayedTasksQueueCheckInterval)
                                                               .setTaskOps(taskOps)
                                                               .setKeys(keys)
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
                                          .setLockTimeout(cronLockDuration)
                                          .setTaskOps(taskOps)
                                          .setKeys(keys)
                                          .setEventParser(eventParser)
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
        .setTaskOps(taskOps)
        .setKeys(keys)
        .setEventParser(eventParser)
        .setQueuesHolder(queuesHolder)
        .create();
    log.info("Thread {} with id {} started", maintenanceService.getName(),
             maintenanceService.threadId());
    maintenanceService.start();
    return maintenanceService;
  }

  public static class Builder {
    private Duration                   recurringTasksQueueAheadDuration = Duration.ofSeconds(THC.Time.T_1_HOUR);
    private Duration                   cronLockDuration                 = Duration.ofSeconds(THC.Time.T_5_MINUTES);
    private Duration                   staleTaskTimeout                 = Duration.ofSeconds(THC.Time.T_10_MINUTES);
    private Duration                   BGThreadCheckInterval            = Duration.ofSeconds(THC.Time.T_10_SECONDS);
    private Duration                   taskProcessorQueueCheckInterval  = Duration.ofMillis(100); // 100 miliseconds
    private Duration                   maintenanceCheckInterval         = Duration.ofMillis(10000);
    private Duration                   delayedTasksQueueCheckInterval   = Duration.ofMillis(1000);
    private Duration                   recurringTasksQueueCheckInterval = Duration.ofMillis(10000);
    private String                     threadGroupName                  = "bgr";
    private BackgroundExecutor         taskExecutor                     = null;
    private Set<String>                availableQueues                  = Set.of("Q:DEF", "Q:LOW", "Q:MED", "Q:HIGH");
    private List<String>               queuesToProcess                  = availableQueues.stream().toList();
    private String                     defaultQueue                     = queuesToProcess.getFirst();
    private TaskProcessorRegistry      taskProcessorRegistry;
    private TaskCallbacks              taskCallbacks                    = TaskCallbacks.noOp();
    private TaskCallbacks              recurringTaskCallbacks           = TaskCallbacks.noOp();
    private Function<Integer, Integer> taskRetryDelay                   = retryCount -> retryCount ^ 3;

    private String                  namespacePrefix = "BTH";
    private Function<String, Event> eventParser;
    private JedisPooled             jedis;
    private JsonMapper              jsonMapper;


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

    /**
     * Sets the event parser.
     *
     * @param parser - a function to convert a string to an {@link Event} object.
     *               This function is used to parse the event string from task to an {@link Event} object.
     *
     *               <p> NOTE: This value is static and will be used for all instances of background runners
     * @return current Builder instance for method chaining.
     */
    public Builder eventParser(Function<String, Event> parser) {
      this.eventParser = parser;
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
     * This function is used to calculate the time to wait before retrying a failed task.
     * The function is given the retry count and should return the time in seconds to wait before retrying.
     * The default value is a function that returns the retry count cubed (x ^ 3).
     *
     * @param taskRetryDelay function to calculate time to wait before retrying a failed task.
     *                       The function is given the retry count and should return the time in seconds to wait before retrying.
     * @return current Builder instance for method chaining.
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
     *                default is 10 minutes
     */
    public Builder staleTaskTimeout(Duration timeout) {
      this.staleTaskTimeout = timeout;
      return this;
    }

    /**
     * @param namespacePrefix - prefix for all keys used by background runner
     *                        this also prefixes the queue names.
     *                        default is "BTH"
     */
    public Builder namespacePrefix(String namespacePrefix) {
      this.namespacePrefix = namespacePrefix;
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
     *                        if null, then all queues in availableQueues will be processed, order of queues is undetermined.
     *                        Example: List.of("q:low", "q:med") to only process low and medium priority queues.
     * @param defaultQueue    - Default queue to use when no queue is specified for a task.
     *                        Must be present in availableQueues. if null, then the first item in queuesToProcess will be used
     *                        Example: "q:def" for default queue
     * @return Builder instance for method chaining
     *
     * @throws TaskConfigurationError if queuesToProcess contains queues not in availableQueues,
     *                                or if defaultQueue is not in availableQueues
     */
    public Builder configureQueues(List<String> availableQueues,
                                   List<String> queuesToProcess,
                                   String defaultQueue) {
      if (ObjectUtils.isEmpty(availableQueues)) throw new TaskConfigurationError("Available queues cannot be null or empty");
      availableQueues.forEach(s -> {
        if (StringUtils.isBlank(s)) throw new TaskConfigurationError("Available queue name cannot have null or blank queue names");
      });
      this.availableQueues = new HashSet<>(availableQueues);
      if (ObjectUtils.isEmpty(queuesToProcess)) {
        this.queuesToProcess = this.availableQueues.stream().toList();
      } else {
        this.queuesToProcess = queuesToProcess;
      }
      this.defaultQueue = StringUtils.isBlank(defaultQueue) ? this.queuesToProcess.getFirst() : defaultQueue;
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
      return new BackgroundRunner(this);
    }

    public Builder taskProcessorRegistry(TaskProcessorRegistry taskProcessorRegistry) {
      this.taskProcessorRegistry = taskProcessorRegistry;
      return this;
    }

    /**
     * Configure the lock time for a cron job run.
     *
     * <p>
     * When a cron job starts, it will try to acquire a lock.
     * set the time for this lock such that all task can be processed within this time duration.
     * Do note that thousands of tasks can be processed at with the default value of 5 minutes
     * so you may not need to change this value unless you have an extremely large number of tasks.
     *
     * @param cronLockDuration - the duration for a cron job lock, default is 5 minutes
     */
    public void cronLockDuration(Duration cronLockDuration) {
      this.cronLockDuration = cronLockDuration;
    }
  }
}

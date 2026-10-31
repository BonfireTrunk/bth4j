package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.Event;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;
import today.bonfire.oss.bth4j.store.TaskStore;

/**
 * Starts and supervises the background services: task processing, scheduled tasks, recurring tasks and maintenance.
 * <p>
 * Create it with {@link Builder}, start it by running it on a thread (or calling {@link #run()}), and stop it with
 * {@link #stopRunner()}. {@link #run()} returns as soon as the services are started, it does not block.
 * <p>
 * Delivery is at least once: a task runs again when the worker holding it died or could not renew its lease, so
 * task processors must be idempotent. Several runners with the same namespace share the work.
 */
public class BackgroundRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundRunner.class);

    final TaskOps taskOps;
    final QueuesHolder queuesHolder;

    private final TaskProcessorRegistry taskProcessorRegistry;
    private final Duration serviceCheckInterval;
    private final Duration leaseDuration;
    private final Duration idlePollInterval;
    private final Duration maintenanceCheckInterval;
    private final Duration scheduledTasksCheckInterval;
    private final Duration recurringTasksCheckInterval;
    private final Duration recurringTasksQueueAheadDuration;
    private final Duration deadTaskRetention;
    private final int claimBatchSize;
    private final BackgroundExecutor taskExecutor;
    private final ThreadGroup threadGroup;
    private final TaskCallbacks taskCallbacks;
    private final TaskCallbacks recurringTaskCallbacks;
    private final Function<Integer, Duration> taskRetryDelay;
    private final ScheduledExecutorService scheduler;
    private final Duration shutdownTimeout;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();
    /** owned here, not by the processor, so that a restarted processor keeps renewing tasks already running */
    private final Map<String, Task> runningTasks = new ConcurrentHashMap<>();

    private volatile boolean stopRunner = false;
    private volatile ScheduledFuture<?> monitoringTask;

    private volatile TaskProcessorService taskProcessorService;
    private volatile CustomThread scheduledTaskService;
    private volatile CustomThread recurringTaskService;
    private volatile CustomThread maintenanceService;

    private BackgroundRunner(Builder builder) {
        threadGroup = new ThreadGroup(builder.threadGroupName);
        serviceCheckInterval = builder.serviceCheckInterval;
        leaseDuration = builder.leaseDuration;
        idlePollInterval = builder.idlePollInterval;
        maintenanceCheckInterval = builder.maintenanceCheckInterval;
        scheduledTasksCheckInterval = builder.scheduledTasksCheckInterval;
        recurringTasksCheckInterval = builder.recurringTasksCheckInterval;
        recurringTasksQueueAheadDuration = builder.recurringTasksQueueAheadDuration;
        deadTaskRetention = builder.deadTaskRetention;
        claimBatchSize = builder.claimBatchSize;
        taskExecutor = builder.taskExecutor != null ? builder.taskExecutor : new DefaultVtExecutor();
        shutdownTimeout = builder.shutdownTimeout;
        taskProcessorRegistry =
                builder.taskProcessorRegistry != null ? builder.taskProcessorRegistry : new TaskProcessorRegistry();
        taskCallbacks = builder.taskCallbacks;
        recurringTaskCallbacks = builder.recurringTaskCallbacks;
        taskRetryDelay = builder.taskRetryDelay;
        queuesHolder = setupQueues(builder.availableQueues, builder.queuesToProcess, builder.defaultQueue);
        taskOps = new TaskOps(
                builder.taskStore, builder.jsonMapper, builder.eventParser, queuesHolder, deadTaskRetention);

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(threadGroup, r, "BackgroundRunner-Monitor");
            thread.setDaemon(true);
            return thread;
        });
    }

    static QueuesHolder setupQueues(Set<String> availableQueues, List<String> queuesToProcess, String defaultQueue) {
        if (queuesToProcess.isEmpty())
            throw new TaskConfigurationError("Queues to process cannot be empty. Please check your configuration");

        queuesToProcess.forEach(s -> {
            if (!availableQueues.contains(s))
                throw new TaskConfigurationError(
                        "Queues to process not found in available queues list. Please check your configuration");
        });

        if (!availableQueues.contains(defaultQueue))
            throw new TaskConfigurationError(
                    "Default queue not found in available queues list. Please check your configuration");

        return new QueuesHolder(availableQueues, queuesToProcess, defaultQueue);
    }

    /**
     * Starts all services and the monitor that restarts a service if its thread dies.
     * Returns after starting, it does not block. Calling it a second time, or after {@link #stopRunner()},
     * logs a warning and does nothing.
     */
    @Override
    public void run() {
        if (!started.compareAndSet(false, true)) {
            log.warn(
                    "Background runner - {} on {} is already started, ignoring call to run()",
                    threadGroup.getName(),
                    taskOps.store());
            return;
        }
        synchronized (lifecycleLock) {
            if (stopRunner) {
                log.warn(
                        "Background runner - {} on {} was stopped before it started",
                        threadGroup.getName(),
                        taskOps.store());
                return;
            }
            taskProcessorService = startTaskProcessorService();
            scheduledTaskService = startScheduledTaskService();
            recurringTaskService = startRecurringTaskService();
            maintenanceService = startMaintenanceService();
            log.info("Background runner - {} on {} started", threadGroup.getName(), taskOps.store());
            monitoringTask = scheduler.scheduleWithFixedDelay(
                    this::checkServices,
                    serviceCheckInterval.toMillis(),
                    serviceCheckInterval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Restarts any service whose thread is no longer alive.
     */
    private void checkServices() {
        try {
            synchronized (lifecycleLock) {
                if (stopRunner) {
                    log.info("Stopping background runner monitoring task");
                    var task = monitoringTask;
                    if (task != null) task.cancel(false);
                    return;
                }
                if (!taskProcessorService.isAlive()) {
                    log.error("Task processor thread is not alive, restarting it");
                    taskProcessorService.stopLeaseRenewal();
                    taskProcessorService = startTaskProcessorService();
                }
                if (!scheduledTaskService.isAlive()) {
                    log.error("ScheduledTaskService thread is not alive, restarting it");
                    scheduledTaskService = startScheduledTaskService();
                }
                if (!recurringTaskService.isAlive()) {
                    log.error("RecurringTaskService thread is not alive, restarting it");
                    recurringTaskService = startRecurringTaskService();
                }
                if (!maintenanceService.isRunning()) {
                    log.error("MaintenanceService is not running, restarting it");
                    maintenanceService = startMaintenanceService();
                }
                log.trace("Current threads in group {}: {}", threadGroup.getName(), threadGroup.activeCount());
            }
        } catch (Exception e) {
            log.error("Error in service monitoring", e);
        }
    }

    /**
     * Gracefully stops the runner: stops claiming tasks, stops all services, shuts down the task executor and
     * waits up to the configured shutdown timeout for tasks that are already running, renewing their leases
     * meanwhile. Tasks still running after the timeout keep their lease until it expires and are then retried.
     * <p>
     * The task executor passed to the builder is shut down and cannot be reused.
     * Call this before closing the Jedis client. Calling it again is a no-op, also while the first call is
     * still waiting.
     */
    public void stopRunner() {
        if (!stopped.compareAndSet(false, true)) {
            log.debug("Background runner {} is already stopped or stopping", threadGroup.getName());
            return;
        }
        log.info("Stopping background runner services - {} on {}", threadGroup.getName(), taskOps.store());

        final TaskProcessorService processor;
        final CustomThread scheduled;
        final CustomThread recurring;
        final CustomThread maintenance;
        // under the lock so that the monitor cannot restart a service that is being stopped
        synchronized (lifecycleLock) {
            stopRunner = true;
            var task = monitoringTask;
            if (task != null) task.cancel(false);
            processor = taskProcessorService;
            scheduled = scheduledTaskService;
            recurring = recurringTaskService;
            maintenance = maintenanceService;
            for (var service : new CustomThread[] {processor, scheduled, recurring, maintenance}) {
                if (service != null) service.stopThread();
            }
        }

        checkServiceStopped(processor, "Task Processor Service");
        checkServiceStopped(scheduled, "Scheduled Task Service");
        checkServiceStopped(recurring, "Recurring Task Service");
        checkServiceStopped(maintenance, "Maintenance Service");

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(shutdownTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // no service submits tasks anymore, let the running ones finish while their leases are still renewed
        taskExecutor.shutdown();
        try {
            if (taskExecutor.awaitTermination(shutdownTimeout)) {
                log.info("Background runner - {} stopped", threadGroup.getName());
            } else {
                log.warn(
                        "Tasks still running after {}. They keep their lease until it expires and are then retried",
                        shutdownTimeout);
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for running tasks to finish");
            Thread.currentThread().interrupt();
        } finally {
            if (processor != null) processor.stopLeaseRenewal();
        }
    }

    private void checkServiceStopped(CustomThread service, String serviceName) {
        if (service != null) {
            try {
                if (service.waitTillDone(shutdownTimeout)) {
                    log.info("{} stopped", serviceName);
                } else {
                    log.warn("{} did not stop within {}", serviceName, shutdownTimeout);
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for {} to stop", serviceName, e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private TaskProcessorService startTaskProcessorService() {
        var service = new TaskProcessorService.Builder()
                .setGroup(threadGroup)
                .setThreadName("taskProcessor")
                .setExecutor(taskExecutor)
                .setTaskOps(taskOps)
                .setTaskProcessorRegistry(taskProcessorRegistry)
                .setCallbacks(taskCallbacks, recurringTaskCallbacks)
                .setRetryDelay(taskRetryDelay)
                .setLease(leaseDuration)
                .setIdlePollInterval(idlePollInterval)
                .setClaimBatchSize(claimBatchSize)
                .setRunning(runningTasks)
                .create();
        service.start();
        log.info("Task processor with id {} started", service.threadId());
        return service;
    }

    private CustomThread startScheduledTaskService() {
        var service = new ScheduledTaskService.Builder()
                .setGroup(threadGroup)
                .setThreadName("ScheduledTaskService")
                .setTaskOps(taskOps)
                .setPollInterval(scheduledTasksCheckInterval)
                .setBatchSize(claimBatchSize * 4)
                .create();
        service.start();
        log.info("Scheduled task service with id {} started", service.threadId());
        return service;
    }

    private CustomThread startRecurringTaskService() {
        var service = new RecurringTaskService.Builder()
                .setGroup(threadGroup)
                .setThreadName("RecurringTaskService")
                .setTaskOps(taskOps)
                .setCheckInterval(recurringTasksCheckInterval)
                .setQueueAheadBy(recurringTasksQueueAheadDuration)
                .setBatchSize(claimBatchSize * 4)
                .create();
        service.start();
        log.info("Recurring task service with id {} started", service.threadId());
        return service;
    }

    private CustomThread startMaintenanceService() {
        var service = new MaintenanceService.Builder()
                .setGroup(threadGroup)
                .setThreadName("Maintenance")
                .setTaskOps(taskOps)
                .setCheckInterval(maintenanceCheckInterval)
                .setDeadRetention(deadTaskRetention)
                .setTaskRetryDelay(taskRetryDelay)
                .setBatchSize(claimBatchSize * 4)
                .create();
        service.start();
        log.info("Maintenance service with id {} started", service.threadId());
        return service;
    }

    public TaskOps taskOps() {
        return taskOps;
    }

    public TaskProcessorRegistry taskProcessorRegistry() {
        return taskProcessorRegistry;
    }

    /** @return the number of tasks running on this instance right now */
    public int runningTasks() {
        var p = taskProcessorService;
        return p == null ? 0 : p.runningCount();
    }

    /**
     * Exponential backoff with jitter: 1 s, 2 s, 4 s ... capped at one hour, each varied by up to 20 percent.
     */
    public static Duration defaultRetryDelay(int attempt) {
        long base = Math.min(Duration.ofHours(1).toMillis(), 1000L << Math.min(20, Math.max(0, attempt - 1)));
        long jitter = (long) (base * 0.2 * ThreadLocalRandom.current().nextDouble(-1, 1));
        return Duration.ofMillis(Math.max(0, base + jitter));
    }

    public static class Builder {
        private Duration recurringTasksQueueAheadDuration = Duration.ofHours(1);
        private Duration leaseDuration = Duration.ofSeconds(60);
        private Duration serviceCheckInterval = Duration.ofSeconds(10);
        private Duration idlePollInterval = Duration.ofMillis(500);
        private Duration maintenanceCheckInterval = Duration.ofSeconds(5);
        private Duration scheduledTasksCheckInterval = Duration.ofSeconds(1);
        private Duration recurringTasksCheckInterval = Duration.ofSeconds(10);
        private Duration deadTaskRetention = Duration.ofDays(30);
        private Duration shutdownTimeout = Duration.ofSeconds(30);
        private int claimBatchSize = 32;
        private String threadGroupName = "bgr";
        private BackgroundExecutor taskExecutor;
        // LinkedHashSet and List.of keep a fixed order, so the default queue is the same on every run
        private Set<String> availableQueues = new LinkedHashSet<>(List.of("Q:DEF", "Q:LOW", "Q:MED", "Q:HIGH"));
        private List<String> queuesToProcess = List.of("Q:DEF", "Q:LOW", "Q:MED", "Q:HIGH");
        private String defaultQueue = "Q:DEF";
        private TaskProcessorRegistry taskProcessorRegistry;
        private TaskCallbacks taskCallbacks = TaskCallbacks.noOp();
        private TaskCallbacks recurringTaskCallbacks = TaskCallbacks.noOp();
        private Function<Integer, Duration> taskRetryDelay = BackgroundRunner::defaultRetryDelay;
        private Function<String, Event> eventParser;
        private TaskStore taskStore;
        private JsonMapper jsonMapper;

        /**
         * @param duration how far ahead of their time occurrences of recurring tasks are created in the
         *                 scheduled set. They run at their scheduled time. Default is 1 hour.
         */
        public Builder recurringTasksQueueAheadDuration(Duration duration) {
            this.recurringTasksQueueAheadDuration = requirePositive(duration, "recurringTasksQueueAheadDuration");
            return this;
        }

        /**
         * @param parser a function to convert an event value to an {@link Event}. Required.
         */
        public Builder eventParser(Function<String, Event> parser) {
            this.eventParser = parser;
            return this;
        }

        /**
         * @param interval time between checks that the service threads are alive; a dead one is restarted.
         *                 Default is 10 seconds.
         */
        public Builder serviceCheckInterval(Duration interval) {
            this.serviceCheckInterval = requirePositive(interval, "serviceCheckInterval");
            return this;
        }

        /**
         * How long a claimed task stays leased without renewal. Leases are renewed every third of this duration
         * while the task runs, so a task may run longer than the lease. A task whose lease expired, because the
         * instance died or could not reach Redis, is retried by maintenance.
         *
         * @param lease the lease duration, default 60 seconds, at least 1 second
         */
        public Builder leaseDuration(Duration lease) {
            requirePositive(lease, "leaseDuration");
            if (lease.toMillis() < 1000) throw new TaskConfigurationError("leaseDuration cannot be less than 1 second");
            this.leaseDuration = lease;
            return this;
        }

        /**
         * @param interval how long the task processor sleeps when a claim found nothing, and how long it waits
         *                 for a free executor slot per attempt. One claim covers all queues, so an idle runner
         *                 makes one store call per interval. Default is 500 milliseconds, minimum 10.
         */
        public Builder idlePollInterval(Duration interval) {
            requirePositive(interval, "idlePollInterval");
            if (interval.toMillis() < 10) throw new TaskConfigurationError("idlePollInterval cannot be less than 10ms");
            this.idlePollInterval = interval;
            return this;
        }

        /**
         * @param interval time between maintenance runs that retry tasks whose lease expired.
         *                 Default is 5 seconds, at least 1 second.
         */
        public Builder maintenanceCheckInterval(Duration interval) {
            requirePositive(interval, "maintenanceCheckInterval");
            if (interval.toMillis() < 1000) throw new TaskConfigurationError("Delay cannot be less than 1 second");
            this.maintenanceCheckInterval = interval;
            return this;
        }

        /**
         * @param interval upper bound of the wait between checks for due scheduled tasks; the scheduler wakes
         *                 earlier when it knows the next due time. Default is 1 second, at least 10 ms.
         */
        public Builder scheduledTasksCheckInterval(Duration interval) {
            requirePositive(interval, "scheduledTasksCheckInterval");
            if (interval.toMillis() < 10) throw new TaskConfigurationError("Delay cannot be less than 10ms");
            this.scheduledTasksCheckInterval = interval;
            return this;
        }

        /**
         * @param interval time between checks for recurring tasks whose next occurrence falls into the
         *                 queue-ahead window. Default is 10 seconds.
         */
        public Builder recurringTasksCheckInterval(Duration interval) {
            this.recurringTasksCheckInterval = requirePositive(interval, "recurringTasksCheckInterval");
            return this;
        }

        /**
         * @param retention how long dead tasks are kept for inspection and {@link TaskOps#retryDeadTask(String)}.
         *                  Default is 30 days.
         */
        public Builder deadTaskRetention(Duration retention) {
            this.deadTaskRetention = requirePositive(retention, "deadTaskRetention");
            return this;
        }

        /**
         * @param size the most tasks claimed from Redis in one call, bounded by the free executor slots.
         *             Default is 32.
         */
        public Builder claimBatchSize(int size) {
            if (size < 1) throw new TaskConfigurationError("claimBatchSize must be at least 1");
            this.claimBatchSize = size;
            return this;
        }

        /**
         * @param threadGroupName name of the thread group of all service threads, default is "bgr". Must not be blank.
         */
        public Builder threadGroupName(String threadGroupName) {
            if (StringUtils.isBlank(threadGroupName))
                throw new TaskConfigurationError("threadGroupName cannot be blank");
            this.threadGroupName = threadGroupName;
            return this;
        }

        /**
         * @param executor executor that runs the tasks. Default is a new {@link DefaultVtExecutor} per runner.
         *                 The runner shuts it down in {@link BackgroundRunner#stopRunner()}.
         */
        public Builder taskExecutor(BackgroundExecutor executor) {
            this.taskExecutor = executor;
            return this;
        }

        /**
         * The delay before a failed task is tried again, given the number of attempts made so far (1 for the
         * first failure). Default is {@link BackgroundRunner#defaultRetryDelay(int)}.
         */
        public Builder taskRetryDelay(Function<Integer, Duration> taskRetryDelay) {
            if (taskRetryDelay == null) throw new TaskConfigurationError("taskRetryDelay cannot be null");
            this.taskRetryDelay = taskRetryDelay;
            return this;
        }

        /**
         * Callbacks for regular tasks. Any of them may be null.
         *
         * @param beforeTask called before a task starts
         * @param onSuccess  called when a task completed successfully
         * @param onError    called when a task failed, after the failure was recorded
         * @param afterTask  called after every task, successful or not
         */
        public Builder taskHandlerDecorators(
                Consumer<Task> beforeTask,
                Consumer<Task> onSuccess,
                BiConsumer<Task, Exception> onError,
                Consumer<Task> afterTask) {
            this.taskCallbacks = new TaskCallbacks(beforeTask, onSuccess, onError, afterTask);
            return this;
        }

        /**
         * Callbacks for recurring tasks, see {@link #taskHandlerDecorators}.
         */
        public Builder recurringTaskHandlerDecorators(
                Consumer<Task> beforeTask,
                Consumer<Task> onSuccess,
                BiConsumer<Task, Exception> onError,
                Consumer<Task> afterTask) {
            this.recurringTaskCallbacks = new TaskCallbacks(beforeTask, onSuccess, onError, afterTask);
            return this;
        }

        /**
         * Configures the queues. Queue names are logical names without the namespace.
         * <p>
         * A queue may appear more than once in {@code queuesToProcess} to give it more turns in the round robin.
         *
         * @param availableQueues all queues tasks can be enqueued to. Duplicates are removed.
         * @param queuesToProcess the queues this instance processes, in round robin order. Must be a subset of
         *                        availableQueues. If null or empty, all available queues are processed in order.
         * @param defaultQueue    the queue of tasks that do not name one. Must be in availableQueues. If null or
         *                        blank, the first queue to process is used.
         * @throws TaskConfigurationError if the configuration is inconsistent
         */
        public Builder configureQueues(
                List<String> availableQueues, List<String> queuesToProcess, String defaultQueue) {
            if (ObjectUtils.isEmpty(availableQueues))
                throw new TaskConfigurationError("Available queues cannot be null or empty");
            availableQueues.forEach(s -> {
                if (StringUtils.isBlank(s))
                    throw new TaskConfigurationError("Available queue name cannot have null or blank queue names");
            });
            // LinkedHashSet keeps the order given by the caller, a HashSet would make it random
            this.availableQueues = new LinkedHashSet<>(availableQueues);
            if (ObjectUtils.isEmpty(queuesToProcess)) {
                this.queuesToProcess = new ArrayList<>(this.availableQueues);
            } else {
                this.queuesToProcess = new ArrayList<>(queuesToProcess);
            }
            this.defaultQueue = StringUtils.isBlank(defaultQueue) ? this.queuesToProcess.getFirst() : defaultQueue;
            return this;
        }

        /**
         * @param taskStore the store that keeps the tasks, for example a {@code RedisTaskStore} from the bth4j-redis
         *                  module. Required. Runners that share a store (and, for Redis, a namespace) share the work.
         */
        public Builder taskStore(TaskStore taskStore) {
            this.taskStore = taskStore;
            return this;
        }

        /**
         * @param jsonMapper mapper used to store and read task data as JSON. Needed when tasks carry data.
         */
        public Builder jsonMapper(JsonMapper jsonMapper) {
            this.jsonMapper = jsonMapper;
            return this;
        }

        /**
         * @param taskProcessorRegistry registry that maps events to task processors.
         *                              If not set, an empty registry is used and processors must be added to it
         *                              through the runner's {@code taskProcessorRegistry()} accessor.
         */
        public Builder taskProcessorRegistry(TaskProcessorRegistry taskProcessorRegistry) {
            this.taskProcessorRegistry = taskProcessorRegistry;
            return this;
        }

        /**
         * How long {@link BackgroundRunner#stopRunner()} waits for each service to stop, and then for running tasks to finish.
         *
         * @param shutdownTimeout maximum wait time, default is 30 seconds. Must not be negative.
         */
        public Builder shutdownTimeout(Duration shutdownTimeout) {
            if (shutdownTimeout == null || shutdownTimeout.isNegative()) {
                throw new TaskConfigurationError("shutdownTimeout cannot be null or negative");
            }
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        /**
         * Builds the runner. Nothing is started and the store is not used until {@link BackgroundRunner#run()}.
         * The builder can be reused; every build gets its own default executor.
         *
         * @throws TaskConfigurationError if the event parser or task store is missing,
         *                                or the queue configuration is invalid
         */
        public BackgroundRunner build() {
            if (eventParser == null) throw new TaskConfigurationError("Cannot build runner without event parser");
            if (taskStore == null) throw new TaskConfigurationError("Cannot build runner without a task store");
            setupQueues(availableQueues, queuesToProcess, defaultQueue);
            if (taskProcessorRegistry == null) {
                log.info(
                        "Default TaskProcessorRegistry used. Make sure to configure/add task processors before starting the runner.");
            }
            return new BackgroundRunner(this);
        }

        private static Duration requirePositive(Duration duration, String name) {
            if (duration == null || duration.isZero() || duration.isNegative()) {
                throw new TaskConfigurationError(name + " must be a positive duration");
            }
            return duration;
        }

        // package-private accessors for tests
        Function<Integer, Duration> getTaskRetryDelay() {
            return taskRetryDelay;
        }

        String getDefaultQueue() {
            return defaultQueue;
        }

        List<String> getQueuesToProcess() {
            return queuesToProcess;
        }

        Set<String> getAvailableQueues() {
            return availableQueues;
        }
    }
}

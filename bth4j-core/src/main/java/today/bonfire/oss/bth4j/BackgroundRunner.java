package today.bonfire.oss.bth4j;

import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;
import today.bonfire.oss.bth4j.executor.FixedExecutor;
import today.bonfire.oss.bth4j.service.MaintenanceService;
import today.bonfire.oss.bth4j.service.Names;
import today.bonfire.oss.bth4j.service.Observers;
import today.bonfire.oss.bth4j.service.RecurringTaskService;
import today.bonfire.oss.bth4j.service.RunnerServices;
import today.bonfire.oss.bth4j.service.ScheduledTaskService;
import today.bonfire.oss.bth4j.service.TaskExecution;
import today.bonfire.oss.bth4j.service.TaskProcessorService;
import today.bonfire.oss.bth4j.spi.FullStore;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * Runs tasks: claims them from the store, executes them with the registered processors, moves scheduled
 * tasks into their queues, creates occurrences of recurring definitions and keeps the store tidy.
 * <p>
 * Build it with {@link #builder()}, register processors on {@link #registry()}, then {@link #start()}.
 * {@link #stop()} drains running tasks within the shutdown timeout. Several runners sharing a store share
 * the work. Delivery is at least once.
 */
public final class BackgroundRunner {
    private static final Logger log = LoggerFactory.getLogger(BackgroundRunner.class);

    private final Builder config;
    private final FullStore store;
    private final TaskProcessorRegistry registry;
    private final TaskClient client;
    private final Names names;
    private final Observers observer;
    private final BackgroundExecutor taskExecutor;
    private final ThreadGroup threadGroup;
    private final ScheduledExecutorService scheduler;
    private final QueuesHolder queues;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();
    private final Map<Long, TaskExecution> runningTasks = new ConcurrentHashMap<>();

    private volatile boolean stopRequested;
    private volatile boolean quiet;
    private volatile int workerId;
    private volatile ScheduledFuture<?> monitoringTask;
    private volatile TaskProcessorService taskProcessorService;
    private volatile CustomThread scheduledTaskService;
    private volatile CustomThread recurringTaskService;
    private volatile CustomThread maintenanceService;
    private volatile RunnerServices services;

    private BackgroundRunner(Builder b) {
        this.config = b;
        this.store = b.store;
        this.registry = b.registry != null ? b.registry : new TaskProcessorRegistry();
        this.threadGroup = new ThreadGroup(b.threadGroupName);
        this.queues = b.queues();
        this.names = new Names(store);
        this.observer = new Observers(b.observer);
        this.taskExecutor = b.taskExecutor != null
                ? b.taskExecutor
                : switch (b.concurrency) {
                    case Concurrency.Fixed f -> new FixedExecutor(f.limit());
                    case Concurrency.Adaptive a -> new DefaultVtExecutor(a.min(), a.max());
                };
        this.client = TaskClient.builder()
                .store(store)
                .jsonMapper(b.jsonMapper)
                .clock(b.clock)
                .idBlockSize(b.idBlockSize)
                .defaultMaxAttempts(b.defaultMaxAttempts)
                .queues(queues)
                .names(names)
                .build();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(threadGroup, r, "BackgroundRunner-Monitor");
            thread.setDaemon(true);
            return thread;
        });
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The producer side on the same store. */
    public TaskClient client() {
        return client;
    }

    /** Register processors here before {@link #start()}. */
    public TaskProcessorRegistry registry() {
        return registry;
    }

    /** @return this runner's worker id, 0 before {@link #start()} */
    public int workerId() {
        return workerId;
    }

    /** @return the number of tasks running on this instance right now */
    public int runningTasks() {
        return runningTasks.size();
    }

    /**
     * Starts all services and the monitor that restarts a service if its thread dies. Returns after
     * starting. Calling it a second time, or after {@link #stop()}, logs a warning and does nothing.
     *
     * @throws TaskConfigurationError when no processor is registered
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            log.warn("Background runner {} is already started, ignoring start()", threadGroup.getName());
            return;
        }
        if (registry.isEmpty()) {
            throw new TaskConfigurationError("No task processor registered; register processors before start()");
        }
        synchronized (lifecycleLock) {
            if (stopRequested) {
                log.warn("Background runner {} was stopped before it started", threadGroup.getName());
                return;
            }
            registry.freeze();
            workerId = store.allocateWorkerId(config.workerLabel);
            var queueIds = new ArrayList<Integer>();
            for (var q : queues.queuesToProcess) queueIds.add(names.queueId(q));
            var eventIds = new HashSet<Integer>();
            for (var e : registry.typeNames()) eventIds.add(names.eventId(e));
            services = new RunnerServices(
                    store,
                    names,
                    client.codecs(),
                    client.ids(),
                    registry,
                    config.retryPolicy,
                    observer,
                    config.clock,
                    !config.succeededRetention.isZero(),
                    config.keepSucceededPayload,
                    !config.deadRetention.isZero(),
                    List.copyOf(queueIds),
                    Set.copyOf(eventIds),
                    workerId);
            taskProcessorService = startTaskProcessorService();
            scheduledTaskService = startScheduledTaskService();
            recurringTaskService = startRecurringTaskService();
            maintenanceService = startMaintenanceService();
            log.info("Background runner {} started as worker {} on {}", threadGroup.getName(), workerId, store.info());
            monitoringTask = scheduler.scheduleWithFixedDelay(
                    this::checkServices,
                    config.serviceCheckInterval.toMillis(),
                    config.serviceCheckInterval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /** Stops claiming new tasks; running tasks finish and every other service keeps going. */
    public void quiet() {
        quiet = true;
        var p = taskProcessorService;
        if (p != null) p.quiet(true);
        log.info("Background runner {} is quiet", threadGroup.getName());
    }

    /** Resumes claiming after {@link #quiet()}. */
    public void resume() {
        quiet = false;
        var p = taskProcessorService;
        if (p != null) p.quiet(false);
        log.info("Background runner {} resumed", threadGroup.getName());
    }

    public boolean isQuiet() {
        return quiet;
    }

    private void checkServices() {
        try {
            synchronized (lifecycleLock) {
                if (stopRequested) {
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
            }
        } catch (Exception e) {
            log.error("Error in service monitoring", e);
        }
    }

    /**
     * Stops the runner: stops claiming, stops all services, shuts down the task executor and waits up to the
     * shutdown timeout for running tasks, renewing their leases meanwhile. Tasks still running afterwards
     * keep their lease until it expires and are then retried. Calling it again is a no-op.
     */
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            log.debug("Background runner {} is already stopped or stopping", threadGroup.getName());
            return;
        }
        log.info("Stopping background runner {}", threadGroup.getName());
        final TaskProcessorService processor;
        final CustomThread scheduled;
        final CustomThread recurring;
        final CustomThread maintenance;
        // under the lock so that the monitor cannot restart a service that is being stopped
        synchronized (lifecycleLock) {
            stopRequested = true;
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
        var timeout = config.shutdownTimeout;
        awaitStop(processor, "Task processor", timeout);
        awaitStop(scheduled, "Scheduled task service", timeout);
        awaitStop(recurring, "Recurring task service", timeout);
        awaitStop(maintenance, "Maintenance service", timeout);

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // no service submits tasks anymore, let the running ones finish while their leases are still renewed
        taskExecutor.shutdown();
        try {
            if (taskExecutor.awaitTermination(timeout)) {
                log.info("Background runner {} stopped", threadGroup.getName());
            } else {
                log.warn(
                        "Tasks still running after {}. They keep their lease until it expires and are then retried",
                        timeout);
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for running tasks to finish");
            Thread.currentThread().interrupt();
        } finally {
            if (processor != null) processor.stopLeaseRenewal();
        }
    }

    private static void awaitStop(CustomThread service, String name, Duration timeout) {
        if (service == null) return;
        try {
            if (service.waitTillDone(timeout)) {
                log.info("{} stopped", name);
            } else {
                log.warn("{} did not stop within {}", name, timeout);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for {} to stop", name, e);
            Thread.currentThread().interrupt();
        }
    }

    private TaskProcessorService startTaskProcessorService() {
        var service = services.processorBuilder()
                .group(threadGroup)
                .threadName("taskProcessor")
                .executor(taskExecutor)
                .running(runningTasks)
                .lease(config.leaseDuration)
                .idlePollInterval(config.idlePollInterval)
                .claimBatchSize(config.claimBatchSize)
                .quiet(quiet)
                .create();
        service.start();
        return service;
    }

    private CustomThread startScheduledTaskService() {
        var service = new ScheduledTaskService.Builder()
                .setGroup(threadGroup)
                .setThreadName("ScheduledTaskService")
                .setStore(store)
                .setPollInterval(config.scheduledTasksCheckInterval)
                .setBatchSize(config.claimBatchSize * 4)
                .create();
        service.start();
        return service;
    }

    private CustomThread startRecurringTaskService() {
        var service = new RecurringTaskService.Builder()
                .group(threadGroup)
                .threadName("RecurringTaskService")
                .store(store)
                .ids(client.ids())
                .observer(observer)
                .clock(config.clock)
                .checkInterval(config.recurringTasksCheckInterval)
                .batchSize(config.claimBatchSize * 4)
                .create();
        service.start();
        return service;
    }

    private CustomThread startMaintenanceService() {
        var retention = new LinkedHashMap<TaskState, Duration>();
        retention.put(TaskState.SUCCEEDED, config.succeededRetention);
        retention.put(TaskState.DEAD, config.deadRetention);
        retention.put(TaskState.CANCELLED, config.cancelledRetention);
        retention.put(TaskState.EXPIRED, config.cancelledRetention);
        var service = new MaintenanceService.Builder()
                .group(threadGroup)
                .threadName("Maintenance")
                .store(store)
                .names(names)
                .retryPolicy(config.retryPolicy)
                .observer(observer)
                .clock(config.clock)
                .checkInterval(config.maintenanceCheckInterval)
                .purgeInterval(config.purgeInterval)
                .retention(retention)
                .keepDead(!config.deadRetention.isZero())
                .batchSize(config.claimBatchSize * 4)
                .purgeBatchSize(config.purgeBatchSize)
                .create();
        service.start();
        return service;
    }

    public static final class Builder {
        private FullStore store;
        private TaskProcessorRegistry registry;
        private JsonMapper jsonMapper;
        private Clock clock = Clock.systemUTC();
        private Concurrency concurrency = Concurrency.defaultConcurrency();
        private BackgroundExecutor taskExecutor;
        private RetryPolicy retryPolicy = RetryPolicy.defaultPolicy();
        private TaskObserver observer;
        private String workerLabel = defaultWorkerLabel();
        private String threadGroupName = "bgr";
        private Duration succeededRetention = Duration.ofHours(48);
        private Duration deadRetention = Duration.ofDays(90);
        private Duration cancelledRetention = Duration.ofHours(48);
        private boolean keepSucceededPayload;
        private Duration purgeInterval = Duration.ofMinutes(1);
        private int purgeBatchSize = 500;
        private Duration leaseDuration = Duration.ofSeconds(60);
        private Duration idlePollInterval = Duration.ofMillis(500);
        private int claimBatchSize = 32;
        private Duration shutdownTimeout = Duration.ofSeconds(30);
        private Duration serviceCheckInterval = Duration.ofSeconds(10);
        private Duration maintenanceCheckInterval = Duration.ofSeconds(5);
        private Duration scheduledTasksCheckInterval = Duration.ofSeconds(1);
        private Duration recurringTasksCheckInterval = Duration.ofSeconds(10);
        private int idBlockSize = 64;
        private int defaultMaxAttempts = 10;
        private Set<String> availableQueues = new LinkedHashSet<>(TaskClient.Builder.DEFAULT_QUEUES);
        private List<String> queuesToProcess = TaskClient.Builder.DEFAULT_QUEUES;
        private String defaultQueue = "Q:DEF";

        private Builder() {}

        private static String defaultWorkerLabel() {
            try {
                return ManagementFactory.getRuntimeMXBean().getName();
            } catch (RuntimeException e) {
                return "worker";
            }
        }

        /** The store, for example a {@code RedisTaskStore} or {@code PostgresTaskStore}. Required. */
        public Builder store(FullStore store) {
            this.store = store;
            return this;
        }

        /** The registry to use; by default the runner creates one, reachable through {@link #registry()}. */
        public Builder registry(TaskProcessorRegistry registry) {
            this.registry = registry;
            return this;
        }

        /** Mapper for JSON payloads and step results. Needed unless every task type has its own codec. */
        public Builder jsonMapper(JsonMapper jsonMapper) {
            this.jsonMapper = jsonMapper;
            return this;
        }

        public Builder clock(Clock clock) {
            if (clock == null) throw new TaskConfigurationError("clock cannot be null");
            this.clock = clock;
            return this;
        }

        /** How many tasks run at once. Default twice the number of processors, fixed. */
        public Builder concurrency(Concurrency concurrency) {
            if (concurrency == null) throw new TaskConfigurationError("concurrency cannot be null");
            this.concurrency = concurrency;
            return this;
        }

        /** A custom executor instead of the one built from {@link #concurrency}. The runner shuts it down. */
        public Builder taskExecutor(BackgroundExecutor executor) {
            this.taskExecutor = executor;
            return this;
        }

        /** Decides the delay after a failed attempt. Default exponential backoff with jitter, capped at 1 hour. */
        public Builder retryPolicy(RetryPolicy retryPolicy) {
            if (retryPolicy == null) throw new TaskConfigurationError("retryPolicy cannot be null");
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder observer(TaskObserver observer) {
            this.observer = observer;
            return this;
        }

        /** Identifies this process in the worker registry. Default is the JVM name (pid@host). */
        public Builder workerLabel(String label) {
            if (StringUtils.isBlank(label) || label.length() > 128) {
                throw new TaskConfigurationError("workerLabel must be 1 to 128 characters");
            }
            this.workerLabel = label;
            return this;
        }

        public Builder threadGroupName(String threadGroupName) {
            if (StringUtils.isBlank(threadGroupName))
                throw new TaskConfigurationError("threadGroupName cannot be blank");
            this.threadGroupName = threadGroupName;
            return this;
        }

        /** How long succeeded tasks are kept for inspection. Zero removes them at once. Default 48 hours. */
        public Builder succeededRetention(Duration retention) {
            this.succeededRetention = requireNonNegative(retention, "succeededRetention");
            return this;
        }

        /** How long dead tasks are kept for inspection and retry. Zero removes them at once. Default 90 days. */
        public Builder deadRetention(Duration retention) {
            this.deadRetention = requireNonNegative(retention, "deadRetention");
            return this;
        }

        /** How long cancelled and expired tasks are kept. Default 48 hours. */
        public Builder cancelledRetention(Duration retention) {
            this.cancelledRetention = requireNonNegative(retention, "cancelledRetention");
            return this;
        }

        /** Keeps the payload of succeeded tasks; by default it is dropped on success to save space. */
        public Builder keepSucceededPayload(boolean keep) {
            this.keepSucceededPayload = keep;
            return this;
        }

        /** Time between purge runs. Default 1 minute. */
        public Builder purgeInterval(Duration interval) {
            this.purgeInterval = requirePositive(interval, "purgeInterval");
            return this;
        }

        /** Tasks removed per purge call; the purge loops until a call removes fewer. Default 500. */
        public Builder purgeBatchSize(int size) {
            if (size < 1) throw new TaskConfigurationError("purgeBatchSize must be at least 1");
            this.purgeBatchSize = size;
            return this;
        }

        /**
         * How long a claimed task stays leased without renewal. Leases are renewed every third of this duration
         * while the task runs. A task whose lease expired is retried by maintenance. Default 60 s, at least 1 s.
         */
        public Builder leaseDuration(Duration lease) {
            requirePositive(lease, "leaseDuration");
            if (lease.toMillis() < 1000) throw new TaskConfigurationError("leaseDuration cannot be less than 1 second");
            this.leaseDuration = lease;
            return this;
        }

        /** How long the processor sleeps when a claim found nothing. Default 500 ms, at least 10 ms. */
        public Builder idlePollInterval(Duration interval) {
            requirePositive(interval, "idlePollInterval");
            if (interval.toMillis() < 10) throw new TaskConfigurationError("idlePollInterval cannot be less than 10ms");
            this.idlePollInterval = interval;
            return this;
        }

        /** The most tasks claimed in one call, bounded by free executor slots. Default 32. */
        public Builder claimBatchSize(int size) {
            if (size < 1) throw new TaskConfigurationError("claimBatchSize must be at least 1");
            this.claimBatchSize = size;
            return this;
        }

        /** How long {@link BackgroundRunner#stop()} waits for services and running tasks. Default 30 s. */
        public Builder shutdownTimeout(Duration timeout) {
            this.shutdownTimeout = requireNonNegative(timeout, "shutdownTimeout");
            return this;
        }

        public Builder serviceCheckInterval(Duration interval) {
            this.serviceCheckInterval = requirePositive(interval, "serviceCheckInterval");
            return this;
        }

        /** Time between checks for expired leases. Default 5 s, at least 1 s. */
        public Builder maintenanceCheckInterval(Duration interval) {
            requirePositive(interval, "maintenanceCheckInterval");
            if (interval.toMillis() < 1000)
                throw new TaskConfigurationError("maintenanceCheckInterval cannot be less than 1 second");
            this.maintenanceCheckInterval = interval;
            return this;
        }

        /** Upper bound of the wait between checks for due scheduled tasks. Default 1 s, at least 10 ms. */
        public Builder scheduledTasksCheckInterval(Duration interval) {
            requirePositive(interval, "scheduledTasksCheckInterval");
            if (interval.toMillis() < 10)
                throw new TaskConfigurationError("scheduledTasksCheckInterval cannot be less than 10ms");
            this.scheduledTasksCheckInterval = interval;
            return this;
        }

        /** Time between checks for due recurring definitions. Default 10 s. */
        public Builder recurringTasksCheckInterval(Duration interval) {
            this.recurringTasksCheckInterval = requirePositive(interval, "recurringTasksCheckInterval");
            return this;
        }

        /** How many ids the client reserves per store call. Default 64. */
        public Builder idBlockSize(int size) {
            if (size < 1 || size > 100_000) throw new TaskConfigurationError("idBlockSize must be 1 to 100000");
            this.idBlockSize = size;
            return this;
        }

        /** Attempts for requests that do not set their own. Default 10. */
        public Builder defaultMaxAttempts(int attempts) {
            if (attempts < 1 || attempts > Short.MAX_VALUE) {
                throw new TaskConfigurationError("defaultMaxAttempts must be 1 to " + Short.MAX_VALUE);
            }
            this.defaultMaxAttempts = attempts;
            return this;
        }

        /**
         * Configures the queues.
         * <p>
         * A queue may appear more than once in {@code queuesToProcess} to give it more turns in the round robin.
         *
         * @param availableQueues all queues tasks can be enqueued to. Duplicates are removed.
         * @param queuesToProcess the queues this instance processes, in round robin order. Must be a subset of
         *                        availableQueues. If null or empty, all available queues are processed in order.
         * @param defaultQueue    the queue of tasks that do not name one. Must be in availableQueues. If null or
         *                        blank, the first queue to process is used.
         */
        public Builder configureQueues(
                List<String> availableQueues, List<String> queuesToProcess, String defaultQueue) {
            if (ObjectUtils.isEmpty(availableQueues))
                throw new TaskConfigurationError("Available queues cannot be empty");
            availableQueues.forEach(s -> {
                if (StringUtils.isBlank(s)) throw new TaskConfigurationError("Queue names cannot be blank");
            });
            // LinkedHashSet keeps the order given by the caller, a HashSet would make it random
            this.availableQueues = new LinkedHashSet<>(availableQueues);
            this.queuesToProcess = ObjectUtils.isEmpty(queuesToProcess)
                    ? new ArrayList<>(this.availableQueues)
                    : new ArrayList<>(queuesToProcess);
            this.defaultQueue = StringUtils.isBlank(defaultQueue) ? this.queuesToProcess.getFirst() : defaultQueue;
            return this;
        }

        QueuesHolder queues() {
            if (queuesToProcess.isEmpty()) throw new TaskConfigurationError("Queues to process cannot be empty");
            for (var q : queuesToProcess) {
                if (!availableQueues.contains(q)) {
                    throw new TaskConfigurationError("Queue to process " + q + " is not an available queue");
                }
            }
            if (!availableQueues.contains(defaultQueue)) {
                throw new TaskConfigurationError("Default queue " + defaultQueue + " is not an available queue");
            }
            return new QueuesHolder(availableQueues, queuesToProcess, defaultQueue);
        }

        /**
         * Builds the runner and initializes the store schema. Nothing else touches the store until
         * {@link BackgroundRunner#start()}.
         */
        public BackgroundRunner build() {
            if (store == null) throw new TaskConfigurationError("Cannot build runner without a store");
            queues();
            return new BackgroundRunner(this);
        }

        private static Duration requirePositive(Duration d, String name) {
            if (d == null || d.isZero() || d.isNegative()) throw new TaskConfigurationError(name + " must be positive");
            return d;
        }

        private static Duration requireNonNegative(Duration d, String name) {
            if (d == null || d.isNegative()) throw new TaskConfigurationError(name + " cannot be null or negative");
            return d;
        }
    }
}

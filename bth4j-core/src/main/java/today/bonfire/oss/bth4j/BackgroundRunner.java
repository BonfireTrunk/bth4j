package today.bonfire.oss.bth4j;

import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.common.JsonMapper;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.DefaultVtExecutor;
import today.bonfire.oss.bth4j.executor.FixedExecutor;
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
public final class BackgroundRunner implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Loggers.RUNNER);

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
    private final RunnerStatus status;
    private final RetryPolicies retryPolicies;
    private final AdminOps admin;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final Admission admission = new Admission();
    private final QueueReservations queueReservations = new QueueReservations();

    private volatile boolean stopRequested;
    private volatile boolean stopSequenceDone;
    private volatile int workerId;
    private volatile Set<Integer> workerQueueIds;
    private volatile Set<Integer> workerTypeIds;
    private volatile ScheduledFuture<?> monitoringTask;
    private volatile TaskProcessorService taskProcessorService;
    private volatile CustomThread scheduledTaskService;
    private volatile CustomThread recurringTaskService;
    private volatile MaintenanceService maintenanceService;
    private volatile RunnerServices services;

    private BackgroundRunner(Builder b) {
        this.config = b;
        this.store = b.store;
        this.registry = b.registry != null ? b.registry : new TaskProcessorRegistry();
        this.threadGroup = new ThreadGroup(b.threadGroupName);
        this.queues = b.queues();
        this.names = new Names(store);
        this.observer = new Observers(b.observer);
        this.status = new RunnerStatus(b.clock);
        this.retryPolicies = new RetryPolicies(b.retryPolicy, b.namedRetryPolicies);
        this.taskExecutor = b.taskExecutor != null
                ? b.taskExecutor
                : switch (b.concurrency) {
                    case Concurrency.Fixed f -> new FixedExecutor(f.limit());
                    case Concurrency.Adaptive a ->
                        new DefaultVtExecutor(
                                a, detail -> status.issue(Issue.Kind.RESOURCE_SIGNAL_UNAVAILABLE, detail, null));
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
        this.admin = new DefaultAdminOps(store, names, b.clock, this::health, status::issues);
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

    /**
     * @return the number of tasks running on this instance right now, including those whose outcome is still being
     *     written; with the built-in executors up to twice the concurrency limit
     */
    public int runningTasks() {
        return admission.running();
    }

    /** @return the executor's current concurrency limit, or empty for an executor that does not expose it */
    public OptionalInt concurrencyLimit() {
        return taskExecutor == null ? OptionalInt.empty() : taskExecutor.limit();
    }

    /**
     * @return the running attempts that passed their maximum run time without stopping; each still holds its
     *     slots until it ends
     */
    public int abandonedAttempts() {
        return queueReservations.abandoned();
    }

    /** Operator view and operations, including this runner's {@link #health()} and {@link #issues()}. */
    public AdminOps admin() {
        return admin;
    }

    public RunnerHealth health() {
        var s = stopped.get()
                ? ended() ? RunnerHealth.Status.STOPPED : RunnerHealth.Status.STOPPING
                : !started.get()
                        ? RunnerHealth.Status.NEW
                        : admission.quiet() ? RunnerHealth.Status.QUIET : RunnerHealth.Status.RUNNING;
        return new RunnerHealth(
                s,
                workerId,
                admission.running(),
                queueReservations.abandoned(),
                status.lastClaimAt(),
                status.lastMaintenanceAt(),
                status.lastStoreErrorAt(),
                status.lastStoreError(),
                status.issueCount());
    }

    /** @return what this runner noticed and an operator should look at, oldest first, at most 100 */
    public List<Issue> issues() {
        return status.issues();
    }

    /**
     * Starts all services and the monitor that restarts a service if its thread dies. Returns after
     * starting. Calling it on a runner that is started does nothing. When the start fails, for example because
     * the store cannot be reached, what was started is stopped again and the exception is rethrown;
     * {@code start()} can then be called again.
     *
     * @throws IllegalStateException when {@link #stop()} or {@link #close()} was called before, also on a runner
     *     that was never started: a stopped runner stays stopped, build a new one
     * @throws TaskConfigurationException when no processor is registered, or a registered task type has a JSON
     *     payload and the runner has no {@code jsonMapper}
     */
    public void start() {
        requireNotStopped();
        if (registry.isEmpty()) {
            throw new TaskConfigurationException("No task processor registered; register processors before start()");
        }
        requireMapperForJsonTypes();
        if (!started.compareAndSet(false, true)) {
            log.debug("Background runner {} is already started", threadGroup.getName());
            return;
        }
        lifecycleLock.lock();
        try {
            if (stopRequested) {
                started.set(false);
                requireNotStopped();
            }
            try {
                startServices();
            } catch (RuntimeException | Error e) {
                abandonStart();
                started.set(false);
                throw e;
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void startServices() {
        registry.freeze();
        var queueIds = new ArrayList<Integer>();
        for (var q : queues.queuesToProcess) queueIds.add(names.queueId(q));
        var policies = new LinkedHashMap<Integer, QueuePolicy>();
        config.queuePolicies.forEach((q, p) -> policies.put(names.queueId(q), p));
        var typeIds = new HashSet<Integer>();
        for (var e : registry.typeNames()) typeIds.add(names.typeId(e));
        workerQueueIds = Set.copyOf(queueIds);
        workerTypeIds = Set.copyOf(typeIds);
        workerId = store.allocateWorkerId(config.workerLabel, workerQueueIds, workerTypeIds);
        // stop() gives up waiting for the lifecycle lock when a store call here hangs; it cannot stop what is not
        // started yet
        if (stopRequested) {
            log.warn("Background runner {} was stopped before it started", threadGroup.getName());
            return;
        }
        services = new RunnerServices(
                store,
                names,
                client.codecs(),
                client.ids(),
                registry,
                retryPolicies,
                observer,
                config.clock,
                status,
                !config.succeededRetention.isZero(),
                config.keepSucceededPayload,
                !config.deadRetention.isZero(),
                config.failureDetail,
                List.copyOf(queueIds),
                policies,
                Set.copyOf(typeIds),
                workerId);
        var storeInfo = store.info();
        scheduledTaskService = startScheduledTaskService();
        recurringTaskService = startRecurringTaskService();
        maintenanceService = startMaintenanceService();
        // the processor goes last, so that a failed start never leaves claimed tasks behind, and only while no
        // stop() has given up on the lifecycle lock in the meantime
        if (stopRequested) {
            log.warn("Background runner {} was stopped while it started", threadGroup.getName());
            abandonStart();
            return;
        }
        taskProcessorService = startTaskProcessorService();
        log.info("Background runner {} started as worker {} on {}", threadGroup.getName(), workerId, storeInfo);
        monitoringTask = scheduler.scheduleWithFixedDelay(
                this::checkServices,
                config.serviceCheckInterval.toMillis(),
                config.serviceCheckInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void requireNotStopped() {
        if (stopped.get()) {
            throw new IllegalStateException("Background runner " + threadGroup.getName()
                    + " was stopped and cannot be started again; build a new runner");
        }
    }

    /** Nothing of a stopped runner runs any more: no attempt, no claim in progress and no service thread. */
    private boolean ended() {
        if (!stopSequenceDone || !admission.idle()) return false;
        var threads =
                new Thread[] {taskProcessorService, scheduledTaskService, recurringTaskService, maintenanceService};
        for (var service : threads) {
            if (service != null && service.isAlive()) return false;
        }
        return true;
    }

    /** Without this check the runner would claim the tasks of such a type and end each one as dead. */
    private void requireMapperForJsonTypes() {
        if (config.jsonMapper != null) return;
        for (var name : registry.typeNames()) {
            var type = registry.get(name).type();
            if (type.hasPayload() && type.codec() == null) {
                throw new TaskConfigurationException("Task type " + name
                        + " has a JSON payload and the runner has no jsonMapper; set one on the builder or give"
                        + " the type a codec");
            }
        }
    }

    /** Stops the services a failed {@link #start()} got going, so that nothing runs and the next start is clean. */
    private void abandonStart() {
        var deadline = System.nanoTime() + waitNanos(config.shutdownTimeout);
        var processor = taskProcessorService;
        var threads = new CustomThread[] {processor, scheduledTaskService, recurringTaskService, maintenanceService};
        for (var service : threads) {
            if (service != null) service.stopThread();
        }
        for (var service : threads) {
            awaitStop(service, "Service of the failed start", remaining(deadline));
        }
        if (processor != null) processor.stopLeaseRenewal();
        taskProcessorService = null;
        scheduledTaskService = null;
        recurringTaskService = null;
        maintenanceService = null;
        services = null;
        workerId = 0;
    }

    private static Duration remaining(long deadline) {
        return Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
    }

    /** Stops claiming new tasks; running tasks finish and every other service keeps going. */
    public void quiet() {
        admission.quiet(true);
        var maintenance = maintenanceService;
        if (maintenance != null) maintenance.heartbeatNow();
        log.info("Background runner {} is quiet", threadGroup.getName());
    }

    /** Resumes claiming after {@link #quiet()}. */
    public void resume() {
        admission.quiet(false);
        var maintenance = maintenanceService;
        if (maintenance != null) maintenance.heartbeatNow();
        log.info("Background runner {} resumed", threadGroup.getName());
    }

    public boolean isQuiet() {
        return admission.quiet();
    }

    /**
     * {@link #quiet()}, then waits until no task runs on this instance and no claim that began before is still
     * in progress, or the timeout passes. The runner stays quiet afterwards; call {@link #resume()} or
     * {@link #stop()}.
     *
     * @return true when every running task finished within the timeout
     */
    public boolean drain(Duration timeout) {
        quiet();
        var deadline = System.nanoTime() + waitNanos(timeout);
        while (!admission.idle()) {
            if (deadline - System.nanoTime() <= 0) return false;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return admission.idle();
            }
        }
        return true;
    }

    private void checkServices() {
        try {
            lifecycleLock.lock();
            try {
                if (stopRequested) {
                    var task = monitoringTask;
                    if (task != null) task.cancel(false);
                    return;
                }
                if (!taskProcessorService.isAlive()) {
                    log.error("Task processor thread is not alive, restarting it");
                    taskProcessorService.stopLeaseRenewal();
                    taskProcessorService = startTaskProcessorService();
                } else if (taskProcessorService.ensureRenewer()) {
                    log.error("Lease renewal thread is not alive, restarted it");
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
            } finally {
                lifecycleLock.unlock();
            }
        } catch (Exception | Error e) {
            // a periodic job that throws is never run again by its executor, so an Error must not leave the check
            log.error("Error in service monitoring", e);
        }
    }

    /**
     * Stops the runner: stops claiming, stops all services, shuts down the task executor and waits up to the
     * shutdown timeout for running tasks, renewing their leases meanwhile. Tasks still running afterwards
     * keep their lease until it expires and are then retried.
     * <p>
     * {@link #health()} reports {@code STOPPING} from this call until the last task, claim and service of this
     * runner has ended, and {@code STOPPED} only then, however long that takes: the shutdown timeout limits how
     * long this call waits, not how long a task runs. A stopped runner cannot be started again. The call can
     * come before {@link #start()} and can be repeated; a repeated call does not wait again.
     *
     * @return true when everything had ended when the call returned, false when the shutdown timeout ran out
     *     first
     */
    public boolean stop() {
        if (!stopped.compareAndSet(false, true)) {
            log.debug("Background runner {} is already stopped or stopping", threadGroup.getName());
            return ended();
        }
        try {
            shutDown();
        } finally {
            stopSequenceDone = true;
        }
        return ended();
    }

    /** Same as {@link #stop()}, for try-with-resources. */
    @Override
    public void close() {
        stop();
    }

    private void shutDown() {
        log.info("Stopping background runner {}", threadGroup.getName());
        // one monotonic deadline for every wait below: the shutdown timeout bounds the whole of stop()
        var deadline = System.nanoTime() + waitNanos(config.shutdownTimeout);
        // before the lock: a start() that is stuck in a store call checks it when the call returns
        stopRequested = true;
        final TaskProcessorService processor;
        final CustomThread scheduled;
        final CustomThread recurring;
        final CustomThread maintenance;
        // under the lock so that the monitor cannot restart a service that is being stopped
        boolean locked = false;
        try {
            locked = lifecycleLock.tryLock(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            if (!locked) {
                log.warn(
                        "Background runner {} is starting or restarting a service and did not release the lifecycle"
                                + " lock within {}, stopping without it",
                        threadGroup.getName(),
                        config.shutdownTimeout);
            }
            var task = monitoringTask;
            if (task != null) task.cancel(false);
            processor = taskProcessorService;
            scheduled = scheduledTaskService;
            recurring = recurringTaskService;
            var maintenanceNow = maintenanceService;
            maintenance = maintenanceNow;
            // tells other runners at once that this one takes no more work
            if (maintenanceNow != null) maintenanceNow.heartbeatNow();
            for (var service : new CustomThread[] {processor, scheduled, recurring, maintenance}) {
                if (service != null) service.stopThread();
            }
        } finally {
            if (locked) lifecycleLock.unlock();
        }
        awaitStop(processor, "Task processor", remaining(deadline));
        awaitStop(scheduled, "Scheduled task service", remaining(deadline));
        awaitStop(recurring, "Recurring task service", remaining(deadline));
        awaitStop(maintenance, "Maintenance service", remaining(deadline));

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // no service submits tasks anymore, let the running ones finish while their leases are still renewed
        taskExecutor.shutdown();
        try {
            if (taskExecutor.awaitTermination(remaining(deadline))) {
                log.info("Background runner {} stopped", threadGroup.getName());
            } else {
                log.warn(
                        "Tasks still running after {}. They keep their lease until it expires and are then retried;"
                                + " the runner reports STOPPING until they have ended",
                        config.shutdownTimeout);
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
                .admission(admission)
                .reservations(queueReservations)
                .lease(config.leaseDuration)
                .defaultMaxRunTime(config.defaultMaxRunTime)
                .idlePollInterval(config.idlePollInterval)
                .claimBatchSize(config.claimBatchSize)
                .create();
        service.start();
        return service;
    }

    /** A wait for a monotonic deadline; one longer than {@link StoredTime#MAX} is cut to it, which nanoseconds can count. */
    private static long waitNanos(Duration wait) {
        return (StoredTime.fits(wait) ? wait : StoredTime.MAX).toNanos();
    }

    private int serviceBatchSize() {
        return (int) Math.min(Integer.MAX_VALUE, config.claimBatchSize * 4L);
    }

    private CustomThread startScheduledTaskService() {
        var service = new ScheduledTaskService.Builder()
                .setGroup(threadGroup)
                .setThreadName("ScheduledTaskService")
                .setStore(store)
                .setClock(config.clock)
                .setStatus(status)
                .setPollInterval(config.scheduledTasksCheckInterval)
                .setBatchSize(serviceBatchSize())
                .create();
        service.start();
        return service;
    }

    private CustomThread startRecurringTaskService() {
        var service = new RecurringTaskService.Builder()
                .group(threadGroup)
                .threadName("RecurringTaskService")
                .store(store)
                .inspection(store)
                .ids(client.ids())
                .observer(observer)
                .clock(config.clock)
                .status(status)
                .checkInterval(config.recurringTasksCheckInterval)
                .batchSize(serviceBatchSize())
                .create();
        service.start();
        return service;
    }

    private MaintenanceService startMaintenanceService() {
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
                .retryPolicies(retryPolicies)
                .observer(observer)
                .clock(config.clock)
                .status(status)
                .checkInterval(config.maintenanceCheckInterval)
                .purgeInterval(config.purgeInterval)
                .retention(retention)
                .keepDead(!config.deadRetention.isZero())
                .failureDetail(config.failureDetail)
                .batchSize(serviceBatchSize())
                .purgeBatchSize(config.purgeBatchSize)
                .worker(workerId, config.workerLabel, workerQueueIds, workerTypeIds, admission::running)
                .accepting(() -> !admission.quiet() && !stopRequested)
                .workerStaleAfter(config.workerStaleAfter)
                .create();
        service.start();
        service.heartbeatNow();
        return service;
    }

    public static final class Builder implements Cloneable {
        private FullStore store;
        private TaskProcessorRegistry registry;
        private JsonMapper jsonMapper;
        private Clock clock = Clock.systemUTC();
        private Concurrency concurrency = Concurrency.defaultConcurrency();
        private boolean concurrencySet;
        private BackgroundExecutor taskExecutor;
        private RetryPolicy retryPolicy = RetryPolicy.defaultPolicy();
        private Map<String, RetryPolicy> namedRetryPolicies = new LinkedHashMap<>();
        private Map<String, QueuePolicy> queuePolicies = new LinkedHashMap<>();
        private Duration workerStaleAfter = Duration.ofMinutes(10);
        private TaskObserver observer;
        private String workerLabel = defaultWorkerLabel();
        private String threadGroupName = "bgr";
        private Duration succeededRetention = Duration.ofMinutes(10);
        private Duration deadRetention = Duration.ofDays(90);
        private Duration cancelledRetention = Duration.ofHours(48);
        private boolean keepSucceededPayload;
        private FailureDetail failureDetail = FailureDetail.FULL;
        private Duration purgeInterval = Duration.ofMinutes(1);
        private int purgeBatchSize = 500;
        private Duration leaseDuration = Duration.ofSeconds(60);
        private Duration defaultMaxRunTime = Duration.ofHours(1);
        private Duration idlePollInterval = Duration.ofMillis(500);
        private int claimBatchSize = 32;
        private Duration shutdownTimeout = Duration.ofSeconds(30);
        private Duration serviceCheckInterval = Duration.ofSeconds(10);
        private Duration maintenanceCheckInterval = Duration.ofSeconds(5);
        private Duration scheduledTasksCheckInterval = Duration.ofSeconds(1);
        private Duration recurringTasksCheckInterval = Duration.ofSeconds(10);
        private int idBlockSize = 64;
        private int defaultMaxAttempts = 10;
        private Queues queues = Queues.of(Queues.GENERAL);
        private List<String> queuesToProcess;

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
            if (clock == null) throw new TaskConfigurationException("clock cannot be null");
            this.clock = clock;
            return this;
        }

        /**
         * How many tasks run at once. Default twice the number of processors, fixed. Not together with
         * {@link #taskExecutor}.
         */
        public Builder concurrency(Concurrency concurrency) {
            if (concurrency == null) throw new TaskConfigurationException("concurrency cannot be null");
            this.concurrency = concurrency;
            this.concurrencySet = true;
            return this;
        }

        /**
         * A custom executor instead of the one built from {@link #concurrency}; set one of the two, not both. The
         * runner shuts it down.
         */
        public Builder taskExecutor(BackgroundExecutor executor) {
            this.taskExecutor = executor;
            return this;
        }

        /** Decides the delay after a failed attempt. Default exponential backoff with jitter, capped at 1 hour. */
        public Builder retryPolicy(RetryPolicy retryPolicy) {
            if (retryPolicy == null) throw new TaskConfigurationException("retryPolicy cannot be null");
            this.retryPolicy = retryPolicy;
            return this;
        }

        /**
         * A policy requests may pick by name with {@code TaskRequest.retryPolicy(name)}. A task naming an
         * unknown policy uses the default one and the runner records an issue.
         */
        public Builder retryPolicy(String name, RetryPolicy retryPolicy) {
            if (StringUtils.isBlank(name) || name.length() > TaskRequest.MAX_RETRY_POLICY_LENGTH) {
                throw new TaskConfigurationException(
                        "retry policy name must be 1 to " + TaskRequest.MAX_RETRY_POLICY_LENGTH + " characters");
            }
            if (retryPolicy == null) throw new TaskConfigurationException("retryPolicy cannot be null");
            this.namedRetryPolicies.put(name, retryPolicy);
            return this;
        }

        /**
         * How this runner shares its slots between queues. The queue must be one it processes; queues without
         * a policy get {@link QueuePolicy#DEFAULT}. Any policy other than the default makes each claim share the
         * slots between the queues by weight and cap, still in one store call.
         */
        public Builder queue(String name, QueuePolicy policy) {
            if (StringUtils.isBlank(name)) throw new TaskConfigurationException("queue name cannot be blank");
            if (policy == null) throw new TaskConfigurationException("policy cannot be null");
            this.queuePolicies.put(name, policy);
            return this;
        }

        /**
         * Worker rows without a heartbeat for this long are deleted by the purge. Default 10 minutes, at most
         * {@link TaskRequest#MAX_TIME_FROM_NOW}.
         */
        public Builder workerStaleAfter(Duration staleAfter) {
            this.workerStaleAfter =
                    requireStorable(requirePositive(staleAfter, "workerStaleAfter"), "workerStaleAfter");
            return this;
        }

        public Builder observer(TaskObserver observer) {
            this.observer = observer;
            return this;
        }

        /** Identifies this process in the worker registry. Default is the JVM name (pid@host). */
        public Builder workerLabel(String label) {
            if (StringUtils.isBlank(label) || label.length() > 128) {
                throw new TaskConfigurationException("workerLabel must be 1 to 128 characters");
            }
            if (!StoredText.storable(label)) throw new TaskConfigurationException("workerLabel contains U+0000");
            this.workerLabel = label;
            return this;
        }

        public Builder threadGroupName(String threadGroupName) {
            if (StringUtils.isBlank(threadGroupName))
                throw new TaskConfigurationException("threadGroupName cannot be blank");
            this.threadGroupName = threadGroupName;
            return this;
        }

        /**
         * How long succeeded tasks are kept for inspection. Zero removes them at once. Default 10 minutes, at most
         * {@link TaskRequest#MAX_TIME_FROM_NOW}.
         */
        public Builder succeededRetention(Duration retention) {
            this.succeededRetention =
                    requireStorable(requireNonNegative(retention, "succeededRetention"), "succeededRetention");
            return this;
        }

        /**
         * How long dead tasks are kept for inspection and retry. Zero removes them at once. Default 90 days, at
         * most {@link TaskRequest#MAX_TIME_FROM_NOW}.
         */
        public Builder deadRetention(Duration retention) {
            this.deadRetention = requireStorable(requireNonNegative(retention, "deadRetention"), "deadRetention");
            return this;
        }

        /**
         * How long cancelled and expired tasks are kept. Default 48 hours, at most
         * {@link TaskRequest#MAX_TIME_FROM_NOW}.
         */
        public Builder cancelledRetention(Duration retention) {
            this.cancelledRetention =
                    requireStorable(requireNonNegative(retention, "cancelledRetention"), "cancelledRetention");
            return this;
        }

        /** Keeps the payload of succeeded tasks; by default it is dropped on success to save space. */
        public Builder keepSucceededPayload(boolean keep) {
            this.keepSucceededPayload = keep;
            return this;
        }

        /**
         * How much of a failure is recorded and reported: {@link FailureDetail#FULL} (default) or
         * {@link FailureDetail#CLASS_NAME_ONLY} for hosts whose exception messages may carry customer data.
         */
        public Builder failureDetail(FailureDetail detail) {
            if (detail == null) throw new TaskConfigurationException("failureDetail must not be null");
            this.failureDetail = detail;
            return this;
        }

        /** Time between purge runs. Default 1 minute, at least 1 ms. */
        public Builder purgeInterval(Duration interval) {
            this.purgeInterval = requireInterval(interval, Duration.ofMillis(1), "purgeInterval");
            return this;
        }

        /** Tasks removed per purge call; the purge loops until a call removes fewer. Default 500. */
        public Builder purgeBatchSize(int size) {
            if (size < 1) throw new TaskConfigurationException("purgeBatchSize must be at least 1");
            this.purgeBatchSize = size;
            return this;
        }

        /**
         * How long a claimed task stays leased without renewal. Leases are renewed every third of this duration
         * while the task runs. A task whose lease expired is retried by maintenance. Default 60 s, at least 1 s and
         * at most {@link TaskRequest#MAX_TIME_FROM_NOW}.
         */
        public Builder leaseDuration(Duration lease) {
            requireStorable(requirePositive(lease, "leaseDuration"), "leaseDuration");
            if (lease.toMillis() < 1000)
                throw new TaskConfigurationException("leaseDuration cannot be less than 1 second");
            this.leaseDuration = lease;
            return this;
        }

        /**
         * The maximum run time of an attempt whose task or schedule sets no {@code maxRunTime}. Past it the runner
         * requests cancellation (and interrupts the processor when its type opted in with
         * {@code interruptOnCancel}); an attempt still running one renewal period later is abandoned: its lease
         * is no longer renewed, so maintenance retries or ends the task, while the stuck thread keeps its slots
         * until it returns and is reported as the issue {@code attempt-abandoned}. Default one hour, at most
         * {@link TaskRequest#MAX_RUN_TIME}.
         */
        public Builder defaultMaxRunTime(Duration maxRunTime) {
            requirePositive(maxRunTime, "defaultMaxRunTime");
            if (maxRunTime.compareTo(TaskRequest.MAX_RUN_TIME) > 0) {
                throw new TaskConfigurationException("defaultMaxRunTime must be at most " + TaskRequest.MAX_RUN_TIME);
            }
            this.defaultMaxRunTime = maxRunTime;
            return this;
        }

        /**
         * How long the processor sleeps when a claim found nothing. When a queue was skipped because it was at its
         * {@link QueuePolicy#maxConcurrent()}, it claims again as soon as a task of a capped queue finishes. Default
         * 500 ms, at least 10 ms.
         */
        public Builder idlePollInterval(Duration interval) {
            this.idlePollInterval = requireInterval(interval, Duration.ofMillis(10), "idlePollInterval");
            return this;
        }

        /** The most tasks claimed in one call, bounded by free executor slots. Default 32. */
        public Builder claimBatchSize(int size) {
            if (size < 1) throw new TaskConfigurationException("claimBatchSize must be at least 1");
            this.claimBatchSize = size;
            return this;
        }

        /** How long {@link BackgroundRunner#stop()} waits for services and running tasks. Default 30 s. */
        public Builder shutdownTimeout(Duration timeout) {
            this.shutdownTimeout = requireNonNegative(timeout, "shutdownTimeout");
            return this;
        }

        /** Time between checks that the runner's services are alive. Default 10 s, at least 1 ms. */
        public Builder serviceCheckInterval(Duration interval) {
            this.serviceCheckInterval = requireInterval(interval, Duration.ofMillis(1), "serviceCheckInterval");
            return this;
        }

        /** Time between checks for expired leases. Default 5 s, at least 1 s. */
        public Builder maintenanceCheckInterval(Duration interval) {
            this.maintenanceCheckInterval =
                    requireInterval(interval, Duration.ofSeconds(1), "maintenanceCheckInterval");
            return this;
        }

        /** Upper bound of the wait between checks for due scheduled tasks. Default 1 s, at least 10 ms. */
        public Builder scheduledTasksCheckInterval(Duration interval) {
            this.scheduledTasksCheckInterval =
                    requireInterval(interval, Duration.ofMillis(10), "scheduledTasksCheckInterval");
            return this;
        }

        /** Time between checks for due recurring definitions. Default 10 s, at least 10 ms. */
        public Builder recurringTasksCheckInterval(Duration interval) {
            this.recurringTasksCheckInterval =
                    requireInterval(interval, Duration.ofMillis(10), "recurringTasksCheckInterval");
            return this;
        }

        /** How many ids the client reserves per store call. Default 64. */
        public Builder idBlockSize(int size) {
            if (size < 1 || size > 100_000) throw new TaskConfigurationException("idBlockSize must be 1 to 100000");
            this.idBlockSize = size;
            return this;
        }

        /** Attempts for requests that do not set their own. Default 10. */
        public Builder defaultMaxAttempts(int attempts) {
            if (attempts < 1 || attempts > TaskRequest.MAX_ATTEMPTS) {
                throw new TaskConfigurationException("defaultMaxAttempts must be 1 to " + TaskRequest.MAX_ATTEMPTS);
            }
            this.defaultMaxAttempts = attempts;
            return this;
        }

        /**
         * The queues of the application: the ones tasks may be enqueued to, and the one used when a request
         * names none. Give every runner and every separate {@link TaskClient} of the application the same value.
         * This runner processes all of them, in their order, unless {@link #processQueues} says otherwise.
         * Default: the one queue {@link Queues#GENERAL}.
         */
        public Builder queues(Queues queues) {
            if (queues == null) throw new TaskConfigurationException("queues cannot be null");
            this.queues = queues;
            return this;
        }

        /**
         * The queues this runner processes, in round robin order; each must be one of {@link #queues(Queues)}.
         * Tasks in the other queues, the default queue included, wait for a runner that processes them.
         * <p>
         * A queue may appear more than once to give it more turns in the round robin. Once a queue has a
         * {@link QueuePolicy} every queue counts once and the weights set the shares.
         */
        public Builder processQueues(List<String> queuesToProcess) {
            if (ObjectUtils.isEmpty(queuesToProcess)) {
                throw new TaskConfigurationException("queues to process cannot be empty");
            }
            this.queuesToProcess = List.copyOf(queuesToProcess);
            return this;
        }

        QueuesHolder queues() {
            var toProcess = queuesToProcess == null ? queues.names() : queuesToProcess;
            for (var q : toProcess) {
                if (!queues.names().contains(q)) {
                    throw new TaskConfigurationException("Queue to process " + q + " is not one of the queues");
                }
            }
            for (var q : queuePolicies.keySet()) {
                if (!toProcess.contains(q)) {
                    throw new TaskConfigurationException(
                            "Queue policy for " + q + ", which this runner does not process");
                }
            }
            return QueuesHolder.of(queues, toProcess);
        }

        /**
         * Builds the runner and initializes the store schema. Nothing else touches the store until
         * {@link BackgroundRunner#start()}.
         */
        public BackgroundRunner build() {
            if (store == null) throw new TaskConfigurationException("Cannot build runner without a store");
            queues();
            if (concurrencySet && taskExecutor != null) {
                throw new TaskConfigurationException("Set concurrency or taskExecutor, not both");
            }
            // the heartbeat is written once per maintenance check; below three of them one late write would
            // get a live worker purged
            if (workerStaleAfter.compareTo(maintenanceCheckInterval.multipliedBy(3)) < 0) {
                throw new TaskConfigurationException(
                        "workerStaleAfter must be at least three times maintenanceCheckInterval");
            }
            return new BackgroundRunner(snapshot());
        }

        /** The runner reads its settings later too, so it gets a copy that further calls on this builder leave alone. */
        private Builder snapshot() {
            try {
                var copy = (Builder) clone();
                copy.namedRetryPolicies = new LinkedHashMap<>(namedRetryPolicies);
                copy.queuePolicies = new LinkedHashMap<>(queuePolicies);
                return copy;
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }

        private static Duration requirePositive(Duration d, String name) {
            if (d == null || d.isZero() || d.isNegative())
                throw new TaskConfigurationException(name + " must be positive");
            return d;
        }

        /** Compares durations, so that one too long to count in milliseconds is refused and does not overflow. */
        private static Duration requireInterval(Duration d, Duration min, String name) {
            if (d == null || d.compareTo(min) < 0) {
                throw new TaskConfigurationException(name + " must be at least " + min.toMillis() + " ms");
            }
            return requireStorable(d, name);
        }

        private static Duration requireNonNegative(Duration d, String name) {
            if (d == null || d.isNegative()) throw new TaskConfigurationException(name + " cannot be null or negative");
            return d;
        }

        private static Duration requireStorable(Duration d, String name) {
            if (!StoredTime.fits(d)) throw new TaskConfigurationException(name + " must be at most " + StoredTime.MAX);
            return d;
        }
    }
}

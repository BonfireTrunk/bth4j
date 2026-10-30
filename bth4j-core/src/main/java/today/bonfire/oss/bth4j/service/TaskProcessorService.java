package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.common.QueuesHolder;
import today.bonfire.oss.bth4j.common.Random;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.CustomThread;

/**
 * Claims tasks and hands them to the executor.
 * <p>
 * Every iteration first reserves executor slots, then claims at most that many tasks in one call, so a
 * claimed task always has a slot. The queues are tried round robin. When nothing was claimed the thread
 * sleeps for the idle poll interval; one claim covers all queues, so an idle runner costs one store call
 * per interval.
 * <p>
 * A renewal thread extends the leases of all running tasks every third of the lease duration. It keeps
 * running after {@link #stopThread()} until {@link #stopLeaseRenewal()} is called, so that tasks still
 * finishing during shutdown keep their leases.
 */
public class TaskProcessorService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(TaskProcessorService.class);

    private final BackgroundExecutor executor;
    private final TaskOps taskOps;
    private final QueuesHolder queuesHolder;
    private final TaskCallbacks regularCallbacks;
    private final TaskCallbacks recurringCallbacks;
    private final Consumer<Task> taskHandler;
    private final Function<Integer, Duration> retryDelay;
    private final Duration lease;
    private final Duration idlePollInterval;
    private final int claimBatchSize;

    private final String instanceId = Random.generateNewToken(6);
    private final AtomicLong claimCounter = new AtomicLong();
    private final AtomicInteger queueOffset = new AtomicInteger();
    /** shared with the runner so that a restarted processor keeps renewing the tasks its predecessor started */
    private final Map<String, Task> running;

    private final Thread renewer;
    private volatile boolean renewalRunning = true;

    private TaskProcessorService(Builder b) {
        super(b.group, b.threadName);
        this.executor = b.executor;
        this.taskOps = b.taskOps;
        this.running = b.running;
        this.queuesHolder = taskOps.queuesHolder();
        this.regularCallbacks = b.regularCallbacks;
        this.recurringCallbacks = b.recurringCallbacks;
        this.retryDelay = b.retryDelay;
        this.lease = b.lease;
        this.idlePollInterval = b.idlePollInterval;
        this.claimBatchSize = b.claimBatchSize;
        this.taskHandler = task -> b.registry.executeTask(task, taskOps::decodeData);
        this.renewer = new Thread(b.group, this::renewLeases, b.threadName + "-lease");
        this.renewer.setDaemon(true);
    }

    @Override
    public synchronized void start() {
        renewer.start();
        super.start();
    }

    @Override
    public void run() {
        try {
            while (canContinueProcessing) {
                try {
                    processOnce();
                } catch (InterruptedException e) {
                    if (canContinueProcessing) {
                        log.error("Task processor thread interrupted", e);
                    } else {
                        log.debug("Task processor thread interrupted while stopping");
                    }
                } catch (Exception e) {
                    log.error("Task processor error, waiting {} before the next attempt", idlePollInterval, e);
                    sleepQuietly(idlePollInterval);
                }
            }
        } finally {
            doneLatch.countDown();
        }
    }

    private void processOnce() throws InterruptedException {
        int slots = executor.reserve(claimBatchSize, idlePollInterval);
        if (slots == 0 || !canContinueProcessing) {
            executor.release(slots);
            return;
        }
        List<Task> tasks;
        try {
            tasks = taskOps.claim(rotatedQueues(), slots, instanceId + ":" + claimCounter.incrementAndGet(), lease);
        } catch (RuntimeException e) {
            executor.release(slots);
            throw e;
        }
        executor.release(slots - tasks.size());
        for (var task : tasks) {
            running.put(task.uniqueId(), task);
            var callbacks = task.event().isRecurring() ? recurringCallbacks : regularCallbacks;
            try {
                executor.execute(
                        new TaskRunnerWrapper(task, taskHandler, callbacks, taskOps, retryDelay, this::finished));
            } catch (RuntimeException e) {
                finished(task);
                returnToQueue(task, e);
            }
        }
        if (tasks.isEmpty()) {
            Thread.sleep(idlePollInterval);
        }
    }

    private void returnToQueue(Task task, RuntimeException cause) {
        try {
            if (!taskOps.unclaim(task)) {
                log.warn("Task {} was rejected by the executor and its lease was lost", task.uniqueId());
            }
        } catch (RuntimeException e) {
            log.error(
                    "Task {} was rejected by the executor and could not be returned to its queue", task.uniqueId(), e);
        }
        log.warn("Executor rejected task {}, returned to its queue", task.uniqueId(), cause);
    }

    /**
     * Removes this attempt from the running tasks. Only this attempt: the same id may already be running again
     * when a retry was claimed while this attempt's callbacks were still executing.
     */
    private void finished(Task task) {
        running.remove(task.uniqueId(), task);
    }

    /** The queues to process, starting one position later on every call. */
    private List<String> rotatedQueues() {
        var queues = queuesHolder.queuesToProcess;
        int n = queues.size();
        int start = Math.floorMod(queueOffset.getAndIncrement(), n);
        var rotated = new ArrayList<String>(n);
        for (int i = 0; i < n; i++) {
            rotated.add(queues.get((start + i) % n));
        }
        return rotated;
    }

    private void renewLeases() {
        var interval = Duration.ofMillis(Math.max(100, lease.toMillis() / 3));
        while (renewalRunning) {
            sleepQuietly(interval);
            if (running.isEmpty()) continue;
            var tasks = List.copyOf(running.values());
            try {
                int renewed = taskOps.renew(tasks, lease);
                if (renewed < tasks.size()) {
                    log.warn(
                            "Only {} of {} leases could be renewed, the others were taken over", renewed, tasks.size());
                }
            } catch (RuntimeException e) {
                log.error("Lease renewal failed for {} running tasks", tasks.size(), e);
            }
        }
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** @return the tasks currently running on this instance */
    public int runningCount() {
        return running.size();
    }

    /** Stops renewing leases. Call it after the executor has terminated. */
    public void stopLeaseRenewal() {
        renewalRunning = false;
        renewer.interrupt();
    }

    public static class Builder {
        private ThreadGroup group;
        private String threadName;
        private BackgroundExecutor executor;
        private TaskOps taskOps;
        private TaskProcessorRegistry registry;
        private TaskCallbacks regularCallbacks = TaskCallbacks.noOp();
        private TaskCallbacks recurringCallbacks = TaskCallbacks.noOp();
        private Function<Integer, Duration> retryDelay = attempt -> Duration.ZERO;
        private Duration lease = Duration.ofSeconds(60);
        private Duration idlePollInterval = Duration.ofSeconds(1);
        private int claimBatchSize = 32;
        private Map<String, Task> running = new ConcurrentHashMap<>();

        public Builder setGroup(ThreadGroup group) {
            this.group = group;
            return this;
        }

        public Builder setThreadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        public Builder setExecutor(BackgroundExecutor executor) {
            this.executor = executor;
            return this;
        }

        public Builder setTaskOps(TaskOps taskOps) {
            this.taskOps = taskOps;
            return this;
        }

        public Builder setTaskProcessorRegistry(TaskProcessorRegistry registry) {
            this.registry = registry;
            return this;
        }

        public Builder setCallbacks(TaskCallbacks regular, TaskCallbacks recurring) {
            this.regularCallbacks = regular == null ? TaskCallbacks.noOp() : regular;
            this.recurringCallbacks = recurring == null ? TaskCallbacks.noOp() : recurring;
            return this;
        }

        public Builder setRetryDelay(Function<Integer, Duration> retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        public Builder setLease(Duration lease) {
            this.lease = lease;
            return this;
        }

        public Builder setIdlePollInterval(Duration idlePollInterval) {
            this.idlePollInterval = idlePollInterval;
            return this;
        }

        public Builder setClaimBatchSize(int claimBatchSize) {
            this.claimBatchSize = claimBatchSize;
            return this;
        }

        /** The tasks being executed, to hand over from a processor that died to its replacement. */
        public Builder setRunning(Map<String, Task> running) {
            this.running = running;
            return this;
        }

        public TaskProcessorService create() {
            if (registry == null) throw new TaskConfigurationError("TaskProcessorRegistry is required");
            if (executor == null) throw new TaskConfigurationError("TaskExecutor is required");
            if (taskOps == null) throw new TaskConfigurationError("TaskOps is required");
            if (group == null || threadName == null)
                throw new TaskConfigurationError("Thread group and name are required");
            return new TaskProcessorService(this);
        }
    }
}

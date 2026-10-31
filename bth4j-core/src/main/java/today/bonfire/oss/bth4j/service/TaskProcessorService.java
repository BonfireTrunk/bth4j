package today.bonfire.oss.bth4j.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationError;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.executor.CustomThread;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * Claims tasks and hands them to the executor.
 * <p>
 * Every iteration first reserves executor slots, then claims at most that many tasks in one call, so a
 * claimed task always has a slot. The queues are tried round robin. When nothing was claimed the thread
 * sleeps for the idle poll interval; one claim covers all queues, so an idle runner costs one store call
 * per interval. A quiet runner keeps the loop but claims nothing.
 * <p>
 * A renewal thread extends the leases of all running tasks every third of the lease duration and passes
 * cancel requests on to the running executions. It keeps running after {@link #stopThread()} until
 * {@link #stopLeaseRenewal()} is called, so that tasks still finishing during shutdown keep their leases.
 */
public class TaskProcessorService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(TaskProcessorService.class);

    private final BackgroundExecutor executor;
    private final TaskExecution.Settings settings;
    private final List<Integer> queueIds;
    private final Set<Integer> knownEventIds;
    private final int workerId;
    private final Duration lease;
    private final Duration idlePollInterval;
    private final int claimBatchSize;
    private final AtomicInteger queueOffset = new AtomicInteger();
    /** shared with the runner so that a restarted processor keeps renewing the tasks its predecessor started */
    private final Map<Long, TaskExecution> running;

    private final Thread renewer;
    private volatile boolean renewalRunning = true;
    private volatile boolean quiet;

    private TaskProcessorService(Builder b) {
        super(b.group, b.threadName);
        this.executor = b.executor;
        this.settings = b.settings;
        this.queueIds = List.copyOf(b.queueIds);
        this.knownEventIds = Set.copyOf(b.knownEventIds);
        this.workerId = b.workerId;
        this.running = b.running;
        this.lease = b.lease;
        this.idlePollInterval = b.idlePollInterval;
        this.claimBatchSize = b.claimBatchSize;
        this.quiet = b.quiet;
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
        if (quiet) {
            Thread.sleep(idlePollInterval);
            return;
        }
        int slots = executor.reserve(claimBatchSize, idlePollInterval);
        if (slots == 0 || !canContinueProcessing) {
            executor.release(slots);
            return;
        }
        List<TaskRecord> tasks;
        try {
            tasks = settings.store().claim(new ClaimRequest(rotatedQueues(), slots, lease, workerId, knownEventIds));
        } catch (RuntimeException e) {
            executor.release(slots);
            throw e;
        }
        executor.release(slots - tasks.size());
        for (var record : tasks) {
            var execution = new TaskExecution(record, settings, this::finished);
            running.put(record.id(), execution);
            try {
                executor.execute(execution);
            } catch (RuntimeException e) {
                finished(execution);
                returnToQueue(execution, e);
            }
        }
        if (tasks.isEmpty()) {
            Thread.sleep(idlePollInterval);
        }
    }

    private void returnToQueue(TaskExecution execution, RuntimeException cause) {
        try {
            if (!settings.store().unclaim(execution.taskId(), execution.revision())) {
                log.warn("Task {} was rejected by the executor and its lease was lost", execution.taskId());
            }
        } catch (RuntimeException e) {
            log.error(
                    "Task {} was rejected by the executor and could not be returned to its queue",
                    execution.taskId(),
                    e);
        }
        log.warn("Executor rejected task {}, returned to its queue", execution.taskId(), cause);
    }

    /**
     * Removes this attempt from the running tasks. Only this attempt: the same id may already be running again
     * when a retry was claimed while this attempt was finishing.
     */
    private void finished(TaskExecution execution) {
        running.remove(execution.taskId(), execution);
    }

    /** The queues to process, starting one position later on every call. */
    private List<Integer> rotatedQueues() {
        int n = queueIds.size();
        int start = Math.floorMod(queueOffset.getAndIncrement(), n);
        var rotated = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            rotated.add(queueIds.get((start + i) % n));
        }
        return rotated;
    }

    private void renewLeases() {
        var interval = Duration.ofMillis(Math.max(100, lease.toMillis() / 3));
        while (renewalRunning) {
            sleepQuietly(interval);
            if (running.isEmpty()) continue;
            var executions = List.copyOf(running.values());
            var refs = new ArrayList<LeaseRef>(executions.size());
            for (var e : executions) refs.add(new LeaseRef(e.taskId(), e.revision()));
            try {
                var statuses = settings.store().renew(refs, lease);
                int lost = 0;
                for (int i = 0; i < statuses.size() && i < executions.size(); i++) {
                    var status = statuses.get(i);
                    var execution = executions.get(i);
                    if (!status.owned()) {
                        lost++;
                        execution.leaseLost();
                    } else if (status.cancelRequested()) {
                        execution.requestCancel();
                    }
                }
                if (lost > 0) {
                    log.warn("{} of {} leases could not be renewed, those tasks were taken over", lost, refs.size());
                }
            } catch (RuntimeException e) {
                log.error("Lease renewal failed for {} running tasks", refs.size(), e);
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

    /** Stops claiming new tasks; running ones finish. */
    public void quiet(boolean quiet) {
        this.quiet = quiet;
    }

    public boolean isQuiet() {
        return quiet;
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
        private TaskExecution.Settings settings;
        private List<Integer> queueIds;
        private Set<Integer> knownEventIds;
        private int workerId;
        private Duration lease = Duration.ofSeconds(60);
        private Duration idlePollInterval = Duration.ofMillis(500);
        private int claimBatchSize = 32;
        private boolean quiet;
        private Map<Long, TaskExecution> running = new ConcurrentHashMap<>();

        public Builder group(ThreadGroup group) {
            this.group = group;
            return this;
        }

        public Builder threadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        public Builder executor(BackgroundExecutor executor) {
            this.executor = executor;
            return this;
        }

        public Builder settings(TaskExecution.Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder queueIds(List<Integer> queueIds) {
            this.queueIds = queueIds;
            return this;
        }

        public Builder knownEventIds(Set<Integer> knownEventIds) {
            this.knownEventIds = knownEventIds;
            return this;
        }

        public Builder workerId(int workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder lease(Duration lease) {
            this.lease = lease;
            return this;
        }

        public Builder idlePollInterval(Duration idlePollInterval) {
            this.idlePollInterval = idlePollInterval;
            return this;
        }

        public Builder claimBatchSize(int claimBatchSize) {
            this.claimBatchSize = claimBatchSize;
            return this;
        }

        public Builder quiet(boolean quiet) {
            this.quiet = quiet;
            return this;
        }

        /** The tasks being executed, to hand over from a processor that died to its replacement. */
        public Builder running(Map<Long, TaskExecution> running) {
            this.running = running;
            return this;
        }

        public TaskProcessorService create() {
            if (settings == null) throw new TaskConfigurationError("settings are required");
            if (executor == null) throw new TaskConfigurationError("executor is required");
            if (queueIds == null || queueIds.isEmpty()) throw new TaskConfigurationError("queueIds are required");
            if (knownEventIds == null) throw new TaskConfigurationError("knownEventIds are required");
            if (group == null || threadName == null) {
                throw new TaskConfigurationError("Thread group and name are required");
            }
            return new TaskProcessorService(this);
        }
    }
}

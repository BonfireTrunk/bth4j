package today.bonfire.oss.bth4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.exceptions.TaskConfigurationException;
import today.bonfire.oss.bth4j.executor.BackgroundExecutor;
import today.bonfire.oss.bth4j.spi.ClaimRequest;
import today.bonfire.oss.bth4j.spi.LeaseRef;
import today.bonfire.oss.bth4j.spi.QueueGrant;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * Claims tasks and hands them to the executor.
 * <p>
 * Every iteration first reserves executor slots, then claims at most that many tasks, so a claimed task
 * always has a slot. Without queue policies one claim covers all queues, tried round robin, so an idle
 * runner costs one store call per poll interval. With queue policies the free slots are shared between the
 * queues in proportion to their weights and capped by their concurrency limits, in one atomic claim. When
 * nothing was claimed the thread sleeps for the idle poll interval. A quiet runner keeps the loop but claims
 * nothing.
 * <p>
 * A renewal thread extends the leases of all running tasks every third of the lease duration, passes cancel
 * requests on to the running executions and requests the cancellation of tasks past their maximum run time.
 * It keeps running after {@link #stopThread()} until {@link #stopLeaseRenewal()} is called, so that tasks
 * still finishing during shutdown keep their leases.
 */
class TaskProcessorService extends CustomThread {
    private static final Logger log = LoggerFactory.getLogger(Loggers.PROCESSOR);

    private final BackgroundExecutor executor;
    private final TaskExecution.Settings settings;
    private final List<Integer> queueIds;
    private final List<Integer> distinctQueueIds;
    private final Map<Integer, QueuePolicy> policies;
    private final boolean policed;
    private final Set<Integer> knownTypeIds;
    private final int runnerId;
    private final Duration lease;
    private final Duration renewInterval;
    private final Duration defaultMaxRunTime;
    private final Duration idlePollInterval;
    private final int claimBatchSize;
    private final AtomicInteger queueOffset = new AtomicInteger();
    /** shared with the runner so that a restarted processor keeps renewing the tasks its predecessor started */
    private final Admission admission;

    private final QueueReservations reservations;
    /** the last shared claim skipped a queue at its cap; written and read by the processor thread only */
    private boolean capBlocked;

    private final String renewerName;
    private final ThreadGroup group;
    private volatile Thread renewer;
    private volatile boolean renewalRunning = true;

    private TaskProcessorService(Builder b) {
        super(b.group, b.threadName);
        this.executor = b.executor;
        this.settings = b.settings;
        this.queueIds = List.copyOf(b.queueIds);
        this.distinctQueueIds = List.copyOf(new LinkedHashSet<>(b.queueIds));
        this.policies = Map.copyOf(b.queuePolicies);
        this.policed = policies.values().stream().anyMatch(p -> !p.equals(QueuePolicy.DEFAULT));
        this.knownTypeIds = Set.copyOf(b.knownTypeIds);
        this.runnerId = b.runnerId;
        this.admission = b.admission;
        this.reservations = b.reservations;
        this.lease = b.lease;
        this.renewInterval = Duration.ofMillis(Math.max(100, lease.toMillis() / 3));
        this.defaultMaxRunTime = b.defaultMaxRunTime;
        this.idlePollInterval = b.idlePollInterval;
        this.claimBatchSize = b.claimBatchSize;
        this.group = b.group;
        this.renewerName = b.threadName + "-lease";
        this.renewer = newRenewer();
    }

    private Thread newRenewer() {
        var thread = new Thread(group, this::renewLeases, renewerName);
        thread.setDaemon(true);
        return thread;
    }

    private final ReentrantLock startLock = new ReentrantLock();

    @Override
    public void start() {
        startLock.lock();
        try {
            renewer.start();
            super.start();
        } finally {
            startLock.unlock();
        }
    }

    /**
     * Starts a new renewal thread when the old one has died, so that the leases of the running tasks are renewed
     * again.
     *
     * @return true when a thread was started
     */
    boolean ensureRenewer() {
        startLock.lock();
        try {
            if (!renewalRunning || renewer.isAlive()) return false;
            renewer = newRenewer();
            renewer.start();
            return true;
        } finally {
            startLock.unlock();
        }
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

    void processOnce() throws InterruptedException {
        if (admission.quiet()) {
            Thread.sleep(idlePollInterval);
            return;
        }
        int slots = executor.reserve(claimBatchSize, idlePollInterval);
        if (slots == 0 || !canContinueProcessing) {
            executor.release(slots);
            return;
        }
        // checked again inside the claim: quiet() may have come after the check above, while reserve() waited
        if (!admission.enterClaim()) {
            executor.release(slots);
            Thread.sleep(idlePollInterval);
            return;
        }
        boolean claimedAny;
        try {
            claimedAny = claimAndSubmit(slots);
        } finally {
            admission.exitClaim();
        }
        if (!claimedAny) {
            if (policed && capBlocked) {
                // a skipped queue may still have work: claim again as soon as one of its slots frees
                reservations.awaitSlot(idlePollInterval);
            } else {
                Thread.sleep(idlePollInterval);
            }
        }
    }

    /**
     * Claims for the reserved slots and starts what was claimed. Every slot is released or handed to an execution
     * on every exit, and a claimed task that was not handed to the executor goes back to its queue.
     *
     * @return true when at least one task was claimed
     */
    private boolean claimAndSubmit(int slots) {
        int owned = slots;
        List<TaskRecord> tasks = List.of();
        int handed = 0;
        try {
            try {
                reservations.drainSignal();
                tasks = policed ? claimShared(slots) : claim(rotatedQueues(), slots);
                settings.status().claimed();
            } catch (RuntimeException e) {
                settings.status().storeError(e);
                throw e;
            }
            if (tasks.size() > slots) tasks = withinSlots(tasks, slots);
            executor.claimed(slots, tasks.size());
            executor.release(slots - tasks.size());
            owned = tasks.size();
            for (var record : tasks) {
                submit(record);
                owned--;
                handed++;
            }
        } finally {
            if (owned > 0) {
                executor.release(owned);
                giveBack(tasks.subList(handed, tasks.size()));
            }
        }
        return !tasks.isEmpty();
    }

    /**
     * Hands a claimed task to the executor. Returns once the executor has the task or has rejected it, in both
     * cases the executor has dealt with the slot; any other failure leaves the slot to the caller.
     */
    private void submit(TaskRecord record) {
        TaskExecution execution = null;
        boolean submitting = false;
        try {
            execution = new TaskExecution(record, settings, this::finished);
            admission.started(execution);
            reservations.reserve(
                    execution,
                    policies.getOrDefault(execution.queueId(), QueuePolicy.DEFAULT)
                                    .maxConcurrent()
                            > 0);
            submitting = true;
            executor.executeWithSlot(execution::run);
        } catch (RuntimeException e) {
            if (execution != null) finished(execution);
            if (!submitting) throw e;
            returnToQueue(execution, e);
        } catch (Error e) {
            if (execution != null) finished(execution);
            throw e;
        }
    }

    /** Claimed tasks that cannot run: they go back to their queue unrun, so they do not wait for a lease to expire. */
    private void giveBack(List<TaskRecord> tasks) {
        for (var task : tasks) {
            try {
                settings.store().unclaim(task.id(), task.revision());
            } catch (RuntimeException e) {
                settings.status().storeError(e);
                log.error("Task {} could not be returned to its queue; its lease expires", task.id(), e);
            }
        }
    }

    private List<TaskRecord> claim(List<Integer> queues, int count) {
        return settings.store()
                .claim(new ClaimRequest(
                        queues.stream()
                                .distinct()
                                .map(q -> new QueueGrant(q, count, count))
                                .toList(),
                        count,
                        lease,
                        runnerId,
                        knownTypeIds));
    }

    /**
     * Shares {@code slots} between the queues by weight, each capped by its remaining concurrency, and claims
     * from each queue with a share. Slots a queue could not fill are offered once more to the queues that
     * filled theirs, and what is left after that to the queues the split gave no share, so an empty queue does
     * not idle the others.
     */
    private List<TaskRecord> claimShared(int slots) {
        var order = rotatedDistinctQueues();
        var caps = new int[order.size()];
        capBlocked = false;
        long totalWeight = 0;
        for (int i = 0; i < order.size(); i++) {
            var policy = policies.getOrDefault(order.get(i), QueuePolicy.DEFAULT);
            int cap = policy.maxConcurrent() == 0
                    ? slots
                    : Math.max(0, policy.maxConcurrent() - reservations.running(order.get(i)));
            caps[i] = Math.min(cap, slots);
            if (caps[i] > 0) totalWeight += policy.weight();
            else capBlocked = true;
        }
        if (totalWeight == 0) return List.of();
        var shares = new int[order.size()];
        int given = 0;
        for (int i = 0; i < order.size(); i++) {
            if (caps[i] == 0) continue;
            int weight =
                    policies.getOrDefault(order.get(i), QueuePolicy.DEFAULT).weight();
            shares[i] = (int) Math.min(caps[i], (long) slots * weight / totalWeight);
            given += shares[i];
        }
        for (int i = 0; given < slots && i < order.size(); i++) {
            if (shares[i] < caps[i]) {
                shares[i]++;
                given++;
            }
        }
        var grants = new ArrayList<QueueGrant>(order.size());
        for (int i = 0; i < order.size(); i++) {
            grants.add(new QueueGrant(order.get(i), shares[i], caps[i]));
        }
        var claimed = settings.store().claim(new ClaimRequest(grants, slots, lease, runnerId, knownTypeIds));
        int left = slots - claimed.size();
        if (left <= 0) return claimed;
        // a store skips a grant without a share, also when the queues with a share have nothing to run
        var skipped = new ArrayList<QueueGrant>();
        for (int i = 0; i < order.size(); i++) {
            if (shares[i] == 0 && caps[i] > 0) {
                skipped.add(new QueueGrant(order.get(i), Math.min(caps[i], left), caps[i]));
            }
        }
        if (skipped.isEmpty()) return claimed;
        try {
            var more = settings.store().claim(new ClaimRequest(skipped, left, lease, runnerId, knownTypeIds));
            if (more.isEmpty()) return claimed;
            var all = new ArrayList<TaskRecord>(claimed.size() + more.size());
            all.addAll(claimed);
            all.addAll(more);
            return all;
        } catch (RuntimeException e) {
            if (claimed.isEmpty()) throw e;
            // the tasks of the first claim are leased to this runner and still have to run
            settings.status().storeError(e);
            return claimed;
        }
    }

    /** A store that returned more tasks than asked for: the extra ones go back to their queue, unrun. */
    private List<TaskRecord> withinSlots(List<TaskRecord> tasks, int slots) {
        var detail = "store returned " + tasks.size() + " tasks for a claim of " + slots;
        settings.status().issue(Issue.Kind.STORE_ERROR, detail, null);
        log.error("The {}, returning the extra ones to their queue", detail);
        for (var extra : tasks.subList(slots, tasks.size())) {
            try {
                settings.store().unclaim(extra.id(), extra.revision());
            } catch (RuntimeException e) {
                settings.status().storeError(e);
                log.error("Task {} could not be returned to its queue; its lease expires", extra.id(), e);
            }
        }
        return tasks.subList(0, slots);
    }

    private void returnToQueue(TaskExecution execution, RuntimeException cause) {
        try {
            if (!settings.store().unclaim(execution.taskId(), execution.revision())) {
                log.warn("Task {} was rejected by the executor and its lease was lost", execution.taskId());
            }
        } catch (RuntimeException e) {
            settings.status().storeError(e);
            log.error(
                    "Task {} was rejected by the executor and could not be returned to its queue",
                    execution.taskId(),
                    e);
        }
        settings.status().issue(Issue.Kind.EXECUTOR_REJECTED, cause.toString(), execution.taskId());
        log.warn("Executor rejected task {}, returned to its queue", execution.taskId(), cause);
    }

    /**
     * Removes this attempt from the running tasks and releases its queue slot. Attempts are tracked by identity:
     * the same id may be running again on this runner while this attempt is still finishing, and each attempt
     * releases its own slot.
     */
    private void finished(TaskExecution execution) {
        admission.finished(execution);
        reservations.release(execution);
    }

    /** The queues to process, starting one position later on every call. */
    private List<Integer> rotatedQueues() {
        return rotate(queueIds);
    }

    private List<Integer> rotatedDistinctQueues() {
        return rotate(distinctQueueIds);
    }

    private List<Integer> rotate(List<Integer> ids) {
        int n = ids.size();
        int start = Math.floorMod(queueOffset.getAndIncrement(), n);
        var rotated = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            rotated.add(ids.get((start + i) % n));
        }
        return rotated;
    }

    private void renewLeases() {
        while (renewalRunning) {
            sleepQuietly(renewInterval);
            try {
                renewOnce();
            } catch (RuntimeException | Error e) {
                // an Error ending this thread would stop every renewal while the runner reports RUNNING
                log.error("Lease renewal round failed, retrying in {}", renewInterval, e);
            }
        }
    }

    /**
     * One renewal round: requests cancellation of attempts past their maximum run time, abandons those still
     * running one renewal period after that, and renews the leases of the others.
     */
    void renewOnce() {
        var attempts = admission.attempts();
        if (attempts.isEmpty()) return;
        var now = settings.clock().instant();
        var executions = new ArrayList<TaskExecution>(attempts.size());
        for (var e : attempts) {
            // an attempt whose processor returned is writing its outcome: it is renewed until that write resolves
            if (!e.reporting()) {
                cancelIfOverrunning(e, now);
                if (abandonIfStuck(e, now)) continue;
            }
            // a lost lease cannot be renewed again; the loss was reported when it was found
            if (e.lostLease()) continue;
            executions.add(e);
        }
        if (executions.isEmpty()) return;
        var refs = new ArrayList<LeaseRef>(executions.size());
        for (var e : executions) refs.add(new LeaseRef(e.taskId(), e.revision()));
        try {
            var statuses = settings.store().renew(refs, lease);
            int lost = 0;
            for (int i = 0; i < statuses.size() && i < executions.size(); i++) {
                var status = statuses.get(i);
                var execution = executions.get(i);
                if (!status.owned()) {
                    // its own outcome write moved the task on; a real takeover shows in that write's result
                    if (execution.reporting()) continue;
                    lost++;
                    execution.leaseLost();
                    settings.status().issue(Issue.Kind.LEASE_LOST, "lease not renewed", execution.taskId());
                } else if (status.cancelRequested()) {
                    execution.requestCancel(null);
                }
            }
            if (lost > 0) {
                log.warn("{} of {} leases could not be renewed, those tasks were taken over", lost, refs.size());
            }
        } catch (RuntimeException e) {
            settings.status().storeError(e);
            log.error("Lease renewal failed for {} running tasks", refs.size(), e);
        }
    }

    private Duration maxRunTime(TaskExecution e) {
        return e.maxRunTime() != null ? e.maxRunTime() : defaultMaxRunTime;
    }

    private void cancelIfOverrunning(TaskExecution e, Instant now) {
        var started = e.startedAt();
        if (started == null || e.overrunSince() != null) return;
        var maxRunTime = maxRunTime(e);
        if (!now.isAfter(started.plus(maxRunTime))) return;
        e.markOverrun(now);
        if (e.isCancelRequested()) return;
        log.warn("Task {} exceeded its maximum run time of {}, requesting cancellation", e.taskId(), maxRunTime);
        e.requestCancel("max run time " + maxRunTime + " exceeded");
    }

    /**
     * An attempt still running one renewal period after its maximum run time stops being renewed, so its lease
     * expires and maintenance retries or ends the task. Its thread and slots stay held until it ends.
     *
     * @return true when the attempt is abandoned
     */
    private boolean abandonIfStuck(TaskExecution e, Instant now) {
        var since = e.overrunSince();
        if (since == null || now.isBefore(since.plus(renewInterval))) return false;
        if (reservations.abandon(e)) {
            var detail = "task " + e.taskId() + " ran past its maximum run time of " + maxRunTime(e)
                    + " and did not stop; its lease is no longer renewed and its slots stay held until it ends ("
                    + reservations.abandoned() + " abandoned attempts hold slots)";
            settings.status().issue(Issue.Kind.ATTEMPT_ABANDONED, detail, e.taskId());
            log.warn(detail);
        }
        return true;
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** @return the tasks currently running on this instance */
    int runningCount() {
        return admission.running();
    }

    /** Stops renewing leases. Call it after the executor has terminated. */
    void stopLeaseRenewal() {
        startLock.lock();
        try {
            renewalRunning = false;
            renewer.interrupt();
        } finally {
            startLock.unlock();
        }
    }

    static class Builder {
        private ThreadGroup group;
        private String threadName;
        private BackgroundExecutor executor;
        private TaskExecution.Settings settings;
        private List<Integer> queueIds;
        private Map<Integer, QueuePolicy> queuePolicies = Map.of();
        private Set<Integer> knownTypeIds;
        private int runnerId;
        private Duration lease = Duration.ofSeconds(60);
        private Duration defaultMaxRunTime = Duration.ofHours(1);
        private Duration idlePollInterval = Duration.ofMillis(500);
        private int claimBatchSize = 32;
        private Admission admission = new Admission();
        private QueueReservations reservations = new QueueReservations();

        Builder group(ThreadGroup group) {
            this.group = group;
            return this;
        }

        Builder threadName(String threadName) {
            this.threadName = threadName;
            return this;
        }

        Builder executor(BackgroundExecutor executor) {
            this.executor = executor;
            return this;
        }

        Builder settings(TaskExecution.Settings settings) {
            this.settings = settings;
            return this;
        }

        Builder queueIds(List<Integer> queueIds) {
            this.queueIds = queueIds;
            return this;
        }

        /** Policies by queue id; queues without one get {@link QueuePolicy#DEFAULT}. */
        Builder queuePolicies(Map<Integer, QueuePolicy> queuePolicies) {
            this.queuePolicies = queuePolicies;
            return this;
        }

        Builder knownTypeIds(Set<Integer> knownTypeIds) {
            this.knownTypeIds = knownTypeIds;
            return this;
        }

        Builder runnerId(int runnerId) {
            this.runnerId = runnerId;
            return this;
        }

        Builder lease(Duration lease) {
            this.lease = lease;
            return this;
        }

        /** The maximum run time of an attempt whose task sets none. */
        Builder defaultMaxRunTime(Duration defaultMaxRunTime) {
            this.defaultMaxRunTime = defaultMaxRunTime;
            return this;
        }

        Builder idlePollInterval(Duration idlePollInterval) {
            this.idlePollInterval = idlePollInterval;
            return this;
        }

        Builder claimBatchSize(int claimBatchSize) {
            this.claimBatchSize = claimBatchSize;
            return this;
        }

        /** The quiet flag, claims and unfinished attempts, shared with the replacement of a processor that died. */
        Builder admission(Admission admission) {
            this.admission = admission;
            return this;
        }

        /** The queue slots held by the running tasks, shared with the replacement of a processor that died. */
        Builder reservations(QueueReservations reservations) {
            this.reservations = reservations;
            return this;
        }

        TaskProcessorService create() {
            if (settings == null) throw new TaskConfigurationException("settings are required");
            if (executor == null) throw new TaskConfigurationException("executor is required");
            if (queueIds == null || queueIds.isEmpty()) throw new TaskConfigurationException("queueIds are required");
            if (knownTypeIds == null) throw new TaskConfigurationException("knownTypeIds are required");
            if (group == null || threadName == null) {
                throw new TaskConfigurationException("Thread group and name are required");
            }
            return new TaskProcessorService(this);
        }
    }
}

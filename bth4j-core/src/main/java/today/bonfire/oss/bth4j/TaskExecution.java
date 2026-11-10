package today.bonfire.oss.bth4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.TaskEvent.Outcome;
import today.bonfire.oss.bth4j.exceptions.TaskCancelledException;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskLeaseLostException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.ExecutionStore;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.InspectionStore;
import today.bonfire.oss.bth4j.spi.ScheduleEffect;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/**
 * One attempt of one claimed task: decodes the payload, runs the processor as its {@link TaskContext} and
 * applies the outcome with the task's revision. Every transition is a no-op when the lease was lost
 * meanwhile. If the store is unreachable when the outcome is applied, the lease expires and maintenance
 * retries the task.
 */
final class TaskExecution implements Runnable, TaskContext {
    private static final Logger log = LoggerFactory.getLogger(Loggers.ATTEMPT);
    private static final int MAX_PROGRESS = 4 * 1024;
    private static final int MAX_STEP_NAME = 64;
    private static final int MAX_STEP_RESULT = 8 * 1024;
    private static final int MAX_STEPS_BYTES = 64 * 1024;
    private static final Duration DEFER = Duration.ofSeconds(5);

    record Settings(
            ExecutionStore store,
            InspectionStore inspection,
            Names names,
            Codecs codecs,
            TaskProcessorRegistry registry,
            RetryPolicies retryPolicies,
            Observers observer,
            Clock clock,
            RunnerStatus status,
            boolean keepSucceeded,
            boolean keepPayload,
            boolean keepDead,
            FailureDetail failureDetail,
            AckBatcher acks) {

        Settings(
                ExecutionStore store,
                InspectionStore inspection,
                Names names,
                Codecs codecs,
                TaskProcessorRegistry registry,
                RetryPolicies retryPolicies,
                Observers observer,
                Clock clock,
                RunnerStatus status,
                boolean keepSucceeded,
                boolean keepPayload,
                boolean keepDead,
                FailureDetail failureDetail) {
            this(
                    store,
                    inspection,
                    names,
                    codecs,
                    registry,
                    retryPolicies,
                    observer,
                    clock,
                    status,
                    keepSucceeded,
                    keepPayload,
                    keepDead,
                    failureDetail,
                    new AckBatcher(store, keepSucceeded, keepPayload));
        }
    }

    private final TaskRecord record;
    private final TaskView view;
    private final Settings s;
    private final Consumer<TaskExecution> onFinished;
    private final Map<String, StepResult> steps;
    private final ReentrantLock stepsLock = new ReentrantLock();
    private final boolean interruptOnCancel;
    private volatile boolean cancelRequested;
    private volatile String cancelReason;
    private volatile boolean leaseLost;
    private volatile Instant startedAt;
    private volatile Instant overrunSince;
    private volatile boolean reporting;
    // Serializes interrupts with detachment so a late signal cannot reach the thread's next task.
    private final ReentrantLock workerLock = new ReentrantLock();
    private Thread worker;
    private boolean leaseLossInterrupted;
    /** the body was interrupted for a cancel request; guarded by workerLock */
    private boolean cancelInterrupted;
    /** the fatal error to rethrow from run(), written by the thread that runs the task only */
    private Error fatal;

    TaskExecution(TaskRecord record, Settings settings, Consumer<TaskExecution> onFinished) {
        this.record = record;
        this.s = settings;
        this.view = settings.names().view(record);
        this.onFinished = onFinished;
        this.steps = new LinkedHashMap<>(record.steps() == null ? Map.of() : record.steps());
        this.cancelRequested = record.cancelRequestedAt() != null;
        this.cancelReason = record.cancelReason();
        var registration = settings.registry().get(view.type());
        this.interruptOnCancel = registration != null && registration.type().isInterruptOnCancel();
    }

    long taskId() {
        return record.id();
    }

    int revision() {
        return record.revision();
    }

    /**
     * The processor learns of it through {@link #checkCancelled()}, and by an interrupt when its type opted in.
     * The interrupt comes once, however often the request is passed on: a body that is cleaning up after it is
     * not interrupted again.
     */
    void requestCancel(String reason) {
        if (reason != null) cancelReason = reason;
        cancelRequested = true;
        interruptWorker(false);
    }

    /** @return when the processor started, or null before that */
    Instant startedAt() {
        return startedAt;
    }

    Duration maxRunTime() {
        return record.maxRunTime();
    }

    /** @return when the runner found this attempt past its maximum run time, or null */
    Instant overrunSince() {
        return overrunSince;
    }

    void markOverrun(Instant at) {
        overrunSince = at;
    }

    int queueId() {
        return record.queueId();
    }

    /**
     * @return true once the attempt has begun writing its outcome; from then on the store may no longer hold
     *     the lease although nothing took the task over
     */
    boolean reporting() {
        return reporting;
    }

    void leaseLost() {
        leaseLost = true;
        interruptWorker(true);
    }

    /** @return true once this attempt is known to have lost its lease */
    boolean lostLease() {
        return leaseLost;
    }

    private void interruptWorker(boolean leaseLoss) {
        if (!interruptOnCancel) return;
        workerLock.lock();
        try {
            if (worker != null && (leaseLoss || !cancelInterrupted)) {
                worker.interrupt();
                if (leaseLoss) leaseLossInterrupted = true;
                else cancelInterrupted = true;
            }
        } finally {
            workerLock.unlock();
        }
    }

    // ---- Runnable -------------------------------------------------------------------------------------

    @Override
    public void run() {
        run(() -> {});
    }

    /**
     * Runs the attempt; {@code freeSlot} runs once the processor has returned, before the outcome is written, so the
     * slot can take the next task while the outcome is on its way to the store. The task stays running, and its
     * lease renewed, until the finished callback has seen it.
     */
    void run(Runnable freeSlot) {
        try {
            runAttempt(freeSlot);
        } catch (Throwable t) {
            if (fatal != null && fatal != t) {
                fatal.addSuppressed(t);
                throw fatal;
            }
            throw t;
        } finally {
            onFinished.accept(this);
        }
        if (fatal != null) throw fatal;
    }

    private void runAttempt(Runnable freeSlot) {
        var started = s.clock().instant();
        startedAt = started;
        emit(new TaskEvent.Started(view, record.workerId() == null ? 0 : record.workerId()));
        var registration = s.registry().get(view.type());
        if (registration == null) {
            freeSlot.run();
            reporting = true;
            defer("no processor registered for " + view.type());
            return;
        }
        Throwable failure = null;
        try {
            workerLock.lock();
            try {
                worker = Thread.currentThread();
            } finally {
                workerLock.unlock();
            }
            if (leaseLost) throw new TaskLeaseLostException("lease of task " + record.id() + " was lost");
            process(registration);
        } catch (Throwable t) {
            failure = t;
            remember(t);
        } finally {
            workerLock.lock();
            try {
                worker = null;
                // Clear cancel and lease-loss interrupts before bookkeeping or the thread's next task.
                Thread.interrupted();
            } finally {
                workerLock.unlock();
            }
        }
        freeSlot.run();
        reporting = true;
        if (leaseLossInterrupted) {
            fail(new TaskLeaseLostException("lease of task " + record.id() + " was lost"));
        } else if (failure == null) {
            succeed(started);
        } else {
            fail(failure);
        }
    }

    /** Keeps a fatal error to rethrow once the bookkeeping is done; an earlier one is attached to the later one. */
    private void remember(Throwable t) {
        if (!Failures.fatal(t) || t == fatal) return;
        var error = (Error) t;
        if (fatal != null) error.addSuppressed(fatal);
        fatal = error;
    }

    private void emit(TaskEvent event) {
        var error = s.observer().emitReporting(event);
        if (error != null) remember(error);
    }

    @SuppressWarnings("unchecked")
    private <T> void process(TaskProcessorRegistry.Registration<?> registration) throws Exception {
        var reg = (TaskProcessorRegistry.Registration<T>) registration;
        T payload = s.codecs().decode(reg.type(), record.encodedPayload());
        TaskProcessor<T> processor = reg.processor();
        processor.process(this, payload);
    }

    private void defer(String reason) {
        log.error("Task {} cannot run on this worker ({}), deferring it by {}", record.id(), reason, DEFER);
        apply(() -> s.store()
                .reschedule(record.id(), record.revision(), s.clock().instant().plus(DEFER)));
        s.status().issue("unknown-type", reason, record.id());
        emit(new TaskEvent.Deferred(record.id(), view.type(), reason));
    }

    private void succeed(Instant started) {
        var rearm = rearmEffect();
        // a re-arm must stay atomic with its own transition, so it keeps the single call
        var outcome = apply(() -> rearm == null
                ? s.acks().ack(record.id(), record.revision())
                : s.store().ack(record.id(), record.revision(), s.keepSucceeded(), s.keepPayload(), rearm));
        if (outcome == Outcome.LOST) {
            log.warn("Task {} completed but its lease was lost, it may run again", record.id());
        }
        emit(new TaskEvent.Succeeded(view, Duration.between(started, s.clock().instant()), outcome));
    }

    /** The re-arm of a fixed-delay occurrence; a header that cannot be read must not stop the acknowledgement. */
    private ScheduleEffect rearmEffect() {
        if (!RecurringTaskService.isFixedDelayOccurrence(record)) return null;
        var delay = record.headers().get(RecurringTaskService.FIXED_DELAY_HEADER);
        try {
            return RecurringTaskService.rearmEffect(record, s.clock().instant().plus(Duration.parse(delay)));
        } catch (RuntimeException e) {
            var detail = "schedule " + record.scheduleId() + " not re-armed: unreadable "
                    + RecurringTaskService.FIXED_DELAY_HEADER + " header";
            s.status().issue("schedule-rearm", detail, record.id());
            log.error("Task {}: {}", record.id(), detail);
            return null;
        }
    }

    private void fail(Throwable e) {
        switch (e) {
            // a cancel request wins over a voluntary reschedule, or a body that reschedules itself could never be
            // cancelled
            case TaskRescheduleException ex
            when cancelRequested -> {
                log.debug("Task {} rescheduled itself while a cancel was requested", record.id());
                cancelled(cancelReason());
            }
            case TaskRescheduleException ex -> {
                var due = StoredTime.after(s.clock().instant(), ex.delay());
                log.info("Rescheduling task {} to {}", record.id(), due);
                var outcome = apply(() -> s.store().reschedule(record.id(), record.revision(), due));
                emit(new TaskEvent.Rescheduled(view, due, outcome));
            }
            case TaskLeaseLostException ex -> {
                log.warn("Task {} stopped: {}", record.id(), text(ex));
                emit(new TaskEvent.Retrying(view, event(ex), s.clock().instant(), Outcome.LOST));
            }
            case TaskCancelledException ex ->
                cancelled(detail() == FailureDetail.FULL ? StoredText.clean(ex.getMessage()) : cancelReason());
            // the processor most likely stopped because of the cancel request (interrupt, closed resource)
            case Throwable ex
            when cancelRequested -> {
                log.debug(
                        "Task {} stopped while a cancel was requested: {}",
                        record.id(),
                        Failures.describe(detail(), ex));
                cancelled(cancelReason());
            }
            case TaskUnrecoverableException ex -> die(ex, "unrecoverable: " + text(ex));
            case TaskDataException ex -> die(ex, "payload: " + text(ex));
            default -> {
                var decision = decide(e);
                switch (decision) {
                    case RetryPolicy.Decision.Dead d -> die(e, d.reason());
                    case RetryPolicy.Decision.RetryAfter r -> retry(e, r.delay());
                }
            }
        }
    }

    private void cancelled(String given) {
        var reason = cancelReason == null ? storedCancelReason(given) : given;
        log.info("Task {} cancelled: {}", record.id(), reason);
        var pause = RecurringTaskService.pauseEffect(record);
        var outcome = apply(() -> s.store().finishCancelled(record.id(), record.revision(), reason, true, pause));
        emit(new TaskEvent.Cancelled(view, reason, outcome));
        afterTerminal(TaskState.CANCELLED, outcome, pause);
    }

    /** A request that arrived through renew carries no reason; the operator's reason is on the record. */
    private String storedCancelReason(String fallback) {
        try {
            return s.inspection()
                    .getTask(record.id())
                    .map(TaskRecord::cancelReason)
                    .orElse(fallback);
        } catch (RuntimeException | Error e) {
            remember(e);
            return fallback;
        }
    }

    private RetryPolicy.Decision decide(Throwable e) {
        var policy = s.retryPolicies().named(record.retryPolicy());
        if (policy == null) {
            s.status().issue("retry-policy", "unknown retry policy " + record.retryPolicy(), record.id());
            log.warn("Task {} names unknown retry policy {}, using the default", record.id(), record.retryPolicy());
            policy = s.retryPolicies().defaultPolicy();
        }
        try {
            var d = policy.decide(view, e);
            return d != null ? d : RetryPolicy.Decision.retryAfter(Duration.ZERO);
        } catch (RuntimeException | Error policyError) {
            remember(policyError);
            if (detail() == FailureDetail.FULL) {
                log.error("Retry policy failed for task {}, retrying at once", record.id(), policyError);
            } else {
                log.error(
                        "Retry policy failed for task {}, retrying at once: {}",
                        record.id(),
                        Failures.describe(detail(), policyError));
            }
            s.status().issue("retry-policy", Failures.describe(detail(), policyError), record.id());
            return RetryPolicy.Decision.retryAfter(Duration.ZERO);
        }
    }

    private void retry(Throwable e, Duration delay) {
        var retryAt = StoredTime.after(s.clock().instant(), delay);
        if (detail() == FailureDetail.FULL) {
            log.warn("Task {} failed on attempt {} of {}", record.id(), record.attempts(), record.maxAttempts(), e);
        } else {
            log.warn(
                    "Task {} failed on attempt {} of {}: {}",
                    record.id(),
                    record.attempts(),
                    record.maxAttempts(),
                    text(e));
        }
        var pause = RecurringTaskService.pauseEffect(record);
        var result = applyFail(failure(e), retryAt, pause);
        switch (result) {
            case RETRY -> emit(new TaskEvent.Retrying(view, event(e), retryAt, Outcome.APPLIED));
            case DEAD -> {
                emit(new TaskEvent.Died(view, event(e), "attempts exhausted", Outcome.APPLIED));
                afterTerminal(TaskState.DEAD, Outcome.APPLIED, pause);
            }
            case LOST -> emit(new TaskEvent.Retrying(view, event(e), retryAt, Outcome.LOST));
            case null -> emit(new TaskEvent.Retrying(view, event(e), retryAt, Outcome.UNKNOWN));
        }
    }

    private void die(Throwable e, String given) {
        var reason = given == null ? "no reason given" : given;
        if (detail() == FailureDetail.FULL) {
            log.error("Task {} failed and will not be retried: {}", record.id(), reason, e);
        } else {
            var name = text(e);
            if (reason.contains(name)) {
                log.error("Task {} failed and will not be retried: {}", record.id(), reason);
            } else {
                log.error("Task {} failed and will not be retried: {} ({})", record.id(), reason, name);
            }
        }
        var pause = RecurringTaskService.pauseEffect(record);
        var result = applyFail(failure(e), null, pause);
        var outcome = result == null ? Outcome.UNKNOWN : result == FailOutcome.LOST ? Outcome.LOST : Outcome.APPLIED;
        emit(new TaskEvent.Died(view, event(e), reason, outcome));
        afterTerminal(TaskState.DEAD, outcome, pause);
    }

    /**
     * The store has already re-armed the definition of a succeeded fixed-delay occurrence, or paused it for any
     * other end, in the same step as the transition, so a job that keeps failing does not fire again until an
     * operator looks at it. Only the issue is raised here, and only when the store paused the definition.
     */
    private void afterTerminal(TaskState state, Outcome outcome, ScheduleEffect pause) {
        if (outcome != Outcome.APPLIED || pause == null || !pause.applied()) return;
        var detail = "schedule " + record.scheduleId() + " paused: occurrence ended " + state;
        s.status().issue("schedule-paused", detail, record.id());
        log.warn(detail);
    }

    /** @return the store's outcome, or null when the store call failed and the outcome is unknown */
    private FailOutcome applyFail(FailureInfo info, Instant retryAt, ScheduleEffect pause) {
        try {
            var r = s.store().fail(record.id(), record.revision(), info, retryAt, s.keepDead(), pause);
            if (r == FailOutcome.LOST) log.warn("Task {} failed but its lease was lost", record.id());
            return r;
        } catch (RuntimeException | Error storeError) {
            remember(storeError);
            logStoreError(storeError);
            return null;
        }
    }

    private Outcome apply(java.util.function.BooleanSupplier transition) {
        try {
            if (transition.getAsBoolean()) return Outcome.APPLIED;
            s.status().issue("lease-lost", "transition rejected, the lease was taken over", record.id());
            return Outcome.LOST;
        } catch (RuntimeException | Error storeError) {
            remember(storeError);
            logStoreError(storeError);
            return Outcome.UNKNOWN;
        }
    }

    private void logStoreError(Throwable e) {
        if (s.status().storeError(e)) {
            log.error(
                    "Unable to update the state of task {}, the outcome is unknown. If it was not applied, the lease"
                            + " expires and maintenance retries the task",
                    record.id(),
                    e);
        } else {
            log.error(
                    "Unable to update the state of task {}, the outcome is unknown (the store error logged before:"
                            + " {})",
                    record.id(),
                    e.toString());
        }
    }

    private FailureInfo failure(Throwable e) {
        var at = s.clock().instant();
        try {
            return Failures.info(detail(), record.attempts(), at, e);
        } catch (RuntimeException | Error t) {
            remember(t);
            return Failures.info(FailureDetail.CLASS_NAME_ONLY, record.attempts(), at, e);
        }
    }

    private FailureDetail detail() {
        return s.failureDetail();
    }

    private Throwable event(Throwable e) {
        return Failures.forEvent(detail(), e);
    }

    private String text(Throwable e) {
        return Failures.text(detail(), e);
    }

    // ---- TaskContext ----------------------------------------------------------------------------------

    @Override
    public TaskView task() {
        return view;
    }

    @Override
    public long id() {
        return record.id();
    }

    @Override
    public int attempt() {
        return record.attempts();
    }

    @Override
    public Map<String, String> headers() {
        return view.headers();
    }

    @Override
    public Clock clock() {
        return s.clock();
    }

    @Override
    public boolean cancelRequested() {
        return cancelRequested;
    }

    @Override
    public void checkCancelled() {
        if (leaseLost) throw new TaskLeaseLostException("lease of task " + record.id() + " was lost");
        if (cancelRequested) throw new TaskCancelledException(cancelReason());
    }

    private String cancelReason() {
        return cancelReason != null ? cancelReason : "cancel requested";
    }

    @Override
    public void progress(String progress) {
        if (progress != null && progress.length() > MAX_PROGRESS) {
            throw new TaskValidationException("progress longer than " + MAX_PROGRESS + " characters");
        }
        if (!s.store().progress(record.id(), record.revision(), StoredText.clean(progress))) {
            leaseLost = true;
            throw new TaskLeaseLostException("lease of task " + record.id() + " was lost");
        }
    }

    @Override
    public <R> R step(String name, Class<R> type, Callable<R> body) throws Exception {
        if (name == null || name.isBlank() || name.length() > MAX_STEP_NAME) {
            throw new TaskValidationException("step name must be 1 to " + MAX_STEP_NAME + " characters");
        }
        var key = StoredText.clean(name);
        StepResult done;
        stepsLock.lock();
        try {
            done = steps.get(key);
        } finally {
            stepsLock.unlock();
        }
        if (done != null) {
            log.debug("Task {} skips step {} completed at {}", record.id(), name, done.at());
            return s.codecs().decodeStep(done.result(), type);
        }
        checkCancelled();
        R result = body.call();
        var encoded = StoredText.clean(s.codecs().encodeStep(result));
        if (encoded != null && encoded.length() > MAX_STEP_RESULT) {
            throw new TaskValidationException("result of step " + name + " exceeds " + MAX_STEP_RESULT + " characters");
        }
        // one section per step so that concurrent steps of one processor never overwrite each other's record
        stepsLock.lock();
        try {
            var updated = new LinkedHashMap<>(steps);
            updated.put(key, new StepResult(s.clock().instant(), encoded));
            long bytes = 0;
            for (var e : updated.entrySet()) {
                bytes += e.getKey().length()
                        + (e.getValue().result() == null
                                ? 0
                                : e.getValue().result().length());
            }
            if (bytes > MAX_STEPS_BYTES) {
                throw new TaskValidationException(
                        "steps of task " + record.id() + " exceed " + MAX_STEPS_BYTES + " bytes");
            }
            if (!s.store().saveSteps(record.id(), record.revision(), updated)) {
                leaseLost = true;
                throw new TaskLeaseLostException(
                        "lease of task " + record.id() + " was lost before step " + name + " could be recorded");
            }
            steps.put(key, updated.get(key));
        } finally {
            stepsLock.unlock();
        }
        return result;
    }

    @Override
    public void step(String name, Step body) throws Exception {
        step(name, Void.class, () -> {
            body.run();
            return null;
        });
    }
}

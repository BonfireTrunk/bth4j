package today.bonfire.oss.bth4j.service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import today.bonfire.oss.bth4j.RetryPolicy;
import today.bonfire.oss.bth4j.TaskContext;
import today.bonfire.oss.bth4j.TaskEvent;
import today.bonfire.oss.bth4j.TaskEvent.Outcome;
import today.bonfire.oss.bth4j.TaskProcessor;
import today.bonfire.oss.bth4j.TaskProcessorRegistry;
import today.bonfire.oss.bth4j.TaskView;
import today.bonfire.oss.bth4j.exceptions.TaskCancelledException;
import today.bonfire.oss.bth4j.exceptions.TaskDataException;
import today.bonfire.oss.bth4j.exceptions.TaskLeaseLostException;
import today.bonfire.oss.bth4j.exceptions.TaskRescheduleException;
import today.bonfire.oss.bth4j.exceptions.TaskUnrecoverableException;
import today.bonfire.oss.bth4j.exceptions.TaskValidationException;
import today.bonfire.oss.bth4j.spi.ExecutionStore;
import today.bonfire.oss.bth4j.spi.FailOutcome;
import today.bonfire.oss.bth4j.spi.FailureInfo;
import today.bonfire.oss.bth4j.spi.StepResult;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/**
 * One attempt of one claimed task: decodes the payload, runs the processor as its {@link TaskContext} and
 * applies the outcome with the task's revision. Every transition is a no-op when the lease was lost
 * meanwhile. If the store is unreachable when the outcome is applied, the lease expires and maintenance
 * retries the task.
 */
public final class TaskExecution implements Runnable, TaskContext {
    private static final Logger log = LoggerFactory.getLogger(TaskExecution.class);
    private static final int MAX_MESSAGE = 2 * 1024;
    private static final int MAX_STACK = 16 * 1024;
    private static final int MAX_PROGRESS = 4 * 1024;
    private static final int MAX_STEP_NAME = 64;
    private static final int MAX_STEP_RESULT = 8 * 1024;
    private static final int MAX_STEPS_BYTES = 64 * 1024;
    private static final Duration DEFER = Duration.ofSeconds(5);

    public record Settings(
            ExecutionStore store,
            Names names,
            Codecs codecs,
            TaskProcessorRegistry registry,
            RetryPolicy retryPolicy,
            Observers observer,
            Clock clock,
            boolean keepSucceeded,
            boolean keepPayload,
            boolean keepDead) {}

    private final TaskRecord record;
    private final TaskView view;
    private final Settings s;
    private final Consumer<TaskExecution> onFinished;
    private final Map<String, StepResult> steps;
    private volatile boolean cancelRequested;
    private volatile boolean leaseLost;

    TaskExecution(TaskRecord record, Settings settings, Consumer<TaskExecution> onFinished) {
        this.record = record;
        this.s = settings;
        this.view = settings.names().view(record);
        this.onFinished = onFinished;
        this.steps = new LinkedHashMap<>(record.steps() == null ? Map.of() : record.steps());
        this.cancelRequested = record.cancelRequestedAt() != null;
    }

    long taskId() {
        return record.id();
    }

    int revision() {
        return record.revision();
    }

    void requestCancel() {
        cancelRequested = true;
    }

    void leaseLost() {
        leaseLost = true;
    }

    // ---- Runnable -------------------------------------------------------------------------------------

    @Override
    public void run() {
        var started = s.clock().instant();
        try {
            s.observer().emit(new TaskEvent.Started(view, record.workerId() == null ? 0 : record.workerId()));
            var registration = s.registry().get(view.event());
            if (registration == null) {
                defer("no processor registered for " + view.event());
                return;
            }
            Exception failure = null;
            try {
                process(registration);
            } catch (Exception e) {
                failure = e;
            }
            if (failure == null) {
                succeed(started);
            } else {
                fail(failure);
            }
        } finally {
            onFinished.accept(this);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void process(TaskProcessorRegistry.Registration<?> registration) throws Exception {
        var reg = (TaskProcessorRegistry.Registration<T>) registration;
        T payload = s.codecs().decode(reg.type(), record.data());
        TaskProcessor<T> processor = reg.processor();
        processor.process(this, payload);
    }

    private void defer(String reason) {
        log.error("Task {} cannot run on this worker ({}), deferring it by {}", record.id(), reason, DEFER);
        apply(() -> s.store()
                .reschedule(record.id(), record.revision(), s.clock().instant().plus(DEFER)));
        s.observer().emit(new TaskEvent.Deferred(record.id(), view.event(), reason));
    }

    private void succeed(Instant started) {
        var outcome = apply(() -> s.store().ack(record.id(), record.revision(), s.keepSucceeded(), s.keepPayload()));
        if (outcome == Outcome.LOST) {
            log.warn("Task {} completed but its lease was lost, it may run again", record.id());
        }
        s.observer()
                .emit(new TaskEvent.Succeeded(
                        view, Duration.between(started, s.clock().instant()), outcome));
    }

    private void fail(Exception e) {
        switch (e) {
            case TaskRescheduleException ex -> {
                var due = s.clock().instant().plus(ex.delay());
                log.info("Rescheduling task {} to {}", record.id(), due);
                var outcome = apply(() -> s.store().reschedule(record.id(), record.revision(), due));
                s.observer().emit(new TaskEvent.Rescheduled(view, due, outcome));
            }
            case TaskLeaseLostException ex -> log.warn("Task {} stopped: {}", record.id(), ex.getMessage());
            case TaskCancelledException ex -> {
                log.info("Task {} cancelled: {}", record.id(), ex.getMessage());
                var outcome = failOutcome(failure(ex), null);
                s.observer().emit(new TaskEvent.Cancelled(view, ex.getMessage(), outcome));
            }
            case TaskUnrecoverableException ex -> die(ex, "unrecoverable: " + ex.getMessage());
            case TaskDataException ex -> die(ex, "payload: " + ex.getMessage());
            default -> {
                var decision = decide(e);
                switch (decision) {
                    case RetryPolicy.Decision.Dead d -> die(e, d.reason());
                    case RetryPolicy.Decision.RetryAfter r -> retry(e, r.delay());
                }
            }
        }
    }

    private RetryPolicy.Decision decide(Exception e) {
        try {
            var d = s.retryPolicy().decide(view, e);
            return d != null ? d : RetryPolicy.Decision.retryAfter(Duration.ZERO);
        } catch (RuntimeException policyError) {
            log.error("Retry policy failed for task {}, retrying at once", record.id(), policyError);
            return RetryPolicy.Decision.retryAfter(Duration.ZERO);
        }
    }

    private void retry(Exception e, Duration delay) {
        var retryAt = s.clock().instant().plus(delay);
        log.warn("Task {} failed on attempt {} of {}", record.id(), record.attempts(), record.maxAttempts(), e);
        var result = applyFail(failure(e), retryAt);
        switch (result) {
            case RETRY -> s.observer().emit(new TaskEvent.Retrying(view, e, retryAt, Outcome.APPLIED));
            case DEAD -> s.observer().emit(new TaskEvent.Died(view, e, "attempts exhausted", Outcome.APPLIED));
            case LOST -> s.observer().emit(new TaskEvent.Retrying(view, e, retryAt, Outcome.LOST));
        }
    }

    private void die(Exception e, String reason) {
        log.error("Task {} failed and will not be retried: {}", record.id(), reason, e);
        var outcome = failOutcome(failure(e), null);
        s.observer().emit(new TaskEvent.Died(view, e, reason, outcome));
    }

    private Outcome failOutcome(FailureInfo info, Instant retryAt) {
        return applyFail(info, retryAt) == FailOutcome.LOST ? Outcome.LOST : Outcome.APPLIED;
    }

    private FailOutcome applyFail(FailureInfo info, Instant retryAt) {
        try {
            var r = s.store().fail(record.id(), record.revision(), info, retryAt, s.keepDead());
            if (r == FailOutcome.LOST) log.warn("Task {} failed but its lease was lost", record.id());
            return r;
        } catch (RuntimeException storeError) {
            logStoreError(storeError);
            return FailOutcome.LOST;
        }
    }

    private Outcome apply(java.util.function.BooleanSupplier transition) {
        try {
            return transition.getAsBoolean() ? Outcome.APPLIED : Outcome.LOST;
        } catch (RuntimeException storeError) {
            logStoreError(storeError);
            return Outcome.LOST;
        }
    }

    private void logStoreError(RuntimeException e) {
        log.error(
                "Unable to update the state of task {}. Its lease expires and maintenance retries it", record.id(), e);
    }

    private FailureInfo failure(Exception e) {
        return new FailureInfo(
                record.attempts(),
                s.clock().instant(),
                e.getClass().getName(),
                StringUtils.abbreviate(StringUtils.defaultString(e.getMessage()), MAX_MESSAGE),
                stack(e));
    }

    private static String stack(Exception e) {
        var w = new StringWriter();
        e.printStackTrace(new PrintWriter(w));
        return StringUtils.abbreviate(w.toString(), MAX_STACK);
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
        return record.cancelReason() != null ? record.cancelReason() : "cancel requested";
    }

    @Override
    public void progress(String progress) {
        if (progress != null && progress.length() > MAX_PROGRESS) {
            throw new TaskValidationException("progress longer than " + MAX_PROGRESS + " characters");
        }
        if (!s.store().progress(record.id(), record.revision(), progress)) {
            leaseLost = true;
            throw new TaskLeaseLostException("lease of task " + record.id() + " was lost");
        }
    }

    @Override
    public <R> R step(String name, Class<R> type, Callable<R> body) throws Exception {
        if (name == null || name.isBlank() || name.length() > MAX_STEP_NAME) {
            throw new TaskValidationException("step name must be 1 to " + MAX_STEP_NAME + " characters");
        }
        var done = steps.get(name);
        if (done != null) {
            log.debug("Task {} skips step {} completed at {}", record.id(), name, done.at());
            return s.codecs().decodeStep(done.result(), type);
        }
        checkCancelled();
        R result = body.call();
        var encoded = s.codecs().encodeStep(result);
        if (encoded != null && encoded.length() > MAX_STEP_RESULT) {
            throw new TaskValidationException("result of step " + name + " exceeds " + MAX_STEP_RESULT + " characters");
        }
        var updated = new LinkedHashMap<>(steps);
        updated.put(name, new StepResult(s.clock().instant(), encoded));
        long bytes = 0;
        for (var e : updated.entrySet()) {
            bytes += e.getKey().length()
                    + (e.getValue().result() == null ? 0 : e.getValue().result().length());
        }
        if (bytes > MAX_STEPS_BYTES) {
            throw new TaskValidationException("steps of task " + record.id() + " exceed " + MAX_STEPS_BYTES + " bytes");
        }
        if (!s.store().saveSteps(record.id(), record.revision(), updated)) {
            leaseLost = true;
            throw new TaskLeaseLostException(
                    "lease of task " + record.id() + " was lost before step " + name + " could be recorded");
        }
        steps.put(name, updated.get(name));
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
